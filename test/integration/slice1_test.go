//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	notifierdelivery "reliable-notifier/internal/delivery"
)

const (
	testCallerKey   = "caller-a-test-key"
	testDestination = "supplier-a"
)

var idempotencySequence atomic.Uint64

type deliveryResponse struct {
	ID                 string `json:"id"`
	Status             string `json:"status"`
	DestinationID      string `json:"destination_id"`
	DestinationVersion int64  `json:"destination_version"`
	Generation         int64  `json:"generation"`
}

type errorResponse struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

func TestSlice1CommittedSubmissionAndStatus(t *testing.T) {
	idempotencyKey := uniqueKey(t)
	submitted := submitDelivery(t, testCallerKey, idempotencyKey, testDestination, []byte(`{"event":"registered"}`))
	if submitted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, body = %s, want %d", submitted.StatusCode, submitted.Body, http.StatusAccepted)
	}
	if submitted.Delivery.ID == "" {
		t.Fatal("accepted delivery has empty ID")
	}
	if submitted.Delivery.Status != "pending" {
		t.Fatalf("accepted status = %q, want pending", submitted.Delivery.Status)
	}
	if submitted.Delivery.DestinationVersion != 1 {
		t.Fatalf("destination version = %d, want 1", submitted.Delivery.DestinationVersion)
	}

	request, err := http.NewRequest(http.MethodGet, appURL()+"/deliveries/"+submitted.Delivery.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+testCallerKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("lookup status = %d, body = %s, want %d", response.StatusCode, body, http.StatusOK)
	}
	var lookedUp deliveryResponse
	if err := json.Unmarshal(body, &lookedUp); err != nil {
		t.Fatalf("decode lookup: %v", err)
	}
	if lookedUp.ID != submitted.Delivery.ID || lookedUp.Status != "pending" {
		t.Fatalf("lookup = %+v, want accepted pending delivery", lookedUp)
	}
}

func TestSlice1CallerIdempotency(t *testing.T) {
	key := uniqueKey(t)
	body := []byte(`{"invoice":"inv-1"}`)
	const attempts = 8

	type concurrentResult struct {
		submitResult
		err error
	}
	results := make(chan concurrentResult, attempts)
	var group sync.WaitGroup
	for range attempts {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := submitDeliveryRequest(testCallerKey, key, testDestination, body)
			results <- concurrentResult{submitResult: result, err: err}
		}()
	}
	group.Wait()
	close(results)

	var deliveryID string
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.StatusCode != http.StatusAccepted {
			t.Fatalf("concurrent submit status = %d, body = %s", result.StatusCode, result.Body)
		}
		if deliveryID == "" {
			deliveryID = result.Delivery.ID
		}
		if result.Delivery.ID != deliveryID {
			t.Fatalf("idempotent IDs differ: %q and %q", deliveryID, result.Delivery.ID)
		}
	}

	conflict := submitDelivery(t, testCallerKey, key, testDestination, []byte(`{"invoice":"changed"}`))
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("changed idempotent request status = %d, body = %s, want %d", conflict.StatusCode, conflict.Body, http.StatusConflict)
	}
	if conflict.ErrorCode != "idempotency_conflict" {
		t.Fatalf("conflict code = %q, want idempotency_conflict", conflict.ErrorCode)
	}
}

func TestSlice1AuthorizationAndValidation(t *testing.T) {
	tests := []struct {
		name          string
		callerKey     string
		destinationID string
		body          []byte
		wantStatus    int
		wantCode      string
	}{
		{
			name:          "missing caller credential",
			destinationID: testDestination,
			body:          []byte(`{}`),
			wantStatus:    http.StatusUnauthorized,
			wantCode:      "unauthorized",
		},
		{
			name:          "destination not authorized",
			callerKey:     testCallerKey,
			destinationID: "supplier-b",
			body:          []byte(`{}`),
			wantStatus:    http.StatusForbidden,
			wantCode:      "destination_forbidden",
		},
		{
			name:          "payload too large",
			callerKey:     testCallerKey,
			destinationID: testDestination,
			body:          bytes.Repeat([]byte("x"), 256*1024+1),
			wantStatus:    http.StatusRequestEntityTooLarge,
			wantCode:      "payload_too_large",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := submitDelivery(t, test.callerKey, uniqueKey(t), test.destinationID, test.body)
			if result.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, body = %s, want %d", result.StatusCode, result.Body, test.wantStatus)
			}
			if result.ErrorCode != test.wantCode {
				t.Fatalf("error code = %q, want %q", result.ErrorCode, test.wantCode)
			}
		})
	}
}

func TestSlice1DeliveryAndOutboxCommitAtomically(t *testing.T) {
	key := uniqueKey(t)
	accepted := submitDelivery(t, testCallerKey, key, testDestination, []byte(`{"atomic":true}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
	}

	pool := integrationPool(t)
	var deliveries int
	var events int
	err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT count(*) FROM deliveries WHERE id = $1),
			(SELECT count(*) FROM outbox_events WHERE delivery_id = $1 AND generation = 0)
	`, accepted.Delivery.ID).Scan(&deliveries, &events)
	if err != nil {
		t.Fatal(err)
	}
	if deliveries != 1 || events != 1 {
		t.Fatalf("delivery rows = %d, initial Outbox rows = %d, want 1 and 1", deliveries, events)
	}
}

func TestSlice1ForcedCommitFailureNeverReturnsAccepted(t *testing.T) {
	pool := integrationPool(t)
	_, err := pool.Exec(t.Context(), `
		CREATE FUNCTION integration_reject_outbox()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'forced integration failure';
		END;
		$$;
		CREATE CONSTRAINT TRIGGER integration_reject_outbox
		AFTER INSERT ON outbox_events
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW
		EXECUTE FUNCTION integration_reject_outbox();
	`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, cleanupErr := pool.Exec(context.Background(), `
			DROP TRIGGER IF EXISTS integration_reject_outbox ON outbox_events;
			DROP FUNCTION IF EXISTS integration_reject_outbox();
		`)
		if cleanupErr != nil {
			t.Errorf("remove forced failure trigger: %v", cleanupErr)
		}
	})

	key := uniqueKey(t)
	result := submitDelivery(t, testCallerKey, key, testDestination, []byte(`{"must":"rollback"}`))
	if result.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("forced commit failure status = %d, body = %s, want %d", result.StatusCode, result.Body, http.StatusServiceUnavailable)
	}

	var count int
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*)
		FROM deliveries
		WHERE caller_id = 'caller-a' AND idempotency_key = $1
	`, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("deliveries after failed transaction = %d, want 0", count)
	}
}

func TestSlice1BacklogCapacityResponse(t *testing.T) {
	pool := integrationPool(t)
	var active int64
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*) FROM deliveries WHERE status IN ('pending', 'delivering')
	`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	store, err := notifierdelivery.NewStore(pool, active+1)
	if err != nil {
		t.Fatal(err)
	}
	api, err := notifierdelivery.NewAPI(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	api.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	firstKey := uniqueKey(t)
	first, err := submitDeliveryRequestAt(server.URL, testCallerKey, firstKey, testDestination, []byte(`{"capacity":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first status = %d, body = %s", first.StatusCode, first.Body)
	}

	repeated, err := submitDeliveryRequestAt(server.URL, testCallerKey, firstKey, testDestination, []byte(`{"capacity":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if repeated.StatusCode != http.StatusAccepted || repeated.Delivery.ID != first.Delivery.ID {
		t.Fatalf("idempotent retry at capacity = %+v, want original accepted delivery", repeated)
	}

	overflow, err := submitDeliveryRequestAt(server.URL, testCallerKey, uniqueKey(t), testDestination, []byte(`{"capacity":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if overflow.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("overflow status = %d, body = %s, want %d", overflow.StatusCode, overflow.Body, http.StatusServiceUnavailable)
	}
	if overflow.ErrorCode != "backlog_capacity_exceeded" {
		t.Fatalf("overflow error code = %q, want backlog_capacity_exceeded", overflow.ErrorCode)
	}
	if overflow.Header.Get("Retry-After") != "60" {
		t.Fatalf("Retry-After = %q, want 60", overflow.Header.Get("Retry-After"))
	}
}

func TestSlice1DestinationVersionsAreImmutable(t *testing.T) {
	pool := integrationPool(t)
	_, err := pool.Exec(t.Context(), `
		UPDATE destination_versions
		SET request_timeout_ms = 1000
		WHERE destination_id = 'supplier-a' AND version = 1
	`)
	if err == nil {
		t.Fatal("destination version mutation unexpectedly succeeded")
	}
}

type submitResult struct {
	StatusCode int
	Body       []byte
	Delivery   deliveryResponse
	ErrorCode  string
	Header     http.Header
}

func submitDelivery(t *testing.T, callerKey, idempotencyKey, destinationID string, body []byte) submitResult {
	t.Helper()
	result, err := submitDeliveryRequest(callerKey, idempotencyKey, destinationID, body)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func submitDeliveryRequest(callerKey, idempotencyKey, destinationID string, body []byte) (submitResult, error) {
	return submitDeliveryRequestAt(appURL(), callerKey, idempotencyKey, destinationID, body)
}

func submitDeliveryRequestAt(baseURL, callerKey, idempotencyKey, destinationID string, body []byte) (submitResult, error) {
	payload, err := json.Marshal(map[string]any{
		"destination_id": destinationID,
		"method":         http.MethodPost,
		"headers": map[string]string{
			"Content-Type": "application/json",
			"X-Event-Type": "test",
		},
		"body_base64": base64.StdEncoding.EncodeToString(body),
	})
	if err != nil {
		return submitResult{}, err
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/deliveries", bytes.NewReader(payload))
	if err != nil {
		return submitResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	if callerKey != "" {
		request.Header.Set("Authorization", "Bearer "+callerKey)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return submitResult{}, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return submitResult{}, err
	}
	result := submitResult{
		StatusCode: response.StatusCode,
		Body:       responseBody,
		Header:     response.Header.Clone(),
	}
	if response.StatusCode == http.StatusAccepted {
		if err := json.Unmarshal(responseBody, &result.Delivery); err != nil {
			return submitResult{}, fmt.Errorf("decode accepted response: %w", err)
		}
	} else {
		var failure errorResponse
		if err := json.Unmarshal(responseBody, &failure); err == nil {
			result.ErrorCode = failure.Error.Code
		}
	}
	return result, nil
}

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required for Slice 1 integration tests")
	}
	pool, err := pgxpool.New(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(t.Context()); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func appURL() string {
	if value := os.Getenv("APP_URL"); value != "" {
		return value
	}
	if value := os.Getenv("APP_HEALTH_URL"); value != "" {
		return strings.TrimSuffix(value, "/healthz")
	}
	return "http://localhost:18080"
}

func uniqueKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), idempotencySequence.Add(1))
}
