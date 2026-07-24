//go:build integration

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	notifierdelivery "reliable-notifier/internal/delivery"
)

const testOperatorKey = "operator-test-key"

func TestSlice5AuditedReplayPreservesLogicalIdentity(t *testing.T) {
	accepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"terminal":"replay-me"}`),
	)
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
	}

	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := store.ClaimDelivery(
		t.Context(),
		accepted.Delivery.ID,
		0,
		"terminal-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	status := 400
	if err := store.CompletePermanent(t.Context(), notifierdelivery.AttemptResult{
		DeliveryID:     task.ID,
		Generation:     task.Generation,
		LeaseOwner:     task.LeaseOwner,
		LeaseToken:     task.LeaseToken,
		ResponseStatus: &status,
		ErrorCategory:  "http_400",
		StartedAt:      time.Now().Add(-time.Millisecond),
		FinishedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	operatorHash := sha256.Sum256([]byte(testOperatorKey))
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO callers (id, api_key_hash, is_operator)
		VALUES ('operator-a', $1, true)
		ON CONFLICT (id) DO UPDATE
		SET api_key_hash = EXCLUDED.api_key_hash, is_operator = true
	`, operatorHash[:]); err != nil {
		t.Fatal(err)
	}

	unauthorized := replayDelivery(t, testCallerKey, task.ID, "caller must not replay")
	if unauthorized.StatusCode != http.StatusForbidden {
		t.Fatalf(
			"caller replay status = %d, body = %s, want %d",
			unauthorized.StatusCode,
			unauthorized.Body,
			http.StatusForbidden,
		)
	}
	invalidReason := replayDelivery(t, testOperatorKey, task.ID, "   ")
	if invalidReason.StatusCode != http.StatusBadRequest {
		t.Fatalf(
			"blank replay reason status = %d, body = %s, want %d",
			invalidReason.StatusCode,
			invalidReason.Body,
			http.StatusBadRequest,
		)
	}

	replayed := replayDelivery(t, testOperatorKey, task.ID, "supplier credential corrected")
	if replayed.StatusCode != http.StatusAccepted {
		t.Fatalf("operator replay status = %d, body = %s", replayed.StatusCode, replayed.Body)
	}
	if replayed.Delivery.ID != task.ID || replayed.Delivery.Generation != 1 ||
		replayed.Delivery.Status != "pending" {
		t.Fatalf("replay response = %+v, want same delivery at pending generation 1", replayed.Delivery)
	}

	var generation int64
	var deliveryStatus string
	var acceptedAt time.Time
	var replayNextAttempt time.Time
	var retryDeadline time.Time
	var destinationVersion int64
	var supplierIdempotency string
	var outboxRows int
	var auditOperator string
	var auditReason string
	var auditTimestamp time.Time
	if err := pool.QueryRow(t.Context(), `
		SELECT
			delivery.generation,
			delivery.status,
			delivery.accepted_at,
			delivery.next_attempt_at,
			delivery.retry_deadline,
			delivery.destination_version,
			delivery.supplier_idempotency_value,
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = delivery.id AND generation = delivery.generation),
			audit.operator_id,
			audit.reason,
			audit.replayed_at
		FROM deliveries AS delivery
		JOIN replay_audits AS audit ON audit.delivery_id = delivery.id
		WHERE delivery.id = $1
	`, task.ID).Scan(
		&generation,
		&deliveryStatus,
		&acceptedAt,
		&replayNextAttempt,
		&retryDeadline,
		&destinationVersion,
		&supplierIdempotency,
		&outboxRows,
		&auditOperator,
		&auditReason,
		&auditTimestamp,
	); err != nil {
		t.Fatal(err)
	}
	if generation != 1 || deliveryStatus != "pending" || outboxRows != 1 {
		t.Fatalf("replayed state generation=%d status=%q outbox=%d", generation, deliveryStatus, outboxRows)
	}
	if !acceptedAt.Equal(task.AcceptedAt) || destinationVersion != task.DestinationVersion ||
		supplierIdempotency != task.SupplierIdempotencyValue {
		t.Fatal("replay changed accepted_at, destination version, or supplier idempotency value")
	}
	if !retryDeadline.Equal(auditTimestamp.Add(24 * time.Hour)) {
		t.Fatalf(
			"replay deadline = %s, audit timestamp + 24h = %s",
			retryDeadline,
			auditTimestamp.Add(24*time.Hour),
		)
	}
	if auditOperator != "operator-a" || auditReason != "supplier credential corrected" ||
		auditTimestamp.IsZero() || !auditTimestamp.Equal(replayNextAttempt) {
		t.Fatalf(
			"audit operator=%q reason=%q at=%s, next_attempt_at=%s",
			auditOperator,
			auditReason,
			auditTimestamp,
			replayNextAttempt,
		)
	}

	duplicate := replayDelivery(t, testOperatorKey, task.ID, "must not advance twice")
	if duplicate.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate replay status = %d, body = %s, want 409", duplicate.StatusCode, duplicate.Body)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*) FROM replay_audits WHERE delivery_id = $1
	`, task.ID).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if outboxRows != 1 {
		t.Fatalf("replay audit rows after duplicate request = %d, want 1", outboxRows)
	}
}

type replayResult struct {
	StatusCode int
	Body       []byte
	Delivery   deliveryResponse
}

func replayDelivery(t *testing.T, apiKey, deliveryID, reason string) replayResult {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"reason": reason})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(
		http.MethodPost,
		appURL()+"/deliveries/"+deliveryID+"/replay",
		bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	result := replayResult{StatusCode: response.StatusCode, Body: body}
	if response.StatusCode == http.StatusAccepted {
		if err := json.Unmarshal(body, &result.Delivery); err != nil {
			t.Fatal(err)
		}
	}
	return result
}
