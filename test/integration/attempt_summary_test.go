//go:build integration

package integration

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	notifierdelivery "reliable-notifier/internal/delivery"
)

func TestStatusIncludesRedactedAttemptSummary(t *testing.T) {
	const seededBody = "attempt-summary-body-must-not-leak"
	accepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"secret":"`+seededBody+`"}`),
	)
	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := store.ClaimDelivery(
		t.Context(),
		accepted.Delivery.ID,
		accepted.Delivery.Generation,
		"attempt-summary-worker",
		30*time.Second,
	)
	if errors.Is(err, notifierdelivery.ErrDeliveryNotClaimable) {
		t.Fatal("attempt summary fixture was unexpectedly claimed by another Worker")
	}
	if err != nil {
		t.Fatal(err)
	}
	responseStatus := http.StatusBadRequest
	if err := store.CompletePermanent(t.Context(), notifierdelivery.AttemptResult{
		DeliveryID:     task.ID,
		Generation:     task.Generation,
		LeaseOwner:     task.LeaseOwner,
		LeaseToken:     task.LeaseToken,
		ResponseStatus: &responseStatus,
		ErrorCategory:  "http_400",
		StartedAt:      time.Now().Add(-time.Millisecond),
		FinishedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequest(
		http.MethodGet,
		appURL()+"/deliveries/"+accepted.Delivery.ID,
		nil,
	)
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
		t.Fatalf("status lookup = %d, body = %s", response.StatusCode, body)
	}
	var decoded struct {
		Attempts []struct {
			Generation     int64  `json:"generation"`
			ResultClass    string `json:"result_class"`
			ResponseStatus *int   `json:"response_status"`
			ErrorCategory  string `json:"error_category"`
			StartedAt      string `json:"started_at"`
			FinishedAt     string `json:"finished_at"`
		} `json:"attempts"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Attempts) != 1 {
		t.Fatalf("attempt summaries = %d, want 1; body=%s", len(decoded.Attempts), body)
	}
	attempt := decoded.Attempts[0]
	if attempt.Generation != 0 ||
		attempt.ResultClass != "permanent_failure" ||
		attempt.ResponseStatus == nil ||
		*attempt.ResponseStatus != http.StatusBadRequest ||
		attempt.ErrorCategory != "http_400" ||
		attempt.StartedAt == "" ||
		attempt.FinishedAt == "" {
		t.Fatalf("attempt summary = %+v", attempt)
	}
	lowerBody := strings.ToLower(string(body))
	for _, forbidden := range []string{
		strings.ToLower(seededBody),
		"lease_token",
		"lease_owner",
		"caller_headers",
		"supplier_idempotency",
	} {
		if strings.Contains(lowerBody, forbidden) {
			t.Fatalf("status response leaked %q: %s", forbidden, body)
		}
	}
}
