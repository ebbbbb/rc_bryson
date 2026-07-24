//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"

	notifierdelivery "reliable-notifier/internal/delivery"
)

func TestBootstrapRejectsConflictingExistingConfiguration(t *testing.T) {
	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}
	suffix := strings.ToLower(strings.ReplaceAll(uniqueKey(t), "/", "-"))
	config := notifierdelivery.BootstrapConfig{
		CallerID:     "bootstrap-caller-" + suffix,
		CallerAPIKey: "bootstrap-key-a",
		Destination: notifierdelivery.DestinationVersion{
			DestinationID:     "bootstrap-destination-" + suffix,
			Version:           1,
			URL:               "https://supplier.example/notify",
			NetworkPolicy:     "public-only",
			AllowedMethods:    []string{"POST"},
			AllowedHeaders:    []string{"content-type"},
			SecretRef:         "env:SUPPLIER_AUTH_VALUE",
			CredentialHeader:  "Authorization",
			IdempotencyHeader: "Idempotency-Key",
			SuccessStatuses:   []int32{},
			RetryStatuses:     []int32{408, 425, 429},
			ConnectTimeout:    3 * time.Second,
			RequestTimeout:    15 * time.Second,
			RatePerSecond:     1,
			MaxConcurrency:    1,
		},
	}

	if err := store.Bootstrap(t.Context(), config); err != nil {
		t.Fatalf("initial bootstrap: %v", err)
	}
	if err := store.Bootstrap(t.Context(), config); err != nil {
		t.Fatalf("identical bootstrap must be idempotent: %v", err)
	}

	changedDestination := config
	changedDestination.Destination.URL = "https://other.example/notify"
	if err := store.Bootstrap(t.Context(), changedDestination); err == nil {
		t.Fatal("bootstrap accepted changed data for an immutable destination version")
	}

	changedCaller := config
	changedCaller.CallerAPIKey = "bootstrap-key-b"
	if err := store.Bootstrap(t.Context(), changedCaller); err == nil {
		t.Fatal("bootstrap accepted a different key for an existing caller")
	}
}
