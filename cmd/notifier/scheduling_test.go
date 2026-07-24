package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"reliable-notifier/internal/delivery"
)

func TestOperationalAlertsAreActionableAndBounded(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	alertOperationalState(logger, delivery.OperationalSnapshot{
		OldestPendingSeconds: 120,
		OldestOutboxSeconds:  90,
		ExpiredLeases:        3,
	})
	logs := output.String()
	for _, required := range []string{
		"delivery backlog is aging",
		"oldest_pending_seconds=120",
		"Outbox publication is delayed",
		"oldest_outbox_seconds=90",
		"expired Worker leases require reconciliation",
		"expired_leases=3",
	} {
		if !strings.Contains(logs, required) {
			t.Fatalf("operational alerts missing %q:\n%s", required, logs)
		}
	}
}

type retentionBatches struct {
	results []int64
	calls   int
}

func (store *retentionBatches) DeleteExpiredTerminal(
	context.Context,
	time.Duration,
	time.Duration,
	int,
) (int64, error) {
	result := store.results[store.calls]
	store.calls++
	return result, nil
}

func TestRetentionCycleDrainsMoreThanOneBatch(t *testing.T) {
	store := &retentionBatches{results: []int64{1000, 1000, 250}}
	deleted, err := drainRetention(t.Context(), store, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2250 || store.calls != 3 {
		t.Fatalf("drainRetention = %d across %d calls, want 2250 across 3", deleted, store.calls)
	}
}
