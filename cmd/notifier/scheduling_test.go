package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

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
