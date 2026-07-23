package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"reliable-notifier/internal/delivery"
)

type snapshotReader func(context.Context) (delivery.OperationalSnapshot, error)
type queueDepthReader func(context.Context) (int, error)

func newMetricsHandler(snapshot snapshotReader, queueDepth queueDepthReader) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
		defer cancel()
		values, err := snapshot(ctx)
		if err != nil {
			http.Error(response, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		depth, err := queueDepth(ctx)
		if err != nil {
			http.Error(response, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(
			response,
			"notifier_oldest_pending_seconds %g\n"+
				"notifier_oldest_outbox_seconds %g\n"+
				"notifier_expired_leases %d\n"+
				"notifier_queue_depth %d\n"+
				"notifier_permanent_failures %d\n",
			values.OldestPendingSeconds,
			values.OldestOutboxSeconds,
			values.ExpiredLeases,
			depth,
			values.PermanentFailures,
		)
		for _, class := range []string{"succeeded", "retryable_failure", "permanent_failure"} {
			_, _ = fmt.Fprintf(
				response,
				"notifier_delivery_results_total{class=%q} %d\n",
				class,
				values.ResultClasses[class],
			)
		}
	})
}
