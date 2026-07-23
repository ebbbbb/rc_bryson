package main

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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
		registry := prometheus.NewRegistry()
		registerGauge := func(name, help string, value float64) {
			gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
			gauge.Set(value)
			registry.MustRegister(gauge)
		}
		registerGauge(
			"notifier_oldest_pending_seconds",
			"Seconds the oldest currently due pending delivery has waited.",
			values.OldestPendingSeconds,
		)
		registerGauge(
			"notifier_oldest_outbox_seconds",
			"Seconds the oldest currently publishable Outbox event has waited.",
			values.OldestOutboxSeconds,
		)
		registerGauge(
			"notifier_expired_leases",
			"Number of currently expired Worker leases.",
			float64(values.ExpiredLeases),
		)
		registerGauge(
			"notifier_queue_depth",
			"Number of ready dispatch signals reported by RabbitMQ.",
			float64(depth),
		)
		registerGauge(
			"notifier_permanent_failures",
			"Number of deliveries currently in permanent failure.",
			float64(values.PermanentFailures),
		)
		results := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "notifier_delivery_results_total",
				Help: "Persisted delivery attempt results by fixed result class.",
			},
			[]string{"class"},
		)
		for _, class := range []string{"succeeded", "retryable_failure", "permanent_failure"} {
			results.WithLabelValues(class).Set(float64(values.ResultClasses[class]))
		}
		registry.MustRegister(results)
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, request)
	})
}
