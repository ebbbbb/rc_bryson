package main

import (
	"context"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"reliable-notifier/internal/delivery"
)

type snapshotReader func(context.Context) (delivery.OperationalSnapshot, error)
type queueDepthReader func(context.Context) (int, error)
type submissionCountsReader func() map[string]uint64

func newMetricsHandler(
	snapshot snapshotReader,
	queueDepth queueDepthReader,
	submissionCounts submissionCountsReader,
) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
		defer cancel()
		values, err := snapshot(ctx)
		if err != nil {
			http.Error(response, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		depth, queueErr := queueDepth(ctx)
		queueUp := 1.0
		queueDepthValue := float64(depth)
		if queueErr != nil {
			queueUp = 0
			queueDepthValue = math.NaN()
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
			queueDepthValue,
		)
		registerGauge(
			"notifier_rabbitmq_up",
			"Whether the RabbitMQ queue probe succeeded.",
			queueUp,
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
		submissions := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "notifier_submissions_total",
				Help: "HTTP delivery submissions by bounded acceptance outcome.",
			},
			[]string{"outcome"},
		)
		counts := map[string]uint64{}
		if submissionCounts != nil {
			counts = submissionCounts()
		}
		for _, outcome := range []string{"accepted", "rejected"} {
			submissions.WithLabelValues(outcome).Add(float64(counts[outcome]))
		}
		registry.MustRegister(submissions)
		promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(response, request)
	})
}
