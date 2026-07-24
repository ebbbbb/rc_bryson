package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"reliable-notifier/internal/delivery"
)

func TestHealthEndpoints(t *testing.T) {
	for _, path := range []string{"/healthz", "/readyz"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			mux := http.NewServeMux()
			registerHealthRoutes(mux, func(context.Context) error { return nil })

			mux.ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
			}
		})
	}
}

func TestReadinessFailsClosedWhileLivenessRemainsHealthy(t *testing.T) {
	ready := func(context.Context) error { return errors.New("database unavailable") }
	mux := http.NewServeMux()
	registerHealthRoutes(mux, ready)
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusNoContent},
		{path: "/readyz", want: http.StatusServiceUnavailable},
	} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != test.want {
			t.Fatalf("%s status = %d, want %d", test.path, response.Code, test.want)
		}
	}
}

func TestMetricsHandlerExposesBoundedOperationalSignals(t *testing.T) {
	const secret = "seeded-secret-must-not-leak"
	handler := newMetricsHandler(
		func(context.Context) (delivery.OperationalSnapshot, error) {
			return delivery.OperationalSnapshot{
				OldestPendingSeconds: 12.5,
				OldestOutboxSeconds:  3,
				ExpiredLeases:        2,
				PermanentFailures:    4,
				ResultClasses: map[string]int64{
					"succeeded":         7,
					"retryable_failure": 5,
					secret:              99,
				},
			}, nil
		},
		func(context.Context) (int, error) { return 9, nil },
		func() map[string]uint64 {
			return map[string]uint64{"accepted": 11, "rejected": 3}
		},
	)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, required := range []string{
		"notifier_oldest_pending_seconds 12.5",
		"notifier_oldest_outbox_seconds 3",
		"notifier_expired_leases 2",
		"notifier_queue_depth 9",
		"notifier_rabbitmq_up 1",
		`notifier_delivery_results_total{class="succeeded"} 7`,
		`notifier_delivery_results_total{class="retryable_failure"} 5`,
		`notifier_submissions_total{outcome="accepted"} 11`,
		`notifier_submissions_total{outcome="rejected"} 3`,
		"notifier_permanent_failures 4",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("metrics missing %q:\n%s", required, text)
		}
	}
	if strings.Contains(text, secret) {
		t.Fatalf("metrics exposed an unbounded class label: %s", text)
	}
}

func TestMetricsHandlerReportsDependencyFailureWithoutDetails(t *testing.T) {
	handler := newMetricsHandler(
		func(context.Context) (delivery.OperationalSnapshot, error) {
			return delivery.OperationalSnapshot{}, errors.New("database-secret")
		},
		func(context.Context) (int, error) { return 0, nil },
		func() map[string]uint64 { return nil },
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if strings.Contains(response.Body.String(), "database-secret") {
		t.Fatalf("metrics error leaked dependency details: %q", response.Body.String())
	}
}

func TestMetricsHandlerPreservesDatabaseMetricsDuringRabbitOutage(t *testing.T) {
	handler := newMetricsHandler(
		func(context.Context) (delivery.OperationalSnapshot, error) {
			return delivery.OperationalSnapshot{
				OldestPendingSeconds: 42,
				OldestOutboxSeconds:  17,
				PermanentFailures:    5,
				ResultClasses:        map[string]int64{},
			}, nil
		},
		func(context.Context) (int, error) {
			return 0, errors.New("rabbitmq-secret")
		},
		func() map[string]uint64 { return nil },
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	for _, required := range []string{
		"notifier_oldest_pending_seconds 42",
		"notifier_oldest_outbox_seconds 17",
		"notifier_permanent_failures 5",
		"notifier_rabbitmq_up 0",
		"notifier_queue_depth NaN",
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("metrics missing %q during RabbitMQ outage:\n%s", required, body)
		}
	}
	if strings.Contains(body, "rabbitmq-secret") {
		t.Fatalf("metrics exposed broker failure details: %s", body)
	}
}
