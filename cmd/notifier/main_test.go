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

			newMux(nil, nil, nil).ServeHTTP(response, request)

			if response.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
			}
		})
	}
}

func TestReadinessFailsClosedWhileLivenessRemainsHealthy(t *testing.T) {
	ready := func(context.Context) error { return errors.New("database unavailable") }
	handler := newMux(nil, ready, nil)
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/healthz", want: http.StatusNoContent},
		{path: "/readyz", want: http.StatusServiceUnavailable},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
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
		`notifier_delivery_results_total{class="succeeded"} 7`,
		`notifier_delivery_results_total{class="retryable_failure"} 5`,
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
