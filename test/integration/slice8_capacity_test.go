//go:build integration && capacity

package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	notifierdelivery "reliable-notifier/internal/delivery"
)

const capacityDestination = "supplier-capacity"

func TestSlice8CapacityAndRestartRecovery(t *testing.T) {
	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Bootstrap(t.Context(), notifierdelivery.BootstrapConfig{
		CallerID:     "caller-a",
		CallerAPIKey: testCallerKey,
		Destination: notifierdelivery.DestinationVersion{
			DestinationID:     capacityDestination,
			Version:           1,
			URL:               "https://fake-supplier.test:8443/notify",
			NetworkPolicy:     "test-only-supplier",
			AllowedMethods:    []string{"POST"},
			AllowedHeaders:    []string{"content-type", "x-event-type"},
			SecretRef:         "env:SUPPLIER_AUTH_VALUE",
			CredentialHeader:  "Authorization",
			IdempotencyHeader: "Idempotency-Key",
			SuccessStatuses:   []int32{},
			RetryStatuses:     []int32{408, 425, 429},
			ConnectTimeout:    3 * time.Second,
			RequestTimeout:    15 * time.Second,
			RatePerSecond:     250,
			MaxConcurrency:    8,
		},
	}); err != nil {
		t.Fatal(err)
	}

	const deliveries = 200
	prefix := fmt.Sprintf("slice8-capacity-%d-", time.Now().UnixNano())
	type result struct {
		id  string
		err error
	}
	results := make(chan result, deliveries)
	var group sync.WaitGroup
	ticker := time.NewTicker(10 * time.Millisecond)
	started := time.Now()
	for index := range deliveries {
		<-ticker.C
		group.Add(1)
		go func() {
			defer group.Done()
			submitted, submitErr := submitDeliveryRequest(
				testCallerKey,
				fmt.Sprintf("%s%d", prefix, index),
				capacityDestination,
				[]byte(fmt.Sprintf(`{"slice":8,"sequence":%d}`, index)),
			)
			if submitErr != nil {
				results <- result{err: submitErr}
				return
			}
			if submitted.StatusCode != 202 {
				results <- result{err: fmt.Errorf(
					"submission %d returned %d: %s",
					index,
					submitted.StatusCode,
					submitted.Body,
				)}
				return
			}
			results <- result{id: submitted.Delivery.ID}
		}()
	}
	ticker.Stop()
	offeredDuration := time.Since(started)
	group.Wait()
	close(results)

	accepted := 0
	for submitted := range results {
		if submitted.err != nil {
			t.Error(submitted.err)
			continue
		}
		if submitted.id == "" {
			t.Error("accepted submission returned an empty delivery ID")
			continue
		}
		accepted++
	}
	if accepted != deliveries {
		t.Fatalf("accepted %d of %d offered submissions", accepted, deliveries)
	}
	offeredRate := float64(deliveries) / offeredDuration.Seconds()
	if offeredRate < 90 || offeredRate > 110 {
		t.Fatalf("offered rate %.1f req/s is outside the declared 100 req/s profile", offeredRate)
	}

	waitForPrefixSucceeded(t, pool, prefix, deliveries, 90*time.Second)
	var persisted int
	var succeeded int
	var p99 float64
	var maximum float64
	if err := pool.QueryRow(t.Context(), `
		WITH first_attempt AS (
			SELECT
				delivery.id,
				EXTRACT(EPOCH FROM
					(MIN(attempt.started_at) - delivery.accepted_at)
				)::float8 AS latency_seconds
			FROM deliveries AS delivery
			JOIN delivery_attempts AS attempt
				ON attempt.delivery_id = delivery.id
			WHERE delivery.caller_id = 'caller-a'
				AND delivery.idempotency_key LIKE $1
			GROUP BY delivery.id, delivery.accepted_at
		)
		SELECT
			(SELECT count(*) FROM deliveries
			 WHERE caller_id = 'caller-a' AND idempotency_key LIKE $1),
			(SELECT count(*) FROM deliveries
			 WHERE caller_id = 'caller-a'
				AND idempotency_key LIKE $1
				AND status = 'succeeded'),
			percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_seconds),
			max(latency_seconds)
		FROM first_attempt
	`, prefix+"%").Scan(&persisted, &succeeded, &p99, &maximum); err != nil {
		t.Fatal(err)
	}
	if persisted != deliveries || succeeded != deliveries {
		t.Fatalf(
			"offered=%d accepted=%d persisted=%d succeeded=%d",
			deliveries,
			accepted,
			persisted,
			succeeded,
		)
	}
	if p99 > 60 {
		t.Fatalf("measured first-attempt p99 %.3fs exceeds proposed 60s SLO", p99)
	}
	t.Logf(
		"CAPACITY_RESULT offered_rate=%.1f/s accepted=%d persisted=%d succeeded=%d p99=%.3fs max=%.3fs",
		offeredRate,
		accepted,
		persisted,
		succeeded,
		p99,
		maximum,
	)

	compose(t, "stop", "rabbitmq")
	queueOutage := submitDelivery(
		t,
		testCallerKey,
		prefix+"queue-outage",
		capacityDestination,
		[]byte(`{"slice":8,"crash":"queue-outage"}`),
	)
	compose(t, "up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
	waitForDeliverySucceeded(t, pool, queueOutage.Delivery.ID, 90*time.Second)

	processRestart := submitDelivery(
		t,
		testCallerKey,
		prefix+"process-restart",
		capacityDestination,
		[]byte(`{"slice":8,"crash":"process-restart"}`),
	)
	compose(t, "restart", "app")
	compose(t, "up", "-d", "--wait", "--wait-timeout", "120", "app")
	waitForDeliverySucceeded(t, pool, processRestart.Delivery.ID, 90*time.Second)

	if err := os.Setenv("WORKER_ENABLED", "false"); err != nil {
		t.Fatal(err)
	}
	compose(t, "up", "-d", "--force-recreate", "--wait", "--wait-timeout", "120", "app")
	databaseRestart := submitDelivery(
		t,
		testCallerKey,
		prefix+"database-restart",
		capacityDestination,
		[]byte(`{"slice":8,"crash":"database-restart"}`),
	)
	compose(t, "restart", "postgres")
	compose(t, "up", "-d", "--wait", "--wait-timeout", "120", "postgres")
	pool = refreshedDatabasePool(t)
	if err := os.Setenv("WORKER_ENABLED", "true"); err != nil {
		t.Fatal(err)
	}
	compose(t, "up", "-d", "--force-recreate", "--wait", "--wait-timeout", "120", "app")
	waitForDeliverySucceeded(t, pool, databaseRestart.Delivery.ID, 90*time.Second)
}

func refreshedDatabasePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	command := exec.Command(
		"docker",
		"compose",
		"--project-name", requiredEnv(t, "COMPOSE_PROJECT_NAME"),
		"--project-directory", requiredEnv(t, "REPO_ROOT"),
		"port",
		"postgres",
		"5432",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve PostgreSQL port after restart: %v\n%s", err, output)
	}
	_, port, err := net.SplitHostPort(strings.TrimSpace(string(output)))
	if err != nil {
		t.Fatalf("parse PostgreSQL endpoint %q: %v", output, err)
	}
	databaseURL := fmt.Sprintf(
		"postgres://notifier:notifier-test-only@127.0.0.1:%s/notifier?sslmode=disable",
		port,
	)
	if err := os.Setenv("DATABASE_URL", databaseURL); err != nil {
		t.Fatal(err)
	}
	return integrationPool(t)
}

func waitForPrefixSucceeded(
	t *testing.T,
	pool *pgxpool.Pool,
	prefix string,
	want int,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var succeeded int
		if err := pool.QueryRow(t.Context(), `
			SELECT count(*)
			FROM deliveries
			WHERE caller_id = 'caller-a'
				AND idempotency_key LIKE $1
				AND status = 'succeeded'
		`, prefix+"%").Scan(&succeeded); err != nil {
			t.Fatal(err)
		}
		if succeeded == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%d capacity deliveries did not all succeed within %s", want, timeout)
}

func waitForDeliverySucceeded(
	t *testing.T,
	pool *pgxpool.Pool,
	deliveryID string,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastStatus string
	var lastErr error
	for time.Now().Before(deadline) {
		var status string
		err := pool.QueryRow(t.Context(), `
			SELECT status FROM deliveries WHERE id = $1
		`, deliveryID).Scan(&status)
		if err == nil && status == "succeeded" {
			return
		}
		lastStatus = status
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	var generation int64
	var unpublished int
	var published int
	var attempts int
	diagnosticErr := pool.QueryRow(t.Context(), `
		SELECT
			generation,
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = deliveries.id AND published_at IS NULL),
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = deliveries.id AND published_at IS NOT NULL),
			(SELECT count(*) FROM delivery_attempts
			 WHERE delivery_id = deliveries.id)
		FROM deliveries
		WHERE id = $1
	`, deliveryID).Scan(&generation, &unpublished, &published, &attempts)
	t.Fatalf(
		"delivery %s did not succeed within %s: status=%q generation=%d "+
			"outbox_unpublished=%d outbox_published=%d attempts=%d "+
			"last_query_error=%v diagnostic_error=%v",
		deliveryID,
		timeout,
		lastStatus,
		generation,
		unpublished,
		published,
		attempts,
		lastErr,
		diagnosticErr,
	)
}
