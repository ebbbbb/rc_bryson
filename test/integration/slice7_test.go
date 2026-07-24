//go:build integration

package integration

import (
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	notifierdelivery "reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
)

func TestSlice7ReconciliationRetentionAndMetrics(t *testing.T) {
	stalled := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"stalled-signal"}`),
	)
	healthyBacklog := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"healthy-capacity-wait"}`),
	)
	runtimeLost := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"runtime-lost-signal"}`),
	)
	expiredLease := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"expired-lease"}`),
	)
	futureRetry := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"future-retry"}`),
	)
	oldSuccess := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"old-success"}`),
	)
	newSuccess := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"new-success"}`),
	)
	oldFailure := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"old-failure"}`),
	)
	pendingOld := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":7,"case":"pending-old"}`),
	)

	metricsResponse, err := http.Get(strings.TrimSuffix(appURL(), "/healthz") + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricsBody, err := io.ReadAll(metricsResponse.Body)
	metricsResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if metricsResponse.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, body = %s", metricsResponse.StatusCode, metricsBody)
	}
	for _, metric := range []string{
		"notifier_oldest_pending_seconds",
		"notifier_oldest_outbox_seconds",
		"notifier_expired_leases",
		"notifier_queue_depth",
		"notifier_delivery_results_total",
		"notifier_submissions_total",
		"notifier_rabbitmq_up",
		"notifier_permanent_failures",
	} {
		if !strings.Contains(string(metricsBody), metric) {
			t.Fatalf("metrics endpoint missing %q:\n%s", metric, metricsBody)
		}
	}

	compose(t, "stop", "app")
	t.Cleanup(func() {
		if err := os.Setenv("WORKER_ENABLED", "false"); err != nil {
			t.Errorf("restore WORKER_ENABLED: %v", err)
			return
		}
		composeCleanup(t, "up", "-d", "--force-recreate", "--no-deps", "app")
	})

	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []struct {
		query string
		id    string
	}{
		{`
			UPDATE outbox_events
			SET
				published_at = clock_timestamp() - interval '10 minutes',
				lease_owner = NULL,
				lease_token = NULL,
				lease_until = NULL
			WHERE delivery_id = $1
		`, stalled.Delivery.ID},
		{`
			UPDATE outbox_events
			SET
				published_at = clock_timestamp() - interval '10 minutes',
				observed_at = clock_timestamp(),
				lease_owner = NULL,
				lease_token = NULL,
				lease_until = NULL
			WHERE delivery_id = $1
		`, healthyBacklog.Delivery.ID},
		{`
			UPDATE deliveries
			SET
				status = 'delivering',
				lease_owner = 'crashed-worker',
				lease_token = gen_random_uuid(),
				lease_until = clock_timestamp() - interval '1 minute',
				updated_at = clock_timestamp() - interval '2 minutes'
			WHERE id = $1
		`, expiredLease.Delivery.ID},
		{`
			UPDATE deliveries
			SET
				status = 'pending',
				generation = 1,
				next_attempt_at = clock_timestamp() + interval '1 hour',
				updated_at = clock_timestamp()
			WHERE id = $1
		`, futureRetry.Delivery.ID},
		{`DELETE FROM outbox_events WHERE delivery_id = $1`, futureRetry.Delivery.ID},
		{`
			UPDATE deliveries
			SET status = 'succeeded', updated_at = clock_timestamp() - interval '8 days'
			WHERE id = $1
		`, oldSuccess.Delivery.ID},
		{`
			UPDATE deliveries
			SET status = 'succeeded', updated_at = clock_timestamp()
			WHERE id = $1
		`, newSuccess.Delivery.ID},
		{`
			UPDATE deliveries
			SET status = 'failed_permanent', updated_at = clock_timestamp() - interval '31 days'
			WHERE id = $1
		`, oldFailure.Delivery.ID},
		{`
			UPDATE deliveries
			SET updated_at = clock_timestamp() - interval '40 days'
			WHERE id = $1
		`, pendingOld.Delivery.ID},
	}
	for _, fixture := range fixtures {
		if _, err := pool.Exec(t.Context(), fixture.query, fixture.id); err != nil {
			t.Fatal(err)
		}
	}

	runConcurrent := func(run func() (int64, error)) int64 {
		t.Helper()
		results := make(chan int64, 2)
		var group sync.WaitGroup
		for range 2 {
			group.Add(1)
			go func() {
				defer group.Done()
				count, runErr := run()
				if runErr != nil {
					t.Errorf("concurrent maintenance: %v", runErr)
				}
				results <- count
			}()
		}
		group.Wait()
		close(results)
		var total int64
		for count := range results {
			total += count
		}
		return total
	}

	if repaired := runConcurrent(func() (int64, error) {
		return store.ReconcileStalledSignals(t.Context(), time.Minute, 10)
	}); repaired != 1 {
		t.Fatalf("concurrent stalled-signal repairs = %d, want 1", repaired)
	}
	var healthyPublished bool
	if err := pool.QueryRow(t.Context(), `
		SELECT published_at IS NOT NULL
		FROM outbox_events
		WHERE delivery_id = $1
	`, healthyBacklog.Delivery.ID).Scan(&healthyPublished); err != nil {
		t.Fatal(err)
	}
	if !healthyPublished {
		t.Fatal("recently observed capacity-waiting signal was falsely republished")
	}
	var stalledGeneration int64
	var stalledPublished bool
	if err := pool.QueryRow(t.Context(), `
		SELECT
			delivery.generation,
			event.published_at IS NOT NULL
		FROM deliveries AS delivery
		JOIN outbox_events AS event
			ON event.delivery_id = delivery.id
			AND event.generation = delivery.generation
		WHERE delivery.id = $1
	`, stalled.Delivery.ID).Scan(&stalledGeneration, &stalledPublished); err != nil {
		t.Fatal(err)
	}
	if stalledGeneration != 0 || stalledPublished {
		t.Fatalf(
			"stalled repair generation=%d published=%t, want same generation unpublished",
			stalledGeneration,
			stalledPublished,
		)
	}

	if recovered := runConcurrent(func() (int64, error) {
		return store.RecoverExpiredDeliveries(t.Context(), 10)
	}); recovered != 1 {
		t.Fatalf("concurrent expired-lease repairs = %d, want 1", recovered)
	}
	var recoveredGeneration int64
	var recoverySignals int
	if err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT generation FROM deliveries WHERE id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = $1 AND generation = 1)
	`, expiredLease.Delivery.ID).Scan(&recoveredGeneration, &recoverySignals); err != nil {
		t.Fatal(err)
	}
	if recoveredGeneration != 1 || recoverySignals != 1 {
		t.Fatalf(
			"lease recovery generation=%d signals=%d, want generation 1 and one signal",
			recoveredGeneration,
			recoverySignals,
		)
	}

	if repaired, err := store.ReconcileStalledSignals(t.Context(), time.Minute, 10); err != nil || repaired != 0 {
		t.Fatalf("future retry reconciliation = (%d, %v), want zero", repaired, err)
	}
	if scheduled, err := store.ScheduleDueRetries(t.Context(), 10); err != nil || scheduled != 0 {
		t.Fatalf("future retry scheduling = (%d, %v), want zero", scheduled, err)
	}
	var futureSignals int
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*) FROM outbox_events WHERE delivery_id = $1
	`, futureRetry.Delivery.ID).Scan(&futureSignals); err != nil {
		t.Fatal(err)
	}
	if futureSignals != 0 {
		t.Fatalf("future retry signals = %d, want zero", futureSignals)
	}

	deleted, err := store.DeleteExpiredTerminal(t.Context(), 7*24*time.Hour, 30*24*time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("retention deleted %d deliveries, want 2", deleted)
	}
	for _, retainedID := range []string{
		newSuccess.Delivery.ID,
		pendingOld.Delivery.ID,
		futureRetry.Delivery.ID,
	} {
		var exists bool
		if err := pool.QueryRow(t.Context(), `
			SELECT EXISTS (SELECT 1 FROM deliveries WHERE id = $1)
		`, retainedID).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("retention deleted protected delivery %s", retainedID)
		}
	}

	snapshot, err := store.OperationalMetrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.OldestPendingSeconds <= 0 ||
		snapshot.ExpiredLeases != 0 ||
		snapshot.PermanentFailures < 1 ||
		snapshot.ResultClasses["retryable_failure"] < 1 {
		t.Fatalf("operational metrics missing required classes: %+v", snapshot)
	}

	connection := connectRabbit(t)
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	for _, queue := range []string{dispatch.QueueName, dispatch.DeferralQueue} {
		if _, err := channel.QueuePurge(queue, false); err != nil {
			t.Fatal(err)
		}
	}
	_ = channel.Close()
	_ = connection.Close()
	if _, err := pool.Exec(t.Context(), `
		UPDATE outbox_events
		SET
			published_at = clock_timestamp() - interval '10 minutes',
			observed_at = NULL,
			lease_owner = NULL,
			lease_token = NULL,
			lease_until = NULL
		WHERE delivery_id = $1
	`, runtimeLost.Delivery.ID); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("WORKER_ENABLED", "true"); err != nil {
		t.Fatal(err)
	}
	compose(t, "up", "-d", "--force-recreate", "--no-deps", "app")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		var generation int64
		var attempts int
		err := pool.QueryRow(t.Context(), `
			SELECT
				status,
				generation,
				(SELECT count(*) FROM delivery_attempts
				 WHERE delivery_id = deliveries.id)
			FROM deliveries
			WHERE id = $1
		`, runtimeLost.Delivery.ID).Scan(&status, &generation, &attempts)
		if err == nil && status == "succeeded" {
			if generation != 0 || attempts != 1 {
				t.Fatalf(
					"runtime signal recovery generation=%d attempts=%d, want 0 and 1",
					generation,
					attempts,
				)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("runtime Reconciler/Publisher/Worker did not recover the removed signal")
}
