//go:build integration

package integration

import (
	"errors"
	"sync"
	"testing"
	"time"

	notifierdelivery "reliable-notifier/internal/delivery"
)

func TestSlice4RetryGenerationSchedulingAndLeaseRecovery(t *testing.T) {
	retryAccepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"result":"retryable"}`),
	)
	permanentAccepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"result":"permanent"}`),
	)
	expiredAccepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"result":"ambiguous-crash"}`),
	)
	deadlineAccepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"result":"deadline-expired-crash"}`),
	)
	schedulerDeadlineAccepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"result":"deadline-expired-pending"}`),
	)
	initialDeadlineAccepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"result":"deadline-expired-initial"}`),
	)
	for _, accepted := range []submitResult{
		retryAccepted,
		permanentAccepted,
		expiredAccepted,
		deadlineAccepted,
		schedulerDeadlineAccepted,
		initialDeadlineAccepted,
	} {
		if accepted.StatusCode != 202 {
			t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
		}
	}

	compose(t, "stop", "app")
	t.Cleanup(func() {
		composeCleanup(t, "up", "-d", "--wait", "--wait-timeout", "120", "app")
	})

	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}

	retryTask, _, err := store.ClaimDelivery(
		t.Context(),
		retryAccepted.Delivery.ID,
		0,
		"retry-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextAttempt := time.Now().Add(time.Hour).UTC()
	retryStatus := 429
	if err := store.CompleteRetry(t.Context(), notifierdelivery.AttemptResult{
		DeliveryID:     retryTask.ID,
		Generation:     retryTask.Generation,
		LeaseOwner:     retryTask.LeaseOwner,
		LeaseToken:     retryTask.LeaseToken,
		ResponseStatus: &retryStatus,
		ErrorCategory:  "http_429",
		StartedAt:      time.Now().Add(-time.Millisecond),
		FinishedAt:     time.Now(),
	}, nextAttempt); err != nil {
		t.Fatal(err)
	}

	var generation int64
	var status string
	var storedNext time.Time
	var retryAttempts int
	var nextOutbox int
	if err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT generation FROM deliveries WHERE id = $1),
			(SELECT status FROM deliveries WHERE id = $1),
			(SELECT next_attempt_at FROM deliveries WHERE id = $1),
			(SELECT count(*) FROM delivery_attempts
			 WHERE delivery_id = $1 AND result_class = 'retryable_failure'),
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = $1 AND generation = 1)
	`, retryTask.ID).Scan(
		&generation,
		&status,
		&storedNext,
		&retryAttempts,
		&nextOutbox,
	); err != nil {
		t.Fatal(err)
	}
	if generation != 1 || status != "pending" || retryAttempts != 1 || nextOutbox != 0 {
		t.Fatalf(
			"retry state generation=%d status=%q attempts=%d nextOutbox=%d",
			generation,
			status,
			retryAttempts,
			nextOutbox,
		)
	}
	if storedNext.Before(nextAttempt.Add(-time.Millisecond)) || storedNext.After(nextAttempt.Add(time.Millisecond)) {
		t.Fatalf("next_attempt_at = %s, want %s", storedNext, nextAttempt)
	}
	for _, signalGeneration := range []int64{0, 1} {
		if _, _, err := store.ClaimDelivery(
			t.Context(),
			retryTask.ID,
			signalGeneration,
			"early-worker",
			30*time.Second,
		); !errors.Is(err, notifierdelivery.ErrDeliveryNotClaimable) {
			t.Fatalf("generation %d early claim = %v, want ErrDeliveryNotClaimable", signalGeneration, err)
		}
	}
	if scheduled, err := store.ScheduleDueRetries(t.Context(), 10); err != nil || scheduled != 0 {
		t.Fatalf("early scheduler = (%d, %v), want zero", scheduled, err)
	}

	if _, err := pool.Exec(t.Context(), `
		UPDATE deliveries
		SET next_attempt_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, retryTask.ID); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	results := make(chan int64, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			count, scheduleErr := store.ScheduleDueRetries(t.Context(), 10)
			if scheduleErr != nil {
				t.Errorf("concurrent scheduler: %v", scheduleErr)
			}
			results <- count
		}()
	}
	group.Wait()
	close(results)
	var scheduled int64
	for count := range results {
		scheduled += count
	}
	if scheduled != 1 {
		t.Fatalf("concurrent scheduler created %d events, want 1", scheduled)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT count(*)
		FROM outbox_events
		WHERE delivery_id = $1 AND generation = 1
	`, retryTask.ID).Scan(&nextOutbox); err != nil {
		t.Fatal(err)
	}
	if nextOutbox != 1 {
		t.Fatalf("generation 1 Outbox rows = %d, want 1", nextOutbox)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT generation FROM deliveries WHERE id = $1
	`, retryTask.ID).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation != 1 {
		t.Fatalf("scheduler advanced generation to %d, want 1", generation)
	}

	permanentTask, _, err := store.ClaimDelivery(
		t.Context(),
		permanentAccepted.Delivery.ID,
		0,
		"permanent-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	permanentStatus := 400
	if err := store.CompletePermanent(t.Context(), notifierdelivery.AttemptResult{
		DeliveryID:     permanentTask.ID,
		Generation:     permanentTask.Generation,
		LeaseOwner:     permanentTask.LeaseOwner,
		LeaseToken:     permanentTask.LeaseToken,
		ResponseStatus: &permanentStatus,
		ErrorCategory:  "http_400",
		StartedAt:      time.Now().Add(-time.Millisecond),
		FinishedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT status
		FROM deliveries
		WHERE id = $1
	`, permanentTask.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed_permanent" {
		t.Fatalf("permanent status = %q, want failed_permanent", status)
	}

	expiredTask, _, err := store.ClaimDelivery(
		t.Context(),
		expiredAccepted.Delivery.ID,
		0,
		"crashed-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpiredDeliveries(t.Context(), 10); err != nil || recovered != 0 {
		t.Fatalf("early lease recovery = (%d, %v), want zero", recovered, err)
	}
	if _, err := pool.Exec(t.Context(), `
		UPDATE deliveries
		SET lease_until = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, expiredTask.ID); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpiredDeliveries(t.Context(), 10); err != nil || recovered != 1 {
		t.Fatalf("expired lease recovery = (%d, %v), want one", recovered, err)
	}
	successStatus := 204
	if err := store.CompleteSuccess(t.Context(), notifierdelivery.AttemptResult{
		DeliveryID:     expiredTask.ID,
		Generation:     expiredTask.Generation,
		LeaseOwner:     expiredTask.LeaseOwner,
		LeaseToken:     expiredTask.LeaseToken,
		ResponseStatus: &successStatus,
		StartedAt:      time.Now().Add(-time.Second),
		FinishedAt:     time.Now(),
	}); !errors.Is(err, notifierdelivery.ErrWorkerLeaseLost) {
		t.Fatalf("expired Worker result = %v, want ErrWorkerLeaseLost", err)
	}
	staleResult := notifierdelivery.AttemptResult{
		DeliveryID:    expiredTask.ID,
		Generation:    expiredTask.Generation,
		LeaseOwner:    expiredTask.LeaseOwner,
		LeaseToken:    expiredTask.LeaseToken,
		ErrorCategory: "stale_result",
		StartedAt:     time.Now().Add(-time.Second),
		FinishedAt:    time.Now(),
	}
	if err := store.CompleteRetry(
		t.Context(),
		staleResult,
		time.Now().Add(time.Minute),
	); !errors.Is(err, notifierdelivery.ErrWorkerLeaseLost) {
		t.Fatalf("expired Worker retry result = %v, want ErrWorkerLeaseLost", err)
	}
	if err := store.CompletePermanent(
		t.Context(),
		staleResult,
	); !errors.Is(err, notifierdelivery.ErrWorkerLeaseLost) {
		t.Fatalf("expired Worker permanent result = %v, want ErrWorkerLeaseLost", err)
	}
	var recoveredOutbox int
	var leaseExpiredAttempts int
	if err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT generation FROM deliveries WHERE id = $1),
			(SELECT status FROM deliveries WHERE id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = $1 AND generation = 1),
			(SELECT count(*) FROM delivery_attempts
			 WHERE delivery_id = $1 AND error_category = 'lease_expired')
	`, expiredTask.ID).Scan(
		&generation,
		&status,
		&recoveredOutbox,
		&leaseExpiredAttempts,
	); err != nil {
		t.Fatal(err)
	}
	if generation != 1 || status != "pending" || recoveredOutbox != 1 || leaseExpiredAttempts != 1 {
		t.Fatalf(
			"recovery generation=%d status=%q outbox=%d attempts=%d",
			generation,
			status,
			recoveredOutbox,
			leaseExpiredAttempts,
		)
	}
	reclaimedTask, _, err := store.ClaimDelivery(
		t.Context(),
		expiredTask.ID,
		1,
		"replacement-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimedTask.SupplierIdempotencyValue != expiredTask.SupplierIdempotencyValue {
		t.Fatalf(
			"reclaimed supplier idempotency = %q, want stable %q",
			reclaimedTask.SupplierIdempotencyValue,
			expiredTask.SupplierIdempotencyValue,
		)
	}

	deadlineTask, _, err := store.ClaimDelivery(
		t.Context(),
		deadlineAccepted.Delivery.ID,
		0,
		"deadline-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		UPDATE deliveries
		SET
			lease_until = clock_timestamp() - interval '1 second',
			retry_deadline = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, deadlineTask.ID); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpiredDeliveries(t.Context(), 10); err != nil || recovered != 1 {
		t.Fatalf("deadline lease recovery = (%d, %v), want one", recovered, err)
	}
	var deadlineOutbox int
	var deadlineAttempts int
	if err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT generation FROM deliveries WHERE id = $1),
			(SELECT status FROM deliveries WHERE id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = $1 AND generation > 0),
			(SELECT count(*) FROM delivery_attempts
			 WHERE delivery_id = $1
			   AND result_class = 'permanent_failure'
			   AND error_category = 'retry_deadline_exhausted')
	`, deadlineTask.ID).Scan(
		&generation,
		&status,
		&deadlineOutbox,
		&deadlineAttempts,
	); err != nil {
		t.Fatal(err)
	}
	if generation != 0 || status != "failed_permanent" || deadlineOutbox != 0 || deadlineAttempts != 1 {
		t.Fatalf(
			"deadline recovery generation=%d status=%q outbox=%d attempts=%d",
			generation,
			status,
			deadlineOutbox,
			deadlineAttempts,
		)
	}

	schedulerDeadlineTask, _, err := store.ClaimDelivery(
		t.Context(),
		schedulerDeadlineAccepted.Delivery.ID,
		0,
		"scheduler-deadline-worker",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteRetry(t.Context(), notifierdelivery.AttemptResult{
		DeliveryID:    schedulerDeadlineTask.ID,
		Generation:    schedulerDeadlineTask.Generation,
		LeaseOwner:    schedulerDeadlineTask.LeaseOwner,
		LeaseToken:    schedulerDeadlineTask.LeaseToken,
		ErrorCategory: "network_or_request_error",
		StartedAt:     time.Now().Add(-time.Millisecond),
		FinishedAt:    time.Now(),
	}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		UPDATE deliveries
		SET
			next_attempt_at = clock_timestamp() - interval '1 second',
			retry_deadline = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, schedulerDeadlineTask.ID); err != nil {
		t.Fatal(err)
	}
	if scheduled, err := store.ScheduleDueRetries(t.Context(), 10); err != nil || scheduled != 0 {
		t.Fatalf("deadline scheduler = (%d, %v), want no Outbox", scheduled, err)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT status FROM deliveries WHERE id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE delivery_id = $1 AND generation = 1)
	`, schedulerDeadlineTask.ID).Scan(&status, &deadlineOutbox); err != nil {
		t.Fatal(err)
	}
	if status != "failed_permanent" || deadlineOutbox != 0 {
		t.Fatalf("deadline scheduler status=%q outbox=%d", status, deadlineOutbox)
	}

	if _, err := pool.Exec(t.Context(), `
		UPDATE deliveries
		SET retry_deadline = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, initialDeadlineAccepted.Delivery.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ClaimDelivery(
		t.Context(),
		initialDeadlineAccepted.Delivery.ID,
		0,
		"too-late-worker",
		30*time.Second,
	); !errors.Is(err, notifierdelivery.ErrDeliveryNotClaimable) {
		t.Fatalf("expired initial claim = %v, want ErrDeliveryNotClaimable", err)
	}
	if scheduled, err := store.ScheduleDueRetries(t.Context(), 10); err != nil || scheduled != 0 {
		t.Fatalf("initial deadline cleanup = (%d, %v), want no Outbox", scheduled, err)
	}
	if err := pool.QueryRow(t.Context(), `
		SELECT status FROM deliveries WHERE id = $1
	`, initialDeadlineAccepted.Delivery.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed_permanent" {
		t.Fatalf("expired initial status = %q, want failed_permanent", status)
	}
}
