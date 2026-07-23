package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
	"reliable-notifier/internal/outbound"
)

type fakeStore struct {
	task        delivery.Delivery
	destination delivery.DestinationVersion
	claimErr    error
	completeErr error
	completes   atomic.Int32
	retries     atomic.Int32
	permanents  atomic.Int32
	complete    chan struct{}
}

func (store *fakeStore) ClaimDelivery(
	context.Context,
	string,
	int64,
	string,
	time.Duration,
) (delivery.Delivery, delivery.DestinationVersion, error) {
	return store.task, store.destination, store.claimErr
}

func (store *fakeStore) CompleteSuccess(context.Context, delivery.AttemptResult) error {
	store.completes.Add(1)
	if store.complete != nil {
		close(store.complete)
	}
	return store.completeErr
}

func (store *fakeStore) CompleteRetry(context.Context, delivery.AttemptResult, time.Time) error {
	store.retries.Add(1)
	return store.completeErr
}

func (store *fakeStore) CompletePermanent(context.Context, delivery.AttemptResult) error {
	store.permanents.Add(1)
	return store.completeErr
}

type fakeSender struct {
	calls   atomic.Int32
	release <-chan struct{}
	result  outbound.Result
	err     error
}

func (sender *fakeSender) Send(
	context.Context,
	delivery.Delivery,
	delivery.DestinationVersion,
) (outbound.Result, error) {
	sender.calls.Add(1)
	if sender.release != nil {
		<-sender.release
	}
	if sender.result.StatusCode == 0 && sender.err == nil {
		return outbound.Result{StatusCode: 204}, nil
	}
	return sender.result, sender.err
}

func TestStaleSignalIsAcknowledgedWithoutSending(t *testing.T) {
	store := &fakeStore{claimErr: delivery.ErrDeliveryNotClaimable}
	sender := &fakeSender{}
	instance := newTestWorker(t, store, sender)

	ack, err := instance.Process(t.Context(), dispatch.Signal{DeliveryID: "delivery-1", Generation: 0})
	if err != nil || !ack {
		t.Fatalf("stale Process = (%v, %v), want ACK and nil", ack, err)
	}
	if sender.calls.Load() != 0 || store.completes.Load() != 0 {
		t.Fatal("stale signal caused a send or result commit")
	}
}

func TestSuccessIsNotCommittedBeforeSupplierResponse(t *testing.T) {
	release := make(chan struct{})
	store := successfulFakeStore()
	sender := &fakeSender{release: release}
	instance := newTestWorker(t, store, sender)
	done := make(chan error, 1)
	go func() {
		_, err := instance.Process(context.Background(), dispatch.Signal{
			DeliveryID: "delivery-1",
			Generation: 0,
		})
		done <- err
	}()

	deadline := time.Now().Add(time.Second)
	for sender.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if sender.calls.Load() != 1 {
		t.Fatal("supplier request did not start")
	}
	if store.completes.Load() != 0 {
		t.Fatal("success was committed before supplier response")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.completes.Load() != 1 {
		t.Fatal("success was not committed after supplier response")
	}
}

func TestStaleResultIsAcknowledgedOnlyAfterFencedDiscard(t *testing.T) {
	store := successfulFakeStore()
	store.completeErr = delivery.ErrWorkerLeaseLost
	sender := &fakeSender{}
	instance := newTestWorker(t, store, sender)

	ack, err := instance.Process(t.Context(), dispatch.Signal{DeliveryID: "delivery-1", Generation: 0})
	if err != nil || !ack {
		t.Fatalf("stale result Process = (%v, %v), want ACK and nil", ack, err)
	}
	if store.completes.Load() != 1 {
		t.Fatal("fenced result was not attempted before ACK decision")
	}
}

func TestDatabaseFailureDoesNotPermitAck(t *testing.T) {
	store := successfulFakeStore()
	store.completeErr = errors.New("database unavailable")
	instance := newTestWorker(t, store, &fakeSender{})

	ack, err := instance.Process(t.Context(), dispatch.Signal{DeliveryID: "delivery-1", Generation: 0})
	if err == nil || ack {
		t.Fatalf("database failure Process = (%v, %v), want no ACK and error", ack, err)
	}
}

func TestRetryableAndPermanentResultsArePersistedDistinctly(t *testing.T) {
	tests := []struct {
		name           string
		sender         *fakeSender
		wantRetries    int32
		wantPermanents int32
	}{
		{
			name:        "HTTP 429",
			sender:      &fakeSender{result: outbound.Result{StatusCode: 429}},
			wantRetries: 1,
		},
		{
			name:        "network failure",
			sender:      &fakeSender{err: errors.New("connection reset")},
			wantRetries: 1,
		},
		{
			name:           "HTTP 400",
			sender:         &fakeSender{result: outbound.Result{StatusCode: 400}},
			wantPermanents: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := successfulFakeStore()
			store.destination.RetryStatuses = []int32{408, 425, 429}
			instance := newTestWorker(t, store, test.sender)

			ack, err := instance.Process(
				t.Context(),
				dispatch.Signal{DeliveryID: "delivery-1", Generation: 0},
			)
			if err != nil || !ack {
				t.Fatalf("Process = (%v, %v), want committed result and ACK", ack, err)
			}
			if got := store.retries.Load(); got != test.wantRetries {
				t.Fatalf("retry commits = %d, want %d", got, test.wantRetries)
			}
			if got := store.permanents.Load(); got != test.wantPermanents {
				t.Fatalf("permanent commits = %d, want %d", got, test.wantPermanents)
			}
		})
	}
}

func TestRetryTimingUsesBoundedRetryAfterAndDeterministicBackoff(t *testing.T) {
	instance := newTestWorker(t, successfulFakeStore(), &fakeSender{})
	instance.jitter = func(maximum time.Duration) time.Duration {
		return maximum / 2
	}
	now := time.Date(2026, time.July, 24, 0, 0, 0, 0, time.UTC)

	if got := instance.nextAttemptAt(now, 4, "7200"); !got.Equal(now.Add(time.Hour)) {
		t.Fatalf("bounded Retry-After = %s, want %s", got, now.Add(time.Hour))
	}
	if got := instance.nextAttemptAt(now, 4, ""); !got.Equal(now.Add(8 * time.Second)) {
		t.Fatalf("generation backoff = %s, want %s", got, now.Add(8*time.Second))
	}
}

func TestResultClassificationDefaults(t *testing.T) {
	destination := delivery.DestinationVersion{
		RetryStatuses: []int32{408, 425, 429},
	}
	tests := []struct {
		name   string
		status int
		err    error
		want   resultClass
	}{
		{name: "success", status: 204, want: resultSuccess},
		{name: "timeout", err: context.DeadlineExceeded, want: resultRetryable},
		{name: "configured retry", status: 429, want: resultRetryable},
		{name: "server failure", status: 503, want: resultRetryable},
		{name: "caller failure", status: 400, want: resultPermanent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classify(test.status, test.err, destination); got != test.want {
				t.Fatalf("classification = %v, want %v", got, test.want)
			}
		})
	}
}

func successfulFakeStore() *fakeStore {
	return &fakeStore{
		task: delivery.Delivery{
			ID:            "delivery-1",
			Method:        "POST",
			Generation:    0,
			LeaseOwner:    "worker-1",
			LeaseToken:    "00000000-0000-4000-8000-000000000001",
			RetryDeadline: time.Now().Add(24 * time.Hour),
		},
		destination: delivery.DestinationVersion{},
	}
}

func newTestWorker(t *testing.T, store Store, sender Sender) *Worker {
	t.Helper()
	instance, err := New(
		store,
		sender,
		"worker-1",
		30*time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return instance
}
