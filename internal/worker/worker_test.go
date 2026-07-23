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

type fakeSender struct {
	calls   atomic.Int32
	release <-chan struct{}
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
	return outbound.Result{StatusCode: 204}, nil
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

func successfulFakeStore() *fakeStore {
	return &fakeStore{
		task: delivery.Delivery{
			ID:         "delivery-1",
			Method:     "POST",
			Generation: 0,
			LeaseOwner: "worker-1",
			LeaseToken: "00000000-0000-4000-8000-000000000001",
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
