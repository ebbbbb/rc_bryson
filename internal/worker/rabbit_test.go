package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
)

type fakeAcknowledger struct {
	acks        atomic.Int32
	nacks       atomic.Int32
	lastRequeue atomic.Bool
}

func (acknowledger *fakeAcknowledger) Ack(uint64, bool) error {
	acknowledger.acks.Add(1)
	return nil
}

func (acknowledger *fakeAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	acknowledger.nacks.Add(1)
	acknowledger.lastRequeue.Store(requeue)
	return nil
}

func (*fakeAcknowledger) Reject(uint64, bool) error {
	return nil
}

type fakeDeferrer struct {
	calls atomic.Int32
}

func (deferrer *fakeDeferrer) Defer(context.Context, dispatch.Signal) error {
	deferrer.calls.Add(1)
	return nil
}

func (*fakeDeferrer) Close() error {
	return nil
}

func TestInvalidSignalIsDeadLetteredWithoutDatabaseAccess(t *testing.T) {
	acknowledger := &fakeAcknowledger{}
	message := amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body: []byte(
			`{"delivery_id":"not-a-uuid","generation":0,` +
				`"trace_id":"00000000-0000-4000-8000-000000000002"}`,
		),
	}
	consumer := &RabbitConsumer{}
	if err := consumer.processMessage(t.Context(), nil, message); err != nil {
		t.Fatal(err)
	}
	if acknowledger.nacks.Load() != 1 || acknowledger.lastRequeue.Load() {
		t.Fatal("invalid signal was not dead-lettered exactly once")
	}
}

func TestTransientProcessingFailureRequeuesAndInterruptsConsumer(t *testing.T) {
	store := successfulFakeStore()
	store.eligibleErr = errors.New("database unavailable")
	instance := newTestWorker(t, store, &fakeSender{})
	acknowledger := &fakeAcknowledger{}
	message := validDeliveryMessage(acknowledger)
	consumer := &RabbitConsumer{deferrer: &fakeDeferrer{}}

	err := consumer.processMessage(t.Context(), instance, message)
	if err == nil {
		t.Fatal("transient processing failure did not interrupt consumer for reconnect backoff")
	}
	if acknowledger.nacks.Load() != 1 || !acknowledger.lastRequeue.Load() {
		t.Fatal("transient processing failure was not requeued")
	}
}

func TestBusyDestinationIsDurablyDeferredBeforeAck(t *testing.T) {
	store := successfulFakeStore()
	store.destination.DestinationID = "slow"
	store.destination.RatePerSecond = 1
	store.destination.MaxConcurrency = 1
	instance := newTestWorker(t, store, &fakeSender{})
	permit, ok := instance.limiter.tryAcquire(store.destination)
	if !ok {
		t.Fatal("failed to reserve initial destination capacity")
	}
	defer permit.release(true)

	acknowledger := &fakeAcknowledger{}
	deferrer := &fakeDeferrer{}
	consumer := &RabbitConsumer{deferrer: deferrer}
	if err := consumer.processMessage(
		t.Context(),
		instance,
		validDeliveryMessage(acknowledger),
	); err != nil {
		t.Fatal(err)
	}
	if deferrer.calls.Load() != 1 || acknowledger.acks.Load() != 1 {
		t.Fatalf(
			"deferrals=%d acks=%d, want durable deferral before one ACK",
			deferrer.calls.Load(),
			acknowledger.acks.Load(),
		)
	}
	if store.claims.Load() != 0 {
		t.Fatalf("busy destination acquired %d leases, want zero", store.claims.Load())
	}
}

type routedStore struct {
	slowIDs map[string]struct{}
	slow    delivery.DestinationVersion
	fast    delivery.DestinationVersion
	claims  atomic.Int32
}

func (store *routedStore) destination(id string) delivery.DestinationVersion {
	if _, slow := store.slowIDs[id]; slow {
		return store.slow
	}
	return store.fast
}

func (store *routedStore) EligibleDestination(
	_ context.Context,
	id string,
	_ int64,
) (delivery.DestinationVersion, error) {
	return store.destination(id), nil
}

func (*routedStore) ObserveSignal(context.Context, string, int64) error {
	return nil
}

func (store *routedStore) ClaimDelivery(
	_ context.Context,
	id string,
	generation int64,
	owner string,
	_ time.Duration,
) (delivery.Delivery, delivery.DestinationVersion, error) {
	store.claims.Add(1)
	return delivery.Delivery{
		ID:            id,
		Method:        "POST",
		Generation:    generation,
		LeaseOwner:    owner,
		LeaseToken:    "00000000-0000-4000-8000-000000000099",
		RetryDeadline: time.Now().Add(time.Hour),
	}, store.destination(id), nil
}

func (*routedStore) CompleteSuccess(context.Context, delivery.AttemptResult) error {
	return nil
}

func (*routedStore) CompleteRetry(context.Context, delivery.AttemptResult, time.Time) error {
	return nil
}

func (*routedStore) CompletePermanent(context.Context, delivery.AttemptResult) error {
	return nil
}

func TestEightBusySignalsReleaseCreditForFollowingDestination(t *testing.T) {
	slow := delivery.DestinationVersion{
		DestinationID:  "slow",
		RatePerSecond:  1,
		MaxConcurrency: 1,
	}
	fast := delivery.DestinationVersion{
		DestinationID:  "fast",
		RatePerSecond:  100,
		MaxConcurrency: 1,
	}
	store := &routedStore{
		slowIDs: make(map[string]struct{}),
		slow:    slow,
		fast:    fast,
	}
	instance, err := New(
		store,
		&fakeSender{},
		"worker-1",
		30*time.Second,
		slog.Default(),
	)
	if err != nil {
		t.Fatal(err)
	}
	permit, ok := instance.limiter.tryAcquire(slow)
	if !ok {
		t.Fatal("failed to occupy slow destination")
	}
	defer permit.release(true)

	deferrer := &fakeDeferrer{}
	consumer := &RabbitConsumer{deferrer: deferrer}
	for index := 1; index <= 8; index++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012x", index)
		store.slowIDs[id] = struct{}{}
		acknowledger := &fakeAcknowledger{}
		if err := consumer.processMessage(
			t.Context(),
			instance,
			validDeliveryMessageFor(acknowledger, id),
		); err != nil {
			t.Fatal(err)
		}
		if acknowledger.acks.Load() != 1 {
			t.Fatalf("slow signal %d did not release broker credit", index)
		}
	}

	fastAcknowledger := &fakeAcknowledger{}
	if err := consumer.processMessage(
		t.Context(),
		instance,
		validDeliveryMessageFor(
			fastAcknowledger,
			"00000000-0000-4000-8000-000000000009",
		),
	); err != nil {
		t.Fatal(err)
	}
	if fastAcknowledger.acks.Load() != 1 ||
		store.claims.Load() != 1 ||
		deferrer.calls.Load() != 8 {
		t.Fatalf(
			"fast acks=%d claims=%d deferrals=%d, want 1, 1, 8",
			fastAcknowledger.acks.Load(),
			store.claims.Load(),
			deferrer.calls.Load(),
		)
	}
}

func validDeliveryMessage(acknowledger amqp.Acknowledger) amqp.Delivery {
	return validDeliveryMessageFor(
		acknowledger,
		"00000000-0000-4000-8000-000000000001",
	)
}

func validDeliveryMessageFor(
	acknowledger amqp.Acknowledger,
	deliveryID string,
) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: acknowledger,
		DeliveryTag:  1,
		Body: []byte(
			`{"delivery_id":"` + deliveryID + `",` +
				`"generation":0,"trace_id":"00000000-0000-4000-8000-000000000002"}`,
		),
	}
}
