package dispatch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"reliable-notifier/internal/delivery"
)

type OutboxStore interface {
	ClaimOutbox(context.Context, string, time.Duration, int) ([]delivery.OutboxEvent, error)
	MarkOutboxPublished(context.Context, delivery.OutboxEvent) error
	ReleaseOutbox(context.Context, delivery.OutboxEvent) error
}

type SignalBroker interface {
	Publish(context.Context, Signal) error
	Close() error
}

type Publisher struct {
	store         OutboxStore
	broker        SignalBroker
	owner         string
	leaseDuration time.Duration
	batchSize     int
}

func NewPublisher(
	store OutboxStore,
	broker SignalBroker,
	owner string,
	leaseDuration time.Duration,
	batchSize int,
) (*Publisher, error) {
	if store == nil || broker == nil || owner == "" || leaseDuration <= 0 || batchSize <= 0 {
		return nil, errors.New("invalid Publisher configuration")
	}
	return &Publisher{
		store:         store,
		broker:        broker,
		owner:         owner,
		leaseDuration: leaseDuration,
		batchSize:     batchSize,
	}, nil
}

func (publisher *Publisher) RunOnce(ctx context.Context) (int, error) {
	events, err := publisher.store.ClaimOutbox(
		ctx,
		publisher.owner,
		publisher.leaseDuration,
		publisher.batchSize,
	)
	if err != nil {
		return 0, err
	}
	published := 0
	for index, event := range events {
		signal := Signal{
			DeliveryID: event.DeliveryID,
			Generation: event.Generation,
			TraceID:    event.TraceID,
		}
		if err := publisher.broker.Publish(ctx, signal); err != nil {
			publisher.releaseRemaining(events[index:])
			return published, fmt.Errorf("publish Outbox event: %w", err)
		}
		if err := publisher.store.MarkOutboxPublished(ctx, event); err != nil {
			publisher.releaseRemaining(events[index:])
			return published, fmt.Errorf("record Outbox publication: %w", err)
		}
		published++
	}
	return published, nil
}

func (publisher *Publisher) releaseRemaining(events []delivery.OutboxEvent) {
	releaseContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, event := range events {
		_ = publisher.store.ReleaseOutbox(releaseContext, event)
	}
}
