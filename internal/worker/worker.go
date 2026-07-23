package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
	"reliable-notifier/internal/outbound"
)

type Store interface {
	ClaimDelivery(
		context.Context,
		string,
		int64,
		string,
		time.Duration,
	) (delivery.Delivery, delivery.DestinationVersion, error)
	CompleteSuccess(context.Context, delivery.AttemptResult) error
}

type Sender interface {
	Send(
		context.Context,
		delivery.Delivery,
		delivery.DestinationVersion,
	) (outbound.Result, error)
}

type Worker struct {
	store         Store
	sender        Sender
	owner         string
	leaseDuration time.Duration
	logger        *slog.Logger
	now           func() time.Time
}

func New(
	store Store,
	sender Sender,
	owner string,
	leaseDuration time.Duration,
	logger *slog.Logger,
) (*Worker, error) {
	if store == nil || sender == nil || owner == "" || leaseDuration <= 0 {
		return nil, errors.New("Worker requires store, sender, owner, and positive lease")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		store:         store,
		sender:        sender,
		owner:         owner,
		leaseDuration: leaseDuration,
		logger:        logger,
		now:           time.Now,
	}, nil
}

// Process returns true only when the broker message can be acknowledged.
func (worker *Worker) Process(ctx context.Context, signal dispatch.Signal) (bool, error) {
	task, destination, err := worker.store.ClaimDelivery(
		ctx,
		signal.DeliveryID,
		signal.Generation,
		worker.owner,
		worker.leaseDuration,
	)
	if errors.Is(err, delivery.ErrDeliveryNotClaimable) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim delivery: %w", err)
	}

	startedAt := worker.now().UTC()
	result, err := worker.sender.Send(ctx, task, destination)
	finishedAt := worker.now().UTC()
	if err != nil {
		return false, fmt.Errorf("send delivery: %w", err)
	}
	if !isSuccess(result.StatusCode, destination.SuccessStatuses) {
		return false, fmt.Errorf("supplier returned non-success status class")
	}
	status := result.StatusCode
	err = worker.store.CompleteSuccess(ctx, delivery.AttemptResult{
		DeliveryID:     task.ID,
		Generation:     task.Generation,
		LeaseOwner:     task.LeaseOwner,
		LeaseToken:     task.LeaseToken,
		ResponseStatus: &status,
		StartedAt:      startedAt,
		FinishedAt:     finishedAt,
	})
	if errors.Is(err, delivery.ErrWorkerLeaseLost) {
		worker.logger.Info(
			"discarded stale Worker result",
			"delivery_id", task.ID,
			"generation", task.Generation,
		)
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("commit delivery success: %w", err)
	}
	return true, nil
}

func isSuccess(status int, configured []int32) bool {
	if len(configured) == 0 {
		return status >= 200 && status < 300
	}
	for _, expected := range configured {
		if status == int(expected) {
			return true
		}
	}
	return false
}
