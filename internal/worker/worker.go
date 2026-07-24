package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
	"reliable-notifier/internal/outbound"
)

type Store interface {
	EligibleDestination(
		context.Context,
		string,
		int64,
	) (delivery.DestinationVersion, error)
	ObserveSignal(context.Context, string, int64) error
	ClaimDelivery(
		context.Context,
		string,
		int64,
		string,
		time.Duration,
	) (delivery.Delivery, delivery.DestinationVersion, error)
	CompleteSuccess(context.Context, delivery.AttemptResult) error
	CompleteRetry(context.Context, delivery.AttemptResult, time.Time) error
	CompletePermanent(context.Context, delivery.AttemptResult) error
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
	jitter        func(time.Duration) time.Duration
	limiter       *destinationLimiter
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
		jitter: func(maximum time.Duration) time.Duration {
			if maximum <= 0 {
				return 0
			}
			return time.Duration(time.Now().UnixNano() % int64(maximum))
		},
		limiter: newDestinationLimiter(),
	}, nil
}

// Process returns true only when the broker message can be acknowledged.
func (worker *Worker) Process(ctx context.Context, signal dispatch.Signal) (bool, error) {
	destination, err := worker.store.EligibleDestination(
		ctx,
		signal.DeliveryID,
		signal.Generation,
	)
	if errors.Is(err, delivery.ErrDeliveryNotClaimable) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("check delivery eligibility: %w", err)
	}

	if err := worker.store.ObserveSignal(
		ctx,
		signal.DeliveryID,
		signal.Generation,
	); err != nil {
		return false, fmt.Errorf("record dispatch signal observation: %w", err)
	}
	permit, ok := worker.limiter.tryAcquire(destination)
	if !ok {
		return false, nil
	}

	task, destination, err := worker.store.ClaimDelivery(
		ctx,
		signal.DeliveryID,
		signal.Generation,
		worker.owner,
		worker.leaseDuration,
	)
	if errors.Is(err, delivery.ErrDeliveryNotClaimable) {
		permit.release(false)
		return true, nil
	}
	if err != nil {
		permit.release(false)
		return false, fmt.Errorf("claim delivery: %w", err)
	}
	defer permit.release(true)

	startedAt := worker.now().UTC()
	result, err := worker.sender.Send(ctx, task, destination)
	finishedAt := worker.now().UTC()
	attempt := delivery.AttemptResult{
		DeliveryID: task.ID,
		Generation: task.Generation,
		LeaseOwner: task.LeaseOwner,
		LeaseToken: task.LeaseToken,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
	if result.StatusCode != 0 {
		status := result.StatusCode
		attempt.ResponseStatus = &status
	}

	switch classify(result.StatusCode, err, destination) {
	case resultSuccess:
		err = worker.store.CompleteSuccess(ctx, attempt)
	case resultPermanent:
		attempt.ErrorCategory = resultErrorCategory(result.StatusCode, err)
		err = worker.store.CompletePermanent(ctx, attempt)
	case resultRetryable:
		attempt.ErrorCategory = resultErrorCategory(result.StatusCode, err)
		nextAttempt := worker.nextAttemptAt(finishedAt, task.Generation, result.RetryAfter)
		if !nextAttempt.Before(task.RetryDeadline) {
			attempt.ErrorCategory = "retry_deadline_exhausted"
			err = worker.store.CompletePermanent(ctx, attempt)
		} else {
			err = worker.store.CompleteRetry(ctx, attempt, nextAttempt)
		}
	}
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

type resultClass int

const (
	resultSuccess resultClass = iota
	resultRetryable
	resultPermanent
)

func classify(status int, sendErr error, destination delivery.DestinationVersion) resultClass {
	if sendErr != nil {
		if outbound.IsPermanent(sendErr) {
			return resultPermanent
		}
		return resultRetryable
	}
	if isSuccess(status, destination.SuccessStatuses) {
		return resultSuccess
	}
	for _, configured := range destination.RetryStatuses {
		if status == int(configured) {
			return resultRetryable
		}
	}
	if status == 408 || status == 425 || status == 429 || status >= 500 {
		return resultRetryable
	}
	return resultPermanent
}

func (worker *Worker) nextAttemptAt(now time.Time, generation int64, retryAfter string) time.Time {
	if delay, ok := parseRetryAfter(now, retryAfter); ok {
		return now.Add(delay)
	}
	exponent := generation
	if exponent > 12 {
		exponent = 12
	}
	maximum := time.Second * time.Duration(1<<exponent)
	if maximum > time.Hour {
		maximum = time.Hour
	}
	return now.Add(worker.jitter(maximum))
}

func parseRetryAfter(now time.Time, raw string) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(time.Hour/time.Second) {
			return time.Hour, true
		}
		delay := time.Duration(seconds) * time.Second
		return delay, true
	}
	at, err := http.ParseTime(raw)
	if err != nil || !at.After(now) {
		return 0, false
	}
	delay := at.Sub(now)
	if delay > time.Hour {
		delay = time.Hour
	}
	return delay, true
}

func resultErrorCategory(status int, err error) string {
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return "timeout"
		}
		return "network_or_request_error"
	}
	return fmt.Sprintf("http_%d", status)
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
