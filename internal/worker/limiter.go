package worker

import (
	"context"
	"sync"
	"time"

	"reliable-notifier/internal/delivery"
)

type destinationLimiter struct {
	mu     sync.Mutex
	states map[string]*destinationLimitState
	now    func() time.Time
}

type destinationLimitState struct {
	active        int
	maxConcurrent int
	minInterval   time.Duration
	nextAllowed   time.Time
}

func newDestinationLimiter() *destinationLimiter {
	return &destinationLimiter{
		states: make(map[string]*destinationLimitState),
		now:    time.Now,
	}
}

func (limiter *destinationLimiter) acquire(
	ctx context.Context,
	destination delivery.DestinationVersion,
) (func(), error) {
	maxConcurrent := destination.MaxConcurrency
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	rate := destination.RatePerSecond
	if rate <= 0 {
		rate = 1
	}
	interval := time.Duration(float64(time.Second) / rate)

	for {
		limiter.mu.Lock()
		state := limiter.states[destination.DestinationID]
		if state == nil {
			state = &destinationLimitState{
				maxConcurrent: maxConcurrent,
				minInterval:   interval,
			}
			limiter.states[destination.DestinationID] = state
		} else {
			if maxConcurrent < state.maxConcurrent {
				state.maxConcurrent = maxConcurrent
			}
			if interval > state.minInterval {
				state.minInterval = interval
			}
		}

		now := limiter.now()
		if state.active < state.maxConcurrent && !now.Before(state.nextAllowed) {
			state.active++
			state.nextAllowed = now.Add(state.minInterval)
			limiter.mu.Unlock()
			return func() {
				limiter.mu.Lock()
				state.active--
				limiter.mu.Unlock()
			}, nil
		}
		wait := state.nextAllowed.Sub(now)
		if wait <= 0 || state.active >= state.maxConcurrent {
			wait = 10 * time.Millisecond
		}
		limiter.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
