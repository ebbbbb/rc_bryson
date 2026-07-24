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
	lastSequence  uint64
	changed       chan struct{}
}

type destinationPermit struct {
	limiter      *destinationLimiter
	state        *destinationLimitState
	sequence     uint64
	previousNext time.Time
	reservedNext time.Time
	released     bool
}

func newDestinationLimiter() *destinationLimiter {
	return &destinationLimiter{
		states: make(map[string]*destinationLimitState),
		now:    time.Now,
	}
}

func (limiter *destinationLimiter) tryAcquire(
	destination delivery.DestinationVersion,
) (*destinationPermit, bool) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	state := limiter.state(destination)
	now := limiter.now()
	if state.active >= state.maxConcurrent || now.Before(state.nextAllowed) {
		return nil, false
	}
	state.active++
	state.lastSequence++
	permit := &destinationPermit{
		limiter:      limiter,
		state:        state,
		sequence:     state.lastSequence,
		previousNext: state.nextAllowed,
		reservedNext: now.Add(state.minInterval),
	}
	state.nextAllowed = permit.reservedNext
	return permit, true
}

func (limiter *destinationLimiter) acquire(
	ctx context.Context,
	destination delivery.DestinationVersion,
) (*destinationPermit, error) {
	for {
		if permit, ok := limiter.tryAcquire(destination); ok {
			return permit, nil
		}
		limiter.mu.Lock()
		state := limiter.state(destination)
		now := limiter.now()
		wait := state.nextAllowed.Sub(now)
		if wait <= 0 || state.active >= state.maxConcurrent {
			wait = 10 * time.Millisecond
		}
		changed := state.changed
		limiter.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (limiter *destinationLimiter) state(
	destination delivery.DestinationVersion,
) *destinationLimitState {
	maxConcurrent := destination.MaxConcurrency
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	rate := destination.RatePerSecond
	if rate <= 0 {
		rate = 1
	}
	interval := time.Duration(float64(time.Second) / rate)
	state := limiter.states[destination.DestinationID]
	if state == nil {
		state = &destinationLimitState{
			maxConcurrent: maxConcurrent,
			minInterval:   interval,
			changed:       make(chan struct{}),
		}
		limiter.states[destination.DestinationID] = state
		return state
	}
	if maxConcurrent < state.maxConcurrent {
		state.maxConcurrent = maxConcurrent
	}
	if interval > state.minInterval {
		state.minInterval = interval
	}
	return state
}

func (permit *destinationPermit) release(claimed bool) {
	permit.limiter.mu.Lock()
	defer permit.limiter.mu.Unlock()
	if permit.released {
		return
	}
	permit.released = true
	permit.state.active--
	if !claimed &&
		permit.state.lastSequence == permit.sequence &&
		permit.state.nextAllowed.Equal(permit.reservedNext) {
		permit.state.nextAllowed = permit.previousNext
	}
	close(permit.state.changed)
	permit.state.changed = make(chan struct{})
}
