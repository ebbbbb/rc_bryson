package worker

import (
	"testing"
	"time"

	"reliable-notifier/internal/delivery"
)

func TestDestinationLimiterDoesNotBlockAnotherDestination(t *testing.T) {
	limiter := newDestinationLimiter()
	first := delivery.DestinationVersion{
		DestinationID:  "slow",
		RatePerSecond:  100,
		MaxConcurrency: 1,
	}
	release, err := limiter.acquire(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}

	blocked := make(chan struct{})
	go func() {
		secondRelease, acquireErr := limiter.acquire(t.Context(), first)
		if acquireErr == nil {
			secondRelease()
		}
		close(blocked)
	}()
	select {
	case <-blocked:
		t.Fatal("same-destination concurrency limit was bypassed")
	case <-time.After(20 * time.Millisecond):
	}

	other := delivery.DestinationVersion{
		DestinationID:  "healthy",
		RatePerSecond:  100,
		MaxConcurrency: 1,
	}
	otherRelease, err := limiter.acquire(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	otherRelease()
	release()
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("same-destination waiter did not resume")
	}
}
