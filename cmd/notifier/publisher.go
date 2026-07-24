package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
)

func runPublisher(
	ctx context.Context,
	store *delivery.Store,
	rabbitURL string,
	logger *slog.Logger,
) {
	owner := fmt.Sprintf("%s-%d-%d", hostname(), os.Getpid(), time.Now().UnixNano())
	for ctx.Err() == nil {
		broker, err := dispatch.NewRabbitBroker(rabbitURL)
		if err != nil {
			logger.Warn("Publisher broker unavailable", "error", "rabbitmq_unavailable")
			if !waitFor(ctx, time.Second) {
				return
			}
			continue
		}
		publisher, err := dispatch.NewPublisher(store, broker, owner, 30*time.Second, 100)
		if err != nil {
			_ = broker.Close()
			logger.Error("Publisher configuration failed", "error", "invalid_configuration")
			return
		}

		for ctx.Err() == nil {
			published, err := publisher.RunOnce(ctx)
			if err != nil {
				logger.Warn("Publisher cycle failed", "error", "publication_failed")
				break
			}
			if published == 0 && !waitFor(ctx, 200*time.Millisecond) {
				_ = broker.Close()
				return
			}
		}
		_ = broker.Close()
		if !waitFor(ctx, time.Second) {
			return
		}
	}
}

func waitFor(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func hostname() string {
	value, err := os.Hostname()
	if err != nil || value == "" {
		return "publisher"
	}
	return value
}
