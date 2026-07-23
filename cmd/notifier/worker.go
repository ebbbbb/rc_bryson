package main

import (
	"context"
	"log/slog"
	"time"

	"reliable-notifier/internal/delivery"
	"reliable-notifier/internal/outbound"
	"reliable-notifier/internal/worker"
)

func runWorker(
	ctx context.Context,
	store *delivery.Store,
	rabbitURL string,
	logger *slog.Logger,
) {
	sender, err := outbound.NewSender(
		nil,
		outbound.EnvironmentSecrets{},
		envOr("BOOTSTRAP_LOCAL_CONFIG", "false") == "true" &&
			envOr("ALLOW_TEST_NETWORK_POLICY", "false") == "true",
		envOr("TEST_SUPPLIER_CA_FILE", ""),
	)
	if err != nil {
		logger.Error("Worker sender configuration failed", "error", "invalid_configuration")
		return
	}
	deliveryWorker, err := worker.New(
		store,
		sender,
		envOr("WORKER_ID", "worker-1"),
		90*time.Second,
		logger,
	)
	if err != nil {
		logger.Error("Worker configuration failed", "error", "invalid_configuration")
		return
	}

	for ctx.Err() == nil {
		consumer, connectErr := worker.NewRabbitConsumer(rabbitURL)
		if connectErr != nil {
			logger.Warn("Worker broker unavailable", "error", "broker_unavailable")
			if !waitWorker(ctx, time.Second) {
				return
			}
			continue
		}
		runErr := consumer.Run(ctx, deliveryWorker)
		_ = consumer.Close()
		if ctx.Err() != nil {
			return
		}
		logger.Warn("Worker consumer interrupted", "error", workerErrorCategory(runErr))
		if !waitWorker(ctx, time.Second) {
			return
		}
	}
}

func waitWorker(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func workerErrorCategory(err error) string {
	if err == nil {
		return "consumer_closed"
	}
	return "delivery_or_broker_failure"
}
