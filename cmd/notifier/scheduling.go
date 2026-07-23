package main

import (
	"context"
	"log/slog"
	"time"

	"reliable-notifier/internal/delivery"
)

func runRetryScheduler(ctx context.Context, store *delivery.Store, logger *slog.Logger) {
	runMaintenanceLoop(ctx, 100*time.Millisecond, func(loopContext context.Context) error {
		_, err := store.ScheduleDueRetries(loopContext, 100)
		return err
	}, "retry_scheduler", logger)
}

func runLeaseReconciler(ctx context.Context, store *delivery.Store, logger *slog.Logger) {
	runMaintenanceLoop(ctx, time.Second, func(loopContext context.Context) error {
		_, err := store.RecoverExpiredDeliveries(loopContext, 100)
		return err
	}, "lease_reconciler", logger)
}

func runMaintenanceLoop(
	ctx context.Context,
	interval time.Duration,
	run func(context.Context) error,
	component string,
	logger *slog.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := run(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("maintenance cycle failed", "component", component, "error", "database_operation_failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
