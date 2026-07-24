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
		if _, err := store.RecoverExpiredDeliveries(loopContext, 100); err != nil {
			return err
		}
		_, err := store.ReconcileStalledSignals(loopContext, time.Minute, 100)
		return err
	}, "lease_reconciler", logger)
}

func runRetention(ctx context.Context, store *delivery.Store, logger *slog.Logger) {
	runMaintenanceLoop(ctx, time.Hour, func(loopContext context.Context) error {
		_, err := drainRetention(loopContext, store, 1000)
		return err
	}, "retention", logger)
}

type retentionStore interface {
	DeleteExpiredTerminal(
		context.Context,
		time.Duration,
		time.Duration,
		int,
	) (int64, error)
}

func drainRetention(ctx context.Context, store retentionStore, batchSize int) (int64, error) {
	var total int64
	for {
		deleted, err := store.DeleteExpiredTerminal(
			ctx,
			7*24*time.Hour,
			30*24*time.Hour,
			batchSize,
		)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted < int64(batchSize) {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

func runOperationalAlerts(ctx context.Context, store *delivery.Store, logger *slog.Logger) {
	runMaintenanceLoop(ctx, 30*time.Second, func(loopContext context.Context) error {
		snapshot, err := store.OperationalMetrics(loopContext)
		if err != nil {
			return err
		}
		alertOperationalState(logger, snapshot)
		return nil
	}, "operational_alerts", logger)
}

func alertOperationalState(logger *slog.Logger, snapshot delivery.OperationalSnapshot) {
	if snapshot.OldestPendingSeconds >= 60 {
		logger.Warn(
			"delivery backlog is aging",
			"oldest_pending_seconds", snapshot.OldestPendingSeconds,
		)
	}
	if snapshot.OldestOutboxSeconds >= 60 {
		logger.Warn(
			"Outbox publication is delayed",
			"oldest_outbox_seconds", snapshot.OldestOutboxSeconds,
		)
	}
	if snapshot.ExpiredLeases > 0 {
		logger.Warn(
			"expired Worker leases require reconciliation",
			"expired_leases", snapshot.ExpiredLeases,
		)
	}
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
