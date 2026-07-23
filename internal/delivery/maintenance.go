package delivery

import (
	"context"
	"fmt"
	"time"
)

type OperationalSnapshot struct {
	OldestPendingSeconds float64
	OldestOutboxSeconds  float64
	ExpiredLeases        int64
	PermanentFailures    int64
	ResultClasses        map[string]int64
}

func (store *Store) ReconcileStalledSignals(
	ctx context.Context,
	watchdog time.Duration,
	limit int,
) (int64, error) {
	if watchdog <= 0 || limit <= 0 {
		return 0, fmt.Errorf("signal reconciliation requires a positive watchdog and limit")
	}
	tag, err := store.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT event.id
			FROM outbox_events AS event
			JOIN deliveries AS delivery
				ON delivery.id = event.delivery_id
				AND delivery.generation = event.generation
			WHERE delivery.status = 'pending'
				AND delivery.next_attempt_at <= clock_timestamp()
				AND delivery.retry_deadline > statement_timestamp()
				AND event.published_at IS NOT NULL
				AND event.published_at <=
					clock_timestamp() - ($1 * interval '1 millisecond')
				AND event.lease_owner IS NULL
				AND event.lease_token IS NULL
				AND event.lease_until IS NULL
			ORDER BY event.published_at, event.created_at
			LIMIT $2
			FOR UPDATE OF event SKIP LOCKED
		)
		UPDATE outbox_events AS event
		SET
			trace_id = gen_random_uuid(),
			available_at = clock_timestamp(),
			published_at = NULL
		FROM candidates
		WHERE event.id = candidates.id
			AND event.published_at IS NOT NULL
	`, watchdog.Milliseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("reconcile stalled delivery signals: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (store *Store) DeleteExpiredTerminal(
	ctx context.Context,
	successRetention time.Duration,
	failureRetention time.Duration,
	limit int,
) (int64, error) {
	if successRetention <= 0 || failureRetention <= 0 || limit <= 0 {
		return 0, fmt.Errorf("retention durations and limit must be positive")
	}
	tag, err := store.pool.Exec(ctx, `
		WITH expired AS (
			SELECT id
			FROM deliveries
			WHERE
				(status = 'succeeded'
					AND updated_at <=
						clock_timestamp() - ($1 * interval '1 millisecond'))
				OR
				(status = 'failed_permanent'
					AND updated_at <=
						clock_timestamp() - ($2 * interval '1 millisecond'))
			ORDER BY updated_at, created_at
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM deliveries AS delivery
		USING expired
		WHERE delivery.id = expired.id
			AND delivery.status IN ('succeeded', 'failed_permanent')
	`, successRetention.Milliseconds(), failureRetention.Milliseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete expired terminal deliveries: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (store *Store) OperationalMetrics(ctx context.Context) (OperationalSnapshot, error) {
	snapshot := OperationalSnapshot{ResultClasses: make(map[string]int64)}
	err := store.pool.QueryRow(ctx, `
		SELECT
			COALESCE((
				SELECT GREATEST(
					0,
					EXTRACT(EPOCH FROM
						(clock_timestamp() - MIN(next_attempt_at))
					)
				)
				FROM deliveries
				WHERE status = 'pending'
					AND next_attempt_at <= clock_timestamp()
			), 0)::float8,
			COALESCE((
				SELECT GREATEST(
					0,
					EXTRACT(EPOCH FROM
						(clock_timestamp() - MIN(available_at))
					)
				)
				FROM outbox_events
				WHERE published_at IS NULL
			), 0)::float8,
			count(*) FILTER (
				WHERE status = 'delivering'
					AND lease_until <= clock_timestamp()
			),
			count(*) FILTER (WHERE status = 'failed_permanent')
		FROM deliveries
	`).Scan(
		&snapshot.OldestPendingSeconds,
		&snapshot.OldestOutboxSeconds,
		&snapshot.ExpiredLeases,
		&snapshot.PermanentFailures,
	)
	if err != nil {
		return OperationalSnapshot{}, fmt.Errorf("read operational delivery metrics: %w", err)
	}
	rows, err := store.pool.Query(ctx, `
		SELECT result_class, count(*)
		FROM delivery_attempts
		GROUP BY result_class
	`)
	if err != nil {
		return OperationalSnapshot{}, fmt.Errorf("read delivery result metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var class string
		var count int64
		if err := rows.Scan(&class, &count); err != nil {
			return OperationalSnapshot{}, fmt.Errorf("scan delivery result metrics: %w", err)
		}
		snapshot.ResultClasses[class] = count
	}
	if err := rows.Err(); err != nil {
		return OperationalSnapshot{}, fmt.Errorf("iterate delivery result metrics: %w", err)
	}
	return snapshot, nil
}
