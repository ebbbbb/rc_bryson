package delivery

import (
	"context"
	"fmt"
)

func (store *Store) ScheduleDueRetries(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("retry schedule limit must be positive")
	}
	tag, err := store.pool.Exec(ctx, `
		WITH expired AS (
			UPDATE deliveries
			SET
				status = 'failed_permanent',
				updated_at = clock_timestamp()
			WHERE status = 'pending'
				AND retry_deadline <= statement_timestamp()
			RETURNING id
		),
		due AS (
			SELECT delivery.id, delivery.generation, delivery.next_attempt_at
			FROM deliveries AS delivery
			WHERE delivery.status = 'pending'
				AND delivery.generation > 0
				AND delivery.next_attempt_at <= clock_timestamp()
				AND delivery.retry_deadline > statement_timestamp()
				AND NOT EXISTS (
					SELECT 1
					FROM outbox_events AS existing
					WHERE existing.delivery_id = delivery.id
						AND existing.generation = delivery.generation
				)
			ORDER BY delivery.next_attempt_at, delivery.created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		INSERT INTO outbox_events (
			id,
			delivery_id,
			generation,
			trace_id,
			available_at
		)
		SELECT
			gen_random_uuid(),
			due.id,
			due.generation,
			gen_random_uuid(),
			due.next_attempt_at
		FROM due
		ON CONFLICT (delivery_id, generation) DO NOTHING
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("schedule due retries: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (store *Store) RecoverExpiredDeliveries(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("lease recovery limit must be positive")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin expired lease recovery: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	rows, err := tx.Query(ctx, `
		WITH expired AS (
			SELECT
				id,
				generation,
				lease_token,
				updated_at AS started_at,
				retry_deadline
			FROM deliveries
			WHERE status = 'delivering'
				AND lease_until <= statement_timestamp()
			ORDER BY lease_until, created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		),
		recovered AS (
			UPDATE deliveries AS delivery
			SET
				status = CASE
					WHEN expired.retry_deadline <= statement_timestamp()
						THEN 'failed_permanent'
					ELSE 'pending'
				END,
				generation = CASE
					WHEN expired.retry_deadline <= statement_timestamp()
						THEN delivery.generation
					ELSE delivery.generation + 1
				END,
				next_attempt_at = clock_timestamp(),
				lease_owner = NULL,
				lease_token = NULL,
				lease_until = NULL,
				updated_at = clock_timestamp()
			FROM expired
			WHERE delivery.id = expired.id
				AND delivery.generation = expired.generation
				AND delivery.status = 'delivering'
			RETURNING
				delivery.id,
				delivery.generation,
				expired.generation AS attempt_generation,
				expired.lease_token,
				expired.started_at,
				delivery.updated_at AS finished_at,
				delivery.status
		),
		recorded AS (
			INSERT INTO delivery_attempts (
				id,
				delivery_id,
				generation,
				lease_token,
				result_class,
				error_category,
				started_at,
				finished_at
			)
			SELECT
				gen_random_uuid(),
				id,
				attempt_generation,
				lease_token,
				CASE
					WHEN status = 'failed_permanent' THEN 'permanent_failure'
					ELSE 'retryable_failure'
				END,
				CASE
					WHEN status = 'failed_permanent' THEN 'retry_deadline_exhausted'
					ELSE 'lease_expired'
				END,
				started_at,
				finished_at
			FROM recovered
		),
		signalled AS (
			INSERT INTO outbox_events (
				id,
				delivery_id,
				generation,
				trace_id,
				available_at
			)
			SELECT
				gen_random_uuid(),
				id,
				generation,
				gen_random_uuid(),
				finished_at
			FROM recovered
			WHERE status = 'pending'
			ON CONFLICT (delivery_id, generation) DO NOTHING
			RETURNING delivery_id
		)
		SELECT id::text
		FROM recovered
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("recover expired leases: %w", err)
	}
	var recovered int64
	for rows.Next() {
		var ignored string
		if err := rows.Scan(&ignored); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan recovered lease: %w", err)
		}
		recovered++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate recovered leases: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit expired lease recovery: %w", err)
	}
	return recovered, nil
}
