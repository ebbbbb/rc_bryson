package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (store *Store) Replay(
	ctx context.Context,
	deliveryID string,
	operatorID string,
	reason string,
) (Delivery, error) {
	auditID, err := newUUID()
	if err != nil {
		return Delivery{}, err
	}
	outboxID, err := newUUID()
	if err != nil {
		return Delivery{}, err
	}
	traceID, err := newUUID()
	if err != nil {
		return Delivery{}, err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return Delivery{}, fmt.Errorf("begin replay: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var replayedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&replayedAt); err != nil {
		return Delivery{}, fmt.Errorf("read replay timestamp: %w", err)
	}
	var replayed Delivery
	var headersJSON []byte
	var fromGeneration int64
	err = tx.QueryRow(ctx, `
		UPDATE deliveries
		SET
			status = 'pending',
			generation = generation + 1,
			next_attempt_at = $2::timestamptz,
			retry_deadline = $2::timestamptz + interval '24 hours',
			lease_owner = NULL,
			lease_token = NULL,
			lease_until = NULL,
			updated_at = $2::timestamptz
		WHERE id = $1
			AND status = 'failed_permanent'
		RETURNING
			id::text,
			caller_id,
			destination_id,
			destination_version,
			method,
			caller_headers,
			body,
			supplier_idempotency_value,
			status,
			generation,
			accepted_at,
			retry_deadline,
			next_attempt_at,
			generation - 1
	`, deliveryID, replayedAt).Scan(
		&replayed.ID,
		&replayed.CallerID,
		&replayed.DestinationID,
		&replayed.DestinationVersion,
		&replayed.Method,
		&headersJSON,
		&replayed.Body,
		&replayed.SupplierIdempotencyValue,
		&replayed.Status,
		&replayed.Generation,
		&replayed.AcceptedAt,
		&replayed.RetryDeadline,
		&replayed.NextAttemptAt,
		&fromGeneration,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if lookupErr := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM deliveries WHERE id = $1)
		`, deliveryID).Scan(&exists); lookupErr != nil {
			return Delivery{}, fmt.Errorf("check replay target: %w", lookupErr)
		}
		if !exists {
			return Delivery{}, ErrNotFound
		}
		return Delivery{}, ErrReplayConflict
	}
	if err != nil {
		return Delivery{}, fmt.Errorf("transition replay delivery: %w", err)
	}
	if err := json.Unmarshal(headersJSON, &replayed.CallerHeaders); err != nil {
		return Delivery{}, fmt.Errorf("decode replayed Headers: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO replay_audits (
			id,
			delivery_id,
			from_generation,
			to_generation,
			operator_id,
			reason,
			replayed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, auditID,
		replayed.ID,
		fromGeneration,
		replayed.Generation,
		operatorID,
		reason,
		replayedAt,
	); err != nil {
		return Delivery{}, fmt.Errorf("record replay audit: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			id,
			delivery_id,
			generation,
			trace_id,
			available_at
		)
		VALUES ($1, $2, $3, $4, $5)
	`, outboxID, replayed.ID, replayed.Generation, traceID, replayedAt); err != nil {
		return Delivery{}, fmt.Errorf("record replay Outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Delivery{}, fmt.Errorf("commit replay: %w", err)
	}
	return replayed, nil
}
