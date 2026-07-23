package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrOutboxLeaseLost = errors.New("Outbox lease was lost")

type OutboxEvent struct {
	ID         string
	DeliveryID string
	Generation int64
	TraceID    string
	LeaseOwner string
	LeaseToken string
}

func (store *Store) ClaimOutbox(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
	limit int,
) ([]OutboxEvent, error) {
	if owner == "" || leaseDuration <= 0 || limit <= 0 {
		return nil, errors.New("invalid Outbox claim parameters")
	}
	token, err := newUUID()
	if err != nil {
		return nil, err
	}
	rows, err := store.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
				AND available_at <= clock_timestamp()
				AND (lease_until IS NULL OR lease_until <= clock_timestamp())
			ORDER BY available_at, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox_events AS event
		SET
			lease_owner = $2,
			lease_token = $3,
			lease_until = clock_timestamp() + ($4 * interval '1 millisecond')
		FROM candidates
		WHERE event.id = candidates.id
		RETURNING
			event.id::text,
			event.delivery_id::text,
			event.generation,
			event.trace_id::text
	`, limit, owner, token, leaseDuration.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim Outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		event := OutboxEvent{
			LeaseOwner: owner,
			LeaseToken: token,
		}
		if err := rows.Scan(
			&event.ID,
			&event.DeliveryID,
			&event.Generation,
			&event.TraceID,
		); err != nil {
			return nil, fmt.Errorf("scan claimed Outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed Outbox events: %w", err)
	}
	return events, nil
}

func (store *Store) MarkOutboxPublished(ctx context.Context, event OutboxEvent) error {
	tag, err := store.pool.Exec(ctx, `
		UPDATE outbox_events
		SET
			published_at = clock_timestamp(),
			lease_owner = NULL,
			lease_token = NULL,
			lease_until = NULL
		WHERE id = $1
			AND published_at IS NULL
			AND lease_owner = $2
			AND lease_token = $3
			AND lease_until > clock_timestamp()
	`, event.ID, event.LeaseOwner, event.LeaseToken)
	if err != nil {
		return fmt.Errorf("mark Outbox event published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrOutboxLeaseLost
	}
	return nil
}

func (store *Store) ReleaseOutbox(ctx context.Context, event OutboxEvent) error {
	tag, err := store.pool.Exec(ctx, `
		UPDATE outbox_events
		SET
			lease_owner = NULL,
			lease_token = NULL,
			lease_until = NULL
		WHERE id = $1
			AND published_at IS NULL
			AND lease_owner = $2
			AND lease_token = $3
	`, event.ID, event.LeaseOwner, event.LeaseToken)
	if err != nil {
		return fmt.Errorf("release Outbox event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var published bool
		err := store.pool.QueryRow(ctx, `
			SELECT published_at IS NOT NULL
			FROM outbox_events
			WHERE id = $1
		`, event.ID).Scan(&published)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrOutboxLeaseLost
		}
		if err != nil {
			return fmt.Errorf("inspect Outbox release: %w", err)
		}
		if !published {
			return ErrOutboxLeaseLost
		}
	}
	return nil
}
