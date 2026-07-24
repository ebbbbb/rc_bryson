package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const admissionLockID int64 = 731047225683

type Store struct {
	pool         *pgxpool.Pool
	backlogLimit int64
}

func NewStore(pool *pgxpool.Pool, backlogLimit int64) (*Store, error) {
	if pool == nil {
		return nil, errors.New("delivery store requires a PostgreSQL pool")
	}
	if backlogLimit <= 0 {
		return nil, errors.New("backlog limit must be positive")
	}
	return &Store{pool: pool, backlogLimit: backlogLimit}, nil
}

func (store *Store) AuthenticatePrincipal(ctx context.Context, apiKey string) (Principal, error) {
	digest := sha256.Sum256([]byte(apiKey))
	var principal Principal
	err := store.pool.QueryRow(ctx, `
		SELECT id, is_operator
		FROM callers
		WHERE api_key_hash = $1
	`, digest[:]).Scan(&principal.CallerID, &principal.IsOperator)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrUnauthorized
	}
	if err != nil {
		return Principal{}, fmt.Errorf("authenticate principal: %w", err)
	}
	return principal, nil
}

func (store *Store) AuthorizedDestination(
	ctx context.Context,
	callerID string,
	destinationID string,
) (DestinationVersion, error) {
	var destination DestinationVersion
	var connectTimeoutMS int
	var requestTimeoutMS int
	err := store.pool.QueryRow(ctx, `
		SELECT
			version.destination_id,
			version.version,
			version.url,
			version.network_policy,
			version.allowed_methods,
			version.allowed_headers,
			version.secret_ref,
			version.credential_header,
			version.idempotency_header,
			version.success_statuses,
			version.retry_statuses,
			version.connect_timeout_ms,
			version.request_timeout_ms,
			version.rate_per_second::float8,
			version.max_concurrency
		FROM caller_destinations AS authorized
		JOIN destinations AS destination
			ON destination.id = authorized.destination_id
		JOIN destination_versions AS version
			ON version.destination_id = destination.id
			AND version.version = destination.current_version
		WHERE authorized.caller_id = $1
			AND authorized.destination_id = $2
	`, callerID, destinationID).Scan(
		&destination.DestinationID,
		&destination.Version,
		&destination.URL,
		&destination.NetworkPolicy,
		&destination.AllowedMethods,
		&destination.AllowedHeaders,
		&destination.SecretRef,
		&destination.CredentialHeader,
		&destination.IdempotencyHeader,
		&destination.SuccessStatuses,
		&destination.RetryStatuses,
		&connectTimeoutMS,
		&requestTimeoutMS,
		&destination.RatePerSecond,
		&destination.MaxConcurrency,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return DestinationVersion{}, ErrDestinationDenied
	}
	if err != nil {
		return DestinationVersion{}, fmt.Errorf("resolve authorized destination: %w", err)
	}
	destination.ConnectTimeout = time.Duration(connectTimeoutMS) * time.Millisecond
	destination.RequestTimeout = time.Duration(requestTimeoutMS) * time.Millisecond
	return destination, nil
}

func (store *Store) Submit(ctx context.Context, submission Submission) (Delivery, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Delivery{}, fmt.Errorf("begin submission transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, admissionLockID); err != nil {
		return Delivery{}, fmt.Errorf("lock admission: %w", err)
	}

	existing, existingHash, err := queryDeliveryByIdempotency(
		ctx,
		tx,
		submission.CallerID,
		submission.IdempotencyKey,
	)
	switch {
	case err == nil:
		if !bytes.Equal(existingHash, submission.RequestHash[:]) {
			return Delivery{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return Delivery{}, fmt.Errorf("commit idempotent lookup: %w", err)
		}
		return existing, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Delivery{}, fmt.Errorf("query idempotent delivery: %w", err)
	}

	var active int64
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM deliveries
		WHERE status IN ('pending', 'delivering')
	`).Scan(&active); err != nil {
		return Delivery{}, fmt.Errorf("count active backlog: %w", err)
	}
	if active >= store.backlogLimit {
		return Delivery{}, ErrBacklogCapacity
	}

	deliveryID, err := newUUID()
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
	if submission.SupplierIdempotencyValue == "" {
		submission.SupplierIdempotencyValue = deliveryID
	}
	headersJSON, err := json.Marshal(submission.CallerHeaders)
	if err != nil {
		return Delivery{}, fmt.Errorf("encode caller Headers: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO deliveries (
			id,
			caller_id,
			idempotency_key,
			request_hash,
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
			next_attempt_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			'pending', 0, $11, $12, $11
		)
	`, deliveryID,
		submission.CallerID,
		submission.IdempotencyKey,
		submission.RequestHash[:],
		submission.DestinationID,
		submission.DestinationVersion,
		submission.Method,
		headersJSON,
		submission.Body,
		submission.SupplierIdempotencyValue,
		submission.AcceptedAt,
		submission.RetryDeadline,
	)
	if err != nil {
		return Delivery{}, fmt.Errorf("insert delivery: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			id,
			delivery_id,
			generation,
			trace_id,
			available_at
		)
		VALUES ($1, $2, 0, $3, $4)
	`, outboxID, deliveryID, traceID, submission.AcceptedAt); err != nil {
		return Delivery{}, fmt.Errorf("insert initial Outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Delivery{}, fmt.Errorf("commit submission: %w", err)
	}

	return Delivery{
		ID:                       deliveryID,
		CallerID:                 submission.CallerID,
		DestinationID:            submission.DestinationID,
		DestinationVersion:       submission.DestinationVersion,
		Method:                   submission.Method,
		CallerHeaders:            submission.CallerHeaders,
		Body:                     bytes.Clone(submission.Body),
		SupplierIdempotencyValue: submission.SupplierIdempotencyValue,
		Status:                   StatusPending,
		Generation:               0,
		AcceptedAt:               submission.AcceptedAt,
		RetryDeadline:            submission.RetryDeadline,
		NextAttemptAt:            submission.AcceptedAt,
	}, nil
}

func (store *Store) Get(ctx context.Context, callerID, deliveryID string) (Delivery, error) {
	delivery, _, err := queryDelivery(ctx, store.pool, `
		WHERE id = $1 AND caller_id = $2
	`, deliveryID, callerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, ErrNotFound
	}
	if err != nil {
		return Delivery{}, fmt.Errorf("get delivery: %w", err)
	}
	return delivery, nil
}

func (store *Store) ListAttemptSummaries(
	ctx context.Context,
	callerID string,
	deliveryID string,
	limit int,
) ([]AttemptSummary, error) {
	if limit <= 0 {
		return nil, errors.New("attempt summary limit must be positive")
	}
	rows, err := store.pool.Query(ctx, `
		SELECT
			attempt.generation,
			attempt.result_class,
			attempt.response_status,
			attempt.error_category,
			attempt.started_at,
			attempt.finished_at
		FROM delivery_attempts AS attempt
		JOIN deliveries AS delivery ON delivery.id = attempt.delivery_id
		WHERE delivery.id = $1
			AND delivery.caller_id = $2
		ORDER BY attempt.started_at DESC, attempt.id DESC
		LIMIT $3
	`, deliveryID, callerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list delivery attempt summaries: %w", err)
	}
	defer rows.Close()

	summaries := make([]AttemptSummary, 0)
	for rows.Next() {
		var summary AttemptSummary
		if err := rows.Scan(
			&summary.Generation,
			&summary.ResultClass,
			&summary.ResponseStatus,
			&summary.ErrorCategory,
			&summary.StartedAt,
			&summary.FinishedAt,
		); err != nil {
			return nil, fmt.Errorf("scan delivery attempt summary: %w", err)
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery attempt summaries: %w", err)
	}
	return summaries, nil
}

func (store *Store) Bootstrap(ctx context.Context, config BootstrapConfig) error {
	if config.CallerID == "" || config.CallerAPIKey == "" ||
		config.Destination.DestinationID == "" || config.Destination.Version <= 0 {
		return errors.New("bootstrap configuration is incomplete")
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin bootstrap: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	apiKeyHash := sha256.Sum256([]byte(config.CallerAPIKey))
	if _, err := tx.Exec(ctx, `
		INSERT INTO callers (id, api_key_hash)
		VALUES ($1, $2)
		ON CONFLICT (id) DO NOTHING
	`, config.CallerID, apiKeyHash[:]); err != nil {
		return fmt.Errorf("bootstrap caller: %w", err)
	}
	var callerMatches bool
	if err := tx.QueryRow(ctx, `
		SELECT api_key_hash = $2
		FROM callers
		WHERE id = $1
	`, config.CallerID, apiKeyHash[:]).Scan(&callerMatches); err != nil {
		return fmt.Errorf("verify bootstrap caller: %w", err)
	}
	if !callerMatches {
		return errors.New("bootstrap caller conflicts with existing configuration")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO destinations (id, current_version)
		VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE
		SET current_version = EXCLUDED.current_version
	`, config.Destination.DestinationID, config.Destination.Version); err != nil {
		return fmt.Errorf("bootstrap destination: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO destination_versions (
			destination_id,
			version,
			url,
			network_policy,
			allowed_methods,
			allowed_headers,
			secret_ref,
			credential_header,
			idempotency_header,
			success_statuses,
			retry_statuses,
			connect_timeout_ms,
			request_timeout_ms,
			rate_per_second,
			max_concurrency
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
		ON CONFLICT (destination_id, version) DO NOTHING
	`,
		config.Destination.DestinationID,
		config.Destination.Version,
		config.Destination.URL,
		config.Destination.NetworkPolicy,
		config.Destination.AllowedMethods,
		config.Destination.AllowedHeaders,
		config.Destination.SecretRef,
		config.Destination.CredentialHeader,
		config.Destination.IdempotencyHeader,
		config.Destination.SuccessStatuses,
		config.Destination.RetryStatuses,
		config.Destination.ConnectTimeout.Milliseconds(),
		config.Destination.RequestTimeout.Milliseconds(),
		config.Destination.RatePerSecond,
		config.Destination.MaxConcurrency,
	); err != nil {
		return fmt.Errorf("bootstrap destination version: %w", err)
	}
	existingDestination, err := queryDestinationVersion(
		ctx,
		tx,
		config.Destination.DestinationID,
		config.Destination.Version,
	)
	if err != nil {
		return fmt.Errorf("verify bootstrap destination version: %w", err)
	}
	if !reflect.DeepEqual(existingDestination, config.Destination) {
		return errors.New("bootstrap destination version conflicts with existing configuration")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO caller_destinations (caller_id, destination_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING
	`, config.CallerID, config.Destination.DestinationID); err != nil {
		return fmt.Errorf("bootstrap destination authorization: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit bootstrap: %w", err)
	}
	return nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryDeliveryByIdempotency(
	ctx context.Context,
	query queryRower,
	callerID string,
	idempotencyKey string,
) (Delivery, []byte, error) {
	delivery, requestHash, err := queryDelivery(
		ctx,
		query,
		`WHERE caller_id = $1 AND idempotency_key = $2`,
		callerID,
		idempotencyKey,
	)
	return delivery, requestHash, err
}

func queryDelivery(
	ctx context.Context,
	query queryRower,
	where string,
	args ...any,
) (Delivery, []byte, error) {
	var delivery Delivery
	var headersJSON []byte
	var requestHash []byte
	err := query.QueryRow(ctx, `
		SELECT
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
			request_hash
		FROM deliveries
	`+where, args...).Scan(
		&delivery.ID,
		&delivery.CallerID,
		&delivery.DestinationID,
		&delivery.DestinationVersion,
		&delivery.Method,
		&headersJSON,
		&delivery.Body,
		&delivery.SupplierIdempotencyValue,
		&delivery.Status,
		&delivery.Generation,
		&delivery.AcceptedAt,
		&delivery.RetryDeadline,
		&delivery.NextAttemptAt,
		&requestHash,
	)
	if err != nil {
		return Delivery{}, nil, err
	}
	if err := json.Unmarshal(headersJSON, &delivery.CallerHeaders); err != nil {
		return Delivery{}, nil, fmt.Errorf("decode caller Headers: %w", err)
	}
	return delivery, requestHash, nil
}
