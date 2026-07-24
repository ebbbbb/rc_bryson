package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrDeliveryNotClaimable = errors.New("delivery signal is stale, early, or not pending")
	ErrWorkerLeaseLost      = errors.New("delivery Worker lease is no longer valid")
)

type AttemptResult struct {
	DeliveryID     string
	Generation     int64
	LeaseOwner     string
	LeaseToken     string
	ResponseStatus *int
	ErrorCategory  string
	StartedAt      time.Time
	FinishedAt     time.Time
}

func (store *Store) EligibleDestination(
	ctx context.Context,
	deliveryID string,
	generation int64,
) (DestinationVersion, error) {
	var destinationID string
	var destinationVersion int64
	err := store.pool.QueryRow(ctx, `
		SELECT destination_id, destination_version
		FROM deliveries
		WHERE id = $1
			AND generation = $2
			AND status = 'pending'
			AND next_attempt_at <= clock_timestamp()
			AND retry_deadline > clock_timestamp()
	`, deliveryID, generation).Scan(&destinationID, &destinationVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return DestinationVersion{}, ErrDeliveryNotClaimable
	}
	if err != nil {
		return DestinationVersion{}, fmt.Errorf("check delivery eligibility: %w", err)
	}
	return queryDestinationVersion(ctx, store.pool, destinationID, destinationVersion)
}

func (store *Store) ClaimDelivery(
	ctx context.Context,
	deliveryID string,
	generation int64,
	owner string,
	leaseDuration time.Duration,
) (Delivery, DestinationVersion, error) {
	if owner == "" || leaseDuration <= 0 {
		return Delivery{}, DestinationVersion{}, errors.New("Worker claim requires owner and positive lease")
	}
	leaseToken, err := newUUID()
	if err != nil {
		return Delivery{}, DestinationVersion{}, err
	}

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return Delivery{}, DestinationVersion{}, fmt.Errorf("begin Worker claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var claimed Delivery
	var headersJSON []byte
	err = tx.QueryRow(ctx, `
		UPDATE deliveries
		SET
			status = 'delivering',
			lease_owner = $3,
			lease_token = $4,
			lease_until = clock_timestamp() + $5::interval,
			updated_at = clock_timestamp()
		WHERE id = $1
			AND generation = $2
			AND status = 'pending'
			AND next_attempt_at <= clock_timestamp()
			AND retry_deadline > clock_timestamp()
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
			lease_owner,
			lease_token::text,
			lease_until
	`, deliveryID, generation, owner, leaseToken, interval(leaseDuration)).Scan(
		&claimed.ID,
		&claimed.CallerID,
		&claimed.DestinationID,
		&claimed.DestinationVersion,
		&claimed.Method,
		&headersJSON,
		&claimed.Body,
		&claimed.SupplierIdempotencyValue,
		&claimed.Status,
		&claimed.Generation,
		&claimed.AcceptedAt,
		&claimed.RetryDeadline,
		&claimed.NextAttemptAt,
		&claimed.LeaseOwner,
		&claimed.LeaseToken,
		&claimed.LeaseUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, DestinationVersion{}, ErrDeliveryNotClaimable
	}
	if err != nil {
		return Delivery{}, DestinationVersion{}, fmt.Errorf("claim delivery: %w", err)
	}
	if err := json.Unmarshal(headersJSON, &claimed.CallerHeaders); err != nil {
		return Delivery{}, DestinationVersion{}, fmt.Errorf("decode claimed Headers: %w", err)
	}

	destination, err := queryDestinationVersion(ctx, tx, claimed.DestinationID, claimed.DestinationVersion)
	if err != nil {
		return Delivery{}, DestinationVersion{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Delivery{}, DestinationVersion{}, fmt.Errorf("commit Worker claim: %w", err)
	}
	return claimed, destination, nil
}

func (store *Store) CompleteSuccess(ctx context.Context, result AttemptResult) error {
	return store.completeResult(ctx, result, "succeeded", nil)
}

func (store *Store) CompleteRetry(
	ctx context.Context,
	result AttemptResult,
	nextAttemptAt time.Time,
) error {
	return store.completeResult(ctx, result, "retryable_failure", &nextAttemptAt)
}

func (store *Store) CompletePermanent(ctx context.Context, result AttemptResult) error {
	return store.completeResult(ctx, result, "permanent_failure", nil)
}

func (store *Store) completeResult(
	ctx context.Context,
	result AttemptResult,
	resultClass string,
	nextAttemptAt *time.Time,
) error {
	attemptID, err := newUUID()
	if err != nil {
		return err
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin success result: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	tag, err := tx.Exec(ctx, `
		UPDATE deliveries
		SET
			status = CASE
				WHEN $5 = 'retryable_failure' THEN 'pending'
				WHEN $5 = 'permanent_failure' THEN 'failed_permanent'
				ELSE 'succeeded'
			END,
			generation = CASE
				WHEN $5 = 'retryable_failure' THEN generation + 1
				ELSE generation
			END,
			next_attempt_at = CASE
				WHEN $5 = 'retryable_failure' THEN $6
				ELSE next_attempt_at
			END,
			lease_owner = NULL,
			lease_token = NULL,
			lease_until = NULL,
			updated_at = clock_timestamp()
		WHERE id = $1
			AND generation = $2
			AND status = 'delivering'
			AND lease_owner = $3
			AND lease_token = $4
			AND lease_until > clock_timestamp()
			AND ($5 <> 'retryable_failure' OR $6 < retry_deadline)
	`, result.DeliveryID,
		result.Generation,
		result.LeaseOwner,
		result.LeaseToken,
		resultClass,
		nextAttemptAt,
	)
	if err != nil {
		return fmt.Errorf("commit fenced %s state: %w", resultClass, err)
	}
	if tag.RowsAffected() != 1 {
		return ErrWorkerLeaseLost
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts (
			id,
			delivery_id,
			generation,
			lease_token,
			result_class,
			response_status,
			error_category,
			started_at,
			finished_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
	`, attemptID,
		result.DeliveryID,
		result.Generation,
		result.LeaseToken,
		resultClass,
		result.ResponseStatus,
		result.ErrorCategory,
		result.StartedAt,
		result.FinishedAt,
	); err != nil {
		return fmt.Errorf("record %s attempt: %w", resultClass, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s result: %w", resultClass, err)
	}
	return nil
}

func queryDestinationVersion(
	ctx context.Context,
	query queryRower,
	destinationID string,
	version int64,
) (DestinationVersion, error) {
	var destination DestinationVersion
	var connectTimeoutMS int
	var requestTimeoutMS int
	err := query.QueryRow(ctx, `
		SELECT
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
			rate_per_second::float8,
			max_concurrency
		FROM destination_versions
		WHERE destination_id = $1 AND version = $2
	`, destinationID, version).Scan(
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
	if err != nil {
		return DestinationVersion{}, fmt.Errorf("load immutable destination version: %w", err)
	}
	destination.ConnectTimeout = time.Duration(connectTimeoutMS) * time.Millisecond
	destination.RequestTimeout = time.Duration(requestTimeoutMS) * time.Millisecond
	return destination, nil
}

func interval(duration time.Duration) string {
	return fmt.Sprintf("%f seconds", duration.Seconds())
}
