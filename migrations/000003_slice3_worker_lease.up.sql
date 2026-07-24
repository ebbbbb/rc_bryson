ALTER TABLE deliveries
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_until timestamptz;

ALTER TABLE destination_versions
    ADD COLUMN credential_header text NOT NULL DEFAULT 'Authorization';

ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_lease_tuple_consistent CHECK (
        (status = 'delivering'
            AND lease_owner IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_until IS NOT NULL)
        OR
        (status <> 'delivering'
            AND lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_until IS NULL)
    );

CREATE TABLE delivery_attempts (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    generation bigint NOT NULL CHECK (generation >= 0),
    lease_token uuid NOT NULL,
    result_class text NOT NULL CHECK (
        result_class IN ('succeeded', 'retryable_failure', 'permanent_failure')
    ),
    response_status integer,
    error_category text,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    CHECK (finished_at >= started_at)
);

CREATE INDEX deliveries_claim_idx
    ON deliveries (next_attempt_at, created_at)
    WHERE status = 'pending';

CREATE INDEX delivery_attempts_delivery_idx
    ON delivery_attempts (delivery_id, started_at);
