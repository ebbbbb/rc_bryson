CREATE TABLE callers (
    id text PRIMARY KEY,
    api_key_hash bytea NOT NULL UNIQUE,
    is_operator boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE destinations (
    id text PRIMARY KEY,
    current_version bigint NOT NULL CHECK (current_version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE destination_versions (
    destination_id text NOT NULL REFERENCES destinations(id),
    version bigint NOT NULL CHECK (version > 0),
    url text NOT NULL,
    network_policy text NOT NULL,
    allowed_methods text[] NOT NULL,
    allowed_headers text[] NOT NULL,
    secret_ref text NOT NULL,
    idempotency_header text NOT NULL,
    success_statuses integer[] NOT NULL DEFAULT '{}',
    retry_statuses integer[] NOT NULL DEFAULT '{408,425,429}',
    connect_timeout_ms integer NOT NULL DEFAULT 3000 CHECK (connect_timeout_ms > 0),
    request_timeout_ms integer NOT NULL DEFAULT 15000 CHECK (
        request_timeout_ms > 0 AND request_timeout_ms <= 60000
    ),
    rate_per_second numeric NOT NULL DEFAULT 1 CHECK (rate_per_second > 0),
    max_concurrency integer NOT NULL DEFAULT 1 CHECK (max_concurrency > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (destination_id, version)
);

ALTER TABLE destinations
    ADD CONSTRAINT destinations_current_version_fk
    FOREIGN KEY (id, current_version)
    REFERENCES destination_versions(destination_id, version)
    DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION reject_destination_version_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'destination versions are immutable';
END;
$$;

CREATE TRIGGER destination_versions_are_immutable
BEFORE UPDATE OR DELETE ON destination_versions
FOR EACH ROW
EXECUTE FUNCTION reject_destination_version_mutation();

CREATE TABLE caller_destinations (
    caller_id text NOT NULL REFERENCES callers(id),
    destination_id text NOT NULL REFERENCES destinations(id),
    PRIMARY KEY (caller_id, destination_id)
);

CREATE TABLE deliveries (
    id uuid PRIMARY KEY,
    caller_id text NOT NULL REFERENCES callers(id),
    idempotency_key text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    destination_id text NOT NULL,
    destination_version bigint NOT NULL,
    method text NOT NULL,
    caller_headers jsonb NOT NULL,
    body bytea NOT NULL,
    supplier_idempotency_value text NOT NULL,
    status text NOT NULL CHECK (
        status IN ('pending', 'delivering', 'succeeded', 'failed_permanent')
    ),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    accepted_at timestamptz NOT NULL,
    retry_deadline timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (caller_id, idempotency_key),
    FOREIGN KEY (destination_id, destination_version)
        REFERENCES destination_versions(destination_id, version)
);

CREATE INDEX deliveries_active_idx
    ON deliveries (status)
    WHERE status IN ('pending', 'delivering');

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    generation bigint NOT NULL CHECK (generation >= 0),
    trace_id uuid NOT NULL,
    available_at timestamptz NOT NULL,
    published_at timestamptz,
    lease_owner text,
    lease_token uuid,
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (delivery_id, generation)
);

CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (available_at, created_at)
    WHERE published_at IS NULL;
