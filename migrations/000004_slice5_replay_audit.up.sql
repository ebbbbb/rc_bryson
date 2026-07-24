CREATE TABLE replay_audits (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    from_generation bigint NOT NULL CHECK (from_generation >= 0),
    to_generation bigint NOT NULL CHECK (to_generation = from_generation + 1),
    operator_id text NOT NULL REFERENCES callers(id),
    reason text NOT NULL CHECK (length(reason) BETWEEN 1 AND 1000),
    replayed_at timestamptz NOT NULL,
    UNIQUE (delivery_id, to_generation)
);

CREATE INDEX replay_audits_delivery_idx
    ON replay_audits (delivery_id, replayed_at);
