ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_lease_is_complete CHECK (
        (
            lease_owner IS NULL
            AND lease_token IS NULL
            AND lease_until IS NULL
        )
        OR
        (
            lease_owner IS NOT NULL
            AND lease_token IS NOT NULL
            AND lease_until IS NOT NULL
        )
    );

CREATE INDEX outbox_events_claim_idx
    ON outbox_events (available_at, lease_until, created_at)
    WHERE published_at IS NULL;
