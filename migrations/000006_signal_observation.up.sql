ALTER TABLE outbox_events
    ADD COLUMN observed_at timestamptz;

DROP INDEX IF EXISTS outbox_events_watchdog_idx;

CREATE INDEX outbox_events_watchdog_idx
    ON outbox_events (published_at, observed_at, created_at)
    WHERE published_at IS NOT NULL
        AND lease_owner IS NULL;
