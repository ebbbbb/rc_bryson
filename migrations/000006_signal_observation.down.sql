DROP INDEX IF EXISTS outbox_events_watchdog_idx;

ALTER TABLE outbox_events
    DROP COLUMN IF EXISTS observed_at;

CREATE INDEX outbox_events_watchdog_idx
    ON outbox_events (published_at, created_at)
    WHERE published_at IS NOT NULL
        AND lease_owner IS NULL;
