CREATE INDEX outbox_events_watchdog_idx
    ON outbox_events (published_at, created_at)
    WHERE published_at IS NOT NULL
        AND lease_owner IS NULL;

CREATE INDEX deliveries_terminal_retention_idx
    ON deliveries (status, updated_at)
    WHERE status IN ('succeeded', 'failed_permanent');
