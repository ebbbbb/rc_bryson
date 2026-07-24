DROP INDEX IF EXISTS outbox_events_claim_idx;
ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_lease_is_complete;
