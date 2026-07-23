DROP TABLE IF EXISTS delivery_attempts;

DROP INDEX IF EXISTS deliveries_claim_idx;

ALTER TABLE destination_versions
    DROP COLUMN IF EXISTS credential_header;

ALTER TABLE deliveries
    DROP CONSTRAINT IF EXISTS deliveries_lease_tuple_consistent,
    DROP COLUMN IF EXISTS lease_until,
    DROP COLUMN IF EXISTS lease_token,
    DROP COLUMN IF EXISTS lease_owner;
