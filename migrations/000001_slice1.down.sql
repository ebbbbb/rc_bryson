DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS caller_destinations;
DROP TRIGGER IF EXISTS destination_versions_are_immutable ON destination_versions;
DROP FUNCTION IF EXISTS reject_destination_version_mutation();
ALTER TABLE IF EXISTS destinations DROP CONSTRAINT IF EXISTS destinations_current_version_fk;
DROP TABLE IF EXISTS destination_versions;
DROP TABLE IF EXISTS destinations;
DROP TABLE IF EXISTS callers;
