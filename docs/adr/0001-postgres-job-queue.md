# ADR 0001: PostgreSQL State with a Message-Queue Signal Layer

- **Status:** Accepted
- **Date:** 2026-07-23

## Context

The API must acknowledge work without waiting for a supplier, survive process and
queue restarts, expose task status, and support retry and manual replay. Writing a
database row and publishing a queue message directly would create a crash window
in which only one side succeeds.

## Decision

Use PostgreSQL as the sole authoritative job store and a durable message queue as
a replaceable dispatch-signal layer. Insert the delivery and its initial Outbox
event in one PostgreSQL transaction. A separate publisher sends Outbox events with
broker confirmation. Workers receive identifiers, then lease and update the
authoritative PostgreSQL row.

Retry timing remains in PostgreSQL. A retryable Worker result advances generation
and writes `next_attempt_at` in its fenced result transaction. The Retry Scheduler
only creates the current generation's Outbox event after that time; it does not
advance generation. The Reconciler repairs expired leases and demonstrable signal
deviations. Queue messages never contain body data, credentials, or authoritative
status.

Go, PostgreSQL, RabbitMQ, and local Docker Compose are approved. Their library-level
choices are recorded separately in ADR 0004 and do not change this ADR's
source-of-truth decision.

Outbox Publisher, Retry Scheduler, and Reconciler name distinct logical
responsibilities. They do not imply three microservices or deployment units; the
MVP may implement them as independent loops in one codebase and process.

The broker dead-letter queue is operational evidence about message transport, not
the source of truth for permanent business failure. Only PostgreSQL records the
`failed_permanent` delivery state.

## Consequences

- API acceptance does not depend on queue availability.
- The database/queue dual-write gap is replaced by idempotent Outbox publication.
- Publisher and broker ACK uncertainty can produce duplicate messages.
- Queue loss is recoverable from PostgreSQL.
- API, publisher, scheduler, and reconciler are separable logical duties. The MVP
  uses one Worker deployment instance; horizontal Worker scaling requires a new ADR
  for destination-global rate coordination.
- The MVP must operate and test more components than a PostgreSQL-only design.
- PostgreSQL capacity and Outbox backlog require explicit monitoring and
  backpressure.

## Rejected alternatives

- **PostgreSQL polling only:** simpler, but rejected for this approved architecture
  because dispatch isolation and independently scalable Workers are required.
- **Direct database write followed by publish:** rejected because a crash between
  operations can silently strand accepted work.
- **Queue as sole source of truth:** rejected because status, idempotency, retry
  schedule, reconciliation, retention, and audit require queryable durable state.
- **Distributed transaction across database and broker:** rejected because it adds
  coordination dependency without eliminating HTTP-side ambiguity.
