# Product Specification

## Purpose

The service accepts outbound HTTP(S) notifications from authorized internal
systems and attempts to deliver them to pre-registered external suppliers. The
caller does not wait for the supplier response. The service provides durable
acceptance, asynchronous at-least-once delivery, status visibility, and controlled
manual replay.

## Actors

- **Caller:** an internal system authenticated with its own credential and
  authorized for a defined set of destinations.
- **Operator:** an authorized person who investigates permanent failures and may
  replay them with an audit reason.
- **Destination owner:** the person responsible for a supplier's URL, allowed
  request shape, credentials, success codes, retry codes, timeout, and rate limit.

## Required behavior

### Submit a delivery

- A caller submits a destination ID, caller-scoped idempotency key, allowed HTTP
  method and headers, and the final request body. The delivery binds the immutable
  destination version current at acceptance.
- The service authenticates the caller, verifies destination authorization,
  validates the request, and enforces a combined header/body limit of 256 KiB.
- The service creates the delivery and its initial Outbox event in one PostgreSQL
  transaction.
- **Only a successful transaction commit may produce `202 Accepted`.** A queue
  outage does not invalidate an already committed submission.
- Repeating `(caller_id, idempotency_key)` with the same normalized request
  returns the original delivery. Reusing it with different content returns a
  conflict and does not change the original.
- The idempotency request hash covers, using unambiguous length-delimited encoding:
  `destination_id`, `destination_version`, uppercase method, canonical caller
  headers, and raw body bytes. Caller header names are lowercased and trimmed,
  values have surrounding optional whitespace removed, and entries are sorted by
  header name. Caller Header names must be unique case-insensitively. The caller
  idempotency key, supplier idempotency Header, and injected credential are
  excluded.

### Deliver

- The service sends only to a pre-registered HTTPS destination.
- URL, network policy, allowed methods and headers, response rules, timeout, and
  secret reference come from the delivery's immutable `destination_version`.
  Secret values are resolved dynamically at attempt time.
- Credentials and the stable supplier idempotency value are injected from the
  destination configuration; callers cannot override them.
- A response is successful when it matches the destination success rule, defaulting
  to any `2xx`.
- **A task must not be marked successful before a qualifying supplier response is
  observed and the success transition is committed.**
- No ordering is promised between deliveries.
- Delivery is at least once. **The same task may be sent more than once**, notably
  when the supplier may have processed a request but the local success transaction
  did not commit.

### Retry and terminal failure

- Network errors, timeouts, `408`, `425`, `429`, and configured `5xx` responses are
  retryable by default. Other `4xx` responses are permanent by default.
- Retryable failures return the delivery to a scheduled pending state using bounded
  exponential backoff with jitter; a bounded `Retry-After` takes precedence. The
  result transaction immediately advances generation and records
  `next_attempt_at`; no signal for that generation is published before it is due.
- Worker result commits are accepted only while delivery ID, generation,
  `delivering` state, lease owner, lease token, and unexpired lease all match.
  A stale Worker whose fenced update affects zero rows discards its result.
- Automatic retries stop at the current retry cycle's deadline, initially 24 hours
  after acceptance.
- **Retryable failure and permanent failure are separate, queryable outcomes.**
  Exhausting the retry window produces `failed_permanent`, not silent deletion.
- A Worker owns a delivery only for a finite lease. **After a Worker crash, an
  expired lease makes the task eligible to be reclaimed.**

### Inspect and replay

- A caller can inspect only its own delivery status and the 20 most recent attempt
  summaries, newest first. Each summary is limited to generation, result class,
  response status, bounded error category, start time, and finish time; it excludes
  request headers and body, credentials, supplier idempotency value, and lease
  metadata.
- An operator may replay a permanently failed delivery with an auditable actor and
  reason. Replay preserves the logical delivery ID, original `accepted_at`,
  destination version, and stable supplier idempotency value; advances generation;
  creates a fresh retry deadline 24 hours after replay; and records operator,
  reason, and timestamp. Replay is not a new business event.
- A successful delivery is retained for 7 days; a permanently failed delivery is
  retained for 30 days. Pending and delivering tasks are never removed by retention
  cleanup.

### Security and observability

- Each caller has an independent credential and destination allowlist.
- Destination versions are immutable. Updating a destination creates a new version
  for future submissions; accepted deliveries continue to use their bound version.
- Logs and metrics contain identifiers, state, result class, status code, latency,
  and bounded error categories only.
- **Logs must not contain sensitive headers**, credentials, or request bodies.
- **No network connection may be initiated to an unapproved target address.**
- Operators can observe submission rate, delivery results, oldest pending age,
  Outbox age, queue depth, expired leases, and permanent failures.

## Reliability contract

The service guarantees durable acceptance after `202`, subject to the durability
of the configured PostgreSQL deployment. It attempts delivery until success or the
current 24-hour retry-cycle deadline. It does not guarantee that a supplier
performed its business operation, and it does not guarantee exactly-once effects.

For the local Docker MVP, process and container restarts must recover accepted
tasks. Simultaneous loss of the host and its PostgreSQL volume is outside the
durability guarantee.

## Decision classification

### Approved

- Go 1.26.x, PostgreSQL, RabbitMQ, and local Docker Compose are the approved base
  stack.
- PostgreSQL is the sole source of truth, bridged to a rebuildable message-queue
  signal layer through Transactional Outbox.
- Acceptance is durable only after the database commit; delivery is at least once.
- Targets and credentials are pre-registered, caller access is scoped, and
  supplier idempotency is stable across retries and replay.
- Each automatic retry cycle lasts at most 24 hours; permanent failures remain
  visible, and audited replay starts a new 24-hour cycle without changing the
  original acceptance time or logical identity.
- Payload size is capped at 256 KiB; successful and permanently failed records use
  7-day and 30-day retention respectively.
- The global active backlog limit defaults to 100,000 and counts `pending` plus
  `delivering` deliveries. It is configurable but is not an SLO. At capacity,
  `POST /deliveries` returns `503`, error code `backlog_capacity_exceeded`, and
  `Retry-After: 60`; an idempotent retry of an already accepted request still
  returns the original delivery.
- The MVP onboards only suppliers with a stable idempotency mechanism.
- ADR 0004 selects standard-library HTTP/testing, pgx, amqp091-go, golang-migrate,
  and Prometheus client on the approved base stack.
- ADR 0005 requires test-first red–green–refactor execution for every business
  slice.

### MVP defaults

- A destination defaults to one concurrent request and one request per second.
- This is a destination-global semantic limit, but the MVP runs exactly one Worker
  deployment instance and may therefore enforce it with an in-process limiter.
  Horizontal Worker scaling requires a new ADR selecting shared coordination;
  Redis is not part of the MVP.
- Success defaults to any `2xx`; network errors, timeouts, `408`, `425`, `429`, and
  configured `5xx` responses default to retryable.
- Connect and total request timeouts default to 3 and 15 seconds, with a
  destination-configured total timeout capped at 60 seconds.
- The Publisher, Retry Scheduler, and Reconciler are logical responsibilities. The
  MVP may run them as independent loops in one codebase and process.

### Unresolved

- The production secret manager, egress-control product, high-availability
  topology, backup policy, and recovery objectives.
- The production capacity model and approval of a first-attempt latency SLO. The
  local Slice 8 profile is measured in `docs/capacity-report.md` and supports the
  proposed target only under its declared conditions.

The proposed first-attempt target of `p99 <= 60 seconds` is not yet an unconditional
guarantee. It can apply only while dependencies are healthy, admission backpressure
has not triggered, the destination is not throttling the work, and offered load is
within the declared per-destination capacity. At the default one request per
second, a single-destination backlog of 60 or more already invalidates that target.

## In scope

- Submission, idempotency, status lookup, and authorized replay APIs.
- PostgreSQL task state and Transactional Outbox.
- Durable message-queue dispatch with explicit acknowledgement and dead lettering.
- Outbox publishing, HTTP Workers, retry scheduling, reconciliation, retention,
  rate limiting, audit, metrics, and structured redacted logs.
- Local Docker environment and a fake HTTPS supplier for integration testing.

## Non-goals

- Exactly-once supplier-side effects.
- Arbitrary URL proxying or caller-provided credentials.
- Understanding or transforming domain events into supplier schemas.
- Supplier-specific code adapters, ordering, priorities, or callbacks to callers.
- Atomicity between an upstream business transaction and submission to this
  service; callers needing that property must use their own transactional Outbox.
- Multi-region operation, host-loss recovery, or production high availability in
  the local MVP.
