# ADR 0002: At-Least-Once Delivery with Stable Idempotency

- **Status:** Accepted
- **Date:** 2026-07-23

## Context

An HTTP supplier can perform its side effect while the connection fails or while
the service crashes before recording success. The service cannot atomically commit
its PostgreSQL state together with an uncontrolled supplier's business operation.

## Decision

Promise durable acceptance and at-least-once delivery, not exactly-once effects.

- Return `202` only after the delivery and initial Outbox event commit.
- Require a caller-scoped submission idempotency key.
- Compare repeated submissions using a SHA-256 request hash over unambiguously
  length-delimited destination ID, immutable destination version, uppercase method,
  canonical single-valued caller Headers, and raw body bytes. Header names are
  lowercase, trimmed, unique case-insensitively, and sorted; values have surrounding
  optional whitespace removed. Exclude the caller idempotency key, supplier
  idempotency Header, and injected credential.
- Assign a stable supplier idempotency value to each logical delivery and inject it
  according to the destination configuration.
- Do not onboard a supplier into the MVP unless its API exposes a stable
  idempotency mechanism.
- Mark success only after a configured successful HTTP response and a committed
  success transition.
- Perform a read-only eligibility and immutable-destination lookup, then attempt to
  obtain destination rate/concurrency capacity before acquiring a delivery lease.
  When capacity is unavailable, confirm a durable delayed replacement signal
  before ACKing the original. The atomic claim must recheck generation, pending
  state, due time, and retry deadline; a failed claim rolls back its rate
  reservation.
- Use finite Worker leases. An expired lease is reclaimable.
- Fence every Worker result update by delivery ID, generation, `delivering` state,
  lease owner, lease token, and lease validity. A zero-row update means ownership
  was lost and the result must be discarded.
- On a retryable result, atomically advance to the next generation and write
  `next_attempt_at`. An old-generation redelivery is ACKed without sending, and
  every lease acquisition also requires `next_attempt_at <= now`.
- Retry transient failures until the current retry-cycle deadline.
- Represent exhausted or non-retryable outcomes as `failed_permanent`.
- Permit audited manual replay while preserving the supplier idempotency value.

Replay is not a new business event: it keeps the logical delivery ID and supplier
idempotency value, immutable destination version, and original `accepted_at`;
advances the scheduling generation; establishes a new deadline 24 hours from
replay; and appends an operator, reason, and timestamp to the audit record.

## Consequences

- Caller submission retries do not create a second logical task when the request is
  unchanged.
- Duplicate queue messages usually do not create duplicate sends because the
  database generation and lease are checked first.
- Deferring for destination capacity cannot consume lease lifetime or all broker
  credit. If delivery state changes before the final claim, the claim fails, its
  rate reservation is rolled back, and no HTTP connection is made.
- Loss of a Worker ACK after a retryable result cannot trigger an early retry: the
  redelivered message carries the old generation, while the new generation has no
  Outbox signal until `next_attempt_at`.
- An expired Worker cannot overwrite a newer result because its generation or lease
  fencing values no longer match.
- A duplicate HTTP send is still possible after an ambiguous attempt.
- Suppliers without idempotency support are outside the MVP.
- No event ordering is guaranteed.
- “Succeeded” means the supplier returned an accepted response, not that its
  downstream business operation completed.
- A queue dead-letter is not a permanent delivery outcome; only a PostgreSQL
  `failed_permanent` transition has that meaning.

## Rejected alternatives

- **Exactly-once claim:** impossible to verify across an ordinary external HTTP
  boundary without supplier participation in a shared transaction or equivalent
  deduplication protocol.
- **At-most-once:** avoiding retries after ambiguous failures would silently lose
  notifications.
- **Infinite retry:** invalid payloads and revoked credentials would consume
  capacity forever and conceal permanent faults.
