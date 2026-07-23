# Architecture

## Design summary

PostgreSQL is the sole source of truth for delivery state. A message queue carries
durable, replaceable dispatch signals containing identifiers only. Transactional
Outbox bridges the database and queue without a direct dual-write requirement.
All decisions about whether work is pending, leased, successful, retryable, or
permanently failed are made from PostgreSQL state.

## Component boundaries

### API

- Authenticates callers and operators and enforces destination authorization.
- Validates method, headers, body size, and idempotency input.
- Atomically inserts a delivery and initial Outbox event.
- Reads caller-scoped status and performs audited operator replay.
- Does not publish directly to the queue or perform supplier HTTP calls.

### PostgreSQL

Stores deliveries, immutable attempt summaries, Outbox events, and replay audit
records. It enforces caller-scoped idempotency and legal state values. It is the
only component whose committed state determines the API response and task status.

### Outbox Publisher

Claims due unpublished Outbox rows, publishes identifier-only messages using
publisher confirmation, then records publication. A crash after publish confirmation
but before recording it creates a duplicate message, which consumers must tolerate.

### Message queue

Provides durable messages, explicit consumer ACK, redelivery, publisher
confirmation, and a dead-letter path. A message contains only `delivery_id`,
`generation`, and `trace_id`. Queue contents are not authoritative and may be
reconstructed from PostgreSQL.

The queue's dead-letter queue diagnoses message-transport or consumer failures. It
is not the record of business delivery failure. A delivery becomes permanently
failed only through a committed PostgreSQL state transition.

### Worker

Consumes a signal, conditionally leases the matching PostgreSQL delivery, performs
one HTTPS attempt outside the database transaction, records the result, and only
then ACKs the signal. Duplicate, stale, or terminal-state messages are acknowledged
without sending. Each lease has an owner and an unguessable token. Every result
transition is fenced by delivery ID, generation, `delivering` state, lease owner,
lease token, and an unexpired lease; a zero-row update is stale and is discarded.

### Retry Scheduler

After a Worker commits a retryable result, the delivery is pending with a future
`next_attempt_at`, an already-advanced generation, and no signal for that
generation. When that time arrives, the scheduler conditionally creates exactly one
Outbox event for the current generation. It does not advance generation, recover
leases, or infer queue loss.

### Reconciler

The reconciler repairs demonstrable liveness deviations; it does not schedule
ordinary retries:

- for an expired `delivering` lease, a compare-and-set transition returns the task
  to pending, advances its generation, and creates a repair Outbox event;
- for a due current-generation pending task whose existing Outbox was published
  but produced no lease within a bounded dispatch watchdog interval, and which has
  no unpublished signal, it makes that same Outbox eligible for republishing
  without advancing business state.

Unique `(delivery_id, generation)` records and conditional updates make concurrent
scheduler/reconciler runs idempotent. A signal can never advance a delivery unless
its generation equals the current PostgreSQL generation.

Publisher, Scheduler, and Reconciler are logical responsibilities, not prescribed
microservices or deployment units. The MVP should prefer independent loops in the
same codebase/process unless measurements require separate scaling.

### Destination registry and secret provider

Every registry version is immutable and defines URL, network policy, methods,
caller-settable headers, credential reference, supplier idempotency mapping,
response classification, timeout, rate, and concurrency. Each delivery binds its
acceptance-time `destination_version`; replay retains it. The secret provider
resolves that version's credential reference at each send, so rotation changes the
secret value without mutating the version. Secrets never enter task rows or queue
messages.

### Observability and maintenance

Metrics and redacted structured logs expose state transitions and failure classes.
Maintenance removes only terminal records after their retention period and applies
admission backpressure when configured backlog limits are reached.

Admission serializes the idempotency lookup and active-count decision in the
submission transaction. The default global limit is 100,000 rows whose state is
`pending` or `delivering`. The value is configurable and does not imply a latency
SLO. A new request at capacity receives `503 backlog_capacity_exceeded` with
`Retry-After: 60`; lookup of an already accepted idempotency key occurs before the
capacity decision.

## State model

The persisted states are:

- `pending`: eligible now or scheduled by `next_attempt_at`;
- `delivering`: held by a Worker until `lease_until`;
- `succeeded`: terminal success;
- `failed_permanent`: terminal automatic failure.

Allowed transitions:

```text
new transaction -> pending
pending         -> delivering
delivering      -> succeeded
delivering      -> pending             (retryable result)
delivering      -> failed_permanent    (permanent result or retry expiry)
delivering      -> pending             (lease expiry reconciliation)
failed_permanent -> pending            (audited manual replay)
```

There is no transition that marks success before an HTTP attempt. Success and
permanent failure are terminal for automatic processing.

Generation has one meaning:

- acceptance creates generation `0` and its immediately available Outbox event;
- a fenced retryable result atomically changes generation `g` to `g+1`, records
  `next_attempt_at`, and creates no Outbox event;
- the Retry Scheduler creates the current-generation Outbox only when due, without
  changing generation;
- expired-lease recovery advances generation once and creates an immediate repair
  Outbox, invalidating the old Worker and old messages;
- audited replay advances generation once and creates an immediate Outbox with a
  fresh 24-hour retry-cycle deadline.

No other operation advances generation.

## Data flows

### Submission

1. Authenticate and authorize the caller.
2. Resolve and bind the current immutable destination version.
3. Compute the request hash from unambiguously length-delimited
   `destination_id`, `destination_version`, uppercase method, canonical caller
   headers, and raw body bytes. Canonical headers are single-valued caller headers
   with lowercase trimmed names and surrounding optional whitespace removed from
   values, sorted by name; names must be unique case-insensitively. Exclude the
   caller idempotency key, supplier idempotency Header, and injected credential.
4. Begin a PostgreSQL transaction.
5. Insert the delivery under `(caller_id, idempotency_key)`, preserving raw body
   bytes, `accepted_at`, generation `0`, and the initial 24-hour retry deadline.
6. Insert generation `0` Outbox event.
7. Commit.
8. Return `202`. If commit is unknown or failed, return failure; the caller retries
   with the same idempotency key.

### Publish

1. Claim an available Outbox row with a bounded publisher lease.
2. Publish the identifier-only message as durable.
3. Wait for broker confirmation.
4. Record `published_at`. On any uncertainty, leave the row eligible for retry.

### Attempt

1. Consume a message without ACK.
2. Atomically change the matching current generation from `pending` to
   `delivering` only when `next_attempt_at <= now`, assigning a finite lease owner,
   unique lease token, and lease deadline.
3. If the message is stale, duplicate, terminal, or early, ACK without connecting.
4. Load the delivery's immutable destination version, resolve its current secret,
   enforce protected headers and destination-global rate/concurrency limits,
   validate DNS/IP policy, and bind the validated IP to the actual TLS connection
   with hostname verification and redirects disabled.
5. Send the HTTPS request outside a database transaction.
6. Commit the attempt plus success, permanent failure, or retryable result using
   the complete fencing predicate. A retryable result advances generation and
   records `next_attempt_at`; if the next attempt would exceed the cycle deadline,
   commit `failed_permanent` instead.
7. If the fenced update affects zero rows, discard the stale result.
8. ACK only after a committed result or a definitive stale-message/result
   decision. Database uncertainty is not a reason to ACK.

### Recovery

- The Retry Scheduler creates an Outbox event for the already-current generation
  only when `next_attempt_at <= now`; it never advances generation.
- The Reconciler returns an expired `delivering` lease to `pending` with a new
  generation and repair event.
- A current-generation signal that has produced no lease by the dispatch watchdog
  deadline may be republished without changing generation; duplicates are safe.
- Queue unavailability accumulates Outbox rows but does not invalidate API
  acceptance. Backpressure protects PostgreSQL from unbounded accumulation.

## Core invariants

1. `202` implies the delivery and initial Outbox event committed atomically.
2. No pre-send path can transition a delivery to `succeeded`.
3. Every `delivering` state has a finite lease and is reclaimable after expiry.
4. Duplicate queue messages are harmless, but duplicate HTTP sends remain possible.
5. Retryable and permanent failures have distinct state transitions and metrics.
6. Sensitive headers, credentials, and bodies never enter logs or queue messages.
7. Destination policy is checked before any DNS resolution or socket connection;
   the actual dialed IP must also pass the network policy.
8. Only a message whose generation equals the delivery's current generation may
   acquire a lease, and `next_attempt_at` must already be due; stale or early
   signals are ACKed without changing state.
9. Success, retryable failure, and permanent failure commits require matching
   delivery ID, generation, `delivering` state, lease owner, lease token, and
   unexpired lease. A stale Worker cannot overwrite a newer generation.
10. A retryable result advances generation before the old message is ACKed. The new
    generation has no Outbox signal before `next_attempt_at`.
11. A delivery uses its immutable acceptance-time destination version for every
    attempt and replay; only the referenced secret value is dynamically resolved.

## Failure model

| Failure point | Observable outcome | Recovery and residual risk |
|---|---|---|
| API before database commit | No `202`; no accepted task | Caller retries with the same idempotency key |
| API after commit before response | Caller sees uncertainty | Retry returns the original task |
| Queue unavailable | Outbox backlog grows | Publisher resumes later; admission backpressure limits growth |
| Publisher after publish, before marking | Duplicate queue message | Database generation/state check suppresses duplicate work where possible |
| Message lost after publication | Current generation never obtains a lease before the watchdog deadline | Reconciler republishes the same generation; PostgreSQL state does not advance |
| Worker before HTTP connection | Lease eventually expires | Task is reclaimed |
| Worker during HTTP or after supplier effect | Result is unknown | Lease expiry causes retry; duplicate supplier effect is possible |
| Old message redelivered after retry result | Message generation is lower than current generation | ACK without a lease or connection; cannot bypass `next_attempt_at` |
| Expired Worker returns after task was reclaimed | Fenced result update affects zero rows | Discard result; newer generation remains unchanged |
| Worker after success commit, before ACK | Queue redelivers | Terminal-state check ACKs without another send |
| Database unavailable during result commit | HTTP result cannot be recorded | Message/lease recovery retries; duplicate send is possible |
| Permanent supplier rejection | Terminal failure | Alert, inspect, then audited manual replay |
| Reconciler outage | Recovery is delayed | Alert on expired leases and oldest pending age |

## Trust and network boundaries

Callers are trusted only for their authorized destination and allowlisted request
fields. Immutable destination versions are privileged. Supplier networks are
untrusted. Before the first and every subsequent attempt, the service validates the
bound HTTPS URL, rejects userinfo and redirects, protects sensitive and hop-by-hop
headers, resolves the hostname, rejects loopback/private/link-local/multicast/
unspecified and metadata addresses, and ensures the HTTP transport dials only a
validated address while retaining the original TLS server name. Production must
also enforce the same policy at an egress proxy or firewall.

The fake HTTPS supplier is reachable only through an explicit test-only policy
that names the Docker test endpoint/network. That policy is unavailable to
production configuration and cannot act as a generic private-network bypass.

## Deployment and capacity constraints

The component names describe responsibility boundaries, not a required
microservice topology. A single MVP binary/process may host the API plus independent
Publisher, Scheduler, and Reconciler loops. The MVP has exactly one Worker
deployment instance, allowing a process-local limiter to enforce the semantic
destination-global rate and concurrency limits. Horizontal Worker scaling is
forbidden until a new ADR selects and validates a shared coordination mechanism;
Redis is not implicitly selected. PostgreSQL and RabbitMQ remain separate durable
dependencies.

First-attempt latency is capacity-dependent and remains a proposed SLO. It may be
measured only while PostgreSQL, the queue, and Workers are healthy; admission
backpressure is inactive; the supplier is responsive; and arrival rate plus
existing backlog fit the configured destination rate/concurrency. With the default
one request per second, 60 queued requests for one destination are already
incompatible with an unconditional `p99 <= 60 seconds` claim.

The MVP rejects onboarding a destination without a stable supplier idempotency
mechanism. Production secret management, production egress enforcement, HA,
backup, and RTO/RPO design remain outside the local MVP.

The reproducible local Slice 8 profile offers 100 submissions/second to a
250 requests/second, 8-concurrent destination with healthy dependencies and no
existing backlog. The latest measurement is recorded in
`docs/capacity-report.md`. It is evidence that the proposed 60-second p99 is
supportable in that profile, not an unconditional or approved SLO.
