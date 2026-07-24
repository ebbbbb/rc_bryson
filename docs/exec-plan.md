# Execution Plan

Implement in the following vertical slices. Each slice must leave the repository
runnable, include its own observable behavior and failure test, and pass all prior
slice tests. A slice is not complete when only a schema or internal abstraction
exists.

## Toolchain Gate — Approved stack verification

Before business implementation, establish the toolchain selected by ADR 0004:
Go 1.26.x with standard-library HTTP routing/testing, pgx, amqp091-go,
golang-migrate, Prometheus client, PostgreSQL, RabbitMQ, and local Docker Compose.
Pin reviewed module/tool versions and checksums. This gate validates compatibility;
it does not reopen the approved base stack.

Executable evidence:

- A disposable compatibility probe demonstrates broker publish confirmation,
  consumer redelivery after missing ACK, and durable-message survival across a
  broker restart.
- A networking probe demonstrates that the chosen HTTP client can dial a previously
  validated IP while verifying TLS against the registered hostname. The probe uses
  only the explicit test-only Docker policy, makes zero connections for forbidden
  fixtures, and is not a service delivery path.

Stop condition: no business code starts until the ADR is approved and both probes
pass. The gate must expose `make verify-gate` as its reproducible evidence command.
Each automated gate uses a unique Compose project, random host ports, and
project-scoped volumes, then removes those resources. `make integration` is a
separate isolated service-integration check, not an alias for the restart and
protocol capability gate.

## Required test-first workflow

For every business slice (1–8), first add its targeted acceptance/failure test and
run it to prove a failure caused by the missing behavior. Only then add the minimum
implementation, make the target green, refactor, and run all prior regression
tests. Store the red command/reason and green evidence with the slice. Coverage may
guide review but cannot replace the named fault scenarios.

## Slice 0 — Runtime contract and local dependencies

Using only ADR 0004's stack, deliver a minimal service process, migration mechanism,
local PostgreSQL, RabbitMQ, and fake HTTPS supplier through Docker. The supplier is
reachable only through an explicit test-only network policy naming the Docker test
endpoint/network; production configuration cannot load that policy.

Independent verification:

- `make up` starts healthy dependencies and an empty service.
- Restarting each container preserves PostgreSQL data and durable queue state.
- `make lint`, `make test`, `make integration`, and `make verify` are stable entry
  points.

Stop condition: all commands run from a clean checkout and dependency restarts
preserve their documented durable state. Evidence command:
`make verify-slice SLICE=0`.

`make source-export-check` is only a source-export reproducibility smoke test; it
does not claim to replace the clean-checkout condition above.

## Slice 1 — Durable submission and caller idempotency

Implement caller authentication, destination authorization, request validation,
immutable destination versions, the delivery state record, initial Outbox record,
status lookup, and backlog admission control. Bind `destination_version` at
acceptance. The insert and Outbox event must share one transaction.

Independent verification:

- A committed submission returns `202` and remains queryable after restart.
- Forced commit failure never returns `202`.
- Same key and request returns the same ID; same key with different content
  conflicts under concurrent submissions.
- Request-hash fixtures prove unambiguous length-delimited hashing of destination
  ID/version, uppercase method, canonical sorted caller headers, and raw body
  bytes, while excluding all three idempotency/credential fields.
- Unauthorized destinations and payloads over 256 KiB are rejected.
- The configurable global active backlog defaults to 100,000 `pending` plus
  `delivering` rows. At capacity a new submission returns `503`,
  `backlog_capacity_exceeded`, and `Retry-After: 60`, while an idempotent retry
  still resolves the original delivery.

Stop condition: the acceptance cases pass against a real PostgreSQL instance,
including forced commit failure and concurrent idempotency races. Evidence command:
`make verify-slice SLICE=1`.

## Slice 2 — Outbox-to-queue dispatch

Implement leased Outbox publication, durable identifier-only messages, publisher
confirmation, publication retry, and queue topology including dead lettering.

Independent verification:

- Submissions succeed while the queue is stopped and publish after it restarts.
- A crash after broker confirmation but before `published_at` creates a duplicate
  signal without a second logical delivery.
- Inspection of normal and dead-letter messages finds no body, credential, or
  sensitive header.

Stop condition: queue-outage and publish-ambiguity tests pass without a lost
accepted task or leaked payload. Evidence command: `make verify-slice SLICE=2`.

## Slice 3 — Secure first HTTPS delivery

Before enabling any service outbound connection, implement message consumption,
generation/due checks, atomic Worker leases, immutable destination-version lookup,
registered targets, disabled redirects, protected Headers, DNS/IP validation,
validated-IP dialing, TLS hostname verification, dynamic secret resolution,
success classification, fenced result recording, and ACK-after-commit.

Independent verification:

- The fake supplier receives the exact allowed method, headers, body, and stable
  idempotency value.
- Only the explicit test-only policy permits the fake supplier's Docker test
  address; the same address is rejected under production policy.
- URL policy, DNS/IP validation, and validated-IP dialing complete before the first
  socket connection, and TLS verifies the registered hostname.
- Redirects and caller overrides of authentication, idempotency, Host,
  hop-by-hop, cookie, or framing Headers are rejected without a follow-up
  connection.
- The task is not `succeeded` before the supplier returns a configured success.
- A duplicate or stale message for a succeeded task is ACKed without another
  network request.
- A success result with mismatched generation, state, lease owner, lease token, or
  expired lease affects zero rows and is discarded.
- Credential injection works while logs, messages, database request metadata, and
  API output remain redacted.

Stop condition: the success path and duplicate/stale-signal cases pass against the
fake HTTPS supplier, every forbidden precondition records zero network connections,
and ACK is observed only after the fenced database commit. Evidence command:
`make verify-slice SLICE=3`.

## Slice 4 — Retry, timeout, lease recovery, and rate isolation

Implement response classification, bounded `Retry-After`, exponential backoff with
jitter, finite leases, retry scheduling, destination-global rate/concurrency
limits, and the 24-hour automatic retry deadline. The MVP has one Worker deployment
instance and uses an in-process limiter; it does not introduce Redis.

Independent verification:

- Network errors, timeouts, configured retry statuses, and permanent `4xx` produce
  distinct persisted outcomes.
- A fenced retryable result atomically advances generation and records
  `next_attempt_at` without creating that generation's Outbox event.
- After a simulated ACK loss, redelivery of the old-generation message is ACKed
  without a lease or HTTP connection. Even a current-generation signal cannot
  acquire before `next_attempt_at`.
- At due time, the Retry Scheduler creates exactly one Outbox event for the current
  generation and does not advance generation.
- Killing a Worker before and during an attempt allows reclaim only after lease
  expiry.
- Every success, retryable, and permanent result with a stale generation, owner, or
  lease token affects zero rows and cannot overwrite reclaimed work.
- An ambiguous attempt may resend with the same supplier idempotency value.
- A Worker waiting for destination capacity holds no delivery lease. After capacity
  is granted, its atomic claim rechecks generation, pending state, due time, and
  retry deadline before any HTTP connection.
- Eight or more signals for a throttled destination release broker credit through
  confirmed durable deferral, so a following healthy destination can proceed.

Stop condition: deterministic clock-driven tests prove retry timing and expiry,
ACK-loss cannot bypass `next_attempt_at`, every stale Worker result is fenced out,
limiter wait time does not consume lease lifetime, and a killed Worker is reclaimed
without an early concurrent lease. Evidence command: `make verify-slice SLICE=4`.

## Slice 5 — Permanent failure and audited replay

Implement terminal failure visibility, operator authorization, replay reason
validation, audit records, and replay using a new scheduling generation while
preserving the logical delivery ID, original `accepted_at`, immutable destination
version, and supplier idempotency value. Replay starts a new retry cycle whose
deadline is exactly 24 hours after replay.

Independent verification:

- Non-retryable and retry-expired tasks become `failed_permanent`.
- Automatic processing does not revive terminal tasks.
- Unauthorized replay fails; authorized replay records actor and reason.
- Replay records operator, reason, and timestamp, advances generation once, and
  creates the current generation's immediate Outbox event.
- Replay creates a new attempt path and deadline without changing the original
  logical identity, `accepted_at`, destination version, or supplier idempotency.

Stop condition: API, database, and audit assertions jointly prove that replay
preserves all required identity fields, advances generation once, sets the
executable 24-hour deadline, and is not a new event. Evidence command:
`make verify-slice SLICE=5`.

## Slice 6 — Adversarial outbound-security hardening

Do not introduce the baseline SSRF boundary here; Slice 3 already requires it
before the first connection. Add adversarial coverage and hardening for resolver
races, mixed DNS answers, IPv4/IPv6 edge cases, redirect variants, Header
smuggling, TLS failures, secret rotation, and log/error redaction.

Independent verification:

- Literal and DNS-resolved loopback, private, link-local, metadata, multicast, and
  unspecified destinations never receive a connection.
- DNS rebinding between validation and connection is ineffective because the
  validated address is the address dialed.
- Redirects and caller overrides of protected headers are rejected.
- Secret rotation changes the dynamically resolved value without mutating the
  delivery's bound destination version.
- The test-only Docker allowance cannot be loaded or expressed through production
  configuration.
- Automated log capture confirms sensitive headers and request bodies are absent.

Stop condition: every forbidden-address and DNS-rebinding fixture records zero
socket connections, production policy cannot enable the test allowance, and the
complete log fixture contains no seeded secret. Evidence command:
`make verify-slice SLICE=6`.

## Slice 7 — Reconciliation, maintenance, and observability

Implement reconciliation for expired leases and missing/old signals, terminal
retention cleanup, structured metrics, health/readiness checks, and actionable
backlog alerts.

Independent verification:

- Removing a queued signal does not strand the task; the running
  Reconciler/Publisher/Worker path recreates and completes it at the same
  generation.
- Reconciliation is idempotent under concurrent instances.
- Ordinary due retries are emitted only by the Retry Scheduler. Expired-lease
  repair advances generation once; missing-signal repair republishes the current
  generation without advancing it.
- Pending tasks are never removed; successful and permanently failed tasks follow
  7-day and 30-day retention.
- Metrics expose submission outcomes, oldest pending age, Outbox age, expired
  leases, queue availability/depth, result classes, and permanent failures without
  high-cardinality secrets or payloads. Database-authoritative metrics remain
  available when RabbitMQ is down.

Stop condition: deleting a signal and expiring a lease are both repaired exactly
as documented under two concurrent reconcilers, while an ordinary future retry is
handled only by the Retry Scheduler and no operation introduces a second
generation interpretation. Evidence command:
`make verify-slice SLICE=7`.

## Slice 8 — Resilience and capacity acceptance

Exercise the complete system under dependency failure and the assumed load.
Document measured limits and operational recovery steps.

Independent verification:

- At 100 submissions per second, no accepted task is silently lost.
- Measure first-attempt latency only under a declared capacity profile with healthy
  dependencies, inactive backpressure, no supplier throttling, and per-destination
  offered load below configured service rate. Report whether the proposed
  `p99 <= 60 seconds` is supportable; do not treat it as approved beforehand.
- Restart API, publisher, queue, Workers, scheduler, reconciler, and PostgreSQL at
  each defined crash window and verify the documented failure model.
- `make verify` passes from a clean checkout, and the product spec, architecture,
  ADRs, and operator instructions match observed behavior.

Stop condition: every crash-window test has an observed result matching the failure
model, no accepted task is silently lost, and the measured capacity report either
justifies a separately approved SLO or leaves it explicitly unresolved. Evidence
command: `make verify-slice SLICE=8`.

The latest local evidence profile and crash-window mapping are recorded in
`docs/capacity-report.md`; operational recovery steps are in `docs/operations.md`.
