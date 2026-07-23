# Local MVP Capacity and Resilience Report

## Scope

This report records local acceptance evidence, not a production capacity promise.
The reproducible command is:

```sh
make capacity
```

The test creates an isolated Docker Compose project with disposable PostgreSQL and
RabbitMQ volumes and Docker-assigned host ports.

## Measured profile

- Date: 2026-07-24
- Application topology: one container hosting the API, Publisher, Worker, Retry
  Scheduler, Reconciler, retention, and alert loops
- Dependencies: one PostgreSQL container, one RabbitMQ container, and a responsive
  fake HTTPS supplier
- Destination policy: 250 requests/second, maximum concurrency 8
- Offered submission rate: 99.9 requests/second for 200 deliveries
- Admission backpressure: inactive; configured active backlog limit 100,000
- Existing destination backlog: zero
- Supplier response: immediate HTTPS `204`

Observed result:

| Measure | Result |
|---|---:|
| Offered | 200 |
| Accepted with `202` | 200 |
| Persisted in PostgreSQL | 200 |
| Eventually succeeded | 200 |
| First-attempt p99 | 0.277 seconds |
| First-attempt maximum | 0.279 seconds |

No accepted task was silently lost. The proposed first-attempt `p99 <= 60 seconds`
is supportable in this declared local profile. It remains unapproved as a general
SLO and must not be extrapolated to the default 1 request/second destination,
non-empty backlogs, supplier throttling, dependency degradation, horizontal
Workers, or production infrastructure.

## Crash-window evidence

| Failure window | Executable evidence | Observed recovery |
|---|---|---|
| API transaction fails before commit | Slice 1 integration trigger | No `202`; neither delivery nor Outbox row remains |
| API response is uncertain after commit | Slice 1 idempotency tests | Same caller key and content return the persisted logical delivery |
| RabbitMQ unavailable after acceptance | Slice 2 and `make capacity` | API still returns `202`; Outbox remains authoritative and publishes after recovery |
| Publisher confirms before PostgreSQL mark | Slice 2 injected mark failure | Duplicate identifier-only signal; one logical delivery |
| Dispatch signal is ineffective or lost | Slice 7 concurrent watchdog test | Same generation is republished without state advancement |
| Worker exits with a live lease | Slices 4 and 7 lease tests | Expired lease advances generation once and becomes reclaimable |
| Supplier result is ambiguous | Slice 4 ambiguous-result fixture | Stable supplier idempotency value is reused; duplicate HTTP effect remains possible |
| Old signal or expired Worker returns | Slices 3 and 4 fencing tests | Conditional update affects zero rows; stale result is discarded |
| Success commits before ACK | Slice 3 terminal-signal test and RabbitMQ redelivery probe | Redelivery is ACKed without another state transition |
| API/Publisher/Worker/Scheduler/Reconciler process restarts | `make capacity` app restart | Accepted task remains in PostgreSQL and reaches success |
| RabbitMQ restarts | `make verify-gate` and `make capacity` | Durable signals survive or are rebuilt from Outbox |
| PostgreSQL restarts with its volume | `make verify-gate` and `make capacity` | Persisted task remains and processing resumes after readiness returns |

The local test does not cover host/volume loss, multi-node failover, production
egress controls, backup restoration, or regional disaster recovery.
