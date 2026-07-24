# Local MVP Operations

## Start and inspect

```sh
make up
curl --fail http://localhost:18080/healthz
curl --fail http://localhost:18080/readyz
curl --fail http://localhost:18080/metrics
```

`healthz` is process liveness. `readyz` requires PostgreSQL because the API must
not return `202` without a database commit. RabbitMQ failure does not make the API
unready: accepted work accumulates durably in Outbox, subject to admission
backpressure.

## Recovery signals

- Rising `notifier_oldest_outbox_seconds` with RabbitMQ errors: restore RabbitMQ
  connectivity and confirm the metric falls. Do not reconstruct tasks from the
  queue; PostgreSQL is authoritative.
- `notifier_rabbitmq_up == 0`: queue depth is reported as unavailable (`NaN`), but
  PostgreSQL-authoritative backlog, lease, result, and permanent-failure metrics
  remain available.
- Rising `notifier_oldest_pending_seconds`: check Worker health, destination
  rate/concurrency, supplier latency, and retry timing before increasing capacity.
- Non-zero `notifier_expired_leases`: confirm the Reconciler is running. Recovery
  may produce duplicate HTTP sends and must retain the supplier idempotency value.
- Rising `notifier_permanent_failures`: inspect the bounded attempt category and
  destination configuration. RabbitMQ DLQ entries are not business terminal state.

Destination rate and concurrency are process-local and keyed by Destination ID. A stricter immutable version takes effect when the Worker observes it; relaxing either value requires restarting the Worker so the limiter can be rebuilt from current configuration.

## Safe local restart

`docker compose restart app`, `rabbitmq`, or `postgres` preserves the named local
volumes. PostgreSQL 18's named volume is mounted at its version-aware parent data
directory, and the gate also removes/recreates the container to verify the data
does not live only in an anonymous volume. After a PostgreSQL restart, wait for
`/readyz`; after RabbitMQ recovery, watch Outbox age and queue depth. `make down`
keeps development volumes. Automated verification uses separate disposable
projects and does not touch this environment.

## Audited replay

Replay only a `failed_permanent` delivery with an operator credential and a
non-empty reason:

```sh
curl --fail-with-body \
  -H 'Authorization: Bearer <operator-api-key>' \
  -H 'Content-Type: application/json' \
  --data '{"reason":"supplier configuration corrected"}' \
  -X POST http://localhost:18080/deliveries/<delivery-id>/replay
```

Replay retains the logical delivery ID, acceptance time, bound destination
version, and supplier idempotency value; it advances generation and starts a new
24-hour retry cycle. Repeating a completed replay request returns a conflict rather
than creating another business event.

## Durability boundary

The local MVP verifies process/container restart recovery while PostgreSQL and
RabbitMQ volumes survive. Host loss, volume corruption, backup/restore, HA, RPO,
and RTO remain production decisions.
