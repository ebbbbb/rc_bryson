# ADR 0004: Go, PostgreSQL, and RabbitMQ Toolchain

- **Status:** Accepted
- **Date:** 2026-07-23

## Context

The architectural contracts require a small HTTP API, PostgreSQL transactions and
leases, RabbitMQ publisher confirmation and manual acknowledgement, migrations,
safe custom HTTPS dialing, deterministic tests, and operational metrics. The base
stack is approved; this ADR fixes the library-level choices needed before business
implementation.

## Approved base stack

- Go 1.26.x; the current development machine uses Go 1.26.2.
- PostgreSQL as the only source of truth.
- RabbitMQ as the rebuildable identifier-only dispatch-signal layer.
- Local Docker Compose for development and integration execution.

## Decision

Use the following minimal toolchain:

- **HTTP routing and server:** Go standard library `net/http` with `ServeMux`.
  Go 1.26 provides the required method/path routing, middleware composition,
  deadlines, and graceful shutdown; a third-party router has no current
  indispensable use.
- **JSON, hashing, TLS, dialing, logging, and unit tests:** standard library
  `encoding/json`, `crypto/sha256`, `crypto/tls`, `net`, `log/slog`, `testing`, and
  `httptest`. These cover the current requirements without wrapper dependencies.
- **PostgreSQL client:** `github.com/jackc/pgx/v5`, including `pgxpool`. A database
  driver and production connection pool are indispensable; pgx exposes
  PostgreSQL-specific transactions, error codes, batch operations, and cancellation
  without an ORM hiding fencing predicates.
- **RabbitMQ client:** `github.com/rabbitmq/amqp091-go`. An AMQP 0-9-1 client is
  indispensable for durable topology, publisher confirms, manual ACK/NACK,
  redelivery metadata, QoS, and dead-letter routing. Application code owns
  reconnect and topology re-declaration because the client does not make delivery
  recovery decisions.
- **Migration tool:** `github.com/golang-migrate/migrate/v4`, pinned as a project
  tool and PostgreSQL driver. Ordered schema versions, locking, and dirty-version
  detection are indispensable once schema changes exist; migrations do not run
  implicitly from every service replica.
- **Metrics:** `github.com/prometheus/client_golang`. Prometheus-compatible
  counters, gauges, histograms, and exposition are required by the observability
  contract and are not supplied by the Go standard library.
- **Integration orchestration:** Docker Compose plus Go's standard test tooling.
  Do not add Testcontainers, assertion frameworks, mocking frameworks, or a
  separate test runner until a concrete test cannot be expressed clearly with
  these facilities.

All external modules and tool binaries must be pinned to reviewed versions with
checksums during the toolchain stage. Do not add an ORM: explicit SQL and checked
affected-row counts are required for generation and lease fencing.

Local development and compatibility probes may use host Go 1.26.x, Shell, `jq`,
and `curl`; `make preflight` checks those tools plus Docker before integration
work. Formatting, `go vet`, module verification, and the RabbitMQ probe therefore
have an explicit host-tool boundary. Unit and race tests use the digest-pinned Go
1.26.2 container by default to keep their compiler environment deterministic.
The edit hook uses local Go 1.26.x `gofmt` only after a successful edit event and
reports formatter failure.

Final application artifacts are always built by the digest-pinned multi-stage
Dockerfile and run as containers. Both automated Compose paths assert that the
final app image starts and reaches its container health check; host `go run` is
never the final runtime evidence.

Automated integration runs use a unique Compose project, project-scoped database
and queue volumes, and Docker-assigned host ports. Cleanup removes that project's
containers, networks, and volumes. The long-lived `make up` development project
uses separate names and is not touched.

## Test-only HTTPS policy

The fake supplier runs with a test certificate and an explicit test-only network
policy naming its Docker test endpoint/network. Test configuration may trust that
certificate and address only. The allowance must not be present in production
configuration, exposed as a general private-network flag, or reused for arbitrary
private destinations.

## Consequences

- The runtime has a small dependency surface and no third-party HTTP router.
- Fencing predicates and transaction boundaries remain visible in SQL.
- RabbitMQ reconnect, confirm handling, and consumer cancellation are explicit
  application responsibilities.
- Migration execution is an operator/tooling action, separate from normal service
  startup. `make migrate ARGS='...'` is its stable pinned entry; the toolchain
  stage contains no business migration.
- Integration tests depend on Docker Compose being available.
- `make verify-gate` owns database/queue restart durability, AMQP semantics, and
  safe-dial compatibility. `make integration` separately owns isolated service
  integration; neither is an alias.
- Adding a dependency requires an ADR amendment or a new ADR stating the current
  requirement that standard library and selected tools cannot meet.

## Rejected alternatives

- **Web framework or third-party router:** no current requirement exceeds
  `net/http`.
- **ORM:** obscures conditional updates and affected-row fencing checks.
- **`database/sql` plus a generic driver:** workable, but pgx provides the needed
  PostgreSQL behavior and pool directly with fewer abstraction layers.
- **Kafka or a cloud queue:** contradicts the approved RabbitMQ local-MVP stack.
- **Redis for rate coordination:** unnecessary with one Worker deployment instance
  and not approved for the MVP.
- **Testcontainers and assertion frameworks:** convenient but not currently
  indispensable; Docker Compose and standard tests provide the required evidence.
