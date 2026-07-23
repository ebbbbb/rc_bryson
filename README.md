# Reliable HTTP Notification Service

An internal service for durably accepting HTTP(S) notifications and delivering
them asynchronously to pre-registered external suppliers.

The planned service accepts a notification without waiting for the supplier,
persists it in PostgreSQL, and dispatches it asynchronously through RabbitMQ.
Transactional Outbox closes the database/queue dual-write gap, while stable
supplier idempotency values limit the impact of at-least-once retries.

> **Project status:** the repository currently contains the engineering
> contracts, application skeleton, and verified development toolchain. Delivery,
> Outbox, Worker, retry, replay, and business schema implementation will be added
> through the test-first slices in `docs/exec-plan.md`. This is not yet a
> production-ready notification service.

## Design at a glance

- **Runtime:** Go 1.26.x
- **System of record:** PostgreSQL
- **Dispatch signal:** RabbitMQ messages containing identifiers only
- **Consistency bridge:** Transactional Outbox
- **Delivery semantic:** at least once
- **Local environment:** Docker Compose

PostgreSQL owns delivery state and retry scheduling. RabbitMQ improves dispatch
isolation and concurrency but is not an authoritative task store. External HTTP
side effects cannot be guaranteed exactly once.

## Repository layout

```text
cmd/                 Application and test-probe entry points
docs/                Product specification, architecture, ADRs, and execution plan
internal/toolchain/  Toolchain dependency anchors only
test/                Integration-test entry points
testdata/            Test-only TLS fixtures
.agents/skills/      Repository development workflows
.codex/              Repository-scoped Codex hooks
```

Detailed project documentation:

- [`docs/product-spec.md`](docs/product-spec.md) — behavior, scope, and non-goals
- [`docs/architecture.md`](docs/architecture.md) — boundaries, state model, and
  failure model
- [`docs/adr/`](docs/adr/) — accepted architectural decisions
- [`docs/exec-plan.md`](docs/exec-plan.md) — independently verifiable vertical
  slices
- [`AGENTS.md`](AGENTS.md) — instructions for repository automation agents

## Prerequisites

Local development and probes require:

- Go 1.26.x
- Docker with Docker Compose
- POSIX shell utilities, Bash, `jq`, and `curl`
- Make

Check the environment before doing other work:

```sh
make preflight
```

Final application builds and runtime checks are containerized. Host Go is used
for local formatting, static analysis, and compatibility probes.

## Local environment

Start PostgreSQL, RabbitMQ, the fake HTTPS supplier, and the empty application
skeleton:

```sh
make up
```

Stop the environment without deleting the development data volumes:

```sh
make down
```

The Compose environment is for local development only. Automated gates create
isolated Compose projects with random host ports and disposable volumes, so they
can run concurrently without modifying the development environment.

## Verification

Common checks:

```sh
make format-check
make lint
make test
make test-race
make integration
```

Run the complete repository verification with:

```sh
make verify
```

`make verify` runs preflight checks, formatting and static analysis, module and
Compose validation, repository skill validation, unit tests, race tests,
toolchain reliability probes, and isolated integration tests. The gate also
builds and health-checks the final application image.

Other stable entries include:

```sh
make verify-gate
make validate-skills
make verify-slice SLICE=0
make migrate ARGS='-version'
```

The migration command exposes the pinned migration tool only; the repository
does not yet contain a business schema. Implementation sequencing and acceptance
evidence live in [`docs/exec-plan.md`](docs/exec-plan.md).

## Security notes

- The certificates and private key under `testdata/certs/` are test-only fixtures
  for the local fake supplier and must never be used as real credentials.
- Do not commit `.env` files, production secrets, supplier credentials, request
  bodies, or sensitive headers.
- Application-level SSRF validation does not replace production egress network
  controls.
