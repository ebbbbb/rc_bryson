# Project Guide

## Navigation

- `docs/product-spec.md` — externally observable behavior, scope, and non-goals.
- `docs/architecture.md` — component boundaries, data flow, state machine, and failure model.
- `docs/adr/` — accepted architectural decisions and their consequences.
- `docs/exec-plan.md` — implementation order as independently verifiable vertical slices.

Read the product spec and all ADRs before changing behavior. Update the relevant
document in the same change when an accepted contract changes.

## Non-negotiable rules

- The approved base stack is Go 1.26.x, PostgreSQL, RabbitMQ, and local Docker Compose.
- PostgreSQL is the sole source of truth; the message queue carries rebuildable dispatch signals only.
- Return `202 Accepted` only after the delivery and initial Outbox event commit atomically.
- Never mark a delivery successful before the outbound HTTP result satisfies its configured success rule.
- Preserve at-least-once semantics: expired leases are reclaimable and duplicate sends are possible.
- Fence every Worker result by delivery ID, generation, delivering state, lease owner, and lease token.
- Advance generation when committing a retryable result; never let an old message bypass `next_attempt_at`.
- Keep retryable and permanent failures distinct and observable.
- Never log credentials, request bodies, or sensitive headers.
- Never connect to an unregistered or network-policy-forbidden destination.
- Establish the complete minimum SSRF boundary before the first outbound HTTP connection.
- Write each slice's failing behavior test before its implementation.
- Validate at trust and construction boundaries; do not add speculative internal
  fallbacks, abstractions without semantic ownership, or compatibility behavior
  without an approved version or historical-data contract.
- After production Go changes, use `$review-maintainability` before completion;
  Hooks track completion evidence but do not perform semantic review.
- Do not claim exactly-once delivery or business-side-effect completion.

## Required project commands

The implementation must expose these stable commands:

- `make verify-gate` — verify the approved toolchain's queue and safe-dial capabilities.
- `make preflight` — check required local Go 1.26.x, shell utilities, and Docker.
- `make format-check` — fail when committed Go formatting is not canonical.
- `make test` — deterministic unit and component tests.
- `make test-race` — run Go tests with the race detector.
- `make integration` — run isolated service-health integration tests.
- `make capacity` — run the isolated 100 submissions/second and restart-recovery
  acceptance profile.
- `make verify-slice SLICE=<n>` — run the executable acceptance evidence for one
  implementation slice.
- `make lint` — check formatting, `go vet`, module checksums, and Compose config.
- `make validate-skills` — validate all repository skills reproducibly.
- `make record-maintainability-review` — record that the current production diff
  received semantic maintainability review; this is not a substitute for review.
- `make migrate ARGS='<arguments>'` — run the pinned migration CLI; migrations
  remain operator actions and no business schema is implicit.
- `make up` / `make down` — start and stop the local Docker environment.
- `make verify` — the sole completion path; run every required layer and provide
  the result recognized by the Stop hook.

## Definition of done

A slice is complete only when its documented behavior is implemented, its failure
path is tested, logs are checked for secret leakage, and `make verify` passes.
Changes to state, persistence, networking, retry, or security also require the
corresponding spec or ADR to remain accurate.
