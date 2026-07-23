# ADR 0005: Test-First Vertical Slices

- **Status:** Accepted
- **Date:** 2026-07-23

## Context

The service's hardest requirements are crash-window, timing, fencing, redelivery,
and network-boundary behaviors. Tests added after implementation can easily confirm
the implementation's shape while missing the contract's failure mode.

## Decision

Every business slice follows a recorded red–green–refactor cycle:

1. Write the smallest test that expresses the slice's next observable behavior or
   failure contract.
2. Run the targeted test and confirm it fails because the behavior is absent, not
   because the fixture, dependency, or assertion is broken.
3. Add the minimum implementation needed to pass.
4. Run the targeted test until green.
5. Refactor without changing behavior.
6. Run the slice evidence command and all existing regression tests.

Each slice retains executable evidence for its important failure path. Time,
randomness, DNS resolution, secret lookup, and failure injection must have
controllable seams where deterministic testing requires them. PostgreSQL and
RabbitMQ semantics are verified against real local containers rather than only
mocks. The HTTPS boundary is verified against the test-only fake supplier.

Coverage is diagnostic, not an acceptance target. A global 100% coverage goal
cannot replace tests for commit uncertainty, duplicate messages, ACK loss, expired
leases, stale Worker fencing, retry deadlines, SSRF, redaction, and reconciliation.

## Consequences

- Each slice begins with a failing test; implementation-first changes do not meet
  the definition of done.
- Review evidence includes the targeted failing command/reason and the later green
  command, without requiring brittle commit choreography.
- Integration tests may be slower, so targeted slice commands remain available
  while `make verify` runs the complete regression suite.
- `make verify` is the single completion command recognized by repository hooks.
  Targeted commands may diagnose a failure, but they do not independently mark a
  repository session verified or require a redundant full run only to clear state.
- Tests assert externally observable state and network effects; they do not couple
  unnecessarily to internal function layout.

## Rejected alternatives

- **Implementation followed by tests:** does not demonstrate that the test can
  detect the missing behavior.
- **Mocks for all infrastructure:** cannot establish PostgreSQL fencing or RabbitMQ
  confirmation/redelivery semantics.
- **Global coverage threshold as the primary gate:** measures executed lines, not
  correctness at ambiguous failure windows.
- **End-to-end tests only:** make root-cause isolation and deterministic timing
  failures unnecessarily difficult.
