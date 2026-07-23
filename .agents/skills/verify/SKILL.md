---
name: verify
description: Run the repository's layered completion verification. Use before marking any implementation task complete, and after fixes that may affect Go formatting, static analysis, unit behavior, races, or integration behavior.
---

# Verify

Run `make verify` as the single completion path. That target owns the
cheap-to-expensive order: preflight, lint, skill validation, unit, race,
toolchain gate, and integration. Do not run those layers separately and then
repeat `make verify` only to satisfy the Stop hook.

If `make verify` fails, stop at that layer, diagnose the root cause, and fix only
in scope. Rerun the narrow failing target while iterating, then run `make verify`
once to obtain the repository's authoritative successful completion result.

Report the `make verify` exit result and the material layer evidence. Do not
declare completion when it fails or is skipped.
