---
name: implement-slice
description: Implement one requested vertical slice from docs/exec-plan.md. Use when the user asks to build or change a numbered slice; enforce observable test-first red, minimal green, refactor, and slice plus regression verification.
---

# Implement Slice

1. Read `AGENTS.md`, the requested section of `docs/exec-plan.md`, and every product, architecture, or ADR contract that section cites. Inspect existing tests and implementation before editing.
2. State the slice boundary and its executable acceptance evidence. Stop if the requested behavior conflicts with an approved contract.
3. Add or modify the narrowest test first. Run its targeted command and capture the failing assertion or error.
4. Confirm the red is caused by the requested behavior being absent—not a broken fixture, dependency, compile environment, or unrelated defect. Do not edit production code until this is true.
5. Implement only enough production behavior for green. Run the same targeted test and record the passing result.
6. Refactor without changing behavior, rerunning the targeted test as needed.
   Remove speculative internal defenses, wrappers without semantic ownership, and
   compatibility paths not required by the contract; do not judge a function by
   statement count alone.
7. Run `make verify-slice SLICE=<n>`, then the existing regression layers required by the slice. If reliability-sensitive paths changed, also use `$reliability-review`; before completion use `$verify`.
8. Report the exact red command and expected failure, green command and result, and final verification commands and results.

Never replace failure-scenario evidence with a coverage percentage, and never claim a red that was not observed.
