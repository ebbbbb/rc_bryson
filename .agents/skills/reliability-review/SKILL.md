---
name: reliability-review
description: Review concrete reliability and security execution paths. Use whenever changes touch delivery state, Outbox, generation, lease or fencing, MQ ACK, retry, replay, HTTP dialing, SSRF controls, credentials, or request logging.
---

# Reliability Review

Read the failure model and invariants in `docs/architecture.md` plus the affected ADRs. Trace only paths present in the change:

1. Enumerate crash points around database commit, HTTP completion, publish confirm, and MQ ACK. Verify database state commits before ACK and ambiguity produces at-least-once behavior rather than loss.
2. Check every state result is fenced by delivery ID, generation, active delivering state, and lease token or owner. A stale worker's update must affect zero rows and its result must be discarded.
3. Trace old-generation, duplicate, reordered, and redelivered messages. They must not bypass `next_attempt_at` or advance terminal state.
4. Trace replay and retry deadlines, including ambiguous HTTP success. Preserve the supplier idempotency value where the contract requires it.
5. Trace destination registration, resolved-IP validation and pinning, redirect behavior, TLS hostname verification, protected headers, secret resolution, and all logging/error paths.

Report only findings with a specific trigger, execution path, violated invariant, and observable impact. If none exist, say which paths were checked; do not invent speculative findings.
