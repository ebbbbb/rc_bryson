---
name: review-maintainability
description: Review completed AI-assisted production-code changes for concrete maintainability and human-comprehension problems. Use after production Go changes and before declaring coding work complete, especially after incremental agent edits, broad diffs, or review-comment fixes; check for speculative defensive branches, thin wrappers, unapproved forward compatibility, overengineering, test-shaped production code, duplicated policy, dead abstractions, hidden state or configuration, surprising lifecycle behavior, and misleading APIs.
---

# Review Maintainability

Review the completed production diff as code another engineer must safely change.
Complexity required by an approved invariant is not itself a finding.

1. Read `AGENTS.md`, the affected contract, the production diff, and the tests that
   specify it. Establish the intended behavior before judging its shape.
2. Trace concrete maintenance paths and look for:
   - defensive checks or fallbacks for states already excluded by an internal
     contract; keep validation at trust, parsing, construction, and resource
     boundaries, and fail clearly when an internal invariant is violated;
   - thin wrappers that neither own policy or an invariant, manage a resource
     lifecycle, remove real duplication, nor provide a current replacement
     boundary. Do not use statement count alone: a short domain operation may
     still be the clearest API;
   - forward-compatibility branches, permissive decoding, migration defaults, or
     conflict suppression without an approved supported-version, rolling-deploy,
     or historical-data requirement;
   - obsolete mechanisms, unused helpers, or compatibility branches left by
     incremental edits;
   - production APIs or branches that exist only to satisfy a test fixture;
   - duplicated rules or parallel implementations that can drift;
   - hidden mutable state, implicit configuration, surprising ownership, or
     resource lifetime that contradicts the API name;
   - abstractions, dependencies, indirection, or configurability without a current
     caller or requirement;
   - silent conflict handling, misleading errors, and operational features that
     are implemented but not practically reachable.
3. For every candidate, identify the exact trigger, execution or future-edit path,
   impact, and the smaller design that preserves the contract. Drop style
   preferences, speculative rewrites, and complexity that directly enforces a
   reliability or security invariant.
4. Run the cheapest relevant static or targeted test evidence. If the current task
   authorizes fixes, correct confirmed problems, rerun the targeted evidence, and
   review the resulting diff again.
5. Report findings in severity order with file and line references. Then list
   important concerns examined but not adopted, including why the current
   complexity is justified. If there are no findings, name the paths reviewed.
6. Only after the review is complete and any authorized fixes are settled, run
   exactly `make record-maintainability-review`. This command is a coordination
   receipt for the Stop hook, not proof of review quality. Before task completion,
   use `$verify` so the independent executable gate also succeeds.

Do not claim that a hook performed semantic review. The hook only detects missing
review evidence for production Go changes and asks Codex to continue with this
skill.
