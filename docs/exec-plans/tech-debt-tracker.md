# Tech Debt Tracker

Track ongoing cleanup work as small, well-scoped items.

## Current items

- Item: Harden run completion semantics to avoid stuck `running` states.
  Owner: unassigned
  Impact: prevents demo/runtime deadlocks when Codex writes partial artifacts or exits unexpectedly.
  Proposed fix: implement strict artifact validation, deterministic terminal transitions, and per-dataset scheduling dedupe.
  Success criteria: no pipeline/optimization run remains `running` indefinitely after process exit/timeout.
  Notes: see `docs/exec-plans/active/runtime-orchestration-hardening.md`.

- Item: Add optimization retry controls and unblock failed candidate sets.
  Owner: unassigned
  Impact: avoids one failed optimization revision blocking future learning on the same query patterns.
  Proposed fix: allow retriable failed revisions, add operator rerun endpoints, and tighten dedupe rules.
  Success criteria: failed optimization can be retried via API/UI without DB edits.
  Notes: see `docs/exec-plans/active/optimization-retry-and-operator-controls.md`.

- Item: Improve source DB durability guarantees.
  Owner: unassigned
  Impact: reduces risk of persistence corruption and poor write scaling under larger imports.
  Proposed fix: atomic write persistence, recovery checks, and storage strategy hardening.
  Success criteria: interrupted writes recover safely and restart integrity is test-verified.
  Notes: see `docs/exec-plans/active/source-db-durability-and-persistence-hardening.md`.

- Item: Separate live query/pipeline events from historical run output.
  Owner: unassigned
  Impact: makes the demo UI easier to trust during live walkthroughs.
  Proposed fix: split active-run streaming panels from persisted event history so a new query only shows its own SQL generation and outcome.
  Success criteria: an operator can distinguish the current live run from older events without reading timestamps closely.
  Notes: should land before polishing the demo UI.

- Item: Add manual rerun controls and clearer operator recovery flows.
  Owner: unassigned
  Impact: makes Codex-generated cleanup behavior easier to inspect and operate after import.
  Proposed fix: add a manual rerun control for failed or outdated pipeline versions and surface recovery guidance directly in the dataset page.
  Success criteria: an operator can trigger a rerun without going through the API or database directly.
  Notes: should land before broader rollout of automatic cleanup to real user data.

- Item: Expand clean-pipeline validation coverage beyond the happy path.
  Owner: unassigned
  Impact: reduces risk as workbook complexity increases.
  Proposed fix: add fixtures and manual checks for inconsistent dates, mixed types, spelling normalization, and larger multi-sheet datasets.
  Success criteria: representative messy workbooks produce explainable findings and valid clean databases.
  Notes: should happen before query execution begins to rely on cleaned schemas as the default target.
