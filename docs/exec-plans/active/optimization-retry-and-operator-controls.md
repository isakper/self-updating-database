# Execution Plan: Optimization Retry and Operator Controls

Updated: 2026-03-19

## Objective

Make optimization behavior recoverable and operator-driven by allowing retries for failed candidate sets, adding explicit rerun controls, and preventing a single failed revision from blocking future progress.

Success criteria:

- Failed optimization revisions for a candidate set can be retried intentionally.
- Existing failed revisions do not permanently block re-evaluation of the same candidate set.
- Operators can trigger optimization reruns without direct DB edits.

Touches:

- Query clustering and optimization loop
- Optimization API surface
- Operator-facing recovery workflow

Invariants:

- Source database remains immutable.
- Every optimization attempt remains fully auditable (candidate set, decision, status, timestamps, errors).

## Non-goals

- Building a full job queue platform.
- Introducing automatic infinite retries.
- Changing clustering semantics in this phase.

## Milestones

- Update optimization dedupe logic so only successful or in-flight revisions block duplicate candidate sets.
- Add explicit retry policy for failed revisions:
  - controlled retry limit per candidate fingerprint
  - backoff strategy and clear terminal failure state
- Add write API endpoint(s) for operator controls:
  - trigger optimization evaluation for dataset
  - retry latest failed optimization revision
- Add optional manual rerun endpoint for pipeline generation when dataset processing is failed/stuck.
- Add frontend operator actions in the dataset/logs surfaces with clear status/error messaging.
- Add persistent reason codes for optimization failure classes (timeout, artifact contract, SQL validation, runtime error).

## Validation

- `pnpm test`
- `pnpm typecheck`
- `pnpm lint`
- API integration checks:
  - create failed optimization revision
  - trigger retry via API
  - verify new revision is created and old failure does not block execution
- Manual operator flow:
  - import workbook
  - seed logs
  - trigger optimization
  - on failure, rerun from UI/API and verify status transitions

## Rollout

- Ship API controls first, then UI controls.
- Keep automatic scheduling unchanged initially; add retries as opt-in behavior via config.
- Add clear docs for operator expectations and failure handling.

## Risks

- Retry loops can create cost spikes if guardrails are weak.
- Manual controls can conflict with automatic scheduling if locking is incomplete.
- Incorrect dedupe keys may create duplicate revisions or suppress valid evaluations.
