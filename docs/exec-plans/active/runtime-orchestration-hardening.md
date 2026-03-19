# Execution Plan: Runtime Orchestration Hardening

Updated: 2026-03-19

## Objective

Harden pipeline and optimization orchestration so runs cannot hang indefinitely on partial artifacts, concurrent duplicate scheduling is prevented, and run completion semantics are deterministic.

Success criteria:

- A pipeline or optimization run never remains in `running` forever when Codex exits or times out.
- Required artifacts are validated as complete and parseable, not just non-empty files.
- Duplicate scheduling for the same dataset/run target does not start parallel conflicting jobs.

Touches:

- Source DB ingestion orchestration
- Optimized DB rebuild orchestration
- Query clustering and optimization trigger runtime

Invariants:

- Source database immutability remains unchanged.
- Pipeline and optimization audit trails remain persisted and inspectable.

## Non-goals

- Redesigning prompt strategy or semantic quality of generated SQL.
- Replacing Codex CLI with another provider in this phase.
- Major UI redesign work.

## Milestones

- Add explicit run-state transitions for terminal failure paths where artifacts are missing, malformed, or partially written.
- Replace non-empty-file readiness checks with strict artifact completion checks:
  - parse and validate `analysis.json` / `decision.json` before success is accepted
  - require expected keys and types in all required artifacts
- Add per-dataset in-flight deduplication for pipeline scheduling.
- Add per-dataset candidate-set in-flight deduplication for optimization scheduling.
- Add cleanup policy for successful temporary workspaces with optional retention for debug mode.
- Add timeout and startup observability fields (timeout reason, elapsed ms, startup failures) to run events.

## Validation

- `pnpm test`
- `pnpm typecheck`
- `pnpm lint`
- Manual failure-injection checks:
  - simulate missing `analysis.json` with existing `pipeline.sql` and verify run transitions to failed + retry/terminal handling
  - simulate Codex timeout and verify no indefinite `running` status
  - issue duplicate schedule requests and verify exactly one active job per dataset
- End-to-end check:
  - upload workbook -> pipeline succeeds
  - import logs -> optimization executes
  - verify run statuses and events are terminally consistent

## Rollout

- Land behind conservative defaults with no behavior change for successful happy-path runs.
- Enable stricter artifact validation first in pipeline flow, then optimization flow.
- Keep feature flags/env overrides for timeout windows during stabilization.

## Risks

- Stricter artifact validation may increase short-term failure rates and surface latent prompt contract mismatches.
- Over-aggressive deduplication could suppress valid retries if keying is incorrect.
- Temporary workspace cleanup may reduce debug visibility unless retention hooks are preserved.
