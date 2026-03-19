# Execution Plan: Source DB Durability and Persistence Hardening

Updated: 2026-03-19

## Objective

Improve source-database persistence guarantees and performance characteristics by hardening write durability, reducing full-file rewrite risk, and clarifying storage behavior under concurrent/large workloads.

Success criteria:

- Source DB writes are crash-safe and atomic at file level.
- Large imports do not require rewriting the entire DB file on every small update path.
- Persistence behavior and limits are documented and test-covered.

Touches:

- Source DB ingestion storage layer
- Query/telemetry persistence layer
- Operational reliability docs

Invariants:

- Source data remains immutable after import.
- Existing schema and auditability guarantees remain intact.

## Non-goals

- Full distributed database migration.
- Reworking domain contracts for ingestion/query/optimization.
- Premature sharding or multi-tenant redesign.

## Milestones

- Introduce atomic file persistence strategy for the current storage path:
  - write to temp file
  - fsync
  - atomic rename/swap
- Add consistency checks at startup for partial/corrupt write detection and graceful recovery behavior.
- Benchmark and document current `sql.js` full-export write profile on representative demo workloads.
- Evaluate and choose next persistence strategy:
  - keep `sql.js` with hardened persistence and bounded scale limits
  - or migrate runtime persistence path to native sqlite driver for transactional disk writes
- If migrating, provide compatibility migration plan from existing `.data/source-datasets.sqlite`.
- Extend tests for restart/reopen scenarios, including power-loss style interrupted writes.

## Validation

- `pnpm test`
- `pnpm typecheck`
- `pnpm lint`
- Durability tests:
  - interrupted write simulation
  - restart and reopen integrity verification
  - checksum or row-count parity checks before/after persistence operations
- Performance checks:
  - import demo workbook and measure ingestion + persistence latency before/after changes

## Rollout

- Phase 1: atomic persistence hardening on current storage implementation.
- Phase 2: optional backend persistence migration behind compatibility checks.
- Phase 3: update operational docs and recommended deployment settings.

## Risks

- Storage migration can introduce subtle compatibility regressions if schema/init order differs.
- Atomic write hardening may increase write latency on slower disks.
- Under-tested recovery logic can produce false-positive corruption handling.
