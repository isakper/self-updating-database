# Testing

Last reviewed: 2026-03-17

## Default stance
- Write the test first when practical, especially for bugs and boundary behavior.
- Prefer deterministic tests that run fast.
- Test behavior and invariants, not implementation details.
- Treat the source-database immutability rule as a first-class invariant.

## Language direction
- Product code is expected to be TypeScript for both backend and frontend.
- The current Python tests in this repo exist only to support transitional doc tooling and should not drive future product architecture.

## Coverage goals
- Target high coverage for core TypeScript backend and frontend logic.
- Focus first on ingestion, pipeline execution, query generation/execution, and clustering behavior.
- If a line is truly untestable, document the reason inline and in the PR description.

## Test taxonomy
- Unit tests: pure TypeScript logic, no network, no real DB.
- Integration tests: real or containerized databases for source DB, optimized DB, and pipeline execution flows.
- Contract tests: shared request/response and schema contracts between `apps/web`, `apps/api`, and `packages/shared`.
- E2E/UI tests: upload flow, query flow, and query history sanity.

## Where tests should go
- Frontend: colocated TypeScript tests in `apps/web`.
- Backend: colocated TypeScript tests in `apps/api` and `packages/*`.
- Transitional Python doc-lint tests may remain under `tests/` until the tooling is migrated.

## Tooling direction
- TypeScript unit and integration tests: `vitest`.
- Frontend interaction tests: Playwright or equivalent when UI stabilizes.
- Contract/schema assertions should reuse the same TypeScript definitions shipped in `packages/shared`.

## What to test
- Workbook ingestion happy path and malformed workbook handling.
- Pipeline generation and execution on representative workbook structures.
- Natural-language request validation and SQL generation safety checks.
- Query execution logging with latency, cost, and failure metadata.
- Query clustering thresholds and optimization trigger decisions.
- Pipeline rollback or revision handling without mutating the source database.

## Manual UI validation
Use Chrome DevTools and the workflow in [docs/UI_VALIDATION.md](UI_VALIDATION.md) for upload and query UX changes.
