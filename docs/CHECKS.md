# Checks

Last reviewed: 2026-03-17

This doc defines the standard local validation loop before opening a PR.

## Current state
- The repo is TypeScript-only.
- The intended application implementation is a TypeScript monorepo with backend and frontend apps.
- Until app tooling exists, keep this document explicit about what is implemented versus what is planned.

## Canonical commands
Document the actual commands here once the TypeScript workspace exists and keep them stable.

- Format: `pnpm format`
- Lint: `pnpm lint`
- Typecheck: `pnpm typecheck`
- Unit tests: `pnpm test`
- Build: `pnpm build`
- Run locally: `pnpm dev`

## Suggested sequence
1. Format
2. Lint and typecheck
3. Unit tests
4. Build
5. Run locally and complete manual checks

## Run locally
When the TypeScript apps land, document the exact workspace commands and environment variables here.

Expected local requirements:
- Per-worktree port configuration
- Local data directories for source DB, optimized DB, and logs
- Test workbook fixtures for ingestion and query verification

## Manual checks
- Upload a representative workbook and verify sheet discovery is correct.
- Confirm the initial optimized query database build succeeds.
- Run one natural-language query end to end.
- Inspect generated SQL and verify it targets the optimized query database.
- Verify a query execution log is stored with latency and cost-oriented metadata.
- Verify the UI validation loop was completed for upload or query-facing changes.
- Verify observability signals exist for one query and one background job.

## Optional single entrypoint
If a one-liner script is added later, keep it as a convenience only and ensure it matches this document.
