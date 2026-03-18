# Execution Plan: Initial Pipeline Authoring and Optimized Query Database

Updated: 2026-03-18

## Objective

Stabilize the first Codex-authored SQL cleaning pipeline flow so uploads can automatically produce an auditable optimized query database from the immutable source database.

## Current Status

The v1 foundation is implemented.

Delivered:

- Pipeline entities and run metadata are stored alongside imported datasets.
- Codex CLI is invoked automatically after a successful source import.
- Codex-generated `pipeline.sql`, `analysis.json`, and `summary.md` artifacts are stored and audited.
- Generated SQL is validated before execution.
- A separate clean SQLite database is built from the immutable source database.
- Pipeline and clean-database status are surfaced through the API and frontend.
- Real Codex CLI runs were verified end to end, including clean-database creation.

This plan stays active because the current implementation is still the first operational version, not the finished product shape.

## Non-goals

- Automatic continuous optimization from query clusters.
- Production-grade review workflows for every pipeline revision.
- Supporting multiple agent engines in v1.
- Incremental or partial rebuilds of the clean database.

## Remaining Milestones

- Expose generated pipeline artifacts directly in the UI so operators can inspect SQL, findings, and summaries after upload.
- Add manual retry and rerun controls for failed or outdated pipeline versions.
- Broaden fixture and manual validation coverage for inconsistent dates, mixed types, and spelling-normalization scenarios.
- Tighten runtime observability around Codex execution duration, retries, and clean-database build failures.
- Define the boundary between "auto-apply safe fixes" and future review-required pipeline changes.

## Validation

- Import a representative workbook and generate an initial pipeline with the real Codex CLI.
- Confirm the clean query database is built without mutating the source database.
- Confirm pipeline metadata, analysis artifacts, and clean-database status are persisted and exposed through the API.
- Re-run the same validation flow locally with `pnpm check`.

## Rollout

- Keep one supported SQL-only pipeline format and one full-rebuild path in v1.
- Prefer conservative, high-confidence transformations that improve queryability without changing business meaning.
- Add operator controls and stronger review surfaces before broadening the scope of automatic cleanup.

## Risks

- Codex-generated SQL still needs strong validation and operational visibility before the system can safely broaden cleaning scope.
- If artifact review remains API-only, debugging pipeline behavior will stay harder than it needs to be.
- Full clean-database rebuilds may become slow on larger workbooks before incremental strategies exist.
