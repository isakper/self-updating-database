# Execution Plan: Demo Transparency and Operator Visibility

Updated: 2026-03-18

## Objective

Make the product legible during demos and debugging by exposing what the system is doing at each major step: source import, Codex analysis, pipeline generation, clean-database build, natural-language query generation, SQL execution, and later optimization work.

## Non-goals

- Final visual polish or brand-level UI design.
- Building a full observability platform with external dashboards.
- Exposing raw low-level logs that overwhelm the demo narrative.

## Milestones

- Keep the delivered upload-to-clean-database status surfaces trustworthy and easy to scan.
- Keep Codex artifacts visible in the UI, including the prompt, `analysis.json`, `summary.md`, and `pipeline.sql`.
- Keep operator-facing views for clean-database readiness, retry state, and last failure reason accurate as the backend evolves.
- Improve the query workspace so the streamed SQL generation, final SQL, execution status, and result rendering feel like one coherent story.
- Add a more structured event/log feed that distinguishes active-run output from historical logs.

## Delivered So Far

- Dataset pages now show import status, pipeline status, clean-database status, retry state, and last failure details.
- Codex pipeline artifacts are visible directly in the UI after a successful run.
- Live pipeline CLI output is streamed into the dataset page over SSE.
- Query pages now show generated SQL, result rows, recent query logs, and streamed SQL-generation output for the active query run.

## Remaining Work

- Add a clearer status timeline instead of relying on stacked status paragraphs and expandable sections.
- Separate historical pipeline/query events from the currently active run so the live panels read cleanly in demos.
- Improve event formatting so system messages, streamed SQL, and failure reasons are visually distinct.
- Add query-specific operator detail for invalid SQL, missing tables, and validation failures without forcing users to read raw logs.
- Decide how much raw Codex CLI chatter should remain visible versus summarized for demo audiences.

## Validation

- Upload a representative workbook and confirm a demo viewer can understand each completed step without reading API responses directly.
- Confirm operators can inspect generated pipeline artifacts from the UI after an import.
- Confirm failure states surface enough detail to explain what broke and what the system will do next.

## Rollout

- Start with the pipeline path that already exists: import, Codex analysis, and clean-database build.
- Reuse the same visibility patterns for query execution and later optimization revisions.
- Prefer simple, reliable status surfaces before adding richer live-log behavior.

## Risks

- If transparency is added only as debug text, the product may still feel opaque in demos.
- If artifact visibility is inconsistent between steps, the system story will remain hard to follow.
- Showing too much raw detail without structure can make the product feel more confusing, not less.
