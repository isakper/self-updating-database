# Design

Last reviewed: 2026-03-17

This document defines product and interaction design principles for the self-updating database POC.

## Product promise
- Make complex workbook data queryable without forcing users to hand-author difficult SQL joins.
- Show users that the system learns from usage by improving the derived query database over time.
- Preserve trust by keeping the original imported data intact and making optimization behavior legible.

## Primary user journeys
- Upload a workbook and confirm the system understood the sheets.
- Wait for the initial Codex-authored transformation pipeline and optimized query database to be created.
- Ask a natural-language question and receive results plus a transparent SQL translation.
- Inspect query history, latency, and failures.
- Review why an optimization recommendation was generated.

## Design principles
- Progressive trust: expose enough detail to make the system believable without overwhelming first-time users.
- Safe learning: always reinforce that the source database is immutable and optimization happens in a derived database.
- Explainability over magic: show generated SQL, execution time, and pipeline revision summaries when useful.
- Fast feedback: uploads, query execution, rebuilds, and optimization jobs should surface clear status states.
- Operational legibility: power users and agents should be able to inspect the system through history, logs, and diagnostics surfaces.

## UX expectations
- The upload flow should surface workbook structure before import is finalized.
- Query results should always be paired with execution metadata, especially when SQL generation fails or is ambiguous.
- The interface should distinguish clearly between user queries and system optimization actions.
- Admin or operator screens should make pipeline version changes auditable.

## Accessibility
- Keyboard-first operation for upload, query submission, and results navigation.
- Visible status updates for long-running ingestion and optimization tasks.
- Query result tables should remain usable with screen readers and responsive layouts.
