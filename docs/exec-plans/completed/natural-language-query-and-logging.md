# Execution Plan: Natural-Language Querying and Query Execution Logging

Updated: 2026-03-18

Status: Completed for v1

## Objective

Deliver the first user-facing query experience: accept a natural-language question, translate it into SQL against the optimized query database, execute it, and store a rich query execution log for every attempt.

## Delivered

- Defined the `NaturalLanguageQueryRequest`, `GeneratedSqlRecord`, and `QueryExecutionLog` contracts used across frontend, backend, and persistence layers.
- Implemented a direct OpenAI Responses query generator for the query path instead of reusing Codex CLI and temp files.
- Added SQL validation and clean-database query execution before returning any result to the UI.
- Added a query workspace with one input box, generated SQL visibility, result-table rendering, and failure states.
- Persisted query logs for successful and failed runs, including prompt, generated SQL, latency timings, row counts, and traceable query log ids.
- Added recent query log visibility to the dataset page so each run is auditable after the fact.
- Added streaming SQL-generation output for the active query run over SSE so the query step is easier to follow during demos.

## Validation

- Verified `fetch first row` end to end against a built clean database.
- Confirmed the UI shows generated SQL and result rows after a successful query.
- Confirmed failed queries still create persisted query logs.
- Confirmed live query-generation output is visible during the active query run.

## Follow-up Work

- Improve streamed-query UX so the active SQL stream is easier to read and less mixed with system-status lines.
- Consider persisting richer per-query event history if we want replayable operator timelines instead of only the latest live run.
- Add clearer user-facing error presentation when the model generates invalid SQL or references nonexistent tables.

## Risks That Remain

- Query quality still depends heavily on prompt quality and schema clarity.
- The current v1 query surface is intentionally narrow and may fail on broader analytical questions.
- Streamed query feedback is useful for demos, but it still needs polish before it feels production-ready.
