# Execution Plan: Workbook Ingestion and Immutable Source Database

Updated: 2026-03-17

## Objective

Implement the first end-to-end ingestion flow that accepts an Excel workbook with multiple sheets, records provenance, and loads an immutable source database that can safely serve as the foundation for all later rebuilds.

## Non-goals

- Building the optimized query database in this milestone.
- Solving every workbook edge case before representative fixture coverage exists.
- Allowing destructive edits to imported source data.

## Milestones

- Define the ingestion contract for workbook upload, import status, and source dataset identity.
- Parse workbook sheets into a stable intermediate representation with provenance metadata.
- Persist imported sheets into source-database tables that can be rebuilt from the original workbook.
- Expose import status and ingestion summaries to the frontend.
- Add representative workbook fixtures that cover multi-sheet relationships and malformed input.

## Validation

- Upload a representative workbook and confirm all expected sheets are discovered.
- Confirm a source dataset record and provenance metadata are stored.
- Confirm import failures are surfaced without leaving partial destructive state.

## Rollout

- Start with local fixture-backed ingestion.
- Expand to larger workbooks only after the source schema and error model are stable.

## Risks

- Workbook shape ambiguity may produce unstable imports unless provenance is explicit.
- If source storage is not normalized enough, later pipeline rebuilds will be brittle.
