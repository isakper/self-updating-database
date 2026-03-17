# Upload to Query Learning Loop

## Goal

- Describe the end-to-end POC that turns uploaded Excel workbooks into a self-improving query experience.

## Users

- Primary: operators or analysts who want answers from workbook-shaped data without writing complex SQL manually.
- Secondary: engineers or agents inspecting generated SQL, query logs, and optimization behavior.

## Flow

1. User uploads an Excel workbook with multiple sheets.
2. The backend imports sheets into an immutable source database and records provenance.
3. Codex CLI analyzes the imported shape and creates an initial transformation pipeline.
4. The pipeline builds a separate optimized query database.
5. User submits a natural-language question.
6. The system translates it into SQL, executes against the optimized query database, and returns results.
7. The system stores a query execution log with prompt, SQL, latency, cost, and outcome metadata.
8. Background clustering groups similar queries and identifies high-frequency expensive patterns.
9. Codex CLI proposes a pipeline revision when a cluster exceeds the optimization threshold.

## Success criteria

- A user can upload a workbook and reach first query success without touching the source database manually.
- Generated SQL is visible for each natural-language query.
- Query logs are rich enough to support clustering and optimization decisions.
- Pipeline revisions can improve the optimized query database without mutating the source database.

## Open questions

- Approval model for applying optimization revisions beyond the POC.
- Exact clustering algorithm and cost formula.
- Whether optimized database rebuilds are full or incremental in early versions.
