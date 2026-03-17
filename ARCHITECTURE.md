# Architecture

Last reviewed: 2026-03-17

This document is the system-of-record for how the self-updating database is structured.

## Product architecture summary
- Users upload Excel workbooks with multiple sheets.
- The ingestion system converts workbook sheets into an immutable source database.
- Codex CLI analyzes imported structure and writes a transformation pipeline stored in the platform database.
- The transformation pipeline builds a separate optimized query database for interactive querying.
- Users submit natural-language questions in the web app.
- The backend translates the request into SQL against the optimized query database, executes it, and logs execution metadata.
- Logged queries are clustered by similarity, frequency, and cost.
- Expensive, high-frequency clusters trigger Codex CLI to revise the transformation pipeline so future queries become cheaper and easier.

## Monorepo shape

### Applications
- `apps/web`: TypeScript frontend for workbook upload, query workspace, query history, and optimization insights.
- `apps/api`: TypeScript backend for ingestion orchestration, natural-language query execution, logging, clustering, and optimization jobs.

### Shared packages
- `packages/shared`: shared TypeScript types, schemas, and API contracts.
- `packages/pipeline-sdk`: pipeline step definitions, validators, and execution helpers.
- `packages/agent-orchestrator`: adapter boundary for invoking Codex CLI and storing prompts, outputs, and audit records.
- `packages/database-core`: ingestion logic, schema metadata, source-to-optimized transforms, clustering services, and optimization services.

### Documentation
- `docs/`: product, operating, and decision docs.

## Core data model

### Source database
- Created from uploaded workbook sheets.
- Treated as immutable after import except for append-only metadata and provenance records.
- Preserves raw structure so pipeline changes can rebuild derived artifacts safely.

### Transformation pipeline
- Authored by Codex CLI and stored as versioned steps.
- Converts imported source data into query-friendly structures.
- Can be revised as the system learns which query patterns matter most.

### Optimized query database
- Separate derived database used for end-user querying.
- Rebuilt or incrementally refreshed from the current pipeline.
- Owns denormalized tables, materialized views, helper dimensions, and other query-acceleration artifacts.

### Query telemetry entities
- `NaturalLanguageQueryRequest`
- `GeneratedSQLRecord`
- `QueryExecutionLog`
- `QueryCluster`
- `OptimizationRevision`

## Backend domain layering
Within backend packages and domains, code should depend only forward through:

`types -> schemas -> repo -> service -> jobs -> api`

Cross-cutting integrations enter through explicit provider or adapter boundaries.

### Layer responsibilities
- `types`: TypeScript domain types and pure helpers.
- `schemas`: parsing and validation schemas at boundaries.
- `repo`: data access for source DB, optimized DB, pipeline storage, and telemetry tables.
- `service`: orchestration and business rules.
- `jobs`: background workflows such as ingestion, clustering, and optimization evaluation.
- `api`: HTTP handlers and transport mapping for the frontend or operator tools.

### Providers and adapters
- Database clients, Codex CLI invocation, storage, and observability are exposed through explicit interfaces.
- Domain code does not directly reach for global clients or process-wide singletons.
- Runtime wiring lives at the application boundary.

## Core backend domains
- `ingestion`: workbook parsing, schema discovery, source DB load, provenance.
- `pipeline`: pipeline definitions, versioning, execution, and rebuild control.
- `query`: natural-language request handling, SQL generation, execution, and result formatting.
- `telemetry`: query execution logs, cost signals, latency signals, and audit records.
- `clustering`: grouping similar query logs and calculating aggregate complexity/value.
- `optimization`: deciding when to invoke Codex CLI and how pipeline revisions are reviewed or applied.

## Frontend areas
- Upload workspace: file selection, workbook inspection, import status, and ingestion summaries.
- Query workspace: natural-language input, generated SQL preview, results table, and query timing/cost hints.
- Query history and diagnostics: recent runs, failures, generated SQL, and trace identifiers.
- Optimization insights admin view: cluster summaries, pipeline revision proposals, and rebuild status.

## System flow
1. User uploads an Excel workbook in `apps/web`.
2. `apps/api` parses sheets, records provenance, and loads an immutable source database.
3. Codex CLI is invoked through `packages/agent-orchestrator` to propose an initial transformation pipeline.
4. `packages/pipeline-sdk` and `packages/database-core` execute the pipeline and build the optimized query database.
5. User sends a natural-language query through the web app.
6. Backend validates the request, generates SQL, executes it against the optimized query database, and returns results.
7. Backend stores a query execution log with prompt, SQL, latency, cost signals, and outcome.
8. Background clustering jobs group similar queries and track high-frequency expensive clusters.
9. Optimization jobs invoke Codex CLI for pipeline revisions when a cluster crosses the configured threshold.
10. Approved revisions update the stored pipeline and rebuild the optimized query database without mutating the source database.

## Invariants
- Source data remains reproducible from the original workbook import.
- Optimizations never destructively rewrite the source database.
- Every executed natural-language query produces a traceable query log.
- Pipeline revisions are versioned and auditable.
- Shared contracts are defined once in TypeScript and consumed by both backend and frontend.
- Boundary inputs are validated before use.
- Observability uses structured logs, metrics, and traces with correlation IDs.

## Architecture linting direction
This repo currently ships a template architecture lint contract and Python-based stub tooling. The intended long-term enforcement path for this project is TypeScript-native.

- Config: `architecture-lint.toml`
- Current entrypoint: `scripts/lint-architecture`
- Preferred future enforcement: ESLint custom rules or `dependency-cruiser` aligned to the layering described here.

## Runtime environments
- Local: TypeScript apps run side-by-side with per-worktree ports and local data directories.
- Staging: representative workbook imports, clustering behavior, and pipeline rebuilds exercised before production rollout.
- Production: source DB, optimized DB, pipeline store, and query telemetry separated with auditable rebuild workflows.

## Observability
See [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) for required logs, metrics, traces, and cluster/optimization telemetry.
