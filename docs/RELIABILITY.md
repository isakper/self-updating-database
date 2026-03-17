# Reliability

Last reviewed: 2026-03-17

Reliability expectations and operational priorities for the self-updating database.

## Reliability goals
- Workbook imports are retryable and provenance is preserved.
- The optimized query database can be rebuilt from source data and pipeline versions.
- Natural-language query failures degrade clearly without corrupting data.
- Clustering and optimization jobs are observable and resumable.

## Service expectations
- Query execution should fail safely and leave source and optimized databases consistent.
- Pipeline revisions should be versioned and reversible.
- Rebuild jobs should be idempotent where practical.
- Operational state should distinguish source import failures, pipeline failures, query failures, and optimization failures.

## Recovery priorities
- Recover query service first so users can continue working on the latest optimized database.
- Recover pipeline execution next so rebuilds and revisions can continue.
- Source database recovery is highest-integrity work; never shortcut provenance restoration.

## Reliability indicators
- Upload success rate
- Time from upload to optimized database ready
- Query success rate
- Query p95 latency
- Rebuild success rate
- Optimization job success rate

## Runbook expectations
- Every long-running job should emit a trace or equivalent correlation identifier.
- Failures should be classifiable by subsystem: ingestion, pipeline, query, clustering, optimization.
- Rollback means restoring a prior pipeline version and rebuilding the optimized database, not mutating the source database.
