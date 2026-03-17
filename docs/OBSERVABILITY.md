# Observability

Last reviewed: 2026-03-17

This document defines the observability model for the self-updating database.

## Goals
- Make ingestion, query execution, clustering, and optimization behavior legible to humans and agents.
- Keep local observability isolated per worktree.
- Preserve correlation from user action to backend job to Codex CLI invocation.

## Required logs
- `service`
- `env`
- `version`
- `trace_id` or `request_id`
- `pipeline_version`
- `source_dataset_id`
- `optimized_dataset_id` when applicable
- `query_log_id` when applicable
- `job_type` for background work

## Required metrics
- workbook import count and failure count
- time from upload accepted to optimized database ready
- natural-language query latency
- query execution failure count
- query cluster count by threshold band
- optimization trigger count
- optimized database rebuild duration

## Required traces
- workbook upload to ingestion completion
- initial pipeline generation
- optimized database rebuild
- natural-language query request to SQL execution
- clustering job
- optimization recommendation or revision flow

## Core observability questions
- Why did a workbook import fail?
- Which generated SQL was executed for a given user query?
- Which query clusters are becoming expensive enough to optimize?
- Which pipeline version produced the current optimized query database?
- Which Codex CLI invocation produced a given optimization revision?

## Local workflow
1. Start the TypeScript apps for the current worktree.
2. Start the local observability stack if configured.
3. Import a workbook.
4. Run at least one natural-language query.
5. Inspect logs, metrics, and traces for import, query, and any background jobs.

## Performance starter budgets
- Upload accepted to import finished: < 60s for representative local fixtures
- First query p95 latency: < 5s in the POC
- Optimized database rebuild for representative fixtures: < 5m locally

## Troubleshooting
- Missing query logs: verify the query service writes `QueryExecutionLog` before response completion.
- Missing optimization traces: verify background jobs propagate trace context into Codex CLI orchestration.
- Missing rebuild metrics: verify pipeline execution emits job lifecycle events.
