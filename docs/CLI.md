# CLI

Last reviewed: 2026-03-21

This doc defines the operator-first CLI workflow for ingestion, pipeline orchestration, and optimization control.

## Prerequisites

- Start the API server: `pnpm dev:api`
- Run CLI commands from the repo root.
- By default, CLI targets `http://127.0.0.1:3001`.
- Override API base URL with `--api-base-url <url>` or `API_BASE_URL`.

## Core flow

1. Upload source workbook (creates dataset and starts first pipeline):
   - `pnpm cli upload workbook apps/web/fixtures/demo-workbooks/retailer-transactions-demo.xlsx`
2. Upload dummy query logs for the dataset:
   - `pnpm cli upload query-logs <datasetId> apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx`
3. Trigger cleaning pipeline rerun explicitly:
   - `pnpm cli pipeline run <datasetId>`
4. Trigger optimization pipeline run explicitly:
   - `pnpm cli optimization run <datasetId>`
5. Optional reproducibility pin (must match the currently active pipeline version for the dataset):
   - `pnpm cli optimization run <datasetId> --base-pipeline-version-id <pipelineVersionId>`

## Operational commands

- List datasets: `pnpm cli dataset list`
- Show one dataset status: `pnpm cli dataset show <datasetId>`
- Watch status transitions: `pnpm cli status <datasetId> --watch --interval-ms 2000`
- Stream codex run events: `pnpm cli events <datasetId>`
- Retry latest failed optimization run: `pnpm cli optimization retry-latest-failed <datasetId>`
- Run natural-language query: `pnpm cli query <datasetId> "show top 10 products by revenue"`

## Notes

- Source database remains immutable; pipeline and optimization only rebuild derived clean databases.
- Trigger commands are asynchronous; use `status` or `events` to confirm completion.
- When `--base-pipeline-version-id` is provided, optimization is accepted only if that id matches the dataset's active pipeline version.
