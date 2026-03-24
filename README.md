# self-updating-database

Docs-first TypeScript monorepo for a self-updating database product.

## CLI-first workflow

Start the API server first:

```bash
pnpm dev:api
```

Then use the CLI:

```bash
pnpm cli <command>
```

### Core flow

1. Upload source workbook:

```bash
pnpm cli upload workbook apps/web/fixtures/demo-workbooks/retailer-transactions-demo.xlsx
```

2. Upload dummy query logs:

```bash
pnpm cli upload query-logs <datasetId> apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx
```

3. Trigger cleaning pipeline:

```bash
pnpm cli pipeline run <datasetId>
```

4. Trigger optimization pipeline:

```bash
pnpm cli optimization run <datasetId>
```

Pin optimization to a specific active cleaned baseline revision:

```bash
pnpm cli optimization run <datasetId> --base-pipeline-version-id <pipelineVersionId>
```

### Optimization flow (current)

Optimization now follows a candidate-first loop driven by repeated query logs:

1. Rank repeated query clusters from query logs.
2. Generate a materially changed candidate pipeline (no-op candidates are rejected when clusters exist).
3. Build a candidate clean database from that pipeline.
4. Run parity against historical benchmark logs.
5. If pass ratio is not above threshold, regenerate/update the candidate pipeline and retry (up to max attempts).
6. On success, apply the candidate pipeline/clean DB.

Validation evidence is passed as context to Codex, but acceptance is decided by post-build parity checks on the candidate database.

Relevant env vars:

- `OPTIMIZATION_PARITY_MIN_PASS_RATIO` (default `1`; strict `>` comparison, so `0.9` means more than 90% must match)
- `OPTIMIZATION_PARITY_MAX_ATTEMPTS` (default `3`)

### Command reference

```bash
pnpm cli dataset list
pnpm cli dataset show <datasetId>
pnpm cli upload workbook <workbook.xlsx>
pnpm cli upload query-logs <datasetId> <query-logs.xlsx>
pnpm cli pipeline run <datasetId>
pnpm cli optimization run <datasetId>
pnpm cli optimization run <datasetId> --base-pipeline-version-id <pipelineVersionId>
pnpm cli optimization retry-latest-failed <datasetId>
pnpm cli status <datasetId>
pnpm cli status <datasetId> --watch --interval-ms 2000
pnpm cli events <datasetId>
pnpm cli query <datasetId> "show top 10 products by revenue"
```

API base URL defaults to `http://127.0.0.1:3001`.
Override with `--api-base-url <url>` or `API_BASE_URL`.

See detailed CLI notes in [docs/CLI.md](docs/CLI.md).

## Latest Eval Results (March 24, 2026)

Dataset: `dataset_ykadj93p`  
Model: `gpt-5.4-mini`  
Reasoning mode: `deliberate`  
Question sets: dataset 1 + dataset 2 (40 questions per scenario)

### Full eval summary

- `scenario_1_raw`: relaxed `34/40`, strict `2/40`, avg SQL `17.79ms`, SQL errors `1`
- `scenario_2_clean`: relaxed `34/40`, strict `1/40`, avg SQL `11.587ms`, SQL errors `0`
- `scenario_3a_optimized`: relaxed `35/40`, strict `0/40`, avg SQL `8.87ms`, SQL errors `0`

### Artifacts

- Full benchmark CSV: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.csv`
- Full benchmark JSON: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.json`
- Scenario 3a with reasoning CSV: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-s3a-gpt54mini-deliberate-with-reasoning.csv`
- Scenario 3a with reasoning JSON: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-s3a-gpt54mini-deliberate-with-reasoning.json`
- One-question cross-scenario check (`q005`) CSV/JSON:
  - `docs/reports/2026-03-24-eval/q005-all-scenarios.csv`
  - `docs/reports/2026-03-24-eval/q005-all-scenarios.json`
- Applied optimized pipeline SQL:
  - `docs/reports/2026-03-24-eval/pipeline_version_5n9pv9xq.sql`
