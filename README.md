# self-updating-database

Docs-first TypeScript monorepo for a self-updating database product.

This repository is about two linked goals:

1. Update the derived query database over time so repeated query patterns become faster to answer.
2. Update that database in a way that still helps a SQL query agent stay accurate when turning natural-language questions into SQL.

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

### Database optimization flow

```mermaid
flowchart LR
  U([User / CLI]) --> W[/Upload Workbook/]
  U --> L[/Upload Query Logs/]

  W --> S[("Immutable Source DB<br/>raw tables")]
  L --> QL[("Query Log Store<br/>prompt + SQL + row count/sample<br/>timing + clean DB id + pattern metadata")]

  S --> C{{Cleanup Pipeline Generation + Run<br/>Codex/LLM}}
  C --> CDB[("Clean DB Revision<br/>active baseline")]

  QL --> CL[[Cluster Repeated Questions]]
  CL --> O{"Optimization Pipeline Generation (Codex/LLM)"}
  CDB --> O

  O --> OCDB[("Candidate Optimized DB Revision")]
  QL --> P{{Parity Validation<br/>Codex/LLM + SQL comparison}}
  OCDB --> P

  P -->|"> threshold"| A([Apply as Active Optimized Revision])
  P -->|"<= threshold"| R[[Retry: Update Candidate Pipeline]]
  R --> O

  classDef llm fill:#fff4db,stroke:#b26a00,stroke-width:2px,color:#222;
  class C,O,P llm;
```

### Text-to-SQL generation flow

```mermaid
flowchart LR
  U([User question]) --> Q{{Natural-language Query API<br/>SQL generation via LLM}}

  S[("Scenario 1<br/>Raw DB")] --> P1["Prompt context<br/>raw schema + table profiles"]
  C[("Scenario 2<br/>Clean DB")] --> P2["Prompt context<br/>clean schema + column descriptions"]
  O[("Scenario 3a<br/>Optimized DB")] --> P3["Prompt context<br/>optimized schema + column descriptions + optimization hints"]

  P1 --> Q
  P2 --> Q
  P3 --> Q

  Q --> SQL[[Single read-only SQL query]]
  SQL --> X[[Execute SQL against selected scenario DB]]
  X --> DB[(Selected scenario DB)]
  DB --> R([Rows + timing])

  classDef llm fill:#fff4db,stroke:#b26a00,stroke-width:2px,color:#222;
  class Q llm;
```

Scenario mapping: Scenario 1 queries the immutable raw DB, Scenario 2 queries the active clean DB revision, and Scenario 3a queries the active optimized DB revision.

### Node deep dives

#### Cleanup pipeline generation + run

- **Purpose:** Turn messy uploaded sheets into a consistent queryable version.
- **Input:** Raw workbook-derived tables from the immutable source database plus the current cleanup pipeline definition.
- **Output:** A clean database revision that becomes the active baseline for querying and later optimization.
- **Logic:** Standardize naming and formatting consistency, keep values at full precision, and avoid rewriting the underlying business meaning because the source database remains immutable.

#### Query log store

- **Purpose:** Persist query runs for auditability, clustering, and optimization.
- **Input:** The natural-language prompt, generated SQL, execution status and timing, clean database id, result column names, row count, optional sampled result rows, and any used optimization objects.
- **Output:** A stored query log, and for successful queries with generated SQL, attached pattern metadata such as `matchedClusterId`, `patternFingerprint`, and `patternSummaryJson`.
- **Logic:** N/A.

#### Cluster repeated questions

- **Purpose:** Identify where users ask the same kind of question repeatedly.
- **Input:** Successful query logs for the same clean database id that have generated SQL plus extracted pattern metadata.
- **Output:** Query clusters keyed by clean database id and pattern fingerprint, with query count, cumulative and average execution latency, latest-seen timestamp, and representative query log ids.
- **Logic:** Build clusters from normalized SQL structure, not from the prompt text. The fingerprint includes relations, parsed single-equality joins, filter shapes with literal values redacted, group-by keys, aggregate expressions, order-by clauses, query kind, optimization-eligibility, and clean database id. Unsupported shapes fall back to a fingerprint of redacted normalized SQL. Optimization candidates are then limited to optimization-eligible clusters seen more than once and ranked by cumulative execution latency, then query count, then recency.

#### Optimization pipeline generation (Codex/LLM)

- **Purpose:** Propose a better derived schema for the current clean baseline.
- **Input:** Top repeated-question clusters, the current pipeline, schema context, and validation feedback from prior attempts.
- **Output:** A candidate optimized pipeline plus an explanation of what changed and optimization hints for downstream SQL generation.
- **Logic:** Redesign the derived schema so common questions are easier for the LLM to translate into correct SQL, typically through clearer naming, reusable rollups, and explicit metric semantics; choose either `pipeline_revision` or `no_change`, while rejecting trivial no-op proposals when optimization demand is clear.

#### Candidate optimized DB revision build

- **Purpose:** Materialize the proposed optimization into a real database revision.
- **Input:** The candidate optimized pipeline and the active cleaned baseline data.
- **Output:** An isolated candidate optimized database revision that is ready for validation.
- **Logic:** Validate the candidate pipeline, build the candidate database, and keep it separate from the active revision until parity checks succeed.

#### Parity validation (Codex/LLM + SQL comparison)

- **Purpose:** Verify that optimization does not break answer correctness.
- **Input:** The candidate optimized database, historical benchmark questions, and expected answers from the baseline.
- **Output:** A pass/fail decision plus diagnostics that either allow promotion or guide the next retry.
- **Logic:** Replay benchmark questions against the candidate database, compare results semantically rather than by brittle formatting, require the pass ratio to exceed the configured threshold, and feed failures back into the next optimization attempt.

#### Natural-language query API (SQL generation via LLM)

- **Purpose:** Convert a user’s natural-language question into safe executable SQL.
- **Input:** The user question plus prompt context from the selected scenario database, including schema, profiles, descriptions, and any optimization hints.
- **Output:** A single read-only SQL query and the resulting rows and timing from the selected scenario database.
- **Logic:** Assemble scenario-specific prompt context, ask the LLM for a read-only query, enforce safety checks, execute against the selected database, and log the outcome so future optimization cycles can learn from the query pattern.

#### Execute SQL against selected scenario DB

- **Purpose:** Run the validated SQL against the database chosen for the current runtime or benchmark scenario.
- **Input:** A validated read-only SQL statement plus the database path for the selected raw, clean, or optimized scenario.
- **Output:** Result rows, column names, row count, and execution timing. In the runtime query API, a sampled subset of rows is also stored in the query log.
- **Logic:** Execute the SQL directly against the selected SQLite database, measure execution time, return the rows to the caller or benchmark harness, and persist summarized result metadata on the runtime query path.

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

### Dataset 1 (random questions) summary

| Scenario                | Accuracy | Avg SQL execution time | Median SQL execution time |
| ----------------------- | -------- | ---------------------- | ------------------------- |
| `scenario_1_raw`        | `15/20`  | `23.429ms`             | `22.310ms`                |
| `scenario_2_clean`      | `14/20`  | `19.792ms`             | `18.876ms`                |
| `scenario_3a_optimized` | `15/20`  | `16.206ms`             | `15.881ms`                |

### Dataset 2 (log-inspired questions) summary

| Scenario                | Accuracy | Avg SQL execution time | Median SQL execution time |
| ----------------------- | -------- | ---------------------- | ------------------------- |
| `scenario_1_raw`        | `19/20`  | `11.855ms`             | `10.834ms`                |
| `scenario_2_clean`      | `20/20`  | `3.381ms`              | `3.056ms`                 |
| `scenario_3a_optimized` | `20/20`  | `1.533ms`              | `1.019ms`                 |

### Artifacts

- Raw dataset id: `dataset_ykadj93p`
- Cleaned pipeline version id: `pipeline_version_koulsetl` (clean DB id: `clean_db_2lrjtp54`)
- Optimized pipeline version id: `pipeline_version_5n9pv9xq` (clean DB id: `clean_db_fyu660ue`)
- Raw workbook: `apps/web/fixtures/demo-workbooks/retailer-transactions-demo.xlsx`
- Query logs workbook: `apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx`
- Full benchmark CSV: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.csv`
- Full benchmark JSON: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.json`
- Applied optimized pipeline SQL:
  - `docs/reports/2026-03-24-eval/pipeline_version_5n9pv9xq.sql`

### Next steps / considerations

- Query clustering is based on generated SQL structure, so wrong generated SQL can steer optimization in the wrong direction.
- Optimization can introduce too many helper objects, which may make schema context harder for query generation models to navigate.
- Parity validation assumes benchmark logs are correct; if logs are wrong or ambiguous, the system can learn and reinforce incorrect behavior.
