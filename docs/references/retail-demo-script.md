# Retail Demo Script

Updated: 2026-03-24

Use this when you want to demo the concept as a short story with one clear claim: repeated retail queries can teach the system which derived structures are worth building, and those structures can make the next wave of questions faster without touching the immutable source database.

This script follows a 5-part flow:

1. Retail data and why bottom-up queries are painful
2. One-slide proof that the optimized path got faster
3. Mermaid diagram for how the system works
4. Detailed benchmark results
5. Findings from trying the concept

Source of truth for the numbers below: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.json`. Use that artifact over summary text elsewhere if totals drift.

## 1. Retail data problem

### Show

- Workbook profile:
  - `Transactions`: 27,675 line-level rows
  - `Items`: 180 rows
  - `Stores`: 12 rows
  - Coverage: Q1 2025 retail transactions
- A few example questions:
  - "Show daily units sold, revenue, and cost for these 3 SKUs across this date range."
  - "Include every date even when there were no sales."
  - "Calculate gross revenue before returns, including VAT."

### Say

This is the kind of retail data where almost every interesting question is a bottom-up calculation. The source workbook is line-level transaction data, so even a simple-looking question often means:

- filtering returns correctly
- choosing gross vs net revenue semantics
- aggregating many transaction lines up to day or SKU grain
- rebuilding a full date spine so zero-sales days still appear
- repeating the same expensive logic over and over for very similar questions

The hard part is not just SQL speed. It is that repeated retail questions often need both heavy computation and precise business semantics.

## 2. One-slide proof

### Show

For the repeated log-inspired retail workload:

- Raw DB: `13/20` correct, `11.262ms` average SQL execution
- Clean DB: `19/20` correct, `3.381ms` average SQL execution
- Optimized DB: `19/20` correct, `1.533ms` average SQL execution

Headline:

- `7.35x` faster average SQL execution from raw to optimized
- `86.4%` lower average SQL execution time from raw to optimized

### Say

The strongest result is not on random one-off questions. It is on the repeated, log-inspired retail workload that mirrors the kinds of bottom-up queries users keep asking. That is exactly where the self-updating database is supposed to help.

## 3. How it works

### Show

```mermaid
flowchart LR
  A["Retail workbook<br/>Transactions, Items, Stores"] --> B["Immutable source DB"]
  B --> C["Canonical cleanup pipeline<br/>transactions_clean, items_clean, stores_clean"]
  C --> D["Natural-language query to SQL"]
  D --> E["Query logs"]
  E --> F["Cluster repeated heavy patterns"]
  F --> G["Optimization pipeline generation"]
  C --> G
  G --> H["Derived optimized tables<br/>business_dates, sku_daily_metrics"]
  H --> D
  H --> I["Benchmark + parity validation"]
  I -->|pass| J["Promote optimized revision"]
  I -->|fail| G
```

### Say

The key idea is that the system never mutates the source database. It learns by changing the derived query layer. In this retail demo, the important optimized structures were:

- `business_dates`: a reusable date spine
- `sku_daily_metrics`: pre-aggregated daily SKU metrics for units, revenue, and cost, with both gross and net semantics

That means the next query no longer has to rebuild those rollups from raw transaction lines every time.

## 4. Detailed results

### Benchmark summary

| Workload                        | Scenario  | Correct | Avg SQL ms | Median SQL ms |
| ------------------------------- | --------- | ------- | ---------: | ------------: |
| Random questions                | Raw       | 18/20   |     23.429 |        22.310 |
| Random questions                | Clean     | 16/20   |     19.792 |        18.876 |
| Random questions                | Optimized | 18/20   |     16.206 |        15.881 |
| Log-inspired repeated questions | Raw       | 13/20   |     11.262 |        10.795 |
| Log-inspired repeated questions | Clean     | 19/20   |      3.381 |         3.056 |
| Log-inspired repeated questions | Optimized | 19/20   |      1.533 |         1.019 |

### Overall across all 40 benchmark questions

| Scenario  | Correct | Avg SQL ms | Median SQL ms |
| --------- | ------- | ---------: | ------------: |
| Raw       | 31/40   |     17.346 |        14.519 |
| Clean     | 35/40   |     11.587 |         5.989 |
| Optimized | 37/40   |      8.870 |         1.951 |

### Best demo examples

These are good examples to speak over because they match the optimization story directly:

| Question                                       | Raw ms | Clean ms | Optimized ms | Speedup raw to optimized |
| ---------------------------------------------- | -----: | -------: | -----------: | -----------------------: |
| `q004` daily SKU-by-date series with zero-fill | 17.961 |    5.685 |        0.635 |                 `28.29x` |
| `q012` daily SKU-by-date series with zero-fill | 14.008 |    1.717 |        0.548 |                 `25.54x` |
| `q006` daily SKU-by-date series with zero-fill | 16.548 |    5.417 |        0.838 |                 `19.76x` |
| `q014` daily SKU-by-date series with zero-fill | 15.483 |    2.157 |        0.831 |                 `18.62x` |

### What changed technically

On the raw path, the model often had to:

- start from `transactions`
- normalize return handling in-query
- aggregate to daily SKU grain
- build a date range
- cross join the SKU list and dates
- left join back to the transaction aggregate

On the optimized path, the model usually only had to:

- pull the date range from `business_dates`
- cross join requested SKUs to that spine
- left join to `sku_daily_metrics`

That is why the repeated retail queries got much faster: the expensive bottom-up rollup moved from query time into the derived database build.

## 5. Findings from trying the concept

### What worked

- Repeated heavy query patterns are where this concept is strongest.
- A clean canonical schema improved correctness a lot before optimization even began.
- Targeted derived tables beat generic denormalization. The useful win came from very specific helper structures, not from making a giant "everything table."
- The immutable-source plus derived-rebuild model is a good fit for trust. We can optimize aggressively without rewriting uploaded source data.

### What did not fully work yet

- Some broad semantic questions still fail. Example: `q018` supplier revenue is wrong in both clean and optimized scenarios, which means the system still needs stronger metric semantics and better parity coverage.
- The evaluation layer still produces noisy false negatives. Several misses are only column-name mismatches, and at least one logged "incorrect" result appears semantically correct on inspection.
- Optimization should stay narrow and benchmark-backed. If the system adds too many helper objects, schema clarity for the LLM can get worse instead of better.

### Honest takeaway

This concept already looks promising for repeated retail analytics questions, especially bottom-up daily rollups with zero-fill requirements. The main thing it has proven is not "AI can answer any retail question." The stronger claim is: when users keep asking the same expensive class of questions, the system can learn a better derived schema and materially reduce query cost on the next round.

## Evidence to keep handy

- Benchmark report: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.json`
- Benchmark CSV: `docs/reports/2026-03-24-eval/sql-benchmark-dataset_ykadj93p-full-deliberate-latest.csv`
- Applied optimized pipeline: `docs/reports/2026-03-24-eval/pipeline_version_5n9pv9xq.sql`
- Demo workbook notes: `apps/web/fixtures/demo-workbooks/README.md`
- SQLite walkthrough: `docs/references/demo-db-walkthrough.md`
