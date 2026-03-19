# Demo workbooks

`retailer-transactions-demo.xlsx` is a realistic multi-sheet retail transaction workbook for demos, ingestion checks, and query walkthroughs.

`retailer-transactions-demo-query-logs.xlsx` and `retailer-transactions-demo-query-logs.json` are companion fixtures with 20 mock query-log rows built around two recurring SKU/day access patterns:

- daily SKU/day rollups for units sold, revenue, and cost across all stores
- the same rollups with missing SKU/day combinations filled back in as zeroes

After importing `retailer-transactions-demo.xlsx`, upload `retailer-transactions-demo-query-logs.xlsx` from the "Query History + SQL Logs" tab to seed repeated query patterns for clustering and optimization demos.

Use the "DB Walkthrough + Flow Diagram" tab to copy SQL snippets for showing the same story directly from SQLite tables in VS Code.

Successful natural-language query runs now store sampled result rows in `query_execution_logs.result_rows_sample_json` so result inspection is also DB-first during demos.

Sheets:

- `Transactions`: line-level sales and returns with store, item, pricing, VAT, promotion, payment, and channel fields.
- `Items`: item master data with category, department, brand, VAT, supplier, and storage attributes.
- `Stores`: store master data with location, format, cluster, and operating attributes.
- `Promotions`: campaign metadata that can be joined to transaction lines through `promotionId`.
- `DailyStoreSummary`: a derived summary sheet that helps demo cross-sheet validation and aggregate queries.

The workbook is generated from [tools/generate-retail-demo-workbook.ts](/Users/perssonisak/Projects/private/self-updating-database/tools/generate-retail-demo-workbook.ts) so the fixture remains reproducible.

The mock query-log fixtures are generated from [tools/generate-retail-demo-query-log-workbook.ts](/Users/perssonisak/Projects/private/self-updating-database/tools/generate-retail-demo-query-log-workbook.ts).
