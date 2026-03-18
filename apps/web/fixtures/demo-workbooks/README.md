# Demo workbooks

`retailer-transactions-demo.xlsx` is a realistic multi-sheet retail transaction workbook for demos, ingestion checks, and query walkthroughs.

Sheets:

- `Transactions`: line-level sales and returns with store, item, pricing, VAT, promotion, payment, and channel fields.
- `Items`: item master data with category, department, brand, VAT, supplier, and storage attributes.
- `Stores`: store master data with location, format, cluster, and operating attributes.
- `Promotions`: campaign metadata that can be joined to transaction lines through `promotionId`.
- `DailyStoreSummary`: a derived summary sheet that helps demo cross-sheet validation and aggregate queries.

The workbook is generated from [tools/generate-retail-demo-workbook.ts](/Users/perssonisak/Projects/private/self-updating-database/tools/generate-retail-demo-workbook.ts) so the fixture remains reproducible.
