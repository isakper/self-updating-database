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

### Command reference

```bash
pnpm cli dataset list
pnpm cli dataset show <datasetId>
pnpm cli upload workbook <workbook.xlsx>
pnpm cli upload query-logs <datasetId> <query-logs.xlsx>
pnpm cli pipeline run <datasetId>
pnpm cli optimization run <datasetId>
pnpm cli optimization retry-latest-failed <datasetId>
pnpm cli status <datasetId>
pnpm cli status <datasetId> --watch --interval-ms 2000
pnpm cli events <datasetId>
pnpm cli query <datasetId> "show top 10 products by revenue"
```

API base URL defaults to `http://127.0.0.1:3001`.
Override with `--api-base-url <url>` or `API_BASE_URL`.

See detailed CLI notes in [docs/CLI.md](docs/CLI.md).
