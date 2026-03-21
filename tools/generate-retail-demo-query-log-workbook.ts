import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import { utils, writeFile, type WorkBook } from "xlsx";

type QueryPatternType = "sku_daily_rollup" | "sku_daily_rollup_zero_fill";

type Scenario = {
  dateEnd: string;
  dateStart: string;
  expectedSparseDays: number;
  label: string;
  skuList: string[];
};

type MockQueryLogRow = {
  cleanDatabaseId: string;
  errorMessage: string;
  executionFinishedAt: string;
  executionLatencyMs: number;
  executionStartedAt: string;
  generatedSql: string;
  generationFinishedAt: string;
  generationLatencyMs: number;
  generationStartedAt: string;
  patternType: QueryPatternType;
  prompt: string;
  queryLogId: string;
  resultColumnNamesJson: string;
  rowCount: number;
  sourceDatasetId: string;
  status: "succeeded";
  summaryMarkdown: string;
  totalLatencyMs: number;
  usedOptimizationObjectsJson: string;
};

const workbookPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx"
);
const jsonPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.json"
);

const sourceDatasetId = "dataset_retailer_transactions_demo";
const cleanDatabaseId = "clean_retailer_transactions_demo_v1";
const resultColumnNamesJson = JSON.stringify([
  "item_sku",
  "business_date",
  "units_sold",
  "revenue_incl_vat",
  "cost_ex_vat",
]);
const rollupPromptTemplates = [
  "Show daily units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT) across all stores for {skus} from {dateStart} to {dateEnd}. Exclude returned rows.",
  "Give me a day-by-day sales view for {skus} between {dateStart} and {dateEnd}, including units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT) for the whole business. Exclude returned rows.",
  "I want the daily SKU totals for {skus} over {dateStart} to {dateEnd} with units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT) rolled up across every store. Exclude returned rows.",
  "Can you break out daily units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT) for {skus} from {dateStart} through {dateEnd}, summed across all stores? Exclude returned rows.",
  "Pull a daily all-store SKU summary for {skus} between {dateStart} and {dateEnd} with units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT). Exclude returned rows.",
];
const zeroFillPromptTemplates = [
  "Show the same daily SKU metrics across all stores for {skus} from {dateStart} to {dateEnd}, but include zero rows for days with no sales. Exclude returned rows.",
  "Give me the daily SKU trend for {skus} between {dateStart} and {dateEnd}, and backfill missing dates with zeroes when nothing sold. Exclude returned rows.",
  "I need a complete SKU-by-day series for {skus} from {dateStart} to {dateEnd}, with units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT) set to 0 on no-sale days. Exclude returned rows.",
  "Return the daily all-store metrics for {skus} over {dateStart} to {dateEnd}, making sure dates with no sales still appear as zeroes. Exclude returned rows.",
  "Build the same daily rollup for {skus} from {dateStart} through {dateEnd}, but don’t drop empty days; fill them with 0s instead. Exclude returned rows.",
];

const scenarios: Scenario[] = [
  {
    dateEnd: "2025-01-07",
    dateStart: "2025-01-01",
    expectedSparseDays: 3,
    label: "week 1 beverages",
    skuList: ["SKU-00001", "SKU-00007", "SKU-00013"],
  },
  {
    dateEnd: "2025-01-14",
    dateStart: "2025-01-08",
    expectedSparseDays: 2,
    label: "week 2 snacks",
    skuList: ["SKU-00022", "SKU-00027", "SKU-00031"],
  },
  {
    dateEnd: "2025-01-21",
    dateStart: "2025-01-15",
    expectedSparseDays: 1,
    label: "week 3 dairy",
    skuList: ["SKU-00044", "SKU-00048", "SKU-00053"],
  },
  {
    dateEnd: "2025-01-31",
    dateStart: "2025-01-22",
    expectedSparseDays: 4,
    label: "late january produce",
    skuList: ["SKU-00061", "SKU-00064", "SKU-00069"],
  },
  {
    dateEnd: "2025-02-07",
    dateStart: "2025-02-01",
    expectedSparseDays: 2,
    label: "early february frozen",
    skuList: ["SKU-00078", "SKU-00082", "SKU-00086"],
  },
  {
    dateEnd: "2025-02-14",
    dateStart: "2025-02-08",
    expectedSparseDays: 3,
    label: "mid february bakery",
    skuList: ["SKU-00094", "SKU-00099", "SKU-00104"],
  },
  {
    dateEnd: "2025-02-21",
    dateStart: "2025-02-15",
    expectedSparseDays: 2,
    label: "late february household",
    skuList: ["SKU-00111", "SKU-00115", "SKU-00119"],
  },
  {
    dateEnd: "2025-02-28",
    dateStart: "2025-02-22",
    expectedSparseDays: 4,
    label: "end february health",
    skuList: ["SKU-00127", "SKU-00132", "SKU-00137"],
  },
  {
    dateEnd: "2025-03-15",
    dateStart: "2025-03-01",
    expectedSparseDays: 5,
    label: "march first half pantry",
    skuList: ["SKU-00146", "SKU-00152", "SKU-00158"],
  },
  {
    dateEnd: "2025-03-31",
    dateStart: "2025-03-16",
    expectedSparseDays: 6,
    label: "march second half drinks",
    skuList: ["SKU-00163", "SKU-00169", "SKU-00175"],
  },
];

const rows = buildRows();
const workbook = createWorkbook(rows);

mkdirSync(dirname(workbookPath), { recursive: true });
writeFile(workbook, workbookPath);
writeFileSync(jsonPath, `${JSON.stringify(rows, null, 2)}\n`, "utf8");

console.log(
  `Wrote ${rows.length} mock query logs to ${workbookPath} and ${jsonPath}`
);

function buildRows(): MockQueryLogRow[] {
  return scenarios.flatMap((scenario, index) => {
    const baseTimestamp = new Date(Date.UTC(2026, 2, 18, 10, index * 9, 0, 0));
    const rollupGenerationLatencyMs = 1200 + index * 70;
    const rollupExecutionLatencyMs = 260 + index * 28;
    const zeroFillGenerationLatencyMs = 1480 + index * 85;
    const zeroFillExecutionLatencyMs = 610 + index * 34;

    return [
      buildQueryLogRow({
        executionLatencyMs: rollupExecutionLatencyMs,
        generationLatencyMs: rollupGenerationLatencyMs,
        generatedSql: buildSkuDailyRollupSql(scenario),
        patternType: "sku_daily_rollup",
        prompt: buildRollupPrompt(scenario, index),
        queryLogId: `query_log_rollup_${String(index + 1).padStart(2, "0")}`,
        rowCount:
          inclusiveDayCount(scenario.dateStart, scenario.dateEnd) *
            scenario.skuList.length -
          scenario.expectedSparseDays,
        startedAt: baseTimestamp,
        summaryMarkdown:
          "Aggregates units sold, revenue, and cost by SKU and business day across all stores.",
      }),
      buildQueryLogRow({
        executionLatencyMs: zeroFillExecutionLatencyMs,
        generationLatencyMs: zeroFillGenerationLatencyMs,
        generatedSql: buildSkuDailyZeroFillSql(scenario),
        patternType: "sku_daily_rollup_zero_fill",
        prompt: buildZeroFillPrompt(scenario, index),
        queryLogId: `query_log_zero_fill_${String(index + 1).padStart(2, "0")}`,
        rowCount:
          inclusiveDayCount(scenario.dateStart, scenario.dateEnd) *
          scenario.skuList.length,
        startedAt: new Date(baseTimestamp.getTime() + 4 * 60 * 1000),
        summaryMarkdown:
          "Builds a complete SKU-by-day series and fills missing sales days with zeroes.",
      }),
    ];
  });
}

function buildQueryLogRow(options: {
  executionLatencyMs: number;
  generationLatencyMs: number;
  generatedSql: string;
  patternType: QueryPatternType;
  prompt: string;
  queryLogId: string;
  rowCount: number;
  startedAt: Date;
  summaryMarkdown: string;
}): MockQueryLogRow {
  const generationFinishedAt = new Date(
    options.startedAt.getTime() + options.generationLatencyMs
  );
  const executionStartedAt = new Date(generationFinishedAt.getTime() + 40);
  const executionFinishedAt = new Date(
    executionStartedAt.getTime() + options.executionLatencyMs
  );

  return {
    cleanDatabaseId,
    errorMessage: "",
    executionFinishedAt: executionFinishedAt.toISOString(),
    executionLatencyMs: options.executionLatencyMs,
    executionStartedAt: executionStartedAt.toISOString(),
    generatedSql: options.generatedSql,
    generationFinishedAt: generationFinishedAt.toISOString(),
    generationLatencyMs: options.generationLatencyMs,
    generationStartedAt: options.startedAt.toISOString(),
    patternType: options.patternType,
    prompt: options.prompt,
    queryLogId: options.queryLogId,
    resultColumnNamesJson,
    rowCount: options.rowCount,
    sourceDatasetId,
    status: "succeeded",
    summaryMarkdown: options.summaryMarkdown,
    totalLatencyMs: options.generationLatencyMs + options.executionLatencyMs,
    usedOptimizationObjectsJson: "[]",
  };
}

function buildRollupPrompt(scenario: Scenario, index: number): string {
  return fillPromptTemplate(
    rollupPromptTemplates[index % rollupPromptTemplates.length] ?? "",
    scenario
  );
}

function buildZeroFillPrompt(scenario: Scenario, index: number): string {
  return fillPromptTemplate(
    zeroFillPromptTemplates[index % zeroFillPromptTemplates.length] ?? "",
    scenario
  );
}

function fillPromptTemplate(template: string, scenario: Scenario): string {
  return template
    .replaceAll("{skus}", scenario.skuList.join(", "))
    .replaceAll("{dateStart}", scenario.dateStart)
    .replaceAll("{dateEnd}", scenario.dateEnd);
}

function buildSkuDailyRollupSql(scenario: Scenario): string {
  return [
    "SELECT",
    "  item_sku,",
    "  business_date,",
    "  SUM(CASE WHEN is_return = 0 THEN units_gross ELSE 0 END) AS units_sold,",
    "  SUM(CASE WHEN is_return = 0 THEN gross_sales_incl_vat ELSE 0 END) AS revenue_incl_vat,",
    "  SUM(CASE WHEN is_return = 0 THEN cogs_ex_vat ELSE 0 END) AS cost_ex_vat",
    "FROM clean_transactions",
    `WHERE business_date >= DATE('${scenario.dateStart}')`,
    `  AND business_date <= DATE('${scenario.dateEnd}')`,
    `  AND item_sku IN (${quoteList(scenario.skuList)})`,
    "GROUP BY item_sku, business_date",
    "ORDER BY item_sku, business_date;",
  ].join("\n");
}

function buildSkuDailyZeroFillSql(scenario: Scenario): string {
  return [
    "WITH date_spine AS (",
    "  SELECT DISTINCT",
    "    business_date,",
    "    1 AS join_key",
    "  FROM clean_transactions",
    `  WHERE business_date >= DATE('${scenario.dateStart}')`,
    `    AND business_date <= DATE('${scenario.dateEnd}')`,
    "),",
    "sku_scope(item_sku, join_key) AS (",
    "  VALUES",
    scenario.skuList
      .map(
        (sku, index) =>
          `    ('${sku}', 1)${index === scenario.skuList.length - 1 ? "" : ","}`
      )
      .join("\n"),
    "),",
    "sku_day_spine AS (",
    "  SELECT",
    "    sku_scope.item_sku,",
    "    date_spine.business_date,",
    "    sku_scope.item_sku || '|' || date_spine.business_date AS sku_day_key",
    "  FROM sku_scope",
    "  JOIN date_spine ON date_spine.join_key = sku_scope.join_key",
    "),",
    "daily_sales AS (",
    "  SELECT",
    "    item_sku,",
    "    business_date,",
    "    item_sku || '|' || business_date AS sku_day_key,",
    "    SUM(CASE WHEN is_return = 0 THEN units_gross ELSE 0 END) AS units_sold,",
    "    SUM(CASE WHEN is_return = 0 THEN gross_sales_incl_vat ELSE 0 END) AS revenue_incl_vat,",
    "    SUM(CASE WHEN is_return = 0 THEN cogs_ex_vat ELSE 0 END) AS cost_ex_vat",
    "  FROM clean_transactions",
    `  WHERE business_date >= DATE('${scenario.dateStart}')`,
    `    AND business_date <= DATE('${scenario.dateEnd}')`,
    `    AND item_sku IN (${quoteList(scenario.skuList)})`,
    "  GROUP BY item_sku, business_date",
    ")",
    "SELECT",
    "  sku_day_spine.item_sku,",
    "  sku_day_spine.business_date,",
    "  COALESCE(daily_sales.units_sold, 0) AS units_sold,",
    "  COALESCE(daily_sales.revenue_incl_vat, 0) AS revenue_incl_vat,",
    "  COALESCE(daily_sales.cost_ex_vat, 0) AS cost_ex_vat",
    "FROM sku_day_spine",
    "LEFT JOIN daily_sales",
    "  ON daily_sales.sku_day_key = sku_day_spine.sku_day_key",
    "ORDER BY sku_day_spine.item_sku, sku_day_spine.business_date;",
  ].join("\n");
}

function quoteList(values: string[]): string {
  return values.map((value) => `'${value}'`).join(", ");
}

function inclusiveDayCount(dateStart: string, dateEnd: string): number {
  const start = new Date(`${dateStart}T00:00:00.000Z`);
  const end = new Date(`${dateEnd}T00:00:00.000Z`);
  const millisecondsPerDay = 24 * 60 * 60 * 1000;

  return Math.floor((end.getTime() - start.getTime()) / millisecondsPerDay) + 1;
}

function createWorkbook(rows: MockQueryLogRow[]): WorkBook {
  const workbook = utils.book_new();
  const queryLogsSheet = utils.json_to_sheet(rows);

  utils.book_append_sheet(workbook, queryLogsSheet, "query_logs");

  return workbook;
}
