import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import * as XLSX from "xlsx";
import type { WorkBook } from "xlsx";

type QueryPatternType = "sku_daily_rollup" | "sku_daily_rollup_zero_fill";

type Scenario = {
  dateEnd: string;
  dateStart: string;
  label: string;
  skuList: string[];
};

type TransactionWorkbookRow = {
  businessDate?: string;
  itemSku?: string;
  returnFlag?: string;
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
const transactionsWorkbookPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-demo.xlsx"
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
const outputContractSentence =
  "Return exactly one row per SKU per business_date, and output only these columns in this order: item_sku, business_date, units_sold, revenue_incl_vat, cost_ex_vat.";
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
    label: "week 1 beverages",
    skuList: ["SKU-00001", "SKU-00007", "SKU-00013"],
  },
  {
    dateEnd: "2025-01-14",
    dateStart: "2025-01-08",
    label: "week 2 snacks",
    skuList: ["SKU-00022", "SKU-00027", "SKU-00031"],
  },
  {
    dateEnd: "2025-01-21",
    dateStart: "2025-01-15",
    label: "week 3 dairy",
    skuList: ["SKU-00044", "SKU-00048", "SKU-00053"],
  },
  {
    dateEnd: "2025-01-31",
    dateStart: "2025-01-22",
    label: "late january produce",
    skuList: ["SKU-00061", "SKU-00064", "SKU-00069"],
  },
  {
    dateEnd: "2025-02-07",
    dateStart: "2025-02-01",
    label: "early february frozen",
    skuList: ["SKU-00078", "SKU-00082", "SKU-00086"],
  },
  {
    dateEnd: "2025-02-14",
    dateStart: "2025-02-08",
    label: "mid february bakery",
    skuList: ["SKU-00094", "SKU-00099", "SKU-00104"],
  },
  {
    dateEnd: "2025-02-21",
    dateStart: "2025-02-15",
    label: "late february household",
    skuList: ["SKU-00111", "SKU-00115", "SKU-00119"],
  },
  {
    dateEnd: "2025-02-28",
    dateStart: "2025-02-22",
    label: "end february health",
    skuList: ["SKU-00127", "SKU-00132", "SKU-00137"],
  },
  {
    dateEnd: "2025-03-15",
    dateStart: "2025-03-01",
    label: "march first half pantry",
    skuList: ["SKU-00146", "SKU-00152", "SKU-00158"],
  },
  {
    dateEnd: "2025-03-31",
    dateStart: "2025-03-16",
    label: "march second half drinks",
    skuList: ["SKU-00163", "SKU-00169", "SKU-00175"],
  },
];

const rows = buildRows();
const workbook = createWorkbook(rows);

mkdirSync(dirname(workbookPath), { recursive: true });
XLSX.writeFile(workbook, workbookPath);
writeFileSync(jsonPath, `${JSON.stringify(rows, null, 2)}\n`, "utf8");

console.log(
  `Wrote ${rows.length} mock query logs to ${workbookPath} and ${jsonPath}`
);

function buildRows(): MockQueryLogRow[] {
  const transactions = loadTransactions();

  return scenarios.flatMap((scenario, index) => {
    const baseTimestamp = new Date(Date.UTC(2026, 2, 18, 10, index * 9, 0, 0));
    const rollupGenerationLatencyMs = 1200 + index * 70;
    const rollupExecutionLatencyMs = 260 + index * 28;
    const zeroFillGenerationLatencyMs = 1480 + index * 85;
    const zeroFillExecutionLatencyMs = 610 + index * 34;
    const sparseRowCount = countSparseSkuDays(transactions, scenario);
    const zeroFillRowCount =
      inclusiveDayCount(scenario.dateStart, scenario.dateEnd) *
      scenario.skuList.length;
    const useZeroFillContractForRollup = scenario.label === "march first half pantry";
    const rollupPrompt = useZeroFillContractForRollup
      ? buildRollupZeroFillPrompt(scenario)
      : buildRollupPrompt(scenario, index);
    const rollupSql = useZeroFillContractForRollup
      ? buildSkuDailyZeroFillSql(scenario)
      : buildSkuDailyRollupSql(scenario);
    const rollupRowCount = useZeroFillContractForRollup
      ? zeroFillRowCount
      : sparseRowCount;

    return [
      buildQueryLogRow({
        executionLatencyMs: rollupExecutionLatencyMs,
        generationLatencyMs: rollupGenerationLatencyMs,
        generatedSql: rollupSql,
        patternType: "sku_daily_rollup",
        prompt: rollupPrompt,
        queryLogId: `query_log_rollup_${String(index + 1).padStart(2, "0")}`,
        rowCount: rollupRowCount,
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
        rowCount: zeroFillRowCount,
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

function buildRollupZeroFillPrompt(scenario: Scenario): string {
  return (
    `Can you break out daily units sold, gross revenue before returns (incl. VAT), and cost (excl. VAT) for ${scenario.skuList.join(", ")} from ${scenario.dateStart} through ${scenario.dateEnd}, summed across all stores? ` +
    "Exclude returned rows from the metrics, include every business_date in the range for every SKU, and set units_sold, revenue_incl_vat, and cost_ex_vat to 0 when a day has no non-return sales (including return-only days). " +
    outputContractSentence
  );
}

function fillPromptTemplate(template: string, scenario: Scenario): string {
  return (
    template
    .replaceAll("{skus}", scenario.skuList.join(", "))
    .replaceAll("{dateStart}", scenario.dateStart)
    .replaceAll("{dateEnd}", scenario.dateEnd) +
    ` ${outputContractSentence}`
  );
}

function buildSkuDailyRollupSql(scenario: Scenario): string {
  return [
    "SELECT",
    "  item_sku,",
    "  business_date,",
    "  SUM(units) AS units_sold,",
    "  SUM(gross_sales_incl_vat) AS revenue_incl_vat,",
    "  SUM(cogs_ex_vat) AS cost_ex_vat",
    "FROM transactions",
    `WHERE business_date >= DATE('${scenario.dateStart}')`,
    `  AND business_date <= DATE('${scenario.dateEnd}')`,
    `  AND item_sku IN (${quoteList(scenario.skuList)})`,
    "  AND LOWER(COALESCE(return_flag, '')) NOT IN ('yes','y','true','1')",
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
    "  FROM transactions",
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
    "    SUM(units) AS units_sold,",
    "    SUM(gross_sales_incl_vat) AS revenue_incl_vat,",
    "    SUM(cogs_ex_vat) AS cost_ex_vat",
    "  FROM transactions",
    `  WHERE business_date >= DATE('${scenario.dateStart}')`,
    `    AND business_date <= DATE('${scenario.dateEnd}')`,
    `    AND item_sku IN (${quoteList(scenario.skuList)})`,
    "    AND LOWER(COALESCE(return_flag, '')) NOT IN ('yes','y','true','1')",
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

function loadTransactions(): TransactionWorkbookRow[] {
  const workbook = XLSX.read(readFileSync(transactionsWorkbookPath), {
    type: "buffer",
  });
  const transactionsSheet = workbook.Sheets.Transactions;

  if (!transactionsSheet) {
    throw new Error(
      `Could not find Transactions sheet in ${transactionsWorkbookPath}.`
    );
  }

  return XLSX.utils.sheet_to_json<TransactionWorkbookRow>(transactionsSheet, {
    raw: false,
  });
}

function countSparseSkuDays(
  transactions: TransactionWorkbookRow[],
  scenario: Scenario
): number {
  const skuSet = new Set(scenario.skuList);
  const skuDaySet = new Set<string>();

  for (const row of transactions) {
    const businessDate = row.businessDate?.trim();
    const itemSku = row.itemSku?.trim();
    const returnFlag = row.returnFlag?.trim().toLowerCase();

    if (
      !businessDate ||
      !itemSku ||
      businessDate < scenario.dateStart ||
      businessDate > scenario.dateEnd ||
      !skuSet.has(itemSku) ||
      returnFlag === "yes" ||
      returnFlag === "y" ||
      returnFlag === "true" ||
      returnFlag === "1"
    ) {
      continue;
    }

    skuDaySet.add(`${itemSku}|${businessDate}`);
  }

  return skuDaySet.size;
}

function inclusiveDayCount(dateStart: string, dateEnd: string): number {
  const start = new Date(`${dateStart}T00:00:00.000Z`);
  const end = new Date(`${dateEnd}T00:00:00.000Z`);
  const millisecondsPerDay = 24 * 60 * 60 * 1000;

  return Math.floor((end.getTime() - start.getTime()) / millisecondsPerDay) + 1;
}

function createWorkbook(rows: MockQueryLogRow[]): WorkBook {
  const workbook = XLSX.utils.book_new();
  const queryLogsSheet = XLSX.utils.json_to_sheet(rows);

  XLSX.utils.book_append_sheet(workbook, queryLogsSheet, "query_logs");

  return workbook;
}
