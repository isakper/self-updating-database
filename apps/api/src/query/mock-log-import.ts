import type {
  QueryExecutionLog,
  WorkbookCellValue,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";

export function importQueryLogsFromWorkbook(options: {
  cleanDatabaseId: string;
  createId: (prefix: string) => string;
  sourceDatasetId: string;
  workbook: WorkbookUploadRequest;
}): QueryExecutionLog[] {
  const sheet = selectQueryLogSheet(options.workbook);

  return sheet.rows.map((row, index) =>
    createImportedQueryLog({
      cleanDatabaseId: options.cleanDatabaseId,
      createId: options.createId,
      row,
      rowIndex: index,
      sourceDatasetId: options.sourceDatasetId,
    })
  );
}

function selectQueryLogSheet(
  workbook: WorkbookUploadRequest
): WorkbookUploadRequest["sheets"][number] {
  const preferredSheet = workbook.sheets.find(
    (sheet) => normalizeKey(sheet.name) === "querylogs"
  );

  if (preferredSheet) {
    return preferredSheet;
  }

  const compatibleSheet = workbook.sheets.find((sheet) =>
    sheet.rows.some(
      (row) =>
        readRequiredString(row, ["prompt"], false) !== null &&
        readRequiredString(row, ["generatedSql", "generated_sql"], false) !==
          null
    )
  );

  if (!compatibleSheet) {
    throw new Error(
      "Query log workbook must include a query_logs sheet or a sheet with prompt and generatedSql columns."
    );
  }

  return compatibleSheet;
}

function createImportedQueryLog(options: {
  cleanDatabaseId: string;
  createId: (prefix: string) => string;
  row: Record<string, WorkbookCellValue>;
  rowIndex: number;
  sourceDatasetId: string;
}): QueryExecutionLog {
  const prompt = readRequiredString(options.row, ["prompt"])!;
  const generatedSql = readRequiredString(options.row, [
    "generatedSql",
    "generated_sql",
    "sqlText",
    "sql_text",
  ])!;
  const status =
    readRequiredString(options.row, ["status"], false) ?? "succeeded";

  if (status !== "succeeded" && status !== "failed") {
    throw new Error(
      `Query log row ${options.rowIndex + 1} has invalid status "${status}".`
    );
  }

  const generationStartedAt = readRequiredIsoString(options.row, [
    "generationStartedAt",
    "generation_started_at",
  ]);
  const generationLatencyMs =
    readOptionalNumber(options.row, [
      "generationLatencyMs",
      "generation_latency_ms",
    ]) ?? 0;
  const generationFinishedAt =
    readOptionalIsoString(options.row, [
      "generationFinishedAt",
      "generation_finished_at",
    ]) ?? addMilliseconds(generationStartedAt, generationLatencyMs);
  const executionLatencyMs =
    readOptionalNumber(options.row, [
      "executionLatencyMs",
      "execution_latency_ms",
    ]) ?? (status === "succeeded" ? 0 : null);
  const executionStartedAt =
    readOptionalIsoString(options.row, [
      "executionStartedAt",
      "execution_started_at",
    ]) ?? (status === "succeeded" ? generationFinishedAt : null);
  const executionFinishedAt =
    readOptionalIsoString(options.row, [
      "executionFinishedAt",
      "execution_finished_at",
    ]) ??
    (status === "succeeded" &&
    executionStartedAt !== null &&
    executionLatencyMs !== null
      ? addMilliseconds(executionStartedAt, executionLatencyMs)
      : null);
  const totalLatencyMs =
    readOptionalNumber(options.row, ["totalLatencyMs", "total_latency_ms"]) ??
    generationLatencyMs + (executionLatencyMs ?? 0);
  const errorMessage = readNullableString(options.row, [
    "errorMessage",
    "error_message",
  ]);
  const summaryMarkdown = readNullableString(options.row, [
    "summaryMarkdown",
    "summary_markdown",
  ]);
  const rowCount =
    readOptionalNumber(options.row, ["rowCount", "row_count"]) ?? null;

  return {
    cleanDatabaseId: options.cleanDatabaseId,
    isBenchmarkLog: true,
    errorMessage,
    executionFinishedAt,
    executionLatencyMs,
    executionStartedAt,
    generatedSql,
    generationFinishedAt,
    generationLatencyMs,
    generationStartedAt,
    matchedClusterId: null,
    optimizationEligible: null,
    patternFingerprint: null,
    patternSummaryJson: null,
    patternVersion: null,
    prompt,
    queryKind: null,
    queryLogId: options.createId("query_log"),
    resultColumnNames: readStringArrayJson(options.row, [
      "resultColumnNamesJson",
      "result_column_names_json",
    ]),
    rowCount,
    sourceDatasetId: options.sourceDatasetId,
    status,
    summaryMarkdown,
    totalLatencyMs,
    usedOptimizationObjects: readStringArrayJson(options.row, [
      "usedOptimizationObjectsJson",
      "used_optimization_objects_json",
    ]),
  };
}

function readStringArrayJson(
  row: Record<string, WorkbookCellValue>,
  aliases: string[]
): string[] {
  const rawValue = readRequiredString(row, aliases, false);

  if (rawValue === null || rawValue.trim() === "") {
    return [];
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(rawValue);
  } catch {
    throw new Error(
      `Expected ${aliases[0] ?? "json array"} to contain valid JSON.`
    );
  }

  if (
    !Array.isArray(parsed) ||
    !parsed.every((value) => typeof value === "string")
  ) {
    throw new Error(
      `Expected ${aliases[0] ?? "json array"} to be a JSON string array.`
    );
  }

  return parsed;
}

function readRequiredIsoString(
  row: Record<string, WorkbookCellValue>,
  aliases: string[]
): string {
  const value = readRequiredString(row, aliases);

  if (value === null) {
    throw new Error(`Expected ${aliases[0] ?? "timestamp"} to be provided.`);
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    throw new Error(
      `Expected ${aliases[0] ?? "timestamp"} to be an ISO timestamp.`
    );
  }

  return date.toISOString();
}

function readOptionalIsoString(
  row: Record<string, WorkbookCellValue>,
  aliases: string[]
): string | null {
  const value = readRequiredString(row, aliases, false);

  if (value === null) {
    return null;
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    throw new Error(
      `Expected ${aliases[0] ?? "timestamp"} to be an ISO timestamp.`
    );
  }

  return date.toISOString();
}

function readOptionalNumber(
  row: Record<string, WorkbookCellValue>,
  aliases: string[]
): number | null {
  const value = readField(row, aliases);

  if (value === undefined || value === null || value === "") {
    return null;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  throw new Error(`Expected ${aliases[0] ?? "number"} to be numeric.`);
}

function readNullableString(
  row: Record<string, WorkbookCellValue>,
  aliases: string[]
): string | null {
  const value = readRequiredString(row, aliases, false);

  if (value === null || value.trim() === "") {
    return null;
  }

  return value;
}

function readRequiredString(
  row: Record<string, WorkbookCellValue>,
  aliases: string[],
  required = true
): string | null {
  const value = readField(row, aliases);

  if (value === undefined || value === null || value === "") {
    if (required) {
      throw new Error(`Expected ${aliases[0] ?? "field"} to be provided.`);
    }

    return null;
  }

  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  throw new Error(`Expected ${aliases[0] ?? "field"} to be a scalar value.`);
}

function readField(
  row: Record<string, WorkbookCellValue>,
  aliases: string[]
): WorkbookCellValue | undefined {
  const entries = new Map(
    Object.entries(row).map(([key, value]) => [normalizeKey(key), value])
  );

  for (const alias of aliases) {
    const value = entries.get(normalizeKey(alias));

    if (value !== undefined) {
      return value;
    }
  }

  return undefined;
}

function normalizeKey(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function addMilliseconds(isoTimestamp: string, milliseconds: number): string {
  return new Date(
    new Date(isoTimestamp).getTime() + milliseconds
  ).toISOString();
}
