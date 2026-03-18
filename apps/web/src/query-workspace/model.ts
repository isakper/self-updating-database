import type { NaturalLanguageQueryResponse } from "../../../../packages/shared/src/index.js";

export interface QueryWorkspaceModel {
  errorMessage: string | null;
  generatedSql: string | null;
  prompt: string;
  queryLogLabel: string;
  resultColumnNames: string[];
  resultRows: string[][];
  rowCountLabel: string;
  summaryMarkdown: string | null;
  timingLabel: string;
}

export function buildQueryWorkspaceModel(options: {
  prompt: string;
  queryErrorMessage?: string;
  queryResponse?: NaturalLanguageQueryResponse;
}): QueryWorkspaceModel {
  const queryLog = options.queryResponse?.queryLog;
  const result = options.queryResponse?.result;

  return {
    errorMessage: options.queryErrorMessage ?? queryLog?.errorMessage ?? null,
    generatedSql: options.queryResponse?.generatedSqlRecord?.sqlText ?? null,
    prompt: options.prompt,
    queryLogLabel: queryLog
      ? `Query log ${queryLog.queryLogId}`
      : "Query has not been run yet",
    resultColumnNames: result?.columnNames ?? [],
    resultRows:
      result?.rows.map((row) =>
        row.map((value) => (value === null ? "NULL" : String(value)))
      ) ?? [],
    rowCountLabel:
      queryLog?.rowCount === null || queryLog?.rowCount === undefined
        ? "No result rows returned yet"
        : `${queryLog.rowCount} row${queryLog.rowCount === 1 ? "" : "s"} returned`,
    summaryMarkdown:
      options.queryResponse?.generatedSqlRecord?.summaryMarkdown ?? null,
    timingLabel: queryLog
      ? `Generation ${queryLog.generationLatencyMs ?? 0}ms, execution ${queryLog.executionLatencyMs ?? 0}ms, total ${queryLog.totalLatencyMs}ms`
      : "No query timings yet",
  };
}
