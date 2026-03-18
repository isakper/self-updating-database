export interface NaturalLanguageQueryRequest {
  prompt: string;
  sourceDatasetId: string;
}

export type QueryExecutionStatus = "succeeded" | "failed";

export type QueryResultCellValue = boolean | number | string | null;

export interface GeneratedSqlRecord {
  generatedAt: string;
  generationStartedAt: string;
  generationLatencyMs: number;
  generator: "codex_cli" | "openai_responses";
  sqlText: string;
  summaryMarkdown: string;
}

export interface QueryExecutionResult {
  columnNames: string[];
  rows: QueryResultCellValue[][];
}

export interface QueryExecutionLog {
  cleanDatabaseId: string;
  errorMessage: string | null;
  executionFinishedAt: string | null;
  executionLatencyMs: number | null;
  executionStartedAt: string | null;
  generatedSql: string | null;
  generationFinishedAt: string | null;
  generationLatencyMs: number | null;
  generationStartedAt: string;
  prompt: string;
  queryLogId: string;
  resultColumnNames: string[];
  rowCount: number | null;
  sourceDatasetId: string;
  status: QueryExecutionStatus;
  summaryMarkdown: string | null;
  totalLatencyMs: number;
}

export interface NaturalLanguageQueryResponse {
  generatedSqlRecord: GeneratedSqlRecord | null;
  queryLog: QueryExecutionLog;
  result: QueryExecutionResult | null;
}
