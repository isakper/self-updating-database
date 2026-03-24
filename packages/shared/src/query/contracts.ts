export interface NaturalLanguageQueryRequest {
  prompt: string;
  reasoningMode?: QueryReasoningMode;
  sourceDatasetId: string;
}

export type QueryReasoningMode = "standard" | "deliberate";

export type QueryExecutionStatus = "succeeded" | "failed";
export type SqlQueryKind = "aggregate" | "detail";

export type QueryResultCellValue = boolean | number | string | null;

export interface SqlQueryPatternSummary {
  aggregates: string[];
  cleanDatabaseId: string;
  filters: string[];
  groupBy: string[];
  joins: string[];
  optimizationEligible: boolean;
  orderBy: string[];
  patternFingerprint: string;
  patternVersion: number;
  queryKind: SqlQueryKind;
  relations: string[];
}

export interface QueryCluster {
  averageExecutionLatencyMs: number;
  cleanDatabaseId: string;
  cumulativeExecutionLatencyMs: number;
  latestOptimizationDecision: OptimizationRevisionDecision | null;
  latestOptimizationRevisionId: string | null;
  latestQueryLogId: string;
  latestSeenAt: string;
  patternFingerprint: string;
  patternSummary: SqlQueryPatternSummary;
  patternVersion: number;
  queryClusterId: string;
  queryCount: number;
  representativeQueryLogIds: string[];
  sourceDatasetId: string;
}

export interface OptimizationHint {
  guidance: string;
  preferredObjects: string[];
  queryClusterId: string;
  title: string;
}

export interface OptimizationCandidateSet {
  baseCleanDatabaseId: string;
  basePipelineVersionId: string;
  candidateSetFingerprint: string;
  queryClusters: QueryCluster[];
  sourceDatasetId: string;
}

export type OptimizationRevisionDecision = "no_change" | "pipeline_revision";
export type OptimizationRevisionStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed";
export type OptimizationFailureReasonCode =
  | "artifact_contract"
  | "missing_artifacts"
  | "process_exit"
  | "retry_exhausted"
  | "runtime_error"
  | "sql_validation"
  | "startup_failure"
  | "timeout";

export interface OptimizationRevision {
  analysisJson: Record<string, unknown>;
  appliedCleanDatabaseId: string | null;
  baseCleanDatabaseId: string;
  basePipelineVersionId: string;
  candidatePipelineVersionId: string | null;
  candidateSet: OptimizationCandidateSet;
  createdAt: string;
  errorMessage: string | null;
  failureReasonCode: OptimizationFailureReasonCode | null;
  optimizationHints: OptimizationHint[];
  optimizationRevisionId: string;
  promptMarkdown: string;
  sourceDatasetId: string;
  status: OptimizationRevisionStatus;
  summaryMarkdown: string;
  decision: OptimizationRevisionDecision;
  updatedAt: string;
}

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
  isBenchmarkLog?: boolean;
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
  matchedClusterId: string | null;
  optimizationEligible: boolean | null;
  patternFingerprint: string | null;
  patternSummaryJson: SqlQueryPatternSummary | null;
  patternVersion: number | null;
  queryKind: SqlQueryKind | null;
  resultColumnNames: string[];
  resultRowsSample?: QueryResultCellValue[][] | null;
  rowCount: number | null;
  sourceDatasetId: string;
  status: QueryExecutionStatus;
  summaryMarkdown: string | null;
  totalLatencyMs: number;
  usedOptimizationObjects: string[];
}

export interface NaturalLanguageQueryResponse {
  generatedSqlRecord: GeneratedSqlRecord | null;
  queryLog: QueryExecutionLog;
  result: QueryExecutionResult | null;
}
