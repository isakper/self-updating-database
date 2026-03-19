export type WorkbookCellValue = boolean | number | string | null;

export interface WorkbookSheetInputRow {
  [columnName: string]: WorkbookCellValue;
}

export interface WorkbookSheetInput {
  name: string;
  rows: WorkbookSheetInputRow[];
}

export interface WorkbookUploadRequest {
  workbookName: string;
  sheets: WorkbookSheetInput[];
}

export type WorkbookImportStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed";

export type PipelineStatus = "queued" | "running" | "succeeded" | "failed";

export type CleanDatabaseStatus = "queued" | "running" | "succeeded" | "failed";

export type CodexRunScope = "pipeline" | "query" | "optimization";

export type CodexRunStream = "stderr" | "stdout" | "system";

export interface SourceSheetSummary {
  sheetName: string;
  columnNames: string[];
  sourceTableName: string;
  rowCount: number;
}

export interface CodexAnalysisFinding {
  confidence: "low" | "medium" | "high";
  kind: string;
  message: string;
  proposedFix: string;
  target: string;
}

export interface CodexAnalysisArtifact {
  findings: CodexAnalysisFinding[];
  sourceDatasetId: string;
  summary: string;
}

export interface PipelineVersionRecord {
  analysisJson: CodexAnalysisArtifact;
  createdAt: string;
  createdBy: "codex_cli";
  pipelineId: string;
  pipelineVersionId: string;
  promptMarkdown: string;
  sourceDatasetId: string;
  sqlText: string;
  summaryMarkdown: string;
}

export interface PipelineRunRecord {
  pipelineVersionId: string;
  retryCount: number;
  runError: string | null;
  runFinishedAt: string | null;
  runId: string;
  runStartedAt: string;
  sourceDatasetId: string;
  status: PipelineStatus;
}

export interface CleanDatabaseSummary {
  builtAt: string;
  cleanDatabaseId: string;
  databaseFilePath: string;
}

export interface ImportProcessingState {
  cleanDatabase: CleanDatabaseSummary | null;
  cleanDatabaseStatus: CleanDatabaseStatus;
  lastPipelineError: string | null;
  nextRetryAt: string | null;
  pipelineRetryCount: number;
  pipelineRun: PipelineRunRecord | null;
  pipelineStatus: PipelineStatus;
  pipelineVersion: PipelineVersionRecord | null;
}

export interface WorkbookImportSummary {
  processing: ImportProcessingState;
  sourceDatasetId: string;
  workbookName: string;
  status: WorkbookImportStatus;
  sheetCount: number;
  totalRowCount: number;
  sheets: SourceSheetSummary[];
  importedAt: string;
}

export interface CodexRunEvent {
  createdAt: string;
  eventId: string;
  message: string;
  queryLogId: string | null;
  scope: CodexRunScope;
  sourceDatasetId: string;
  stream: CodexRunStream;
}
