import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";

export interface UploadWorkspaceModel {
  cleanDatabaseLabel: string;
  cleanDatabaseStatusBadge: WorkbookImportSummary["processing"]["cleanDatabaseStatus"];
  headline: string;
  lastPipelineError: string | null;
  nextRetryLabel: string | null;
  pipelineStatusBadge: WorkbookImportSummary["processing"]["pipelineStatus"];
  pipelineVersionLabel: string;
  statusBadge: WorkbookImportSummary["status"];
  datasetLabel: string;
  sheetBreakdown: string[];
  shouldAutoRefresh: boolean;
  totalRowsLabel: string;
}

export function buildUploadWorkspaceModel(
  summary: WorkbookImportSummary
): UploadWorkspaceModel {
  return {
    cleanDatabaseLabel: summary.processing.cleanDatabase
      ? `Clean database ${summary.processing.cleanDatabase.cleanDatabaseId}`
      : "Clean database not ready yet",
    cleanDatabaseStatusBadge: summary.processing.cleanDatabaseStatus,
    headline: `${summary.workbookName} imported`,
    lastPipelineError: summary.processing.lastPipelineError,
    nextRetryLabel: summary.processing.nextRetryAt
      ? `Next retry at ${summary.processing.nextRetryAt}`
      : null,
    pipelineStatusBadge: summary.processing.pipelineStatus,
    pipelineVersionLabel: summary.processing.pipelineVersion
      ? `Pipeline version ${summary.processing.pipelineVersion.pipelineVersionId}`
      : "Pipeline version pending",
    statusBadge: summary.status,
    datasetLabel: `Source dataset ${summary.sourceDatasetId}`,
    sheetBreakdown: summary.sheets.map(
      (sheet) => `${sheet.sheetName}: ${sheet.rowCount} rows`
    ),
    shouldAutoRefresh:
      summary.processing.pipelineStatus === "queued" ||
      summary.processing.pipelineStatus === "running",
    totalRowsLabel: `${summary.totalRowCount} rows preserved in the immutable source database`,
  };
}
