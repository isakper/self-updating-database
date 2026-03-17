import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";

export interface UploadWorkspaceModel {
  headline: string;
  statusBadge: WorkbookImportSummary["status"];
  datasetLabel: string;
  sheetBreakdown: string[];
  totalRowsLabel: string;
}

export function buildUploadWorkspaceModel(
  summary: WorkbookImportSummary
): UploadWorkspaceModel {
  return {
    headline: `${summary.workbookName} imported`,
    statusBadge: summary.status,
    datasetLabel: `Source dataset ${summary.sourceDatasetId}`,
    sheetBreakdown: summary.sheets.map(
      (sheet) => `${sheet.sheetName}: ${sheet.rowCount} rows`
    ),
    totalRowsLabel: `${summary.totalRowCount} rows preserved in the immutable source database`,
  };
}
