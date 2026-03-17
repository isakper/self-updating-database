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

export interface SourceSheetSummary {
  sheetName: string;
  columnNames: string[];
  rowCount: number;
}

export interface WorkbookImportSummary {
  sourceDatasetId: string;
  workbookName: string;
  status: WorkbookImportStatus;
  sheetCount: number;
  totalRowCount: number;
  sheets: SourceSheetSummary[];
  importedAt: string;
}
