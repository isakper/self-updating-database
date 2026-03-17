export interface SourceDataset {
  id: string;
  workbookName: string;
  importedAt: string;
  sheets: SourceSheet[];
}

export interface SourceSheet {
  sheetId: string;
  name: string;
  columns: string[];
  rows: SourceRow[];
}

export interface SourceRow {
  rowId: string;
  sourceRowNumber: number;
  values: Record<string, boolean | number | string | null>;
}
