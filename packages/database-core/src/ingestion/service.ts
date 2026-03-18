import type {
  ImportProcessingState,
  SourceSheetSummary,
  WorkbookImportSummary,
  WorkbookUploadRequest,
} from "../../../shared/src/index.js";
import type { IngestionRepository } from "./repo.js";
import type { SourceDataset, SourceSheet } from "./types.js";

export interface IngestWorkbookOptions {
  repository: IngestionRepository;
  request: WorkbookUploadRequest;
  now?: Date;
  createId?: (prefix: string) => string;
}

export interface IngestWorkbookResult {
  dataset: SourceDataset;
  summary: WorkbookImportSummary;
}

export function ingestWorkbook(
  options: IngestWorkbookOptions
): IngestWorkbookResult {
  const { repository, request } = options;

  if (request.sheets.length === 0) {
    throw new Error("Workbook upload must include at least one sheet.");
  }

  const createId = options.createId ?? defaultCreateId;
  const importedAt = (options.now ?? new Date()).toISOString();
  const datasetId = createId("dataset");

  const sheets = request.sheets.map((sheet) => {
    const columns = deriveColumnNames(sheet.rows);
    const sheetId = createId("sheet");

    return {
      sheetId,
      name: sheet.name,
      columns,
      sourceTableName: `source_sheet_${sheetId}`,
      rows: sheet.rows.map((row, index) => ({
        rowId: createId("row"),
        sourceRowNumber: index + 1,
        values: buildRowValues(columns, row),
      })),
    } satisfies SourceSheet;
  });

  const dataset: SourceDataset = {
    id: datasetId,
    workbookName: request.workbookName,
    importedAt,
    sheets,
  };

  repository.save(dataset);

  return {
    dataset,
    summary: createWorkbookImportSummary(
      dataset,
      createQueuedImportProcessingState()
    ),
  };
}

export function createWorkbookImportSummary(
  dataset: SourceDataset,
  processing: ImportProcessingState
): WorkbookImportSummary {
  const sheets: SourceSheetSummary[] = dataset.sheets.map((sheet) => ({
    sheetName: sheet.name,
    columnNames: sheet.columns,
    sourceTableName: sheet.sourceTableName,
    rowCount: sheet.rows.length,
  }));

  return {
    processing,
    sourceDatasetId: dataset.id,
    workbookName: dataset.workbookName,
    status: "succeeded",
    sheetCount: sheets.length,
    totalRowCount: sheets.reduce((sum, sheet) => sum + sheet.rowCount, 0),
    sheets,
    importedAt: dataset.importedAt,
  };
}

export function createQueuedImportProcessingState(): ImportProcessingState {
  return {
    cleanDatabase: null,
    cleanDatabaseStatus: "queued",
    lastPipelineError: null,
    nextRetryAt: null,
    pipelineRetryCount: 0,
    pipelineRun: null,
    pipelineStatus: "queued",
    pipelineVersion: null,
  };
}

function deriveColumnNames(
  rows: WorkbookUploadRequest["sheets"][number]["rows"]
): string[] {
  const columns = new Set<string>();

  for (const row of rows) {
    for (const columnName of Object.keys(row)) {
      columns.add(columnName);
    }
  }

  return [...columns];
}

function buildRowValues(
  columns: string[],
  row: WorkbookUploadRequest["sheets"][number]["rows"][number]
): Record<string, boolean | number | string | null> {
  return Object.fromEntries(
    columns.map((columnName) => [columnName, row[columnName] ?? null])
  );
}

function defaultCreateId(prefix: string): string {
  return `${prefix}_${Math.random().toString(36).slice(2, 10)}`;
}
