import type {
  SourceDataset,
  SourceRow,
} from "../../../../packages/database-core/src/index.js";
import type {
  SourceDatasetProfile,
  SourceSheetProfile,
} from "../../../../packages/agent-orchestrator/src/index.js";

const MAX_SAMPLE_ROWS_PER_SHEET = 12;
const MAX_SAMPLE_VALUES_PER_COLUMN = 5;

export function buildSourceDatasetProfile(
  dataset: SourceDataset
): SourceDatasetProfile {
  return {
    sourceDatasetId: dataset.id,
    totalRowCount: dataset.sheets.reduce(
      (totalRows, sheet) => totalRows + sheet.rows.length,
      0
    ),
    workbookName: dataset.workbookName,
    sheetProfiles: dataset.sheets.map((sheet) => ({
      columnProfiles: sheet.columns.map((columnName) =>
        buildColumnProfile(columnName, sheet.rows)
      ),
      rowCount: sheet.rows.length,
      sampleRows: sampleRows(sheet.rows).map((row) => row.values),
      sheetName: sheet.name,
      sourceTableName: sheet.sourceTableName,
    })),
  };
}

function buildColumnProfile(
  columnName: string,
  rows: SourceRow[]
): SourceSheetProfile["columnProfiles"][number] {
  const values = rows
    .map((row) => row.values[columnName] ?? null)
    .filter((value) => value !== null);
  const nullCount = rows.length - values.length;
  const stringifiedDistinctValues = [...new Set(values.map(formatCellValue))];

  return {
    columnName,
    inferredType: inferColumnType(values),
    nonNullCount: values.length,
    nullCount,
    sampleValues: stringifiedDistinctValues.slice(
      0,
      MAX_SAMPLE_VALUES_PER_COLUMN
    ),
  };
}

function inferColumnType(
  values: Array<boolean | number | string | null>
): SourceSheetProfile["columnProfiles"][number]["inferredType"] {
  if (values.length === 0) {
    return "empty";
  }

  const seenTypes = new Set(
    values.map((value) => {
      if (typeof value === "boolean") {
        return "boolean";
      }

      if (typeof value === "number") {
        return "number";
      }

      if (typeof value === "string") {
        return isDateLike(value) ? "date-like" : "string";
      }

      return "empty";
    })
  );

  return seenTypes.size === 1
    ? (seenTypes.values().next()
        .value as SourceSheetProfile["columnProfiles"][number]["inferredType"])
    : "mixed";
}

function sampleRows(rows: SourceRow[]): SourceRow[] {
  if (rows.length <= MAX_SAMPLE_ROWS_PER_SHEET) {
    return rows;
  }

  const half = Math.floor(MAX_SAMPLE_ROWS_PER_SHEET / 2);
  return [...rows.slice(0, half), ...rows.slice(-half)];
}

function isDateLike(value: string): boolean {
  const trimmed = value.trim();

  return (
    /^\d{4}[-/]\d{1,2}[-/]\d{1,2}$/.test(trimmed) ||
    /^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$/.test(trimmed)
  );
}

function formatCellValue(value: boolean | number | string | null): string {
  if (value === null) {
    return "NULL";
  }

  return String(value);
}
