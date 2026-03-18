import { read, utils, type WorkBook } from "xlsx";

import type {
  WorkbookSheetInput,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";

export function parseWorkbookFile(options: {
  fileBuffer: Buffer;
  fileName: string;
}): WorkbookUploadRequest {
  const workbook: WorkBook = read(options.fileBuffer, {
    type: "buffer",
  });

  const sheets: WorkbookSheetInput[] = workbook.SheetNames.map((sheetName) => {
    const worksheet = workbook.Sheets[sheetName];

    if (!worksheet) {
      return {
        name: sheetName,
        rows: [],
      };
    }

    const rows = utils
      .sheet_to_json<Record<string, unknown>>(worksheet, {
        defval: null,
      })
      .map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([key, value]) => [
            key,
            normalizeCellValue(value),
          ])
        )
      );

    return {
      name: sheetName,
      rows,
    };
  });

  return {
    workbookName: options.fileName,
    sheets,
  };
}

function normalizeCellValue(value: unknown): boolean | number | string | null {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  return JSON.stringify(value);
}
