import { describe, expect, it } from "vitest";

import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";
import { buildUploadWorkspaceModel } from "./model.js";

describe("upload workspace model", () => {
  it("maps import summaries into upload workspace copy", () => {
    const summary: WorkbookImportSummary = {
      sourceDatasetId: "dataset_123",
      workbookName: "forecast.xlsx",
      status: "succeeded",
      sheetCount: 2,
      totalRowCount: 10,
      importedAt: "2026-03-17T14:00:00.000Z",
      sheets: [
        {
          sheetName: "Forecast",
          columnNames: ["Month", "Revenue"],
          rowCount: 6,
        },
        {
          sheetName: "Assumptions",
          columnNames: ["Key", "Value"],
          rowCount: 4,
        },
      ],
    };

    expect(buildUploadWorkspaceModel(summary)).toStrictEqual({
      headline: "forecast.xlsx imported",
      statusBadge: "succeeded",
      datasetLabel: "Source dataset dataset_123",
      sheetBreakdown: ["Forecast: 6 rows", "Assumptions: 4 rows"],
      totalRowsLabel: "10 rows preserved in the immutable source database",
    });
  });
});
