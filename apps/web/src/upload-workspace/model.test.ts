import { describe, expect, it } from "vitest";

import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";
import { buildUploadWorkspaceModel } from "./model.js";

describe("upload workspace model", () => {
  it("maps import summaries into upload workspace copy", () => {
    const summary: WorkbookImportSummary = {
      processing: {
        cleanDatabase: null,
        cleanDatabaseStatus: "queued",
        lastPipelineError: null,
        nextRetryAt: null,
        pipelineRetryCount: 0,
        pipelineRun: null,
        pipelineStatus: "queued",
        pipelineVersion: null,
      },
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
          sourceTableName: "source_sheet_sheet_1",
          rowCount: 6,
        },
        {
          sheetName: "Assumptions",
          columnNames: ["Key", "Value"],
          sourceTableName: "source_sheet_sheet_2",
          rowCount: 4,
        },
      ],
    };

    expect(buildUploadWorkspaceModel(summary)).toStrictEqual({
      cleanDatabaseLabel: "Clean database not ready yet",
      cleanDatabaseStatusBadge: "queued",
      headline: "forecast.xlsx imported",
      lastPipelineError: null,
      latestQueryLogs: [],
      nextRetryLabel: null,
      pipelineStatusBadge: "queued",
      pipelineVersionLabel: "Pipeline version pending",
      statusBadge: "succeeded",
      datasetLabel: "Source dataset dataset_123",
      sheetBreakdown: ["Forecast: 6 rows", "Assumptions: 4 rows"],
      shouldAutoRefresh: true,
      totalRowsLabel: "10 rows preserved in the immutable source database",
    });
  });
});
