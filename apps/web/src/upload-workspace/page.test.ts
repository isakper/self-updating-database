import { describe, expect, it } from "vitest";

import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";
import { renderUploadWorkspacePage } from "./page.js";

describe("renderUploadWorkspacePage", () => {
  it("renders import summaries into the workspace page", () => {
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
      sourceDatasetId: "dataset_1",
      workbookName: "sales-workbook.xlsx",
      status: "succeeded",
      sheetCount: 1,
      totalRowCount: 2,
      importedAt: "2026-03-17T15:00:00.000Z",
      sheets: [
        {
          sheetName: "Orders",
          columnNames: ["OrderId", "Amount"],
          sourceTableName: "source_sheet_sheet_1",
          rowCount: 2,
        },
      ],
    };

    const html = renderUploadWorkspacePage({ importSummary: summary });

    expect(html).toContain("sales-workbook.xlsx imported");
    expect(html).toContain("Source dataset dataset_1");
    expect(html).toContain("Orders: 2 rows");
    expect(html).toContain("Pipeline version pending");
  });
});
