import { describe, expect, it } from "vitest";

import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";
import { renderUploadWorkspacePage } from "./page.js";

describe("renderUploadWorkspacePage", () => {
  it("renders import summaries into the workspace page", () => {
    const summary: WorkbookImportSummary = {
      processing: {
        cleanDatabase: {
          builtAt: "2026-03-17T15:01:00.000Z",
          cleanDatabaseId: "clean_db_1",
          databaseFilePath: ".data/clean.sqlite",
        },
        cleanDatabaseStatus: "succeeded",
        lastPipelineError: null,
        nextRetryAt: null,
        pipelineRetryCount: 0,
        pipelineRun: null,
        pipelineStatus: "succeeded",
        pipelineVersion: {
          analysisJson: {
            findings: [],
            sourceDatasetId: "dataset_1",
            summary: "summary",
          },
          createdAt: "2026-03-17T15:00:30.000Z",
          createdBy: "codex_cli",
          pipelineId: "pipeline_dataset_1",
          pipelineVersionId: "pipeline_version_1",
          promptMarkdown: "prompt",
          sourceDatasetId: "dataset_1",
          sqlText: "SELECT 1;",
          summaryMarkdown: "summary",
        },
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
    expect(html).toContain("Pipeline version pipeline_version_1");
    expect(html).toContain("Natural-language query");
    expect(html).toContain("Run query");
    expect(html).toContain("Live SQL generation");
    expect(html).toContain('id="query-stream-output"');
    expect(html).toContain("Codex prompt");
    expect(html).toContain("Pipeline SQL");
    expect(html).toContain('id="import-result-root"');
    expect(html).toContain('id="query-workspace-root"');
    expect(html).toContain("EventSource('/events/' + datasetId)");
    expect(html).not.toContain('http-equiv="refresh"');
  });
});
