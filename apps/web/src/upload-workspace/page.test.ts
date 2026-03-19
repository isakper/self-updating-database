import { describe, expect, it } from "vitest";

import type {
  QueryExecutionLog,
  WorkbookImportSummary,
} from "../../../../packages/shared/src/index.js";
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

    const queryLogs: QueryExecutionLog[] = [
      {
        cleanDatabaseId: "clean_db_1",
        errorMessage: null,
        executionFinishedAt: "2026-03-17T15:01:05.000Z",
        executionLatencyMs: 12,
        executionStartedAt: "2026-03-17T15:01:04.988Z",
        generatedSql: "SELECT order_id FROM orders LIMIT 1;",
        generationFinishedAt: "2026-03-17T15:01:04.980Z",
        generationLatencyMs: 1800,
        generationStartedAt: "2026-03-17T15:01:03.180Z",
        matchedClusterId: null,
        optimizationEligible: null,
        patternFingerprint: null,
        patternSummaryJson: null,
        patternVersion: null,
        prompt: "fetch first row",
        queryKind: null,
        queryLogId: "query_log_1",
        resultColumnNames: ["order_id"],
        rowCount: 1,
        sourceDatasetId: "dataset_1",
        status: "succeeded",
        summaryMarkdown: null,
        totalLatencyMs: 1812,
        usedOptimizationObjects: [],
      },
    ];

    const html = renderUploadWorkspacePage({
      importSummary: summary,
      queryLogs,
    });

    expect(html).toContain("Upload Excel + Build Clean Database");
    expect(html).toContain("Ask Questions in Plain English");
    expect(html).toContain("Query History + SQL Logs");
    expect(html).toContain("DB Walkthrough + Flow Diagram");
    expect(html).toContain("Workbook:");
    expect(html).toContain("sales-workbook.xlsx");
    expect(html).toContain("Upload Excel as DB");
    expect(html).toContain("Generated Pipeline");
    expect(html).toContain("Pipeline version:");
    expect(html).toContain("pipeline_version_1");
    expect(html).toContain("Natural-language query");
    expect(html).toContain("Run query");
    expect(html).toContain("Live SQL generation");
    expect(html).toContain("Live Codex CLI output");
    expect(html).toContain("Recent query history");
    expect(html).toContain("Upload mock query-log workbook.");
    expect(html).toContain("Upload mock query logs");
    expect(html).toContain("Rerun pipeline");
    expect(html).toContain("Run optimization now");
    expect(html).toContain("Retry latest failed optimization");
    expect(html).toContain("SELECT order_id FROM orders LIMIT 1;");
    expect(html).toContain('id="query-stream-output"');
    expect(html).toContain("Pipeline SQL");
    expect(html).toContain("Codex findings");
    expect(html).toContain('id="import-result-root"');
    expect(html).toContain('id="query-workspace-root"');
    expect(html).toContain('id="query-logs-root"');
    expect(html).toContain('id="database-demo-root"');
    expect(html).toContain("EventSource('/events/' + datasetId)");
    expect(html).toContain("setActiveTab('query')");
    expect(html).not.toContain('http-equiv="refresh"');
    expect(html).not.toContain("Start here: import the Excel workbook");
    expect(html).not.toContain("Upload a multi-sheet Excel workbook");
    expect(html).not.toContain("This is the live Codex CLI stream");
  });
});
