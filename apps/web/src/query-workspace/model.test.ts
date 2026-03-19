import { describe, expect, it } from "vitest";

import type { NaturalLanguageQueryResponse } from "../../../../packages/shared/src/index.js";
import { buildQueryWorkspaceModel } from "./model.js";

describe("query workspace model", () => {
  it("maps query responses into renderable workspace copy", () => {
    const queryResponse: NaturalLanguageQueryResponse = {
      generatedSqlRecord: {
        generatedAt: "2026-03-18T12:00:01.000Z",
        generationStartedAt: "2026-03-18T12:00:00.000Z",
        generationLatencyMs: 1000,
        generator: "codex_cli",
        sqlText:
          "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
        summaryMarkdown: "Aggregates revenue by region.",
      },
      queryLog: {
        cleanDatabaseId: "clean_db_1",
        errorMessage: null,
        executionFinishedAt: "2026-03-18T12:00:01.200Z",
        executionLatencyMs: 200,
        executionStartedAt: "2026-03-18T12:00:01.000Z",
        generatedSql:
          "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
        generationFinishedAt: "2026-03-18T12:00:01.000Z",
        generationLatencyMs: 1000,
        generationStartedAt: "2026-03-18T12:00:00.000Z",
        matchedClusterId: null,
        optimizationEligible: null,
        patternFingerprint: null,
        patternSummaryJson: null,
        patternVersion: null,
        prompt: "Show total revenue by region",
        queryKind: null,
        queryLogId: "query_log_1",
        resultColumnNames: ["region", "total_amount"],
        rowCount: 1,
        sourceDatasetId: "dataset_1",
        status: "succeeded",
        summaryMarkdown: "Aggregates revenue by region.",
        totalLatencyMs: 1200,
        usedOptimizationObjects: [],
      },
      result: {
        columnNames: ["region", "total_amount"],
        rows: [["North", 25]],
      },
    };

    expect(
      buildQueryWorkspaceModel({
        prompt: "Show total revenue by region",
        queryResponse,
      })
    ).toStrictEqual({
      errorMessage: null,
      generatedSql:
        "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
      prompt: "Show total revenue by region",
      queryLogLabel: "Query log query_log_1",
      resultColumnNames: ["region", "total_amount"],
      resultRows: [["North", "25"]],
      rowCountLabel: "1 row returned",
      summaryMarkdown: "Aggregates revenue by region.",
      timingLabel: "Generation 1000ms, execution 200ms, total 1200ms",
    });
  });
});
