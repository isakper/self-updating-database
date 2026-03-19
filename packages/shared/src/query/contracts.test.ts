import { describe, expect, it } from "vitest";

import type {
  NaturalLanguageQueryRequest,
  NaturalLanguageQueryResponse,
} from "./contracts.js";

describe("query contracts", () => {
  it("defines the initial natural language query request shape", () => {
    const request: NaturalLanguageQueryRequest = {
      prompt: "Show total revenue by region",
      sourceDatasetId: "dataset_123",
    };

    expect(request).toStrictEqual({
      prompt: "Show total revenue by region",
      sourceDatasetId: "dataset_123",
    });
  });

  it("defines the initial natural language query response shape", () => {
    const response: NaturalLanguageQueryResponse = {
      generatedSqlRecord: {
        generatedAt: "2026-03-18T10:00:00.000Z",
        generationStartedAt: "2026-03-18T09:59:58.900Z",
        generationLatencyMs: 1200,
        generator: "openai_responses",
        sqlText:
          "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
        summaryMarkdown: "Aggregates revenue by region from clean_orders.",
      },
      queryLog: {
        cleanDatabaseId: "clean_db_123",
        errorMessage: null,
        executionFinishedAt: "2026-03-18T10:00:01.500Z",
        executionLatencyMs: 300,
        executionStartedAt: "2026-03-18T10:00:01.200Z",
        generatedSql:
          "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
        generationFinishedAt: "2026-03-18T10:00:01.100Z",
        generationLatencyMs: 1100,
        generationStartedAt: "2026-03-18T09:59:58.900Z",
        matchedClusterId: null,
        optimizationEligible: null,
        patternFingerprint: null,
        patternSummaryJson: null,
        patternVersion: null,
        prompt: "Show total revenue by region",
        queryKind: null,
        queryLogId: "query_log_123",
        resultColumnNames: ["region", "total_amount"],
        rowCount: 2,
        sourceDatasetId: "dataset_123",
        status: "succeeded",
        summaryMarkdown: "Aggregates revenue by region from clean_orders.",
        totalLatencyMs: 1500,
        usedOptimizationObjects: [],
      },
      result: {
        columnNames: ["region", "total_amount"],
        rows: [
          ["North", 1200],
          ["South", 800],
        ],
      },
    };

    expect(response.queryLog.status).toBe("succeeded");
    expect(response.result?.columnNames).toStrictEqual([
      "region",
      "total_amount",
    ]);
  });
});
