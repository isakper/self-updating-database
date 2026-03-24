import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { InMemorySourceDatasetRepository } from "../../../../packages/database-core/src/index.js";
import { ingestWorkbook } from "../../../../packages/database-core/src/ingestion/service.js";
import type { WorkbookUploadRequest } from "../../../../packages/shared/src/index.js";
import { parseWorkbookFile } from "../../../web/src/upload-workspace/parse-workbook-file.js";
import { createQueryApi, QueryApiError } from "./api.js";

describe("query api", () => {
  it("generates SQL, executes against the clean database, and logs the query", async () => {
    const repository = seedRepository();
    const scheduledDatasets: string[] = [];
    const queryApi = createQueryApi({
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
      now: createDeterministicNow([
        "2026-03-18T12:00:00.000Z",
        "2026-03-18T12:00:01.000Z",
        "2026-03-18T12:00:01.100Z",
        "2026-03-18T12:00:01.250Z",
      ]),
      queryLearningLoop: {
        schedule(sourceDatasetId) {
          scheduledDatasets.push(sourceDatasetId);
        },
      },
      queryExecutor: {
        executeQuery() {
          return Promise.resolve({
            columnNames: ["region", "total_amount"],
            rows: [["North", 25]],
          });
        },
      },
      queryGenerator: {
        generateSql(options) {
          options.onDelta?.(
            "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;"
          );
          return Promise.resolve({
            model: "gpt-5-mini",
            sqlText:
              "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
            prompt: "query prompt",
          });
        },
      },
      repository,
      sqlValidator: {
        validate() {
          return {
            errors: [],
            isValid: true,
          };
        },
      },
    });

    const response = await queryApi.runNaturalLanguageQuery({
      prompt: "Show total revenue by region",
      sourceDatasetId: "dataset_1",
    });

    expect(response.generatedSqlRecord?.sqlText).toContain("SELECT region");
    expect(response.generatedSqlRecord?.generator).toBe("openai_responses");
    expect(response.result?.rows).toStrictEqual([["North", 25]]);
    expect(repository.listQueryExecutionLogs("dataset_1")).toHaveLength(1);
    const storedLog = repository.listQueryExecutionLogs("dataset_1")[0];
    expect(storedLog?.cleanDatabaseId).toBe("clean_db_1");
    expect(storedLog?.matchedClusterId).toMatch(/^query_cluster_clean_db_1_/);
    expect(storedLog?.optimizationEligible).toBe(true);
    expect(storedLog?.patternFingerprint).toEqual(expect.any(String));
    expect(storedLog?.queryKind).toBe("aggregate");
    expect(storedLog?.rowCount).toBe(1);
    expect(storedLog?.resultRowsSample).toStrictEqual([["North", 25]]);
    expect(storedLog?.status).toBe("succeeded");
    expect(storedLog?.usedOptimizationObjects).toStrictEqual([]);
    expect(scheduledDatasets).toStrictEqual(["dataset_1"]);
    expect(
      repository
        .listCodexRunEvents("dataset_1")
        .filter((event) => event.scope === "query")
        .map((event) => event.message)
        .join(" ")
    ).toContain("SELECT region");
  });

  it("logs failed query attempts when generated SQL is invalid", async () => {
    const repository = seedRepository();
    const queryApi = createQueryApi({
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
      now: createDeterministicNow([
        "2026-03-18T12:00:00.000Z",
        "2026-03-18T12:00:01.000Z",
        "2026-03-18T12:00:01.050Z",
      ]),
      queryExecutor: {
        executeQuery() {
          return Promise.reject(new Error("should not execute"));
        },
      },
      queryGenerator: {
        generateSql() {
          return Promise.resolve({
            model: "gpt-5-mini",
            sqlText: "DROP TABLE clean_orders;",
            prompt: "query prompt",
          });
        },
      },
      repository,
      sqlValidator: {
        validate() {
          return {
            errors: ["Generated query SQL contains a forbidden SQL statement."],
            isValid: false,
          };
        },
      },
    });

    await expect(
      queryApi.runNaturalLanguageQuery({
        prompt: "Delete all rows",
        sourceDatasetId: "dataset_1",
      })
    ).rejects.toBeInstanceOf(QueryApiError);

    expect(repository.listQueryExecutionLogs("dataset_1")[0]).toMatchObject({
      errorMessage: "Generated query SQL contains a forbidden SQL statement.",
      status: "failed",
    });
  });

  it("imports mock query logs from the demo workbook and schedules learning", () => {
    const repository = seedRepository();
    const scheduledDatasets: string[] = [];
    const queryApi = createQueryApi({
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
      queryExecutor: {
        executeQuery() {
          throw new Error("not used");
        },
      },
      queryGenerator: {
        generateSql() {
          throw new Error("not used");
        },
      },
      queryLearningLoop: {
        schedule(sourceDatasetId) {
          scheduledDatasets.push(sourceDatasetId);
        },
      },
      repository,
      sqlValidator: {
        validate() {
          return {
            errors: [],
            isValid: true,
          };
        },
      },
    });

    const workbook = parseWorkbookFile({
      fileBuffer: readFileSync(
        resolve(
          "apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx"
        )
      ),
      fileName: "retailer-transactions-demo-query-logs.xlsx",
    });
    const imported = queryApi.importQueryLogs({
      sourceDatasetId: "dataset_1",
      workbook,
    });
    const importedLogs = repository.listQueryExecutionLogs("dataset_1");

    expect(imported.importedCount).toBe(20);
    expect(importedLogs).toHaveLength(20);
    expect(
      importedLogs.every((log) => log.cleanDatabaseId === "clean_db_1")
    ).toBe(true);
    expect(
      importedLogs.every((log) => log.sourceDatasetId === "dataset_1")
    ).toBe(true);
    expect(importedLogs.every((log) => log.patternFingerprint !== null)).toBe(
      true
    );
    expect(scheduledDatasets).toStrictEqual(["dataset_1"]);
  });

  it("passes pipeline column descriptions to the SQL generator when available", async () => {
    const repository = seedRepository();
    repository.saveImportProcessingState("dataset_1", {
      cleanDatabase: {
        builtAt: "2026-03-18T11:00:30.000Z",
        cleanDatabaseId: "clean_db_1",
        databaseFilePath: ".data/test-clean.sqlite",
      },
      cleanDatabaseStatus: "succeeded",
      lastPipelineError: null,
      nextRetryAt: null,
      pipelineRetryCount: 0,
      pipelineRun: null,
      pipelineStatus: "succeeded",
      pipelineVersion: {
        analysisJson: {
          columnDescriptions: [
            {
              columnName: "units_gross",
              description: "Gross units excluding return rows.",
              tableName: "daily_sales",
            },
          ],
          findings: [],
          sourceDatasetId: "dataset_1",
          summary: "Synthetic test pipeline metadata",
        },
        createdAt: "2026-03-18T11:00:20.000Z",
        createdBy: "codex_cli",
        pipelineId: "pipeline_1",
        pipelineVersionId: "pipeline_version_1",
        promptMarkdown: "prompt",
        sourceDatasetId: "dataset_1",
        sqlText: "SELECT 1;",
        summaryMarkdown: "summary",
      },
    });

    let capturedColumnDescriptions:
      | Array<{ columnName: string; description: string; tableName: string }>
      | undefined;

    const queryApi = createQueryApi({
      queryExecutor: {
        executeQuery() {
          return Promise.resolve({
            columnNames: ["ok"],
            rows: [[1]],
          });
        },
      },
      queryGenerator: {
        generateSql(options) {
          capturedColumnDescriptions = options.columnDescriptions;
          return Promise.resolve({
            model: "gpt-5-mini",
            prompt: "query prompt",
            sqlText: "SELECT 1 AS ok;",
          });
        },
      },
      repository,
      sqlValidator: {
        validate() {
          return {
            errors: [],
            isValid: true,
          };
        },
      },
    });

    await queryApi.runNaturalLanguageQuery({
      prompt: "Show daily gross units",
      sourceDatasetId: "dataset_1",
    });

    expect(capturedColumnDescriptions).toStrictEqual([
      {
        columnName: "units_gross",
        description: "Gross units excluding return rows.",
        tableName: "daily_sales",
      },
    ]);
  });

  it("forwards deliberate reasoning mode to the SQL generator", async () => {
    const repository = seedRepository();
    let capturedReasoningMode: "standard" | "deliberate" | undefined;
    const queryApi = createQueryApi({
      queryExecutor: {
        executeQuery() {
          return Promise.resolve({
            columnNames: ["value"],
            rows: [[1]],
          });
        },
      },
      queryGenerator: {
        generateSql(options) {
          capturedReasoningMode = options.reasoningMode;
          return Promise.resolve({
            model: "gpt-5-mini",
            prompt: options.prompt,
            sqlText: "SELECT 1 AS value;",
          });
        },
      },
      repository,
      sqlValidator: {
        validate() {
          return {
            errors: [],
            isValid: true,
          };
        },
      },
    });

    await queryApi.runNaturalLanguageQuery({
      prompt: "Show top stores",
      reasoningMode: "deliberate",
      sourceDatasetId: "dataset_1",
    });

    expect(capturedReasoningMode).toBe("deliberate");
  });
});

function seedRepository(): InMemorySourceDatasetRepository {
  const repository = new InMemorySourceDatasetRepository();
  const workbook: WorkbookUploadRequest = {
    workbookName: "sales.xlsx",
    sheets: [
      {
        name: "Orders",
        rows: [
          { OrderId: "A-1", Amount: 25, Region: "North" },
          { OrderId: "A-2", Amount: 30, Region: "South" },
        ],
      },
    ],
  };
  const result = ingestWorkbook({
    repository,
    request: workbook,
    now: new Date("2026-03-18T11:00:00.000Z"),
    createId: (() => {
      let counter = 0;
      return (prefix: string) => `${prefix}_${++counter}`;
    })(),
  });

  repository.saveImportProcessingState(result.dataset.id, {
    cleanDatabase: {
      builtAt: "2026-03-18T11:00:30.000Z",
      cleanDatabaseId: "clean_db_1",
      databaseFilePath: ".data/test-clean.sqlite",
    },
    cleanDatabaseStatus: "succeeded",
    lastPipelineError: null,
    nextRetryAt: null,
    pipelineRetryCount: 0,
    pipelineRun: null,
    pipelineStatus: "succeeded",
    pipelineVersion: null,
  });

  return repository;
}

function createDeterministicNow(isoTimes: string[]): () => Date {
  let index = 0;

  return () => {
    const isoTime =
      isoTimes[Math.min(index, isoTimes.length - 1)] ??
      isoTimes[0] ??
      "2026-03-18T12:00:00.000Z";
    index += 1;
    return new Date(isoTime);
  };
}
