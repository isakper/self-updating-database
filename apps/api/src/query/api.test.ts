import { describe, expect, it } from "vitest";

import { InMemorySourceDatasetRepository } from "../../../../packages/database-core/src/index.js";
import { ingestWorkbook } from "../../../../packages/database-core/src/ingestion/service.js";
import type { WorkbookUploadRequest } from "../../../../packages/shared/src/index.js";
import { createQueryApi, QueryApiError } from "./api.js";

describe("query api", () => {
  it("generates SQL, executes against the clean database, and logs the query", async () => {
    const repository = seedRepository();
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
    expect(repository.listQueryExecutionLogs("dataset_1")[0]).toMatchObject({
      cleanDatabaseId: "clean_db_1",
      rowCount: 1,
      status: "succeeded",
    });
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
