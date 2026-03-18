import { describe, expect, it } from "vitest";

import {
  InMemorySourceDatasetRepository,
  type IngestionRepository,
} from "../../../../packages/database-core/src/index.js";
import { ingestWorkbook } from "../../../../packages/database-core/src/ingestion/service.js";
import type { WorkbookUploadRequest } from "../../../../packages/shared/src/index.js";
import { createPipelineRetryScheduler } from "./pipeline.js";

describe("pipeline retry scheduler", () => {
  it("stores pipeline and clean-database success after a scheduled run", async () => {
    const repository = seedRepository();
    const scheduler = createPipelineRetryScheduler({
      cleanDatabaseBuilder: {
        buildCleanDatabase(options) {
          return Promise.resolve({
            builtAt: options.builtAt,
            cleanDatabaseId: options.cleanDatabaseId,
            databaseFilePath: options.cleanDatabasePath,
          });
        },
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-dbs",
      codexPipelineGenerator: {
        generatePipelineArtifacts() {
          return Promise.resolve({
            analysisJson: {
              findings: [
                {
                  confidence: "high",
                  kind: "column-name",
                  message: "Normalize Order ID",
                  proposedFix: "Rename to order_id",
                  target: "source_sheet_sheet_1.Order ID",
                },
              ],
              sourceDatasetId: "dataset_1",
              summary: "Normalize order columns",
            },
            prompt: "prompt",
            sqlText: `
              DROP TABLE IF EXISTS cleaned_orders;
              CREATE TABLE cleaned_orders AS
              SELECT trim("Order ID") AS order_id
              FROM source.source_sheet_sheet_1;
            `,
            summaryMarkdown: "Created cleaned_orders",
            workspacePath: "/tmp/codex-workspace",
          });
        },
      },
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
      repository,
      retryDelayMs: 1,
      sourceDatabasePath: ".data/source-datasets.sqlite",
      sqlValidator: {
        validate: (sqlText: string) => ({
          errors: [],
          isValid: sqlText.includes("CREATE TABLE"),
        }),
      },
    });

    scheduler.schedule("dataset_1");
    await scheduler.drain();

    const processingState = repository.getImportProcessingState("dataset_1");

    expect(processingState).toMatchObject({
      cleanDatabase: {
        cleanDatabaseId: "clean_db_3",
      },
      cleanDatabaseStatus: "succeeded",
      pipelineRetryCount: 0,
      pipelineStatus: "succeeded",
      pipelineVersion: {
        pipelineVersionId: "pipeline_version_1",
      },
    });
  });

  it("retries failed analysis runs up to the configured limit", async () => {
    const repository = seedRepository();
    const scheduler = createPipelineRetryScheduler({
      cleanDatabaseBuilder: {
        buildCleanDatabase() {
          return Promise.reject(new Error("build should not run"));
        },
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-dbs",
      codexPipelineGenerator: {
        generatePipelineArtifacts() {
          return Promise.reject(new Error("Codex failed"));
        },
      },
      repository,
      retryDelayMs: 1,
      sourceDatabasePath: ".data/source-datasets.sqlite",
      sqlValidator: {
        validate: () => ({
          errors: [],
          isValid: true,
        }),
      },
    });

    scheduler.schedule("dataset_1");

    for (let attempt = 0; attempt < 6; attempt += 1) {
      await scheduler.drain();
      await new Promise((resolve) => {
        setTimeout(resolve, 2);
      });
    }

    const processingState = repository.getImportProcessingState("dataset_1");

    expect(processingState).toMatchObject({
      cleanDatabaseStatus: "failed",
      lastPipelineError: "Codex failed",
      pipelineRetryCount: 5,
      pipelineStatus: "failed",
    });
    expect(processingState?.nextRetryAt).toBeNull();
  });
});

function seedRepository(): IngestionRepository {
  const repository = new InMemorySourceDatasetRepository();
  const workbook: WorkbookUploadRequest = {
    workbookName: "sales.xlsx",
    sheets: [
      {
        name: "Orders",
        rows: [
          { "Order ID": "A-1", "Order Date": "2026/03/17" },
          { "Order ID": "A-2", "Order Date": "17-03-2026" },
        ],
      },
    ],
  };
  const result = ingestWorkbook({
    repository,
    request: workbook,
    now: new Date("2026-03-18T12:00:00.000Z"),
    createId: (() => {
      let counter = 0;
      return (prefix: string) => `${prefix}_${++counter}`;
    })(),
  });

  repository.saveImportProcessingState(
    result.dataset.id,
    result.summary.processing
  );

  return repository;
}
