import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { InMemorySourceDatasetRepository } from "../../../../packages/database-core/src/index.js";
import type { SourceDataset } from "../../../../packages/database-core/src/index.js";
import type {
  PipelineVersionRecord,
  QueryExecutionLog,
} from "../../../../packages/shared/src/index.js";
import { validatePipelineSql } from "../../../../packages/pipeline-sdk/src/index.js";
import { parseWorkbookFile } from "../../../web/src/upload-workspace/parse-workbook-file.js";
import { createQueryApi } from "../query/api.js";
import { createQueryLearningLoop } from "./service.js";

describe("createQueryLearningLoop", () => {
  it("ranks eligible clusters and sends only the top 2 groups to Codex", async () => {
    const repository = createRepository();
    const candidateSets: string[][] = [];
    const loop = createQueryLearningLoop({
      cleanDatabaseBuilder: {
        buildCleanDatabase: () =>
          Promise.reject(
            new Error("build should not run for no-change decisions")
          ),
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-databases",
      codexOptimizationGenerator: {
        generateOptimizationArtifacts(options) {
          candidateSets.push(
            options.candidateSet.queryClusters.map((cluster) =>
              cluster.patternSummary.groupBy.join(",")
            )
          );

          return Promise.resolve({
            analysisJson: {
              findings: [],
              sourceDatasetId: options.sourceDatasetId,
              summary: "No change needed.",
            },
            decision: "no_change",
            optimizationHints: [],
            prompt: "prompt",
            sqlText: CURRENT_PIPELINE.sqlText,
            summaryMarkdown: "No change.",
            workspacePath: "/tmp/fake",
          });
        },
      },
      repository,
      sourceDatabasePath: ".data/source.sqlite",
      sqlValidator: {
        validate: validatePipelineSql,
      },
    });

    seedQueryLogs(repository);
    loop.schedule("dataset_1");
    await loop.drain();

    expect(candidateSets).toHaveLength(1);
    expect(candidateSets[0]).toStrictEqual([
      "clean_orders.region",
      "clean_orders.product",
    ]);
    expect(repository.listOptimizationRevisions("dataset_1")[0]?.decision).toBe(
      "no_change"
    );
  });

  it("applies a revised pipeline only after the candidate clean database builds", async () => {
    const repository = createRepository();
    seedQueryLogs(repository);
    const loop = createQueryLearningLoop({
      cleanDatabaseBuilder: {
        buildCleanDatabase(options) {
          return Promise.resolve({
            builtAt: options.builtAt,
            cleanDatabaseId: options.cleanDatabaseId,
            databaseFilePath: `${options.cleanDatabasePath}.built`,
          });
        },
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-databases",
      codexOptimizationGenerator: {
        generateOptimizationArtifacts(options) {
          return Promise.resolve({
            analysisJson: {
              findings: [],
              sourceDatasetId: options.sourceDatasetId,
              summary: "Add helper table.",
            },
            decision: "pipeline_revision",
            optimizationHints: [
              {
                guidance: "Prefer agg_orders_by_region.",
                preferredObjects: ["agg_orders_by_region"],
                queryClusterId:
                  options.candidateSet.queryClusters[0]?.queryClusterId ?? "",
                title: "Regional totals",
              },
            ],
            prompt: "prompt",
            sqlText: `
              DROP TABLE IF EXISTS clean_orders;
              CREATE TABLE clean_orders AS SELECT 1 AS ok;
              DROP TABLE IF EXISTS agg_orders_by_region;
              CREATE TABLE agg_orders_by_region AS
              SELECT 1 AS region, 1 AS total_amount;
            `,
            summaryMarkdown: "Pipeline revised.",
            workspacePath: "/tmp/fake",
          });
        },
      },
      repository,
      sourceDatabasePath: ".data/source.sqlite",
      sqlValidator: {
        validate: validatePipelineSql,
      },
    });

    const beforeDataset = repository.getById("dataset_1");
    const beforeState = repository.getImportProcessingState("dataset_1");

    loop.schedule("dataset_1");
    await loop.drain();

    const afterState = repository.getImportProcessingState("dataset_1");
    const revision = repository.listOptimizationRevisions("dataset_1")[0];

    expect(revision?.decision).toBe("pipeline_revision");
    expect(revision?.status).toBe("succeeded");
    expect(afterState?.cleanDatabase?.cleanDatabaseId).not.toBe(
      beforeState?.cleanDatabase?.cleanDatabaseId
    );
    expect(afterState?.pipelineVersion?.pipelineVersionId).not.toBe(
      beforeState?.pipelineVersion?.pipelineVersionId
    );
    expect(repository.listActiveOptimizationHints("dataset_1")).toStrictEqual([
      {
        guidance: "Prefer agg_orders_by_region.",
        preferredObjects: ["agg_orders_by_region"],
        queryClusterId:
          revision?.candidateSet.queryClusters[0]?.queryClusterId ?? "",
        title: "Regional totals",
      },
    ]);
    expect(repository.getById("dataset_1")).toStrictEqual(beforeDataset);
  });

  it("keeps the current clean database active when the optimized build fails", async () => {
    const repository = createRepository();
    seedQueryLogs(repository);
    const loop = createQueryLearningLoop({
      cleanDatabaseBuilder: {
        buildCleanDatabase: () => Promise.reject(new Error("Build exploded")),
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-databases",
      codexOptimizationGenerator: {
        generateOptimizationArtifacts(options) {
          return Promise.resolve({
            analysisJson: {
              findings: [],
              sourceDatasetId: options.sourceDatasetId,
              summary: "Try a revised pipeline.",
            },
            decision: "pipeline_revision",
            optimizationHints: [],
            prompt: "prompt",
            sqlText:
              "DROP TABLE IF EXISTS clean_orders; CREATE TABLE clean_orders AS SELECT 1 AS ok;",
            summaryMarkdown: "Pipeline revised.",
            workspacePath: "/tmp/fake",
          });
        },
      },
      repository,
      sourceDatabasePath: ".data/source.sqlite",
      sqlValidator: {
        validate: validatePipelineSql,
      },
    });

    const beforeState = repository.getImportProcessingState("dataset_1");
    loop.schedule("dataset_1");
    await loop.drain();

    const afterState = repository.getImportProcessingState("dataset_1");
    const revision = repository.listOptimizationRevisions("dataset_1")[0];

    expect(revision?.status).toBe("failed");
    expect(afterState?.cleanDatabase).toStrictEqual(beforeState?.cleanDatabase);
    expect(afterState?.pipelineVersion).toStrictEqual(
      beforeState?.pipelineVersion
    );
  });

  it("groups imported demo logs into two repeated patterns and triggers optimization", async () => {
    const repository = createRepository();
    const workbook = parseWorkbookFile({
      fileBuffer: readFileSync(
        resolve(
          "apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx"
        )
      ),
      fileName: "retailer-transactions-demo-query-logs.xlsx",
    });
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
    const candidateSets: Array<
      Array<{ queryCount: number; queryKind: string }>
    > = [];
    const loop = createQueryLearningLoop({
      cleanDatabaseBuilder: {
        buildCleanDatabase: () =>
          Promise.reject(
            new Error("build should not run for this no-change demo test")
          ),
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-databases",
      codexOptimizationGenerator: {
        generateOptimizationArtifacts(options) {
          candidateSets.push(
            options.candidateSet.queryClusters.map((cluster) => ({
              queryCount: cluster.queryCount,
              queryKind: cluster.patternSummary.queryKind,
            }))
          );

          return Promise.resolve({
            analysisJson: {
              findings: [],
              sourceDatasetId: options.sourceDatasetId,
              summary: "No change needed.",
            },
            decision: "no_change",
            optimizationHints: [],
            prompt: "prompt",
            sqlText: CURRENT_PIPELINE.sqlText,
            summaryMarkdown: "No change.",
            workspacePath: "/tmp/fake",
          });
        },
      },
      repository,
      sourceDatabasePath: ".data/source.sqlite",
      sqlValidator: {
        validate: validatePipelineSql,
      },
    });

    queryApi.importQueryLogs({
      sourceDatasetId: "dataset_1",
      workbook,
    });
    loop.schedule("dataset_1");
    await loop.drain();

    expect(candidateSets).toHaveLength(1);
    expect(candidateSets[0]).toStrictEqual([
      { queryCount: 10, queryKind: "detail" },
      { queryCount: 10, queryKind: "aggregate" },
    ]);
    expect(repository.listQueryClusters("dataset_1")).toHaveLength(2);
    expect(repository.listOptimizationRevisions("dataset_1")[0]?.decision).toBe(
      "no_change"
    );
  });

  it("applies a second pipeline revision when the imported demo logs trigger optimization", async () => {
    const repository = createRepository();
    const workbook = parseWorkbookFile({
      fileBuffer: readFileSync(
        resolve(
          "apps/web/fixtures/demo-workbooks/retailer-transactions-demo-query-logs.xlsx"
        )
      ),
      fileName: "retailer-transactions-demo-query-logs.xlsx",
    });
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
    const beforeState = repository.getImportProcessingState("dataset_1");
    const loop = createQueryLearningLoop({
      cleanDatabaseBuilder: {
        buildCleanDatabase(options) {
          return Promise.resolve({
            builtAt: options.builtAt,
            cleanDatabaseId: options.cleanDatabaseId,
            databaseFilePath: `${options.cleanDatabasePath}.built`,
          });
        },
      },
      cleanDatabaseDirectoryPath: ".data/test-clean-databases",
      codexOptimizationGenerator: {
        generateOptimizationArtifacts(options) {
          return Promise.resolve({
            analysisJson: {
              findings: [
                {
                  confidence: "high",
                  kind: "precompute",
                  message:
                    "The imported SKU/day patterns justify a helper daily fact table.",
                  proposedFix:
                    "Create transactions_daily_sku and keep transactions_clean for detailed access.",
                  target: options.candidateSet.queryClusters
                    .map((cluster) => cluster.queryClusterId)
                    .join(", "),
                },
              ],
              sourceDatasetId: options.sourceDatasetId,
              summary: "Add a helper daily SKU table.",
            },
            decision: "pipeline_revision",
            optimizationHints: options.candidateSet.queryClusters.map(
              (cluster) => ({
                guidance:
                  cluster.patternSummary.queryKind === "aggregate"
                    ? "Prefer transactions_daily_sku for repeated SKU/day rollups."
                    : "Prefer sku_day_zero_fill for zero-filled SKU/day time series.",
                preferredObjects:
                  cluster.patternSummary.queryKind === "aggregate"
                    ? ["transactions_daily_sku"]
                    : ["sku_day_zero_fill"],
                queryClusterId: cluster.queryClusterId,
                title:
                  cluster.patternSummary.queryKind === "aggregate"
                    ? "Daily SKU rollups"
                    : "Zero-filled SKU series",
              })
            ),
            prompt: "prompt",
            sqlText: `
              DROP TABLE IF EXISTS transactions_clean;
              CREATE TABLE transactions_clean AS
              SELECT 1 AS item_sku, '2025-01-01' AS business_date, 1.0 AS units, 2.0 AS gross_sales_incl_vat, 1.0 AS cogs_ex_vat;
              DROP TABLE IF EXISTS transactions_daily_sku;
              CREATE TABLE transactions_daily_sku AS
              SELECT item_sku, business_date, SUM(units) AS units_sold, SUM(gross_sales_incl_vat) AS revenue_incl_vat, SUM(cogs_ex_vat) AS cost_ex_vat
              FROM transactions_clean
              GROUP BY item_sku, business_date;
              DROP TABLE IF EXISTS sku_day_zero_fill;
              CREATE TABLE sku_day_zero_fill AS
              SELECT item_sku, business_date, units_sold, revenue_incl_vat, cost_ex_vat
              FROM transactions_daily_sku;
            `,
            summaryMarkdown: "Pipeline revised to add daily SKU helper tables.",
            workspacePath: "/tmp/fake",
          });
        },
      },
      repository,
      sourceDatabasePath: ".data/source.sqlite",
      sqlValidator: {
        validate: validatePipelineSql,
      },
    });

    queryApi.importQueryLogs({
      sourceDatasetId: "dataset_1",
      workbook,
    });
    loop.schedule("dataset_1");
    await loop.drain();

    const afterState = repository.getImportProcessingState("dataset_1");
    const revision = repository.listOptimizationRevisions("dataset_1")[0];
    const activeHints = repository.listActiveOptimizationHints("dataset_1");

    expect(revision?.decision).toBe("pipeline_revision");
    expect(revision?.status).toBe("succeeded");
    expect(revision?.candidateSet.queryClusters).toHaveLength(2);
    expect(afterState?.cleanDatabase?.cleanDatabaseId).not.toBe(
      beforeState?.cleanDatabase?.cleanDatabaseId
    );
    expect(afterState?.pipelineVersion?.pipelineVersionId).not.toBe(
      beforeState?.pipelineVersion?.pipelineVersionId
    );
    expect(afterState?.pipelineVersion?.sqlText).toContain(
      "CREATE TABLE transactions_daily_sku AS"
    );
    expect(activeHints).toHaveLength(2);
    expect(
      [...activeHints.map((hint) => hint.preferredObjects[0])].sort()
    ).toStrictEqual(["sku_day_zero_fill", "transactions_daily_sku"]);
  });
});

const CURRENT_PIPELINE: PipelineVersionRecord = {
  analysisJson: {
    findings: [],
    sourceDatasetId: "dataset_1",
    summary: "Current pipeline.",
  },
  createdAt: "2026-03-18T10:00:00.000Z",
  createdBy: "codex_cli",
  pipelineId: "pipeline_dataset_1",
  pipelineVersionId: "pipeline_version_current",
  promptMarkdown: "prompt",
  sourceDatasetId: "dataset_1",
  sqlText:
    "DROP TABLE IF EXISTS clean_orders; CREATE TABLE clean_orders AS SELECT 1 AS ok;",
  summaryMarkdown: "Current pipeline.",
};

function createRepository(): InMemorySourceDatasetRepository {
  const repository = new InMemorySourceDatasetRepository();
  const dataset: SourceDataset = {
    id: "dataset_1",
    importedAt: "2026-03-18T09:00:00.000Z",
    sheets: [],
    workbookName: "sales.xlsx",
  };

  repository.save(dataset);
  repository.saveImportProcessingState(dataset.id, {
    cleanDatabase: {
      builtAt: "2026-03-18T09:00:30.000Z",
      cleanDatabaseId: "clean_db_1",
      databaseFilePath: ".data/test-clean.sqlite",
    },
    cleanDatabaseStatus: "succeeded",
    lastPipelineError: null,
    nextRetryAt: null,
    pipelineRetryCount: 0,
    pipelineRun: null,
    pipelineStatus: "succeeded",
    pipelineVersion: CURRENT_PIPELINE,
  });
  repository.savePipelineVersion(CURRENT_PIPELINE);

  return repository;
}

function seedQueryLogs(repository: InMemorySourceDatasetRepository): void {
  [
    createQueryLog({
      executionLatencyMs: 400,
      prompt: "Show revenue by region",
      queryLogId: "query_log_1",
      sqlText:
        "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;",
      startedAt: "2026-03-18T10:00:00.000Z",
    }),
    createQueryLog({
      executionLatencyMs: 300,
      prompt: "Show revenue by region again",
      queryLogId: "query_log_2",
      sqlText:
        "SELECT o.region, SUM(o.amount) FROM clean_orders o GROUP BY o.region;",
      startedAt: "2026-03-18T10:01:00.000Z",
    }),
    createQueryLog({
      executionLatencyMs: 250,
      prompt: "Show revenue by product",
      queryLogId: "query_log_3",
      sqlText:
        "SELECT product, SUM(amount) AS total_amount FROM clean_orders GROUP BY product;",
      startedAt: "2026-03-18T10:02:00.000Z",
    }),
    createQueryLog({
      executionLatencyMs: 250,
      prompt: "Show revenue by product again",
      queryLogId: "query_log_4",
      sqlText:
        "SELECT o.product, SUM(o.amount) FROM clean_orders o GROUP BY o.product;",
      startedAt: "2026-03-18T10:03:00.000Z",
    }),
    createQueryLog({
      executionLatencyMs: 100,
      prompt: "Show revenue by country",
      queryLogId: "query_log_5",
      sqlText:
        "SELECT country, SUM(amount) AS total_amount FROM clean_orders GROUP BY country;",
      startedAt: "2026-03-18T10:04:00.000Z",
    }),
    createQueryLog({
      executionLatencyMs: 100,
      prompt: "Show revenue by country again",
      queryLogId: "query_log_6",
      sqlText:
        "SELECT o.country, SUM(o.amount) FROM clean_orders o GROUP BY o.country;",
      startedAt: "2026-03-18T10:05:00.000Z",
    }),
  ].forEach((queryLog) => {
    repository.saveQueryExecutionLog(queryLog);
  });
}

function createQueryLog(options: {
  executionLatencyMs: number;
  prompt: string;
  queryLogId: string;
  sqlText: string;
  startedAt: string;
}): QueryExecutionLog {
  const finishedAt = new Date(
    new Date(options.startedAt).getTime() + options.executionLatencyMs
  ).toISOString();

  return {
    cleanDatabaseId: "clean_db_1",
    errorMessage: null,
    executionFinishedAt: finishedAt,
    executionLatencyMs: options.executionLatencyMs,
    executionStartedAt: options.startedAt,
    generatedSql: options.sqlText,
    generationFinishedAt: options.startedAt,
    generationLatencyMs: 10,
    generationStartedAt: options.startedAt,
    matchedClusterId: null,
    optimizationEligible: null,
    patternFingerprint: null,
    patternSummaryJson: null,
    patternVersion: null,
    prompt: options.prompt,
    queryKind: null,
    queryLogId: options.queryLogId,
    resultColumnNames: ["dimension", "total_amount"],
    rowCount: 2,
    sourceDatasetId: "dataset_1",
    status: "succeeded",
    summaryMarkdown: "summary",
    totalLatencyMs: options.executionLatencyMs + 10,
    usedOptimizationObjects: [],
  };
}
