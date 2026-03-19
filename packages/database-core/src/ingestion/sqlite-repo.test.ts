import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import type {
  CodexRunEvent,
  OptimizationRevision,
  QueryCluster,
  QueryExecutionLog,
} from "../../../shared/src/index.js";
import type { SourceDataset } from "./types.js";
import {
  openSourceDatabase,
  SqliteSourceDatasetRepository,
} from "./sqlite-repo.js";

describe("SqliteSourceDatasetRepository", () => {
  const tempDirectories: string[] = [];

  afterEach(() => {
    tempDirectories.splice(0).forEach((directoryPath) => {
      rmSync(directoryPath, { force: true, recursive: true });
    });
  });

  it("persists source datasets across repository instances", async () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), "source-db-"));
    tempDirectories.push(tempDirectory);

    const databaseFilePath = join(tempDirectory, "source-datasets.sqlite");
    const firstDatabase = await openSourceDatabase({ databaseFilePath });
    const firstRepository = new SqliteSourceDatasetRepository({
      connection: firstDatabase,
    });

    const dataset: SourceDataset = {
      id: "dataset_1",
      workbookName: "sales.xlsx",
      importedAt: "2026-03-18T00:00:00.000Z",
      sheets: [
        {
          sheetId: "sheet_1",
          name: "Orders",
          columns: ["OrderId", "Amount"],
          sourceTableName: "source_sheet_sheet_1",
          rows: [
            {
              rowId: "row_1",
              sourceRowNumber: 1,
              values: {
                Amount: 25,
                OrderId: "A-1",
              },
            },
          ],
        },
      ],
    };

    firstRepository.save(dataset);
    firstDatabase.close();

    const secondDatabase = await openSourceDatabase({ databaseFilePath });
    const secondRepository = new SqliteSourceDatasetRepository({
      connection: secondDatabase,
    });

    expect(secondRepository.getById("dataset_1")).toStrictEqual(dataset);
    expect(secondRepository.list()).toStrictEqual([dataset]);

    secondDatabase.close();
  });

  it("persists query execution logs across repository instances", async () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), "source-db-query-log-"));
    tempDirectories.push(tempDirectory);

    const databaseFilePath = join(tempDirectory, "source-datasets.sqlite");
    const firstDatabase = await openSourceDatabase({ databaseFilePath });
    const firstRepository = new SqliteSourceDatasetRepository({
      connection: firstDatabase,
    });

    firstRepository.save({
      id: "dataset_1",
      workbookName: "sales.xlsx",
      importedAt: "2026-03-18T00:00:00.000Z",
      sheets: [],
    });

    const queryLog: QueryExecutionLog = {
      cleanDatabaseId: "clean_db_1",
      errorMessage: null,
      executionFinishedAt: "2026-03-18T00:00:02.000Z",
      executionLatencyMs: 200,
      executionStartedAt: "2026-03-18T00:00:01.800Z",
      generatedSql: "SELECT * FROM clean_orders;",
      generationFinishedAt: "2026-03-18T00:00:01.700Z",
      generationLatencyMs: 1700,
      generationStartedAt: "2026-03-18T00:00:00.000Z",
      matchedClusterId: null,
      optimizationEligible: null,
      patternFingerprint: null,
      patternSummaryJson: null,
      patternVersion: null,
      prompt: "Show all orders",
      queryKind: null,
      queryLogId: "query_log_1",
      resultColumnNames: ["order_id"],
      rowCount: 1,
      sourceDatasetId: "dataset_1",
      status: "succeeded",
      summaryMarkdown: "Returns all cleaned orders.",
      totalLatencyMs: 2000,
      usedOptimizationObjects: [],
    };

    firstRepository.saveQueryExecutionLog(queryLog);
    firstDatabase.close();

    const secondDatabase = await openSourceDatabase({ databaseFilePath });
    const secondRepository = new SqliteSourceDatasetRepository({
      connection: secondDatabase,
    });

    expect(secondRepository.listQueryExecutionLogs("dataset_1")).toStrictEqual([
      queryLog,
    ]);

    secondDatabase.close();
  });

  it("persists Codex run events across repository instances", async () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), "source-db-run-events-"));
    tempDirectories.push(tempDirectory);

    const databaseFilePath = join(tempDirectory, "source-datasets.sqlite");
    const firstDatabase = await openSourceDatabase({ databaseFilePath });
    const firstRepository = new SqliteSourceDatasetRepository({
      connection: firstDatabase,
    });

    firstRepository.save({
      id: "dataset_1",
      workbookName: "sales.xlsx",
      importedAt: "2026-03-18T00:00:00.000Z",
      sheets: [],
    });

    const runEvent: CodexRunEvent = {
      createdAt: "2026-03-18T00:00:00.000Z",
      eventId: "codex_run_event_1",
      message: "Generating pipeline...",
      queryLogId: null,
      scope: "pipeline",
      sourceDatasetId: "dataset_1",
      stream: "stdout",
    };

    firstRepository.saveCodexRunEvent(runEvent);
    firstDatabase.close();

    const secondDatabase = await openSourceDatabase({ databaseFilePath });
    const secondRepository = new SqliteSourceDatasetRepository({
      connection: secondDatabase,
    });

    expect(secondRepository.listCodexRunEvents("dataset_1")).toStrictEqual([
      runEvent,
    ]);

    secondDatabase.close();
  });

  it("persists query clusters and optimization revisions across repository instances", async () => {
    const tempDirectory = mkdtempSync(
      join(tmpdir(), "source-db-optimization-")
    );
    tempDirectories.push(tempDirectory);

    const databaseFilePath = join(tempDirectory, "source-datasets.sqlite");
    const firstDatabase = await openSourceDatabase({ databaseFilePath });
    const firstRepository = new SqliteSourceDatasetRepository({
      connection: firstDatabase,
    });

    firstRepository.save({
      id: "dataset_1",
      workbookName: "sales.xlsx",
      importedAt: "2026-03-18T00:00:00.000Z",
      sheets: [],
    });

    const cluster: QueryCluster = {
      averageExecutionLatencyMs: 250,
      cleanDatabaseId: "clean_db_1",
      cumulativeExecutionLatencyMs: 500,
      latestOptimizationDecision: "pipeline_revision",
      latestOptimizationRevisionId: "optimization_revision_1",
      latestQueryLogId: "query_log_2",
      latestSeenAt: "2026-03-18T00:05:00.000Z",
      patternFingerprint: "fingerprint_1",
      patternSummary: {
        aggregates: ["sum(clean_orders.amount)"],
        cleanDatabaseId: "clean_db_1",
        filters: [],
        groupBy: ["clean_orders.region"],
        joins: [],
        optimizationEligible: true,
        orderBy: [],
        patternFingerprint: "fingerprint_1",
        patternVersion: 1,
        queryKind: "aggregate",
        relations: ["clean_orders"],
      },
      patternVersion: 1,
      queryClusterId: "query_cluster_1",
      queryCount: 2,
      representativeQueryLogIds: ["query_log_1", "query_log_2"],
      sourceDatasetId: "dataset_1",
    };
    const revision: OptimizationRevision = {
      analysisJson: {
        findings: [],
        sourceDatasetId: "dataset_1",
        summary: "Revised pipeline.",
      },
      appliedCleanDatabaseId: "clean_db_2",
      baseCleanDatabaseId: "clean_db_1",
      basePipelineVersionId: "pipeline_version_1",
      candidatePipelineVersionId: "pipeline_version_2",
      candidateSet: {
        baseCleanDatabaseId: "clean_db_1",
        basePipelineVersionId: "pipeline_version_1",
        candidateSetFingerprint: "candidate_set_1",
        queryClusters: [cluster],
        sourceDatasetId: "dataset_1",
      },
      createdAt: "2026-03-18T00:06:00.000Z",
      decision: "pipeline_revision",
      errorMessage: null,
      optimizationHints: [
        {
          guidance: "Prefer agg_orders_by_region.",
          preferredObjects: ["agg_orders_by_region"],
          queryClusterId: "query_cluster_1",
          title: "Regional totals",
        },
      ],
      optimizationRevisionId: "optimization_revision_1",
      promptMarkdown: "prompt",
      sourceDatasetId: "dataset_1",
      status: "succeeded",
      summaryMarkdown: "summary",
      updatedAt: "2026-03-18T00:07:00.000Z",
    };

    firstRepository.upsertQueryCluster(cluster);
    firstRepository.saveOptimizationRevision(revision);
    firstDatabase.close();

    const secondDatabase = await openSourceDatabase({ databaseFilePath });
    const secondRepository = new SqliteSourceDatasetRepository({
      connection: secondDatabase,
    });

    expect(secondRepository.listQueryClusters("dataset_1")).toStrictEqual([
      cluster,
    ]);
    expect(
      secondRepository.listOptimizationRevisions("dataset_1")
    ).toStrictEqual([revision]);
    expect(
      secondRepository.listActiveOptimizationHints("dataset_1")
    ).toStrictEqual(revision.optimizationHints);

    secondDatabase.close();
  });
});
