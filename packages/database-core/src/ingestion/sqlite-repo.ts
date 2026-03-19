import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import initSqlJs, {
  type BindParams,
  type Database,
  type SqlValue,
} from "sql.js";

import type {
  CleanDatabaseSummary,
  CodexRunEvent,
  ImportProcessingState,
  OptimizationHint,
  OptimizationRevision,
  PipelineRunRecord,
  PipelineVersionRecord,
  QueryCluster,
  QueryExecutionLog,
} from "../../../shared/src/index.js";
import type { IngestionRepository } from "./repo.js";
import type { SourceDataset, SourceRow, SourceSheet } from "./types.js";

export interface OpenSourceDatabaseOptions {
  databaseFilePath: string;
}

export interface SourceDatabaseConnection {
  close(): void;
  database: Database;
  databaseFilePath: string;
  persist(): void;
}

export async function openSourceDatabase(
  options: OpenSourceDatabaseOptions
): Promise<SourceDatabaseConnection> {
  mkdirSync(dirname(options.databaseFilePath), { recursive: true });

  const SQL = await initSqlJs();
  const database = existsSync(options.databaseFilePath)
    ? new SQL.Database(readFileSync(options.databaseFilePath))
    : new SQL.Database();

  initializeSourceDatabase(database);

  return {
    database,
    databaseFilePath: options.databaseFilePath,
    close() {
      database.close();
    },
    persist() {
      writeFileSync(options.databaseFilePath, Buffer.from(database.export()));
    },
  };
}

export class SqliteSourceDatasetRepository implements IngestionRepository {
  readonly #connection: SourceDatabaseConnection;

  constructor(options: { connection: SourceDatabaseConnection }) {
    this.#connection = options.connection;
    initializeSourceDatabase(this.#connection.database);
  }

  save(dataset: SourceDataset): void {
    const database = this.#connection.database;
    database.run("BEGIN");

    try {
      database.run(
        `
          INSERT INTO source_datasets (id, workbook_name, imported_at)
          VALUES ($id, $workbookName, $importedAt)
        `,
        {
          $id: dataset.id,
          $importedAt: dataset.importedAt,
          $workbookName: dataset.workbookName,
        }
      );

      dataset.sheets.forEach((sheet, sheetOrder) => {
        database.run(
          `
            INSERT INTO source_sheets (
              sheet_id,
              dataset_id,
              name,
              source_table_name,
              column_names_json,
              sheet_order
            )
            VALUES (
              $sheetId,
              $datasetId,
              $name,
              $sourceTableName,
              $columnNamesJson,
              $sheetOrder
            )
          `,
          {
            $columnNamesJson: JSON.stringify(sheet.columns),
            $datasetId: dataset.id,
            $name: sheet.name,
            $sheetId: sheet.sheetId,
            $sheetOrder: sheetOrder,
            $sourceTableName: sheet.sourceTableName,
          }
        );

        createSourceSheetTable(database, sheet);

        sheet.rows.forEach((row) => {
          database.run(
            `
              INSERT INTO source_rows (row_id, sheet_id, source_row_number, values_json)
              VALUES ($rowId, $sheetId, $sourceRowNumber, $valuesJson)
            `,
            {
              $rowId: row.rowId,
              $sheetId: sheet.sheetId,
              $sourceRowNumber: row.sourceRowNumber,
              $valuesJson: JSON.stringify(row.values),
            }
          );

          insertSourceSheetRow(database, sheet, row);
        });
      });

      database.run("COMMIT");
      this.#connection.persist();
    } catch (error) {
      database.run("ROLLBACK");
      throw error;
    }
  }

  getById(datasetId: string): SourceDataset | undefined {
    const datasetRow = readRows(
      this.#connection.database,
      `
        SELECT id, workbook_name, imported_at
        FROM source_datasets
        WHERE id = $datasetId
      `,
      { $datasetId: datasetId }
    )[0];

    if (!datasetRow) {
      return undefined;
    }

    const sheets = readRows(
      this.#connection.database,
      `
        SELECT sheet_id, name, source_table_name, column_names_json
        FROM source_sheets
        WHERE dataset_id = $datasetId
        ORDER BY sheet_order ASC
      `,
      { $datasetId: datasetId }
    ).map((sheetRow) => {
      const sheetId = readString(sheetRow, "sheet_id");

      return {
        sheetId,
        name: readString(sheetRow, "name"),
        columns: readStringArray(sheetRow, "column_names_json"),
        sourceTableName: readString(sheetRow, "source_table_name"),
        rows: readRows(
          this.#connection.database,
          `
            SELECT row_id, source_row_number, values_json
            FROM source_rows
            WHERE sheet_id = $sheetId
            ORDER BY source_row_number ASC
          `,
          { $sheetId: sheetId }
        ).map(parseSourceRow),
      } satisfies SourceSheet;
    });

    return {
      id: readString(datasetRow, "id"),
      importedAt: readString(datasetRow, "imported_at"),
      workbookName: readString(datasetRow, "workbook_name"),
      sheets,
    };
  }

  list(): SourceDataset[] {
    return readRows(
      this.#connection.database,
      `
        SELECT id
        FROM source_datasets
        ORDER BY imported_at DESC, id DESC
      `
    ).flatMap((datasetRow) => {
      const dataset = this.getById(readString(datasetRow, "id"));
      return dataset ? [dataset] : [];
    });
  }

  saveImportProcessingState(
    datasetId: string,
    processingState: ImportProcessingState
  ): void {
    this.#connection.database.run(
      `
        INSERT INTO import_processing_state (
          dataset_id,
          pipeline_status,
          pipeline_version_id,
          pipeline_retry_count,
          clean_database_status,
          clean_database_id,
          clean_database_path,
          clean_database_built_at,
          last_pipeline_error,
          next_retry_at,
          pipeline_run_id
        )
        VALUES (
          $datasetId,
          $pipelineStatus,
          $pipelineVersionId,
          $pipelineRetryCount,
          $cleanDatabaseStatus,
          $cleanDatabaseId,
          $cleanDatabasePath,
          $cleanDatabaseBuiltAt,
          $lastPipelineError,
          $nextRetryAt,
          $pipelineRunId
        )
        ON CONFLICT(dataset_id) DO UPDATE SET
          pipeline_status = excluded.pipeline_status,
          pipeline_version_id = excluded.pipeline_version_id,
          pipeline_retry_count = excluded.pipeline_retry_count,
          clean_database_status = excluded.clean_database_status,
          clean_database_id = excluded.clean_database_id,
          clean_database_path = excluded.clean_database_path,
          clean_database_built_at = excluded.clean_database_built_at,
          last_pipeline_error = excluded.last_pipeline_error,
          next_retry_at = excluded.next_retry_at,
          pipeline_run_id = excluded.pipeline_run_id
      `,
      {
        $cleanDatabaseBuiltAt: processingState.cleanDatabase?.builtAt ?? null,
        $cleanDatabaseId:
          processingState.cleanDatabase?.cleanDatabaseId ?? null,
        $cleanDatabasePath:
          processingState.cleanDatabase?.databaseFilePath ?? null,
        $cleanDatabaseStatus: processingState.cleanDatabaseStatus,
        $datasetId: datasetId,
        $lastPipelineError: processingState.lastPipelineError,
        $nextRetryAt: processingState.nextRetryAt,
        $pipelineRetryCount: processingState.pipelineRetryCount,
        $pipelineRunId: processingState.pipelineRun?.runId ?? null,
        $pipelineStatus: processingState.pipelineStatus,
        $pipelineVersionId:
          processingState.pipelineVersion?.pipelineVersionId ?? null,
      }
    );
    this.#connection.persist();
  }

  getImportProcessingState(
    datasetId: string
  ): ImportProcessingState | undefined {
    const stateRow = readRows(
      this.#connection.database,
      `
        SELECT
          pipeline_status,
          pipeline_version_id,
          pipeline_retry_count,
          clean_database_status,
          clean_database_id,
          clean_database_path,
          clean_database_built_at,
          last_pipeline_error,
          next_retry_at,
          pipeline_run_id
        FROM import_processing_state
        WHERE dataset_id = $datasetId
      `,
      { $datasetId: datasetId }
    )[0];

    if (!stateRow) {
      return undefined;
    }

    return {
      cleanDatabase: parseCleanDatabaseSummary(stateRow),
      cleanDatabaseStatus: readStatus(stateRow, "clean_database_status"),
      lastPipelineError: readNullableString(stateRow, "last_pipeline_error"),
      nextRetryAt: readNullableString(stateRow, "next_retry_at"),
      pipelineRetryCount: readNumber(stateRow, "pipeline_retry_count"),
      pipelineRun: this.getLatestPipelineRun(datasetId) ?? null,
      pipelineStatus: readStatus(stateRow, "pipeline_status"),
      pipelineVersion: this.getLatestPipelineVersion(datasetId) ?? null,
    };
  }

  savePipelineVersion(versionRecord: PipelineVersionRecord): void {
    this.#connection.database.run(
      `
        INSERT INTO pipeline_versions (
          pipeline_id,
          pipeline_version_id,
          source_dataset_id,
          prompt_markdown,
          sql_text,
          analysis_json,
          summary_markdown,
          created_at,
          created_by
        )
        VALUES (
          $pipelineId,
          $pipelineVersionId,
          $sourceDatasetId,
          $promptMarkdown,
          $sqlText,
          $analysisJson,
          $summaryMarkdown,
          $createdAt,
          $createdBy
        )
      `,
      {
        $analysisJson: JSON.stringify(versionRecord.analysisJson),
        $createdAt: versionRecord.createdAt,
        $createdBy: versionRecord.createdBy,
        $pipelineId: versionRecord.pipelineId,
        $pipelineVersionId: versionRecord.pipelineVersionId,
        $promptMarkdown: versionRecord.promptMarkdown,
        $sourceDatasetId: versionRecord.sourceDatasetId,
        $sqlText: versionRecord.sqlText,
        $summaryMarkdown: versionRecord.summaryMarkdown,
      }
    );
    this.#connection.persist();
  }

  getLatestPipelineVersion(
    datasetId: string
  ): PipelineVersionRecord | undefined {
    const versionRow = readRows(
      this.#connection.database,
      `
        SELECT
          pipeline_id,
          pipeline_version_id,
          source_dataset_id,
          prompt_markdown,
          sql_text,
          analysis_json,
          summary_markdown,
          created_at,
          created_by
        FROM pipeline_versions
        WHERE source_dataset_id = $datasetId
        ORDER BY created_at DESC, pipeline_version_id DESC
        LIMIT 1
      `,
      { $datasetId: datasetId }
    )[0];

    if (!versionRow) {
      return undefined;
    }

    return parsePipelineVersionRecord(versionRow);
  }

  savePipelineRun(runRecord: PipelineRunRecord): void {
    this.#connection.database.run(
      `
        INSERT INTO pipeline_runs (
          run_id,
          pipeline_version_id,
          source_dataset_id,
          status,
          run_started_at,
          run_finished_at,
          retry_count,
          run_error
        )
        VALUES (
          $runId,
          $pipelineVersionId,
          $sourceDatasetId,
          $status,
          $runStartedAt,
          $runFinishedAt,
          $retryCount,
          $runError
        )
        ON CONFLICT(run_id) DO UPDATE SET
          status = excluded.status,
          run_finished_at = excluded.run_finished_at,
          retry_count = excluded.retry_count,
          run_error = excluded.run_error
      `,
      {
        $pipelineVersionId: runRecord.pipelineVersionId,
        $retryCount: runRecord.retryCount,
        $runError: runRecord.runError,
        $runFinishedAt: runRecord.runFinishedAt,
        $runId: runRecord.runId,
        $runStartedAt: runRecord.runStartedAt,
        $sourceDatasetId: runRecord.sourceDatasetId,
        $status: runRecord.status,
      }
    );
    this.#connection.persist();
  }

  getLatestPipelineRun(datasetId: string): PipelineRunRecord | undefined {
    const runRow = readRows(
      this.#connection.database,
      `
        SELECT
          run_id,
          pipeline_version_id,
          source_dataset_id,
          status,
          run_started_at,
          run_finished_at,
          retry_count,
          run_error
        FROM pipeline_runs
        WHERE source_dataset_id = $datasetId
        ORDER BY run_started_at DESC, run_id DESC
        LIMIT 1
      `,
      { $datasetId: datasetId }
    )[0];

    if (!runRow) {
      return undefined;
    }

    return parsePipelineRunRecord(runRow);
  }

  listRetryableDatasetIds(nowIso: string): string[] {
    return readRows(
      this.#connection.database,
      `
        SELECT dataset_id
        FROM import_processing_state
        WHERE pipeline_status != 'succeeded'
          AND pipeline_retry_count < 5
          AND (next_retry_at IS NULL OR next_retry_at <= $nowIso)
      `,
      { $nowIso: nowIso }
    ).map((row) => readString(row, "dataset_id"));
  }

  saveCodexRunEvent(runEvent: CodexRunEvent): void {
    this.#connection.database.run(
      `
        INSERT INTO codex_run_events (
          event_id,
          source_dataset_id,
          scope,
          stream,
          message,
          created_at,
          query_log_id
        )
        VALUES (
          $eventId,
          $sourceDatasetId,
          $scope,
          $stream,
          $message,
          $createdAt,
          $queryLogId
        )
      `,
      {
        $createdAt: runEvent.createdAt,
        $eventId: runEvent.eventId,
        $message: runEvent.message,
        $queryLogId: runEvent.queryLogId,
        $scope: runEvent.scope,
        $sourceDatasetId: runEvent.sourceDatasetId,
        $stream: runEvent.stream,
      }
    );
    this.#connection.persist();
  }

  listCodexRunEvents(sourceDatasetId: string, limit = 200): CodexRunEvent[] {
    return readRows(
      this.#connection.database,
      `
        SELECT
          event_id,
          source_dataset_id,
          scope,
          stream,
          message,
          created_at,
          query_log_id
        FROM codex_run_events
        WHERE source_dataset_id = $sourceDatasetId
        ORDER BY created_at ASC, event_id ASC
        LIMIT $limit
      `,
      {
        $limit: limit,
        $sourceDatasetId: sourceDatasetId,
      }
    ).map(parseCodexRunEvent);
  }

  saveQueryExecutionLog(queryLog: QueryExecutionLog): void {
    this.#connection.database.run(
      `
        INSERT INTO query_execution_logs (
          query_log_id,
          source_dataset_id,
          clean_database_id,
          prompt,
          generated_sql,
          summary_markdown,
          status,
          error_message,
          generation_started_at,
          generation_finished_at,
          generation_latency_ms,
          execution_started_at,
          execution_finished_at,
          execution_latency_ms,
          total_latency_ms,
          row_count,
          result_column_names_json,
          pattern_fingerprint,
          pattern_version,
          query_kind,
          pattern_summary_json,
          optimization_eligible,
          matched_cluster_id,
          used_optimization_objects_json
        )
        VALUES (
          $queryLogId,
          $sourceDatasetId,
          $cleanDatabaseId,
          $prompt,
          $generatedSql,
          $summaryMarkdown,
          $status,
          $errorMessage,
          $generationStartedAt,
          $generationFinishedAt,
          $generationLatencyMs,
          $executionStartedAt,
          $executionFinishedAt,
          $executionLatencyMs,
          $totalLatencyMs,
          $rowCount,
          $resultColumnNamesJson,
          $patternFingerprint,
          $patternVersion,
          $queryKind,
          $patternSummaryJson,
          $optimizationEligible,
          $matchedClusterId,
          $usedOptimizationObjectsJson
        )
      `,
      {
        $cleanDatabaseId: queryLog.cleanDatabaseId,
        $errorMessage: queryLog.errorMessage,
        $executionFinishedAt: queryLog.executionFinishedAt,
        $executionLatencyMs: queryLog.executionLatencyMs,
        $executionStartedAt: queryLog.executionStartedAt,
        $matchedClusterId: queryLog.matchedClusterId,
        $optimizationEligible:
          queryLog.optimizationEligible === null
            ? null
            : queryLog.optimizationEligible
              ? 1
              : 0,
        $patternFingerprint: queryLog.patternFingerprint,
        $patternSummaryJson: queryLog.patternSummaryJson
          ? JSON.stringify(queryLog.patternSummaryJson)
          : null,
        $patternVersion: queryLog.patternVersion,
        $queryKind: queryLog.queryKind,
        $generatedSql: queryLog.generatedSql,
        $generationFinishedAt: queryLog.generationFinishedAt,
        $generationLatencyMs: queryLog.generationLatencyMs,
        $generationStartedAt: queryLog.generationStartedAt,
        $prompt: queryLog.prompt,
        $queryLogId: queryLog.queryLogId,
        $resultColumnNamesJson: JSON.stringify(queryLog.resultColumnNames),
        $rowCount: queryLog.rowCount,
        $sourceDatasetId: queryLog.sourceDatasetId,
        $status: queryLog.status,
        $summaryMarkdown: queryLog.summaryMarkdown,
        $totalLatencyMs: queryLog.totalLatencyMs,
        $usedOptimizationObjectsJson: JSON.stringify(
          queryLog.usedOptimizationObjects
        ),
      }
    );
    this.#connection.persist();
  }

  listQueryExecutionLogs(
    sourceDatasetId: string,
    limit = 20
  ): QueryExecutionLog[] {
    return readRows(
      this.#connection.database,
      `
        SELECT
          query_log_id,
          source_dataset_id,
          clean_database_id,
          prompt,
          generated_sql,
          summary_markdown,
          status,
          error_message,
          generation_started_at,
          generation_finished_at,
          generation_latency_ms,
          execution_started_at,
          execution_finished_at,
          execution_latency_ms,
          total_latency_ms,
          row_count,
          result_column_names_json,
          pattern_fingerprint,
          pattern_version,
          query_kind,
          pattern_summary_json,
          optimization_eligible,
          matched_cluster_id,
          used_optimization_objects_json
        FROM query_execution_logs
        WHERE source_dataset_id = $sourceDatasetId
        ORDER BY generation_started_at DESC, query_log_id DESC
        LIMIT $limit
      `,
      {
        $limit: limit,
        $sourceDatasetId: sourceDatasetId,
      }
    ).map(parseQueryExecutionLog);
  }

  updateQueryExecutionLogPatternMetadata(options: {
    matchedClusterId: string | null;
    optimizationEligible: boolean;
    patternFingerprint: string;
    patternSummaryJson: QueryExecutionLog["patternSummaryJson"];
    patternVersion: number;
    queryKind: QueryExecutionLog["queryKind"];
    queryLogId: string;
    usedOptimizationObjects?: string[];
  }): void {
    this.#connection.database.run(
      `
        UPDATE query_execution_logs
        SET
          pattern_fingerprint = $patternFingerprint,
          pattern_version = $patternVersion,
          query_kind = $queryKind,
          pattern_summary_json = $patternSummaryJson,
          optimization_eligible = $optimizationEligible,
          matched_cluster_id = $matchedClusterId,
          used_optimization_objects_json = COALESCE(
            $usedOptimizationObjectsJson,
            used_optimization_objects_json
          )
        WHERE query_log_id = $queryLogId
      `,
      {
        $matchedClusterId: options.matchedClusterId,
        $optimizationEligible: options.optimizationEligible ? 1 : 0,
        $patternFingerprint: options.patternFingerprint,
        $patternSummaryJson: JSON.stringify(options.patternSummaryJson),
        $patternVersion: options.patternVersion,
        $queryKind: options.queryKind,
        $queryLogId: options.queryLogId,
        $usedOptimizationObjectsJson: options.usedOptimizationObjects
          ? JSON.stringify(options.usedOptimizationObjects)
          : null,
      }
    );
    this.#connection.persist();
  }

  upsertQueryCluster(cluster: QueryCluster): void {
    this.#connection.database.run(
      `
        INSERT INTO query_clusters (
          query_cluster_id,
          source_dataset_id,
          clean_database_id,
          pattern_fingerprint,
          pattern_version,
          pattern_summary_json,
          query_count,
          cumulative_execution_latency_ms,
          average_execution_latency_ms,
          latest_seen_at,
          latest_query_log_id,
          representative_query_log_ids_json,
          latest_optimization_revision_id,
          latest_optimization_decision
        )
        VALUES (
          $queryClusterId,
          $sourceDatasetId,
          $cleanDatabaseId,
          $patternFingerprint,
          $patternVersion,
          $patternSummaryJson,
          $queryCount,
          $cumulativeExecutionLatencyMs,
          $averageExecutionLatencyMs,
          $latestSeenAt,
          $latestQueryLogId,
          $representativeQueryLogIdsJson,
          $latestOptimizationRevisionId,
          $latestOptimizationDecision
        )
        ON CONFLICT(query_cluster_id) DO UPDATE SET
          pattern_summary_json = excluded.pattern_summary_json,
          query_count = excluded.query_count,
          cumulative_execution_latency_ms =
            excluded.cumulative_execution_latency_ms,
          average_execution_latency_ms = excluded.average_execution_latency_ms,
          latest_seen_at = excluded.latest_seen_at,
          latest_query_log_id = excluded.latest_query_log_id,
          representative_query_log_ids_json =
            excluded.representative_query_log_ids_json,
          latest_optimization_revision_id =
            excluded.latest_optimization_revision_id,
          latest_optimization_decision = excluded.latest_optimization_decision
      `,
      {
        $averageExecutionLatencyMs: cluster.averageExecutionLatencyMs,
        $cleanDatabaseId: cluster.cleanDatabaseId,
        $cumulativeExecutionLatencyMs: cluster.cumulativeExecutionLatencyMs,
        $latestOptimizationDecision: cluster.latestOptimizationDecision,
        $latestOptimizationRevisionId: cluster.latestOptimizationRevisionId,
        $latestQueryLogId: cluster.latestQueryLogId,
        $latestSeenAt: cluster.latestSeenAt,
        $patternFingerprint: cluster.patternFingerprint,
        $patternSummaryJson: JSON.stringify(cluster.patternSummary),
        $patternVersion: cluster.patternVersion,
        $queryClusterId: cluster.queryClusterId,
        $queryCount: cluster.queryCount,
        $representativeQueryLogIdsJson: JSON.stringify(
          cluster.representativeQueryLogIds
        ),
        $sourceDatasetId: cluster.sourceDatasetId,
      }
    );
    this.#connection.persist();
  }

  listQueryClusters(sourceDatasetId: string, limit = 20): QueryCluster[] {
    return readRows(
      this.#connection.database,
      `
        SELECT
          query_cluster_id,
          source_dataset_id,
          clean_database_id,
          pattern_fingerprint,
          pattern_version,
          pattern_summary_json,
          query_count,
          cumulative_execution_latency_ms,
          average_execution_latency_ms,
          latest_seen_at,
          latest_query_log_id,
          representative_query_log_ids_json,
          latest_optimization_revision_id,
          latest_optimization_decision
        FROM query_clusters
        WHERE source_dataset_id = $sourceDatasetId
        ORDER BY cumulative_execution_latency_ms DESC,
          query_count DESC,
          latest_seen_at DESC,
          query_cluster_id DESC
        LIMIT $limit
      `,
      {
        $limit: limit,
        $sourceDatasetId: sourceDatasetId,
      }
    ).map(parseQueryCluster);
  }

  saveOptimizationRevision(revision: OptimizationRevision): void {
    this.#connection.database.run(
      `
        INSERT INTO optimization_revisions (
          optimization_revision_id,
          source_dataset_id,
          base_clean_database_id,
          base_pipeline_version_id,
          candidate_pipeline_version_id,
          applied_clean_database_id,
          candidate_set_json,
          prompt_markdown,
          summary_markdown,
          analysis_json,
          optimization_hints_json,
          decision,
          status,
          error_message,
          created_at,
          updated_at
        )
        VALUES (
          $optimizationRevisionId,
          $sourceDatasetId,
          $baseCleanDatabaseId,
          $basePipelineVersionId,
          $candidatePipelineVersionId,
          $appliedCleanDatabaseId,
          $candidateSetJson,
          $promptMarkdown,
          $summaryMarkdown,
          $analysisJson,
          $optimizationHintsJson,
          $decision,
          $status,
          $errorMessage,
          $createdAt,
          $updatedAt
        )
        ON CONFLICT(optimization_revision_id) DO UPDATE SET
          candidate_pipeline_version_id = excluded.candidate_pipeline_version_id,
          applied_clean_database_id = excluded.applied_clean_database_id,
          candidate_set_json = excluded.candidate_set_json,
          prompt_markdown = excluded.prompt_markdown,
          summary_markdown = excluded.summary_markdown,
          analysis_json = excluded.analysis_json,
          optimization_hints_json = excluded.optimization_hints_json,
          decision = excluded.decision,
          status = excluded.status,
          error_message = excluded.error_message,
          updated_at = excluded.updated_at
      `,
      {
        $analysisJson: JSON.stringify(revision.analysisJson),
        $appliedCleanDatabaseId: revision.appliedCleanDatabaseId,
        $baseCleanDatabaseId: revision.baseCleanDatabaseId,
        $basePipelineVersionId: revision.basePipelineVersionId,
        $candidatePipelineVersionId: revision.candidatePipelineVersionId,
        $candidateSetJson: JSON.stringify(revision.candidateSet),
        $createdAt: revision.createdAt,
        $decision: revision.decision,
        $errorMessage: revision.errorMessage,
        $optimizationHintsJson: JSON.stringify(revision.optimizationHints),
        $optimizationRevisionId: revision.optimizationRevisionId,
        $promptMarkdown: revision.promptMarkdown,
        $sourceDatasetId: revision.sourceDatasetId,
        $status: revision.status,
        $summaryMarkdown: revision.summaryMarkdown,
        $updatedAt: revision.updatedAt,
      }
    );
    this.#connection.persist();
  }

  listOptimizationRevisions(
    sourceDatasetId: string,
    limit = 20
  ): OptimizationRevision[] {
    return readRows(
      this.#connection.database,
      `
        SELECT
          optimization_revision_id,
          source_dataset_id,
          base_clean_database_id,
          base_pipeline_version_id,
          candidate_pipeline_version_id,
          applied_clean_database_id,
          candidate_set_json,
          prompt_markdown,
          summary_markdown,
          analysis_json,
          optimization_hints_json,
          decision,
          status,
          error_message,
          created_at,
          updated_at
        FROM optimization_revisions
        WHERE source_dataset_id = $sourceDatasetId
        ORDER BY created_at DESC, optimization_revision_id DESC
        LIMIT $limit
      `,
      {
        $limit: limit,
        $sourceDatasetId: sourceDatasetId,
      }
    ).map(parseOptimizationRevision);
  }

  listActiveOptimizationHints(sourceDatasetId: string): OptimizationHint[] {
    const row = readRows(
      this.#connection.database,
      `
        SELECT optimization_hints_json
        FROM optimization_revisions
        WHERE source_dataset_id = $sourceDatasetId
          AND status = 'succeeded'
          AND decision = 'pipeline_revision'
          AND applied_clean_database_id IS NOT NULL
        ORDER BY updated_at DESC, optimization_revision_id DESC
        LIMIT 1
      `,
      { $sourceDatasetId: sourceDatasetId }
    )[0];

    return row
      ? (readJsonValue(row, "optimization_hints_json") as OptimizationHint[])
      : [];
  }
}

function initializeSourceDatabase(database: Database): void {
  database.run(`
    CREATE TABLE IF NOT EXISTS source_datasets (
      id TEXT PRIMARY KEY,
      workbook_name TEXT NOT NULL,
      imported_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS source_sheets (
      sheet_id TEXT PRIMARY KEY,
      dataset_id TEXT NOT NULL,
      name TEXT NOT NULL,
      source_table_name TEXT NOT NULL,
      column_names_json TEXT NOT NULL,
      sheet_order INTEGER NOT NULL,
      FOREIGN KEY (dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS source_rows (
      row_id TEXT PRIMARY KEY,
      sheet_id TEXT NOT NULL,
      source_row_number INTEGER NOT NULL,
      values_json TEXT NOT NULL,
      FOREIGN KEY (sheet_id) REFERENCES source_sheets(sheet_id)
    );

    CREATE TABLE IF NOT EXISTS import_processing_state (
      dataset_id TEXT PRIMARY KEY,
      pipeline_status TEXT NOT NULL,
      pipeline_version_id TEXT,
      pipeline_retry_count INTEGER NOT NULL,
      clean_database_status TEXT NOT NULL,
      clean_database_id TEXT,
      clean_database_path TEXT,
      clean_database_built_at TEXT,
      last_pipeline_error TEXT,
      next_retry_at TEXT,
      pipeline_run_id TEXT,
      FOREIGN KEY (dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_versions (
      pipeline_version_id TEXT PRIMARY KEY,
      pipeline_id TEXT NOT NULL,
      source_dataset_id TEXT NOT NULL,
      prompt_markdown TEXT NOT NULL DEFAULT '',
      sql_text TEXT NOT NULL,
      analysis_json TEXT NOT NULL,
      summary_markdown TEXT NOT NULL,
      created_at TEXT NOT NULL,
      created_by TEXT NOT NULL,
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_id TEXT PRIMARY KEY,
      pipeline_version_id TEXT NOT NULL,
      source_dataset_id TEXT NOT NULL,
      status TEXT NOT NULL,
      run_started_at TEXT NOT NULL,
      run_finished_at TEXT,
      retry_count INTEGER NOT NULL,
      run_error TEXT,
      FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id),
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS query_execution_logs (
      query_log_id TEXT PRIMARY KEY,
      source_dataset_id TEXT NOT NULL,
      clean_database_id TEXT NOT NULL,
      prompt TEXT NOT NULL,
      generated_sql TEXT,
      summary_markdown TEXT,
      status TEXT NOT NULL,
      error_message TEXT,
      generation_started_at TEXT NOT NULL,
      generation_finished_at TEXT,
      generation_latency_ms INTEGER,
      execution_started_at TEXT,
      execution_finished_at TEXT,
      execution_latency_ms INTEGER,
      total_latency_ms INTEGER NOT NULL,
      row_count INTEGER,
      result_column_names_json TEXT NOT NULL,
      pattern_fingerprint TEXT,
      pattern_version INTEGER,
      query_kind TEXT,
      pattern_summary_json TEXT,
      optimization_eligible INTEGER,
      matched_cluster_id TEXT,
      used_optimization_objects_json TEXT NOT NULL DEFAULT '[]',
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS query_clusters (
      query_cluster_id TEXT PRIMARY KEY,
      source_dataset_id TEXT NOT NULL,
      clean_database_id TEXT NOT NULL,
      pattern_fingerprint TEXT NOT NULL,
      pattern_version INTEGER NOT NULL,
      pattern_summary_json TEXT NOT NULL,
      query_count INTEGER NOT NULL,
      cumulative_execution_latency_ms INTEGER NOT NULL,
      average_execution_latency_ms INTEGER NOT NULL,
      latest_seen_at TEXT NOT NULL,
      latest_query_log_id TEXT NOT NULL,
      representative_query_log_ids_json TEXT NOT NULL,
      latest_optimization_revision_id TEXT,
      latest_optimization_decision TEXT,
      UNIQUE(source_dataset_id, clean_database_id, pattern_fingerprint),
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS optimization_revisions (
      optimization_revision_id TEXT PRIMARY KEY,
      source_dataset_id TEXT NOT NULL,
      base_clean_database_id TEXT NOT NULL,
      base_pipeline_version_id TEXT NOT NULL,
      candidate_pipeline_version_id TEXT,
      applied_clean_database_id TEXT,
      candidate_set_json TEXT NOT NULL,
      prompt_markdown TEXT NOT NULL,
      summary_markdown TEXT NOT NULL,
      analysis_json TEXT NOT NULL,
      optimization_hints_json TEXT NOT NULL,
      decision TEXT NOT NULL,
      status TEXT NOT NULL,
      error_message TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS codex_run_events (
      event_id TEXT PRIMARY KEY,
      source_dataset_id TEXT NOT NULL,
      scope TEXT NOT NULL,
      stream TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at TEXT NOT NULL,
      query_log_id TEXT,
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE INDEX IF NOT EXISTS idx_source_sheets_dataset_id
      ON source_sheets(dataset_id, sheet_order);

    CREATE INDEX IF NOT EXISTS idx_source_rows_sheet_id
      ON source_rows(sheet_id, source_row_number);

    CREATE INDEX IF NOT EXISTS idx_pipeline_versions_dataset_id
      ON pipeline_versions(source_dataset_id, created_at);

    CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dataset_id
      ON pipeline_runs(source_dataset_id, run_started_at);

    CREATE INDEX IF NOT EXISTS idx_query_execution_logs_dataset_id
      ON query_execution_logs(source_dataset_id, generation_started_at);

    CREATE INDEX IF NOT EXISTS idx_query_clusters_dataset_id
      ON query_clusters(source_dataset_id, cumulative_execution_latency_ms, latest_seen_at);

    CREATE INDEX IF NOT EXISTS idx_optimization_revisions_dataset_id
      ON optimization_revisions(source_dataset_id, created_at);

    CREATE INDEX IF NOT EXISTS idx_codex_run_events_dataset_id
      ON codex_run_events(source_dataset_id, created_at);
  `);

  ensureColumnExists(
    database,
    "pipeline_versions",
    "prompt_markdown",
    "TEXT NOT NULL DEFAULT ''"
  );
  ensureColumnExists(
    database,
    "query_execution_logs",
    "pattern_fingerprint",
    "TEXT"
  );
  ensureColumnExists(
    database,
    "query_execution_logs",
    "pattern_version",
    "INTEGER"
  );
  ensureColumnExists(database, "query_execution_logs", "query_kind", "TEXT");
  ensureColumnExists(
    database,
    "query_execution_logs",
    "pattern_summary_json",
    "TEXT"
  );
  ensureColumnExists(
    database,
    "query_execution_logs",
    "optimization_eligible",
    "INTEGER"
  );
  ensureColumnExists(
    database,
    "query_execution_logs",
    "matched_cluster_id",
    "TEXT"
  );
  ensureColumnExists(
    database,
    "query_execution_logs",
    "used_optimization_objects_json",
    "TEXT NOT NULL DEFAULT '[]'"
  );
}

function createSourceSheetTable(database: Database, sheet: SourceSheet): void {
  const columnDefinitions = [
    `"__source_row_number" INTEGER NOT NULL`,
    ...sheet.columns.map((columnName) => `${escapeIdentifier(columnName)}`),
  ].join(", ");

  database.run(
    `CREATE TABLE ${escapeIdentifier(sheet.sourceTableName)} (${columnDefinitions})`
  );
}

function insertSourceSheetRow(
  database: Database,
  sheet: SourceSheet,
  row: SourceRow
): void {
  const insertColumns = [
    `"__source_row_number"`,
    ...sheet.columns.map((columnName) => escapeIdentifier(columnName)),
  ].join(", ");
  const insertValues = [
    `$sourceRowNumber`,
    ...sheet.columns.map((_, index) => `$column_${index}`),
  ].join(", ");
  const params: Record<string, SqlValue> = {
    $sourceRowNumber: row.sourceRowNumber,
  };

  sheet.columns.forEach((columnName, index) => {
    params[`$column_${index}`] = toSqlValue(row.values[columnName] ?? null);
  });

  database.run(
    `INSERT INTO ${escapeIdentifier(sheet.sourceTableName)} (${insertColumns}) VALUES (${insertValues})`,
    params
  );
}

function readRows(
  database: Database,
  sql: string,
  params?: BindParams
): Array<Record<string, SqlValue>> {
  const [result] = database.exec(sql, params);

  if (!result) {
    return [];
  }

  return result.values.map((rowValues) =>
    Object.fromEntries(
      result.columns.map((columnName, index) => [
        columnName,
        rowValues[index] ?? null,
      ])
    )
  );
}

function parseSourceRow(row: Record<string, SqlValue>): SourceRow {
  return {
    rowId: readString(row, "row_id"),
    sourceRowNumber: readNumber(row, "source_row_number"),
    values: readJsonRecord(row, "values_json"),
  };
}

function parsePipelineVersionRecord(
  row: Record<string, SqlValue>
): PipelineVersionRecord {
  return {
    analysisJson: readJsonValue(
      row,
      "analysis_json"
    ) as PipelineVersionRecord["analysisJson"],
    createdAt: readString(row, "created_at"),
    createdBy: "codex_cli",
    pipelineId: readString(row, "pipeline_id"),
    pipelineVersionId: readString(row, "pipeline_version_id"),
    promptMarkdown: readString(row, "prompt_markdown"),
    sourceDatasetId: readString(row, "source_dataset_id"),
    sqlText: readString(row, "sql_text"),
    summaryMarkdown: readString(row, "summary_markdown"),
  };
}

function parsePipelineRunRecord(
  row: Record<string, SqlValue>
): PipelineRunRecord {
  return {
    pipelineVersionId: readString(row, "pipeline_version_id"),
    retryCount: readNumber(row, "retry_count"),
    runError: readNullableString(row, "run_error"),
    runFinishedAt: readNullableString(row, "run_finished_at"),
    runId: readString(row, "run_id"),
    runStartedAt: readString(row, "run_started_at"),
    sourceDatasetId: readString(row, "source_dataset_id"),
    status: readStatus(row, "status"),
  };
}

function parseCleanDatabaseSummary(
  row: Record<string, SqlValue>
): CleanDatabaseSummary | null {
  const cleanDatabaseId = readNullableString(row, "clean_database_id");
  const databaseFilePath = readNullableString(row, "clean_database_path");
  const builtAt = readNullableString(row, "clean_database_built_at");

  if (!cleanDatabaseId || !databaseFilePath || !builtAt) {
    return null;
  }

  return {
    builtAt,
    cleanDatabaseId,
    databaseFilePath,
  };
}

function parseQueryExecutionLog(
  row: Record<string, SqlValue>
): QueryExecutionLog {
  return {
    cleanDatabaseId: readString(row, "clean_database_id"),
    errorMessage: readNullableString(row, "error_message"),
    executionFinishedAt: readNullableString(row, "execution_finished_at"),
    executionLatencyMs: readNullableNumber(row, "execution_latency_ms"),
    executionStartedAt: readNullableString(row, "execution_started_at"),
    generatedSql: readNullableString(row, "generated_sql"),
    generationFinishedAt: readNullableString(row, "generation_finished_at"),
    generationLatencyMs: readNullableNumber(row, "generation_latency_ms"),
    generationStartedAt: readString(row, "generation_started_at"),
    matchedClusterId: readNullableString(row, "matched_cluster_id"),
    optimizationEligible: readNullableBoolean(row, "optimization_eligible"),
    patternFingerprint: readNullableString(row, "pattern_fingerprint"),
    patternSummaryJson: readNullableJsonValue(
      row,
      "pattern_summary_json"
    ) as QueryExecutionLog["patternSummaryJson"],
    patternVersion: readNullableNumber(row, "pattern_version"),
    prompt: readString(row, "prompt"),
    queryKind: readNullableString(
      row,
      "query_kind"
    ) as QueryExecutionLog["queryKind"],
    queryLogId: readString(row, "query_log_id"),
    resultColumnNames: readStringArray(row, "result_column_names_json"),
    rowCount: readNullableNumber(row, "row_count"),
    sourceDatasetId: readString(row, "source_dataset_id"),
    status: readString(row, "status") as QueryExecutionLog["status"],
    summaryMarkdown: readNullableString(row, "summary_markdown"),
    totalLatencyMs: readNumber(row, "total_latency_ms"),
    usedOptimizationObjects: readStringArray(
      row,
      "used_optimization_objects_json"
    ),
  };
}

function parseCodexRunEvent(row: Record<string, SqlValue>): CodexRunEvent {
  return {
    createdAt: readString(row, "created_at"),
    eventId: readString(row, "event_id"),
    message: readString(row, "message"),
    queryLogId: readNullableString(row, "query_log_id"),
    scope: readString(row, "scope") as CodexRunEvent["scope"],
    sourceDatasetId: readString(row, "source_dataset_id"),
    stream: readString(row, "stream") as CodexRunEvent["stream"],
  };
}

function parseQueryCluster(row: Record<string, SqlValue>): QueryCluster {
  return {
    averageExecutionLatencyMs: readNumber(row, "average_execution_latency_ms"),
    cleanDatabaseId: readString(row, "clean_database_id"),
    cumulativeExecutionLatencyMs: readNumber(
      row,
      "cumulative_execution_latency_ms"
    ),
    latestOptimizationDecision: readNullableString(
      row,
      "latest_optimization_decision"
    ) as QueryCluster["latestOptimizationDecision"],
    latestOptimizationRevisionId: readNullableString(
      row,
      "latest_optimization_revision_id"
    ),
    latestQueryLogId: readString(row, "latest_query_log_id"),
    latestSeenAt: readString(row, "latest_seen_at"),
    patternFingerprint: readString(row, "pattern_fingerprint"),
    patternSummary: readJsonValue(
      row,
      "pattern_summary_json"
    ) as QueryCluster["patternSummary"],
    patternVersion: readNumber(row, "pattern_version"),
    queryClusterId: readString(row, "query_cluster_id"),
    queryCount: readNumber(row, "query_count"),
    representativeQueryLogIds: readStringArray(
      row,
      "representative_query_log_ids_json"
    ),
    sourceDatasetId: readString(row, "source_dataset_id"),
  };
}

function parseOptimizationRevision(
  row: Record<string, SqlValue>
): OptimizationRevision {
  return {
    analysisJson: readJsonValue(
      row,
      "analysis_json"
    ) as OptimizationRevision["analysisJson"],
    appliedCleanDatabaseId: readNullableString(
      row,
      "applied_clean_database_id"
    ),
    baseCleanDatabaseId: readString(row, "base_clean_database_id"),
    basePipelineVersionId: readString(row, "base_pipeline_version_id"),
    candidatePipelineVersionId: readNullableString(
      row,
      "candidate_pipeline_version_id"
    ),
    candidateSet: readJsonValue(
      row,
      "candidate_set_json"
    ) as OptimizationRevision["candidateSet"],
    createdAt: readString(row, "created_at"),
    decision: readString(row, "decision") as OptimizationRevision["decision"],
    errorMessage: readNullableString(row, "error_message"),
    optimizationHints: readJsonValue(
      row,
      "optimization_hints_json"
    ) as OptimizationRevision["optimizationHints"],
    optimizationRevisionId: readString(row, "optimization_revision_id"),
    promptMarkdown: readString(row, "prompt_markdown"),
    sourceDatasetId: readString(row, "source_dataset_id"),
    status: readString(row, "status") as OptimizationRevision["status"],
    summaryMarkdown: readString(row, "summary_markdown"),
    updatedAt: readString(row, "updated_at"),
  };
}

function readString(row: Record<string, SqlValue>, key: string): string {
  const value = row[key];

  if (typeof value !== "string") {
    throw new Error(`Expected ${key} to be a string.`);
  }

  return value;
}

function readNullableString(
  row: Record<string, SqlValue>,
  key: string
): string | null {
  const value = row[key];

  if (value === null) {
    return null;
  }

  if (typeof value !== "string") {
    throw new Error(`Expected ${key} to be a nullable string.`);
  }

  return value;
}

function readNumber(row: Record<string, SqlValue>, key: string): number {
  const value = row[key];

  if (typeof value !== "number") {
    throw new Error(`Expected ${key} to be a number.`);
  }

  return value;
}

function readNullableBoolean(
  row: Record<string, SqlValue>,
  key: string
): boolean | null {
  const value = row[key];

  if (value === null) {
    return null;
  }

  if (typeof value !== "number") {
    throw new Error(`Expected ${key} to be a nullable numeric boolean.`);
  }

  return value !== 0;
}

function readNullableNumber(
  row: Record<string, SqlValue>,
  key: string
): number | null {
  const value = row[key];

  if (value === null) {
    return null;
  }

  if (typeof value !== "number") {
    throw new Error(`Expected ${key} to be a nullable number.`);
  }

  return value;
}

function readStatus(row: Record<string, SqlValue>, key: string) {
  return readString(row, key) as
    | ImportProcessingState["pipelineStatus"]
    | ImportProcessingState["cleanDatabaseStatus"];
}

function readStringArray(row: Record<string, SqlValue>, key: string): string[] {
  const value = readJsonValue(row, key);

  if (
    !Array.isArray(value) ||
    value.some((entry) => typeof entry !== "string")
  ) {
    throw new Error(`Expected ${key} to decode to a string array.`);
  }

  return value as string[];
}

function readJsonRecord(
  row: Record<string, SqlValue>,
  key: string
): Record<string, boolean | number | string | null> {
  const value = readJsonValue(row, key);

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Expected ${key} to decode to an object.`);
  }

  const valueRecord = value as Record<string, unknown>;
  const record: Record<string, boolean | number | string | null> = {};

  Object.entries(valueRecord).forEach(([entryKey, entryValue]) => {
    if (
      entryValue === null ||
      typeof entryValue === "boolean" ||
      typeof entryValue === "number" ||
      typeof entryValue === "string"
    ) {
      record[entryKey] = entryValue;
      return;
    }

    throw new Error(
      `Expected ${key}.${entryKey} to be a scalar workbook cell value.`
    );
  });

  return record;
}

function readJsonValue(row: Record<string, SqlValue>, key: string): unknown {
  return JSON.parse(readString(row, key)) as unknown;
}

function readNullableJsonValue(
  row: Record<string, SqlValue>,
  key: string
): unknown {
  const value = readNullableString(row, key);
  return value === null ? null : (JSON.parse(value) as unknown);
}

function escapeIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function toSqlValue(value: SourceRow["values"][string]): SqlValue {
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  return value;
}

function ensureColumnExists(
  database: Database,
  tableName: string,
  columnName: string,
  definitionSql: string
): void {
  const existingColumns = readRows(
    database,
    `PRAGMA table_info(${tableName})`
  ).map((row) => readString(row, "name"));

  if (existingColumns.includes(columnName)) {
    return;
  }

  database.run(
    `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definitionSql}`
  );
}
