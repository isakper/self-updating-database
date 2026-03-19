import type { SqlQueryGenerator } from "../../../../packages/agent-orchestrator/src/index.js";
import {
  buildPatternMetadataUpdate,
  detectUsedOptimizationObjects,
  type IngestionRepository,
} from "../../../../packages/database-core/src/index.js";
import type {
  CleanDatabaseSummary,
  CodexRunEvent,
  GeneratedSqlRecord,
  NaturalLanguageQueryRequest,
  NaturalLanguageQueryResponse,
  QueryExecutionLog,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";
import type {
  QueryExecutor,
  QuerySqlValidationResult,
} from "../../../../packages/pipeline-sdk/src/index.js";
import { importQueryLogsFromWorkbook } from "./mock-log-import.js";

export interface QuerySqlValidator {
  validate(sqlText: string): QuerySqlValidationResult;
}

export interface QueryApi {
  importQueryLogs(options: {
    sourceDatasetId: string;
    workbook: WorkbookUploadRequest;
  }): { importedCount: number };
  listQueryExecutionLogs(
    sourceDatasetId: string,
    limit?: number
  ): QueryExecutionLog[];
  runNaturalLanguageQuery(
    request: NaturalLanguageQueryRequest
  ): Promise<NaturalLanguageQueryResponse>;
}

export interface CreateQueryApiOptions {
  createId?: (prefix: string) => string;
  now?: () => Date;
  onRunEvent?: (runEvent: CodexRunEvent) => void;
  queryLearningLoop?: { schedule: (sourceDatasetId: string) => void };
  queryExecutor: QueryExecutor;
  queryGenerator: SqlQueryGenerator;
  repository: IngestionRepository;
  sqlValidator: QuerySqlValidator;
}

export class QueryApiError extends Error {
  readonly payload: {
    generatedSqlRecord: GeneratedSqlRecord | null;
    queryLog: QueryExecutionLog | null;
  };
  readonly statusCode: number;

  constructor(options: {
    cause?: Error;
    generatedSqlRecord?: GeneratedSqlRecord | null;
    message: string;
    queryLog?: QueryExecutionLog | null;
    statusCode: number;
  }) {
    super(options.message, options.cause ? { cause: options.cause } : {});
    this.name = "QueryApiError";
    this.payload = {
      generatedSqlRecord: options.generatedSqlRecord ?? null,
      queryLog: options.queryLog ?? null,
    };
    this.statusCode = options.statusCode;
  }
}

export function createQueryApi(options: CreateQueryApiOptions): QueryApi {
  const createId =
    options.createId ??
    ((prefix: string) =>
      `${prefix}_${Math.random().toString(36).slice(2, 10)}`);
  const now = options.now ?? (() => new Date());

  return {
    importQueryLogs(request) {
      const readyDataset = getReadyDatasetState(
        options.repository,
        request.sourceDatasetId
      );
      const importedQueryLogs = importQueryLogsFromWorkbook({
        cleanDatabaseId: readyDataset.cleanDatabase.cleanDatabaseId,
        createId,
        sourceDatasetId: request.sourceDatasetId,
        workbook: request.workbook,
      }).map((queryLog) => {
        const patternMetadata = buildPatternMetadataUpdate({ queryLog });

        return patternMetadata === null
          ? queryLog
          : {
              ...queryLog,
              matchedClusterId: patternMetadata.matchedClusterId,
              optimizationEligible: patternMetadata.optimizationEligible,
              patternFingerprint: patternMetadata.patternFingerprint,
              patternSummaryJson: patternMetadata.patternSummaryJson,
              patternVersion: patternMetadata.patternVersion,
              queryKind: patternMetadata.queryKind,
              usedOptimizationObjects:
                patternMetadata.usedOptimizationObjects ??
                queryLog.usedOptimizationObjects,
            };
      });

      importedQueryLogs.forEach((queryLog) => {
        options.repository.saveQueryExecutionLog(queryLog);
      });
      options.queryLearningLoop?.schedule(request.sourceDatasetId);

      return {
        importedCount: importedQueryLogs.length,
      };
    },
    listQueryExecutionLogs(sourceDatasetId, limit) {
      return options.repository.listQueryExecutionLogs(sourceDatasetId, limit);
    },
    async runNaturalLanguageQuery(request) {
      const { cleanDatabase } = getReadyDatasetState(
        options.repository,
        request.sourceDatasetId
      );

      const generationStartedAt = now();
      const optimizationHints = options.repository.listActiveOptimizationHints(
        request.sourceDatasetId
      );
      let generatedSqlRecord: GeneratedSqlRecord | null = null;
      publishRunEvent({
        createId,
        message: "Starting SQL generation.",
        now,
        options,
        sourceDatasetId: request.sourceDatasetId,
        stream: "system",
      });

      try {
        const generated = await options.queryGenerator.generateSql({
          cleanDatabaseId: cleanDatabase.cleanDatabaseId,
          cleanDatabasePath: cleanDatabase.databaseFilePath,
          optimizationHints,
          onDelta: (chunk) => {
            publishRunEvent({
              createId,
              message: chunk,
              now,
              options,
              sourceDatasetId: request.sourceDatasetId,
              stream: "stdout",
            });
          },
          prompt: request.prompt,
          sourceDatasetId: request.sourceDatasetId,
        });
        const generationFinishedAt = now();
        const generationLatencyMs =
          generationFinishedAt.getTime() - generationStartedAt.getTime();

        generatedSqlRecord = {
          generatedAt: generationFinishedAt.toISOString(),
          generationStartedAt: generationStartedAt.toISOString(),
          generationLatencyMs,
          generator: "openai_responses",
          sqlText: generated.sqlText,
          summaryMarkdown: "",
        };

        publishRunEvent({
          createId,
          message: "Finished SQL generation. Validating query.",
          now,
          options,
          sourceDatasetId: request.sourceDatasetId,
          stream: "system",
        });

        const validation = options.sqlValidator.validate(generated.sqlText);

        if (!validation.isValid) {
          throw new Error(validation.errors.join(" "));
        }

        const executionStartedAt = now();
        publishRunEvent({
          createId,
          message: "Running validated SQL against the clean database.",
          now,
          options,
          sourceDatasetId: request.sourceDatasetId,
          stream: "system",
        });
        const result = await options.queryExecutor.executeQuery({
          cleanDatabasePath: cleanDatabase.databaseFilePath,
          sqlText: generated.sqlText,
        });
        const executionFinishedAt = now();
        const usedOptimizationObjects = detectUsedOptimizationObjects({
          optimizationHints,
          sqlText: generated.sqlText,
        });
        const queryLog = createQueryExecutionLog({
          cleanDatabaseId: cleanDatabase.cleanDatabaseId,
          executionFinishedAt,
          executionStartedAt,
          generatedSqlRecord,
          prompt: request.prompt,
          queryLogId: createId("query_log"),
          result,
          sourceDatasetId: request.sourceDatasetId,
          status: "succeeded",
          usedOptimizationObjects,
        });
        const patternMetadata = buildPatternMetadataUpdate({
          queryLog,
          usedOptimizationObjects,
        });
        const enrichedQueryLog =
          patternMetadata === null
            ? queryLog
            : {
                ...queryLog,
                matchedClusterId: patternMetadata.matchedClusterId,
                optimizationEligible: patternMetadata.optimizationEligible,
                patternFingerprint: patternMetadata.patternFingerprint,
                patternSummaryJson: patternMetadata.patternSummaryJson,
                patternVersion: patternMetadata.patternVersion,
                queryKind: patternMetadata.queryKind,
              };

        options.repository.saveQueryExecutionLog(enrichedQueryLog);
        options.queryLearningLoop?.schedule(request.sourceDatasetId);
        publishRunEvent({
          createId,
          message: `Query succeeded with ${enrichedQueryLog.rowCount ?? 0} row${enrichedQueryLog.rowCount === 1 ? "" : "s"}.`,
          now,
          options,
          queryLogId: enrichedQueryLog.queryLogId,
          sourceDatasetId: request.sourceDatasetId,
          stream: "system",
        });

        return {
          generatedSqlRecord,
          queryLog: enrichedQueryLog,
          result,
        };
      } catch (error) {
        const failureFinishedAt = now();
        const queryLog = createQueryExecutionLog({
          cleanDatabaseId: cleanDatabase.cleanDatabaseId,
          errorMessage:
            error instanceof Error
              ? error.message
              : "Query generation or execution failed.",
          executionFinishedAt: null,
          executionStartedAt: null,
          generatedSqlRecord,
          prompt: request.prompt,
          queryLogId: createId("query_log"),
          result: null,
          sourceDatasetId: request.sourceDatasetId,
          status: "failed",
          totalFinishedAt: failureFinishedAt,
          totalStartedAt: generationStartedAt,
        });

        options.repository.saveQueryExecutionLog(queryLog);
        publishRunEvent({
          createId,
          message: queryLog.errorMessage ?? "Query execution failed.",
          now,
          options,
          queryLogId: queryLog.queryLogId,
          sourceDatasetId: request.sourceDatasetId,
          stream: "system",
        });

        throw new QueryApiError({
          generatedSqlRecord,
          message: queryLog.errorMessage ?? "Query execution failed.",
          queryLog,
          statusCode: 422,
          ...(error instanceof Error ? { cause: error } : {}),
        });
      }
    },
  };
}

function getReadyDatasetState(
  repository: IngestionRepository,
  sourceDatasetId: string
): {
  cleanDatabase: CleanDatabaseSummary;
} {
  const dataset = repository.getById(sourceDatasetId);

  if (!dataset) {
    throw new QueryApiError({
      message: "Source dataset not found.",
      statusCode: 404,
    });
  }

  const processingState = repository.getImportProcessingState(sourceDatasetId);
  const cleanDatabase = processingState?.cleanDatabase;

  if (
    !processingState ||
    processingState.cleanDatabaseStatus !== "succeeded" ||
    !cleanDatabase
  ) {
    throw new QueryApiError({
      message: "Clean database is not ready for querying yet.",
      statusCode: 409,
    });
  }

  return {
    cleanDatabase,
  };
}

function publishRunEvent(options: {
  createId: (prefix: string) => string;
  message: string;
  now: () => Date;
  options: CreateQueryApiOptions;
  queryLogId?: string;
  sourceDatasetId: string;
  stream: CodexRunEvent["stream"];
}): void {
  const runEvent: CodexRunEvent = {
    createdAt: options.now().toISOString(),
    eventId: options.createId("codex_run_event"),
    message: options.message,
    queryLogId: options.queryLogId ?? null,
    scope: "query",
    sourceDatasetId: options.sourceDatasetId,
    stream: options.stream,
  };

  options.options.repository.saveCodexRunEvent(runEvent);
  options.options.onRunEvent?.(runEvent);
}

function createQueryExecutionLog(options: {
  cleanDatabaseId: string;
  errorMessage?: string;
  executionFinishedAt: Date | null;
  executionStartedAt: Date | null;
  generatedSqlRecord: GeneratedSqlRecord | null;
  prompt: string;
  queryLogId: string;
  result:
    | NaturalLanguageQueryResponse["result"]
    | {
        columnNames: string[];
        rows: Array<Array<boolean | number | string | null>>;
      }
    | null;
  sourceDatasetId: string;
  status: QueryExecutionLog["status"];
  totalFinishedAt?: Date;
  totalStartedAt?: Date;
  usedOptimizationObjects?: string[];
}): QueryExecutionLog {
  const totalStartedAt =
    options.totalStartedAt ??
    new Date(
      options.generatedSqlRecord?.generationStartedAt ??
        new Date().toISOString()
    );
  const totalFinishedAt =
    options.totalFinishedAt ?? options.executionFinishedAt ?? totalStartedAt;

  return {
    cleanDatabaseId: options.cleanDatabaseId,
    errorMessage: options.errorMessage ?? null,
    executionFinishedAt: options.executionFinishedAt?.toISOString() ?? null,
    executionLatencyMs:
      options.executionStartedAt && options.executionFinishedAt
        ? options.executionFinishedAt.getTime() -
          options.executionStartedAt.getTime()
        : null,
    executionStartedAt: options.executionStartedAt?.toISOString() ?? null,
    generatedSql: options.generatedSqlRecord?.sqlText ?? null,
    generationFinishedAt: options.generatedSqlRecord?.generatedAt ?? null,
    generationLatencyMs:
      options.generatedSqlRecord?.generationLatencyMs ?? null,
    generationStartedAt:
      options.generatedSqlRecord?.generationStartedAt ??
      totalStartedAt.toISOString(),
    matchedClusterId: null,
    optimizationEligible: null,
    patternFingerprint: null,
    patternSummaryJson: null,
    patternVersion: null,
    prompt: options.prompt,
    queryKind: null,
    queryLogId: options.queryLogId,
    resultColumnNames: options.result?.columnNames ?? [],
    rowCount: options.result?.rows.length ?? null,
    sourceDatasetId: options.sourceDatasetId,
    status: options.status,
    summaryMarkdown: options.generatedSqlRecord?.summaryMarkdown ?? null,
    totalLatencyMs: totalFinishedAt.getTime() - totalStartedAt.getTime(),
    usedOptimizationObjects: options.usedOptimizationObjects ?? [],
  };
}
