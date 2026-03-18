import type { SqlQueryGenerator } from "../../../../packages/agent-orchestrator/src/index.js";
import type { IngestionRepository } from "../../../../packages/database-core/src/index.js";
import type {
  CodexRunEvent,
  GeneratedSqlRecord,
  NaturalLanguageQueryRequest,
  NaturalLanguageQueryResponse,
  QueryExecutionLog,
} from "../../../../packages/shared/src/index.js";
import type {
  QueryExecutor,
  QuerySqlValidationResult,
} from "../../../../packages/pipeline-sdk/src/index.js";

export interface QuerySqlValidator {
  validate(sqlText: string): QuerySqlValidationResult;
}

export interface QueryApi {
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
    listQueryExecutionLogs(sourceDatasetId, limit) {
      return options.repository.listQueryExecutionLogs(sourceDatasetId, limit);
    },
    async runNaturalLanguageQuery(request) {
      const dataset = options.repository.getById(request.sourceDatasetId);

      if (!dataset) {
        throw new QueryApiError({
          message: "Source dataset not found.",
          statusCode: 404,
        });
      }

      const processingState = options.repository.getImportProcessingState(
        request.sourceDatasetId
      );
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

      const generationStartedAt = now();
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
        });

        options.repository.saveQueryExecutionLog(queryLog);
        publishRunEvent({
          createId,
          message: `Query succeeded with ${queryLog.rowCount ?? 0} row${queryLog.rowCount === 1 ? "" : "s"}.`,
          now,
          options,
          queryLogId: queryLog.queryLogId,
          sourceDatasetId: request.sourceDatasetId,
          stream: "system",
        });

        return {
          generatedSqlRecord,
          queryLog,
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
    prompt: options.prompt,
    queryLogId: options.queryLogId,
    resultColumnNames: options.result?.columnNames ?? [],
    rowCount: options.result?.rows.length ?? null,
    sourceDatasetId: options.sourceDatasetId,
    status: options.status,
    summaryMarkdown: options.generatedSqlRecord?.summaryMarkdown ?? null,
    totalLatencyMs: totalFinishedAt.getTime() - totalStartedAt.getTime(),
  };
}
