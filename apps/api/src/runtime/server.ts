import { resolve } from "node:path";
import { createServer } from "node:http";

import {
  createCodexCliOptimizationGenerator,
  createCodexCliPipelineGenerator,
  createOpenAiSqlQueryGenerator,
} from "../../../../packages/agent-orchestrator/src/index.js";
import {
  openSourceDatabase,
  SqliteSourceDatasetRepository,
} from "../../../../packages/database-core/src/index.js";
import {
  createSqliteQueryExecutor,
  createSqliteCleanDatabaseBuilder,
  validateQuerySql,
  validatePipelineSql,
} from "../../../../packages/pipeline-sdk/src/index.js";
import { createIngestionApi } from "../ingestion/api.js";
import { handleIngestionRequest } from "../ingestion/http.js";
import { createPipelineRetryScheduler } from "../ingestion/pipeline.js";
import { createOptimizationApi } from "../optimization/api.js";
import { handleOptimizationRequest } from "../optimization/http.js";
import { createQueryLearningLoop } from "../optimization/service.js";
import { createQueryApi } from "../query/api.js";
import { handleQueryRequest } from "../query/http.js";
import { createCodexRunEventHub } from "./codex-run-events.js";
import { loadLocalEnvironment } from "../../../shared/load-env.js";

export interface ApiServerOptions {
  cleanDatabaseDirectoryPath?: string;
  databaseFilePath?: string;
  port?: number;
}

export async function startApiServer(options: ApiServerOptions = {}): Promise<{
  close: () => Promise<void>;
  port: number;
}> {
  const codexRetainWorkspaces = readBooleanFromEnv("CODEX_RETAIN_WORKSPACES");
  const codexPipelineCommandTimeoutMs = readPositiveIntegerFromEnv(
    "CODEX_PIPELINE_COMMAND_TIMEOUT_MS"
  );
  const codexOptimizationCommandTimeoutMs = readPositiveIntegerFromEnv(
    "CODEX_OPTIMIZATION_COMMAND_TIMEOUT_MS"
  );
  const codexPlaywrightMcpStartupTimeoutSec = readPositiveIntegerFromEnv(
    "CODEX_PLAYWRIGHT_MCP_STARTUP_TIMEOUT_SEC"
  );
  const optimizationRetryBackoffMs = readPositiveIntegerFromEnv(
    "OPTIMIZATION_RETRY_BACKOFF_MS"
  );
  const optimizationRetryLimitPerCandidate = readPositiveIntegerFromEnv(
    "OPTIMIZATION_RETRY_LIMIT_PER_CANDIDATE"
  );
  const optimizationParityMaxAttempts = readPositiveIntegerFromEnv(
    "OPTIMIZATION_PARITY_MAX_ATTEMPTS"
  );
  const optimizationValidationMaxLogs = readPositiveIntegerFromEnv(
    "OPTIMIZATION_VALIDATION_MAX_LOGS"
  );
  const optimizationValidationFullResultMaxRows = readPositiveIntegerFromEnv(
    "OPTIMIZATION_VALIDATION_FULL_RESULT_MAX_ROWS"
  );
  const optimizationValidationFullResultMaxCells = readPositiveIntegerFromEnv(
    "OPTIMIZATION_VALIDATION_FULL_RESULT_MAX_CELLS"
  );
  const optimizationValidationPerLogTimeoutMs = readPositiveIntegerFromEnv(
    "OPTIMIZATION_VALIDATION_PER_LOG_TIMEOUT_MS"
  );
  const optimizationParityMinPassRatio = readRatioFromEnv(
    "OPTIMIZATION_PARITY_MIN_PASS_RATIO"
  );
  const database = await openSourceDatabase({
    databaseFilePath: resolve(
      options.databaseFilePath ??
        process.env.SOURCE_DATABASE_PATH ??
        ".data/source-datasets.sqlite"
    ),
  });
  const repository = new SqliteSourceDatasetRepository({
    connection: database,
  });
  const codexRunEventHub = createCodexRunEventHub();
  const pipelineRetryScheduler = createPipelineRetryScheduler({
    cleanDatabaseBuilder: createSqliteCleanDatabaseBuilder(),
    cleanDatabaseDirectoryPath: resolve(
      options.cleanDatabaseDirectoryPath ??
        process.env.CLEAN_DATABASE_DIRECTORY_PATH ??
        ".data/clean-databases"
    ),
    codexPipelineGenerator: createCodexCliPipelineGenerator({
      ...(process.env.CODEX_PIPELINE_MODEL
        ? { model: process.env.CODEX_PIPELINE_MODEL }
        : {}),
      ...(codexPipelineCommandTimeoutMs !== undefined
        ? { commandTimeoutMs: codexPipelineCommandTimeoutMs }
        : {}),
      ...(codexPlaywrightMcpStartupTimeoutSec !== undefined
        ? {
            playwrightMcpStartupTimeoutSec: codexPlaywrightMcpStartupTimeoutSec,
          }
        : {}),
      ...(codexRetainWorkspaces !== undefined
        ? { retainWorkspaceOnSuccess: codexRetainWorkspaces }
        : {}),
    }),
    onRunEvent: (runEvent) => {
      codexRunEventHub.publish(runEvent);
    },
    repository,
    sourceDatabasePath: database.databaseFilePath,
    sqlValidator: {
      validate: validatePipelineSql,
    },
  });
  const api = createIngestionApi({
    pipelineRetryScheduler,
    repository,
  });
  const queryLearningLoop = createQueryLearningLoop({
    cleanDatabaseBuilder: createSqliteCleanDatabaseBuilder(),
    cleanDatabaseDirectoryPath: resolve(
      options.cleanDatabaseDirectoryPath ??
        process.env.CLEAN_DATABASE_DIRECTORY_PATH ??
        ".data/clean-databases"
    ),
    codexOptimizationGenerator: createCodexCliOptimizationGenerator({
      ...(process.env.CODEX_OPTIMIZATION_MODEL
        ? { model: process.env.CODEX_OPTIMIZATION_MODEL }
        : {}),
      ...(codexOptimizationCommandTimeoutMs !== undefined
        ? { commandTimeoutMs: codexOptimizationCommandTimeoutMs }
        : {}),
      ...(codexPlaywrightMcpStartupTimeoutSec !== undefined
        ? {
            playwrightMcpStartupTimeoutSec: codexPlaywrightMcpStartupTimeoutSec,
          }
        : {}),
      ...(codexRetainWorkspaces !== undefined
        ? { retainWorkspaceOnSuccess: codexRetainWorkspaces }
        : {}),
    }),
    ...(optimizationRetryBackoffMs !== undefined
      ? { optimizationRetryBackoffMs }
      : {}),
    ...(optimizationRetryLimitPerCandidate !== undefined
      ? { optimizationRetryLimitPerCandidate }
      : {}),
    ...(optimizationParityMaxAttempts !== undefined
      ? { optimizationParityMaxAttempts }
      : {}),
    ...(optimizationValidationMaxLogs !== undefined
      ? { optimizationValidationMaxLogs }
      : {}),
    ...(optimizationValidationFullResultMaxRows !== undefined
      ? { optimizationValidationFullResultMaxRows }
      : {}),
    ...(optimizationValidationFullResultMaxCells !== undefined
      ? { optimizationValidationFullResultMaxCells }
      : {}),
    ...(optimizationValidationPerLogTimeoutMs !== undefined
      ? { optimizationValidationPerLogTimeoutMs }
      : {}),
    ...(optimizationParityMinPassRatio !== undefined
      ? { optimizationParityMinPassRatio }
      : {}),
    onRunEvent: (runEvent) => {
      codexRunEventHub.publish(runEvent);
    },
    queryGenerator: createOpenAiSqlQueryGenerator(),
    querySqlValidator: {
      validate: validateQuerySql,
    },
    queryExecutor: createSqliteQueryExecutor(),
    repository,
    sourceDatabasePath: database.databaseFilePath,
    sqlValidator: {
      validate: validatePipelineSql,
    },
  });
  const queryApi = createQueryApi({
    onRunEvent: (runEvent) => {
      codexRunEventHub.publish(runEvent);
    },
    queryLearningLoop,
    queryExecutor: createSqliteQueryExecutor(),
    queryGenerator: createOpenAiSqlQueryGenerator(),
    repository,
    sqlValidator: {
      validate: validateQuerySql,
    },
  });
  const optimizationApi = createOptimizationApi({
    queryLearningLoop,
    repository,
  });
  const port = options.port ?? Number(process.env.API_PORT ?? "3001");

  pipelineRetryScheduler.resumePendingWork();
  repository.list().forEach((dataset) => {
    queryLearningLoop.schedule(dataset.id);
  });

  const server = createServer((request, response) => {
    void (async () => {
      try {
        if (
          request.method === "GET" &&
          request.url?.startsWith("/api/stream/")
        ) {
          const requestUrl = new URL(request.url, "http://localhost");
          const datasetId = requestUrl.pathname.split("/").at(-1);

          if (!datasetId) {
            response.writeHead(400, {
              "content-type": "application/json; charset=utf-8",
            });
            response.end(JSON.stringify({ error: "Dataset id is required." }));
            return;
          }

          codexRunEventHub.handleSse(
            datasetId,
            request,
            response,
            repository.listCodexRunEvents(datasetId)
          );
          return;
        }

        const handledQueryRequest = await handleQueryRequest({
          api: queryApi,
          request,
          response,
        });

        if (handledQueryRequest) {
          return;
        }

        const handledOptimizationRequest = await handleOptimizationRequest({
          api: optimizationApi,
          request,
          response,
        });

        if (handledOptimizationRequest) {
          return;
        }

        const handled = await handleIngestionRequest({
          api,
          request,
          response,
        });

        if (!handled) {
          response.writeHead(404, {
            "content-type": "application/json; charset=utf-8",
          });
          response.end(JSON.stringify({ error: "Route not found." }));
        }
      } catch (error) {
        console.error("API server request failed", error);
        response.writeHead(500, {
          "content-type": "application/json; charset=utf-8",
        });
        response.end(JSON.stringify({ error: "Internal server error." }));
      }
    })();
  });

  await new Promise<void>((resolve) => {
    server.listen(port, resolve);
  });

  return {
    port,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          pipelineRetryScheduler
            .drain()
            .then(() => {
              queryLearningLoop
                .drain()
                .then(() => {
                  database.close();
                  resolve();
                })
                .catch(reject);
            })
            .catch(reject);
        });
      }),
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  loadLocalEnvironment();
  const started = await startApiServer();
  console.log(`API server listening on http://127.0.0.1:${started.port}`);
}

function readPositiveIntegerFromEnv(name: string): number | undefined {
  const rawValue = process.env[name];

  if (!rawValue) {
    return undefined;
  }

  const parsed = Number(rawValue);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return undefined;
  }

  return parsed;
}

function readBooleanFromEnv(name: string): boolean | undefined {
  const rawValue = process.env[name];

  if (!rawValue) {
    return undefined;
  }

  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") {
    return true;
  }

  if (normalized === "false" || normalized === "0" || normalized === "no") {
    return false;
  }

  return undefined;
}

function readRatioFromEnv(name: string): number | undefined {
  const rawValue = process.env[name];
  if (!rawValue) {
    return undefined;
  }
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    return undefined;
  }
  return parsed;
}
