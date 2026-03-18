import { resolve } from "node:path";
import { createServer } from "node:http";

import { createCodexCliPipelineGenerator } from "../../../../packages/agent-orchestrator/src/index.js";
import { createOpenAiSqlQueryGenerator } from "../../../../packages/agent-orchestrator/src/index.js";
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
    codexPipelineGenerator: createCodexCliPipelineGenerator(),
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
  const queryApi = createQueryApi({
    onRunEvent: (runEvent) => {
      codexRunEventHub.publish(runEvent);
    },
    queryExecutor: createSqliteQueryExecutor(),
    queryGenerator: createOpenAiSqlQueryGenerator(),
    repository,
    sqlValidator: {
      validate: validateQuerySql,
    },
  });
  const port = options.port ?? Number(process.env.API_PORT ?? "3001");

  pipelineRetryScheduler.resumePendingWork();

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
              database.close();
              resolve();
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
