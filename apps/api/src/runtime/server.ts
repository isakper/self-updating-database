import { resolve } from "node:path";
import { createServer } from "node:http";

import { createCodexCliPipelineGenerator } from "../../../../packages/agent-orchestrator/src/index.js";
import {
  openSourceDatabase,
  SqliteSourceDatasetRepository,
} from "../../../../packages/database-core/src/index.js";
import {
  createSqliteCleanDatabaseBuilder,
  validatePipelineSql,
} from "../../../../packages/pipeline-sdk/src/index.js";
import { createIngestionApi } from "../ingestion/api.js";
import { handleIngestionRequest } from "../ingestion/http.js";
import { createPipelineRetryScheduler } from "../ingestion/pipeline.js";

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
  const pipelineRetryScheduler = createPipelineRetryScheduler({
    cleanDatabaseBuilder: createSqliteCleanDatabaseBuilder(),
    cleanDatabaseDirectoryPath: resolve(
      options.cleanDatabaseDirectoryPath ??
        process.env.CLEAN_DATABASE_DIRECTORY_PATH ??
        ".data/clean-databases"
    ),
    codexPipelineGenerator: createCodexCliPipelineGenerator(),
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
  const port = options.port ?? Number(process.env.API_PORT ?? "3001");

  pipelineRetryScheduler.resumePendingWork();

  const server = createServer((request, response) => {
    void (async () => {
      try {
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
  const started = await startApiServer();
  console.log(`API server listening on http://127.0.0.1:${started.port}`);
}
