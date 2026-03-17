import { createServer } from "node:http";

import { createInMemoryIngestionApi } from "../ingestion/api.js";
import { handleIngestionRequest } from "../ingestion/http.js";

export interface ApiServerOptions {
  port?: number;
}

export async function startApiServer(options: ApiServerOptions = {}): Promise<{
  close: () => Promise<void>;
  port: number;
}> {
  const api = createInMemoryIngestionApi();
  const port = options.port ?? Number(process.env.API_PORT ?? "3001");

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

          resolve();
        });
      }),
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const started = await startApiServer();
  console.log(`API server listening on http://127.0.0.1:${started.port}`);
}
