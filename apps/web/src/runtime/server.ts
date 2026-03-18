import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";

import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";
import { parseWorkbookFile } from "../upload-workspace/parse-workbook-file.js";
import { renderUploadWorkspacePage } from "../upload-workspace/page.js";
import { readWorkbookUpload } from "./multipart.js";

export interface WebServerOptions {
  apiBaseUrl?: string;
  port?: number;
}

export async function startWebServer(options: WebServerOptions = {}): Promise<{
  close: () => Promise<void>;
  port: number;
}> {
  const apiBaseUrl =
    options.apiBaseUrl ?? process.env.API_BASE_URL ?? "http://127.0.0.1:3001";
  const port = options.port ?? Number(process.env.WEB_PORT ?? "3000");

  const server = createServer((request, response) => {
    void (async () => {
      try {
        const requestUrl = new URL(request.url ?? "/", "http://localhost");

        if (request.method === "GET" && requestUrl.pathname === "/health") {
          response.writeHead(200, {
            "content-type": "application/json; charset=utf-8",
          });
          response.end(JSON.stringify({ status: "ok" }));
          return;
        }

        if (request.method === "GET" && requestUrl.pathname === "/") {
          respondHtml(response, renderUploadWorkspacePage());
          return;
        }

        if (
          request.method === "GET" &&
          requestUrl.pathname.startsWith("/imports/")
        ) {
          const datasetId = requestUrl.pathname.split("/").at(-1);

          if (!datasetId) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                errorMessage: "Dataset id is required.",
              }),
              400
            );
            return;
          }

          const importResponse = await fetch(
            `${apiBaseUrl}/api/imports/${datasetId}`
          );
          const payload = await readImportResponse(importResponse);

          if (!importResponse.ok || !payload.summary) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                errorMessage: payload.error ?? "Import status not found.",
              }),
              importResponse.status
            );
            return;
          }

          respondHtml(
            response,
            renderUploadWorkspacePage({
              importSummary: payload.summary,
            })
          );
          return;
        }

        if (request.method === "POST" && requestUrl.pathname === "/imports") {
          const workbookUpload = await readWorkbookUpload(request);
          const workbookRequest = parseWorkbookFile(workbookUpload);
          const workbookJson = JSON.stringify(workbookRequest);

          try {
            const importResponse = await fetch(`${apiBaseUrl}/api/imports`, {
              method: "POST",
              headers: {
                "content-type": "application/json; charset=utf-8",
              },
              body: workbookJson,
            });

            const payload = await readImportResponse(importResponse);

            if (!importResponse.ok || !payload.summary) {
              respondHtml(
                response,
                renderUploadWorkspacePage({
                  errorMessage: payload.error ?? "Import failed.",
                }),
                importResponse.status
              );
              return;
            }

            response.writeHead(303, {
              location: `/imports/${payload.summary.sourceDatasetId}`,
            });
            response.end();
            return;
          } catch (error) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                errorMessage:
                  error instanceof Error ? error.message : "Import failed.",
              }),
              500
            );
            return;
          }
        }

        response.writeHead(404, {
          "content-type": "text/plain; charset=utf-8",
        });
        response.end("Not found");
      } catch (error) {
        console.error("Web server request failed", error);
        respondHtml(response, "<h1>Internal server error</h1>", 500);
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

async function readImportResponse(response: Response): Promise<{
  error?: string;
  summary?: WorkbookImportSummary;
}> {
  const payload: unknown = await response.json();

  if (!isImportResponse(payload)) {
    throw new Error("Import API returned an unexpected response.");
  }

  return payload;
}

function isImportResponse(
  value: unknown
): value is { error?: string; summary?: WorkbookImportSummary } {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  if ("error" in candidate && typeof candidate.error !== "string") {
    return false;
  }

  if ("summary" in candidate && typeof candidate.summary !== "object") {
    return false;
  }

  return true;
}

function respondHtml(
  response: ServerResponse<IncomingMessage>,
  html: string,
  statusCode = 200
): void {
  response.writeHead(statusCode, {
    "content-type": "text/html; charset=utf-8",
  });
  response.end(html);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const started = await startWebServer();
  console.log(`Web server listening on http://127.0.0.1:${started.port}`);
}
