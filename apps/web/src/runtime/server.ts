import { createServer, request as httpRequest } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import { once } from "node:events";
import { request as httpsRequest } from "node:https";

import type {
  NaturalLanguageQueryResponse,
  QueryExecutionLog,
  WorkbookImportSummary,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";
import { parseWorkbookFile } from "../upload-workspace/parse-workbook-file.js";
import {
  renderUploadWorkspacePage,
  renderWorkspaceFragments,
} from "../upload-workspace/page.js";
import { readWorkbookUpload } from "./multipart.js";
import { loadLocalEnvironment } from "../../../shared/load-env.js";

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
          if (requestUrl.pathname.endsWith("/view-state")) {
            const datasetId = requestUrl.pathname.split("/").at(-2);

            if (!datasetId) {
              response.writeHead(400, {
                "content-type": "application/json; charset=utf-8",
              });
              response.end(
                JSON.stringify({ error: "Dataset id is required." })
              );
              return;
            }

            const viewState = await fetchViewState(apiBaseUrl, datasetId);

            if (!viewState.importSummary) {
              response.writeHead(viewState.statusCode, {
                "content-type": "application/json; charset=utf-8",
              });
              response.end(
                JSON.stringify({
                  error: viewState.error ?? "Import status not found.",
                })
              );
              return;
            }

            response.writeHead(200, {
              "content-type": "application/json; charset=utf-8",
            });
            response.end(
              JSON.stringify(
                renderWorkspaceFragments({
                  importSummary: viewState.importSummary,
                  queryLogs: viewState.queryLogs,
                })
              )
            );
            return;
          }

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

          const { payload, statusCode } = await requestJsonFromApi<{
            error?: string;
            summary?: WorkbookImportSummary;
          }>({
            validate: isImportResponse,
            url: `${apiBaseUrl}/api/imports/${datasetId}`,
          });
          const queryLogsPayload = await requestJsonFromApi<{
            error?: string;
            queryLogs?: QueryExecutionLog[];
          }>({
            validate: isQueryLogsResponse,
            url: `${apiBaseUrl}/api/query-logs/${datasetId}`,
          });

          if (statusCode < 200 || statusCode >= 300 || !payload.summary) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                errorMessage: payload.error ?? "Import status not found.",
              }),
              statusCode
            );
            return;
          }

          respondHtml(
            response,
            renderUploadWorkspacePage({
              activeTab: parseRequestedTab(requestUrl.searchParams.get("tab")),
              importSummary: payload.summary,
              queryLogs: queryLogsPayload.payload.queryLogs ?? [],
            })
          );
          return;
        }

        if (
          request.method === "GET" &&
          requestUrl.pathname.startsWith("/events/")
        ) {
          const datasetId = requestUrl.pathname.split("/").at(-1);

          if (!datasetId) {
            response.writeHead(400, {
              "content-type": "text/plain; charset=utf-8",
            });
            response.end("Dataset id is required.");
            return;
          }

          await proxyEventStream({
            apiBaseUrl,
            datasetId,
            request,
            response,
          });
          return;
        }

        if (request.method === "POST" && requestUrl.pathname === "/imports") {
          const workbookUpload = await readWorkbookUpload(request);
          const workbookRequest = parseWorkbookFile(workbookUpload);
          const workbookJson = JSON.stringify(workbookRequest);

          try {
            const { payload, statusCode } = await requestJsonFromApi<{
              error?: string;
              summary?: WorkbookImportSummary;
            }>({
              body: workbookJson,
              headers: {
                "content-type": "application/json; charset=utf-8",
              },
              method: "POST",
              validate: isImportResponse,
              url: `${apiBaseUrl}/api/imports`,
            });

            if (statusCode < 200 || statusCode >= 300 || !payload.summary) {
              respondHtml(
                response,
                renderUploadWorkspacePage({
                  errorMessage: payload.error ?? "Import failed.",
                }),
                statusCode
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

        if (
          request.method === "POST" &&
          requestUrl.pathname.startsWith("/imports/") &&
          requestUrl.pathname.endsWith("/query-logs/import")
        ) {
          const datasetId = requestUrl.pathname.split("/").at(-2);

          if (!datasetId) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                activeTab: "logs",
                errorMessage: "Dataset id is required.",
              }),
              400
            );
            return;
          }

          const workbookUpload = await readWorkbookUpload(request);
          const workbookRequest = parseWorkbookFile(workbookUpload);

          try {
            const imported = await importQueryLogs({
              apiBaseUrl,
              datasetId,
              workbookRequest,
            });

            response.writeHead(303, {
              location: `/imports/${datasetId}?tab=logs&imported=${imported.importedCount}`,
            });
            response.end();
            return;
          } catch (error) {
            const viewState = await fetchViewState(apiBaseUrl, datasetId);

            respondHtml(
              response,
              renderUploadWorkspacePage({
                activeTab: "logs",
                ...(viewState.importSummary
                  ? { importSummary: viewState.importSummary }
                  : {}),
                queryLogs: viewState.queryLogs,
                queryLogsErrorMessage:
                  error instanceof Error
                    ? error.message
                    : "Mock query-log import failed.",
              }),
              400
            );
            return;
          }
        }

        if (
          request.method === "POST" &&
          requestUrl.pathname.startsWith("/imports/") &&
          requestUrl.pathname.endsWith("/query-partial")
        ) {
          const datasetId = requestUrl.pathname.split("/").at(-2);

          if (!datasetId) {
            response.writeHead(400, {
              "content-type": "application/json; charset=utf-8",
            });
            response.end(JSON.stringify({ error: "Dataset id is required." }));
            return;
          }

          const prompt = await readFormField(request, "prompt");
          const importViewState = await fetchViewState(apiBaseUrl, datasetId);

          if (!importViewState.importSummary) {
            response.writeHead(importViewState.statusCode, {
              "content-type": "application/json; charset=utf-8",
            });
            response.end(
              JSON.stringify({
                error: importViewState.error ?? "Import status not found.",
              })
            );
            return;
          }

          try {
            const queryRequestBody = JSON.stringify({
              prompt,
              sourceDatasetId: datasetId,
            });
            const { payload, statusCode } = await requestJsonFromApi<{
              error?: string;
              generatedSqlRecord?: NaturalLanguageQueryResponse["generatedSqlRecord"];
              queryLog?: NaturalLanguageQueryResponse["queryLog"];
              queryResult?: NaturalLanguageQueryResponse;
            }>({
              body: queryRequestBody,
              headers: {
                "content-type": "application/json; charset=utf-8",
              },
              method: "POST",
              validate: isQueryResponse,
              url: `${apiBaseUrl}/api/queries`,
            });
            const queryLogsPayload = await requestJsonFromApi<{
              error?: string;
              queryLogs?: QueryExecutionLog[];
            }>({
              validate: isQueryLogsResponse,
              url: `${apiBaseUrl}/api/query-logs/${datasetId}`,
            });
            const fragments = renderWorkspaceFragments({
              importSummary: importViewState.importSummary,
              ...(statusCode < 200 || statusCode >= 300 || !payload.queryResult
                ? { queryErrorMessage: payload.error ?? "Query failed." }
                : {}),
              queryLogs: queryLogsPayload.payload.queryLogs ?? [],
              queryPrompt: prompt,
              ...(payload.queryResult
                ? { queryResponse: payload.queryResult }
                : {}),
            });

            response.writeHead(
              statusCode >= 200 && statusCode < 300 ? 200 : statusCode,
              {
                "content-type": "application/json; charset=utf-8",
              }
            );
            response.end(
              JSON.stringify({
                ...fragments,
                ...(payload.error ? { error: payload.error } : {}),
              })
            );
            return;
          } catch (error) {
            const fragments = renderWorkspaceFragments({
              importSummary: importViewState.importSummary,
              queryErrorMessage:
                error instanceof Error ? error.message : "Query failed.",
              queryLogs: importViewState.queryLogs,
              queryPrompt: prompt,
            });

            response.writeHead(500, {
              "content-type": "application/json; charset=utf-8",
            });
            response.end(
              JSON.stringify({
                ...fragments,
                error: error instanceof Error ? error.message : "Query failed.",
              })
            );
            return;
          }
        }

        if (
          request.method === "POST" &&
          requestUrl.pathname.startsWith("/imports/") &&
          requestUrl.pathname.endsWith("/query")
        ) {
          const datasetId = requestUrl.pathname.split("/").at(-2);

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

          const prompt = await readFormField(request, "prompt");
          const { payload: importPayload, statusCode: importStatusCode } =
            await requestJsonFromApi<{
              error?: string;
              summary?: WorkbookImportSummary;
            }>({
              url: `${apiBaseUrl}/api/imports/${datasetId}`,
              validate: isImportResponse,
            });

          if (
            importStatusCode < 200 ||
            importStatusCode >= 300 ||
            !importPayload.summary
          ) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                errorMessage: importPayload.error ?? "Import status not found.",
              }),
              importStatusCode
            );
            return;
          }

          try {
            const queryRequestBody = JSON.stringify({
              prompt,
              sourceDatasetId: datasetId,
            });
            const { payload, statusCode } = await requestJsonFromApi<{
              error?: string;
              generatedSqlRecord?: NaturalLanguageQueryResponse["generatedSqlRecord"];
              queryLog?: NaturalLanguageQueryResponse["queryLog"];
              queryResult?: NaturalLanguageQueryResponse;
            }>({
              body: queryRequestBody,
              headers: {
                "content-type": "application/json; charset=utf-8",
              },
              method: "POST",
              validate: isQueryResponse,
              url: `${apiBaseUrl}/api/queries`,
            });

            if (statusCode < 200 || statusCode >= 300 || !payload.queryResult) {
              respondHtml(
                response,
                renderUploadWorkspacePage(
                  payload.generatedSqlRecord && payload.queryLog
                    ? {
                        importSummary: importPayload.summary,
                        queryLogs: [payload.queryLog],
                        queryErrorMessage: payload.error ?? "Query failed.",
                        queryPrompt: prompt,
                        queryResponse: {
                          generatedSqlRecord: payload.generatedSqlRecord,
                          queryLog: payload.queryLog,
                          result: null,
                        },
                      }
                    : {
                        importSummary: importPayload.summary,
                        queryLogs: [],
                        queryErrorMessage: payload.error ?? "Query failed.",
                        queryPrompt: prompt,
                      }
                ),
                statusCode
              );
              return;
            }

            respondHtml(
              response,
              renderUploadWorkspacePage({
                importSummary: importPayload.summary,
                queryLogs: [payload.queryResult.queryLog],
                queryPrompt: prompt,
                queryResponse: payload.queryResult,
              })
            );
            return;
          } catch (error) {
            respondHtml(
              response,
              renderUploadWorkspacePage({
                importSummary: importPayload.summary,
                queryLogs: [],
                queryErrorMessage:
                  error instanceof Error ? error.message : "Query failed.",
                queryPrompt: prompt,
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
        if (!response.headersSent) {
          respondHtml(response, "<h1>Internal server error</h1>", 500);
          return;
        }

        if (!response.writableEnded) {
          response.end();
        }
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

async function importQueryLogs(options: {
  apiBaseUrl: string;
  datasetId: string;
  workbookRequest: WorkbookUploadRequest;
}): Promise<{ importedCount: number }> {
  const { payload, statusCode } = await requestJsonFromApi<{
    error?: string;
    importedCount?: number;
  }>({
    body: JSON.stringify(options.workbookRequest),
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
    method: "POST",
    url: `${options.apiBaseUrl}/api/query-logs/${options.datasetId}/import`,
    validate: isQueryLogImportResponse,
  });

  if (
    statusCode < 200 ||
    statusCode >= 300 ||
    payload.importedCount === undefined
  ) {
    throw new Error(payload.error ?? "Mock query-log import failed.");
  }

  return {
    importedCount: payload.importedCount,
  };
}

async function fetchViewState(
  apiBaseUrl: string,
  datasetId: string
): Promise<{
  error?: string;
  importSummary?: WorkbookImportSummary;
  queryLogs: QueryExecutionLog[];
  statusCode: number;
}> {
  const { payload, statusCode } = await requestJsonFromApi<{
    error?: string;
    summary?: WorkbookImportSummary;
  }>({
    validate: isImportResponse,
    url: `${apiBaseUrl}/api/imports/${datasetId}`,
  });
  const queryLogsPayload = await requestJsonFromApi<{
    error?: string;
    queryLogs?: QueryExecutionLog[];
  }>({
    validate: isQueryLogsResponse,
    url: `${apiBaseUrl}/api/query-logs/${datasetId}`,
  });

  return {
    queryLogs: queryLogsPayload.payload.queryLogs ?? [],
    statusCode,
    ...(payload.error ? { error: payload.error } : {}),
    ...(payload.summary ? { importSummary: payload.summary } : {}),
  };
}

async function proxyEventStream(options: {
  apiBaseUrl: string;
  datasetId: string;
  request: IncomingMessage;
  response: ServerResponse<IncomingMessage>;
}): Promise<void> {
  const upstreamResponse = await fetch(
    `${options.apiBaseUrl}/api/stream/${options.datasetId}`,
    {
      headers: {
        accept: "text/event-stream",
      },
    }
  );

  if (!upstreamResponse.ok || !upstreamResponse.body) {
    options.response.writeHead(upstreamResponse.status || 502, {
      "content-type": "text/plain; charset=utf-8",
    });
    options.response.end("Unable to open Codex event stream.");
    return;
  }

  options.response.writeHead(200, {
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "content-type": "text/event-stream; charset=utf-8",
  });

  const abortController = new AbortController();
  const closeUpstream = () => {
    abortController.abort();
    options.response.end();
  };

  options.request.on("close", closeUpstream);
  options.response.on("close", closeUpstream);

  try {
    for await (const chunk of upstreamResponse.body) {
      if (!options.response.write(chunk)) {
        await once(options.response, "drain");
      }
    }
  } finally {
    options.request.off("close", closeUpstream);
    options.response.off("close", closeUpstream);
    options.response.end();
  }
}

async function requestJsonFromApi<T>(options: {
  body?: string;
  headers?: Record<string, string>;
  method?: "GET" | "POST";
  url: string;
  validate?: (value: unknown) => value is T;
}): Promise<{ payload: T; statusCode: number }> {
  const url = new URL(options.url);
  const requestFn = url.protocol === "https:" ? httpsRequest : httpRequest;

  return await new Promise((resolve, reject) => {
    const request = requestFn(
      url,
      {
        headers: options.headers,
        method: options.method ?? "GET",
      },
      (response) => {
        const chunks: Uint8Array[] = [];

        response.on("data", (chunk: Buffer | string) => {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        response.on("end", () => {
          try {
            const body = Buffer.concat(chunks).toString("utf8");
            const parsed: unknown = JSON.parse(body);

            if (options.validate && !options.validate(parsed)) {
              reject(new Error("API returned an unexpected response."));
              return;
            }

            resolve({
              payload: parsed as T,
              statusCode: response.statusCode ?? 500,
            });
          } catch (error) {
            reject(
              error instanceof Error
                ? error
                : new Error("API response could not be parsed.")
            );
          }
        });
        response.on("error", reject);
      }
    );

    request.on("error", reject);

    if (options.body) {
      request.write(options.body);
    }

    request.end();
  });
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

function isQueryResponse(value: unknown): value is {
  error?: string;
  generatedSqlRecord?: NaturalLanguageQueryResponse["generatedSqlRecord"];
  queryLog?: NaturalLanguageQueryResponse["queryLog"];
  queryResult?: NaturalLanguageQueryResponse;
} {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  if ("error" in candidate && typeof candidate.error !== "string") {
    return false;
  }

  return true;
}

function isQueryLogsResponse(
  value: unknown
): value is { error?: string; queryLogs?: QueryExecutionLog[] } {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  if ("error" in candidate && typeof candidate.error !== "string") {
    return false;
  }

  if ("queryLogs" in candidate && !Array.isArray(candidate.queryLogs)) {
    return false;
  }

  return true;
}

function isQueryLogImportResponse(
  value: unknown
): value is { error?: string; importedCount?: number } {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;

  if ("error" in candidate && typeof candidate.error !== "string") {
    return false;
  }

  if (
    "importedCount" in candidate &&
    typeof candidate.importedCount !== "number"
  ) {
    return false;
  }

  return true;
}

async function readFormField(
  request: IncomingMessage,
  fieldName: string
): Promise<string> {
  const chunks: Uint8Array[] = [];

  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  const formData = new URLSearchParams(Buffer.concat(chunks).toString("utf8"));
  const value = formData.get(fieldName)?.trim();

  if (!value) {
    throw new Error(`${fieldName} is required.`);
  }

  return value;
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

function parseRequestedTab(value: string | null): "logs" | "query" | "upload" {
  if (value === "query" || value === "logs") {
    return value;
  }

  return "upload";
}

if (import.meta.url === `file://${process.argv[1]}`) {
  loadLocalEnvironment();
  const started = await startWebServer();
  console.log(`Web server listening on http://127.0.0.1:${started.port}`);
}
