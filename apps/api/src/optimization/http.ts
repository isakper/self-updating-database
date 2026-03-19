import type { IncomingMessage, ServerResponse } from "node:http";

import type { OptimizationApi } from "./api.js";

export function handleOptimizationRequest(options: {
  api: OptimizationApi;
  request: IncomingMessage;
  response: ServerResponse<IncomingMessage>;
}): boolean {
  const requestUrl = new URL(options.request.url ?? "/", "http://localhost");

  if (
    options.request.method === "GET" &&
    requestUrl.pathname.startsWith("/api/optimization-insights/")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-1);

    if (!datasetId) {
      writeJson(options.response, 400, { error: "Dataset id is required." });
      return true;
    }

    writeJson(options.response, 200, options.api.getInsights(datasetId));
    return true;
  }

  if (
    options.request.method === "POST" &&
    requestUrl.pathname.startsWith("/api/optimization-runs/")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-1);

    if (!datasetId) {
      writeJson(options.response, 400, { error: "Dataset id is required." });
      return true;
    }

    writeJson(options.response, 202, options.api.triggerRun(datasetId));
    return true;
  }

  if (
    options.request.method === "POST" &&
    requestUrl.pathname.startsWith("/api/optimization-retries/")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-1);

    if (!datasetId) {
      writeJson(options.response, 400, { error: "Dataset id is required." });
      return true;
    }

    const result = options.api.retryLatestFailedRevision(datasetId);
    writeJson(options.response, result.accepted ? 202 : 409, result);
    return true;
  }

  return false;
}

function writeJson(
  response: ServerResponse<IncomingMessage>,
  statusCode: number,
  payload: {
    error?: string;
    accepted?: boolean;
    message?: string;
    optimizationRevisions?: unknown;
    queryClusters?: unknown;
  }
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}
