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

  return false;
}

function writeJson(
  response: ServerResponse<IncomingMessage>,
  statusCode: number,
  payload: {
    error?: string;
    optimizationRevisions?: unknown;
    queryClusters?: unknown;
  }
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}
