import type { IncomingMessage, ServerResponse } from "node:http";

import { readJsonBody } from "../ingestion/http.js";
import type { OptimizationApi } from "./api.js";

export async function handleOptimizationRequest(options: {
  api: OptimizationApi;
  request: IncomingMessage;
  response: ServerResponse<IncomingMessage>;
}): Promise<boolean> {
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

    let basePipelineVersionId: string | undefined;
    // Node's IncomingMessage header bag typing is broader than needed here.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const contentLengthHeader = options.request.headers["content-length"];
    const contentLengthRaw = Array.isArray(contentLengthHeader)
      ? contentLengthHeader[0]
      : contentLengthHeader;
    const parsedContentLength =
      contentLengthRaw === undefined ? Number.NaN : Number(contentLengthRaw);
    const hasBody =
      options.request.headers["transfer-encoding"] !== undefined ||
      (!Number.isNaN(parsedContentLength) && parsedContentLength > 0);

    if (hasBody) {
      try {
        const requestBody: unknown = await readJsonBody(options.request);
        const requestBodyObject =
          requestBody !== null && typeof requestBody === "object"
            ? (requestBody as Record<string, unknown>)
            : {};
        const requestedBasePipelineVersionId =
          requestBodyObject["basePipelineVersionId"];
        if (requestedBasePipelineVersionId !== undefined) {
          if (
            typeof requestedBasePipelineVersionId !== "string" ||
            requestedBasePipelineVersionId.trim().length === 0
          ) {
            writeJson(options.response, 400, {
              error:
                "basePipelineVersionId must be a non-empty string when provided.",
            });
            return true;
          }
          basePipelineVersionId = requestedBasePipelineVersionId.trim();
        }
      } catch {
        writeJson(options.response, 400, {
          error: "Invalid JSON body for optimization run request.",
        });
        return true;
      }
    }

    const result =
      basePipelineVersionId === undefined
        ? options.api.triggerRun(datasetId)
        : options.api.triggerRun(datasetId, { basePipelineVersionId });
    writeJson(options.response, result.accepted ? 202 : 409, result);
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
