import type { IncomingMessage, ServerResponse } from "node:http";

import type { WorkbookUploadRequest } from "../../../../packages/shared/src/index.js";
import type { IngestionApi } from "./api.js";

export async function handleIngestionRequest(options: {
  api: IngestionApi;
  request: IncomingMessage;
  response: ServerResponse<IncomingMessage>;
}): Promise<boolean> {
  const { api, request, response } = options;
  const requestUrl = new URL(request.url ?? "/", "http://localhost");

  if (request.method === "GET" && requestUrl.pathname === "/api/health") {
    writeJson(response, 200, { status: "ok" });
    return true;
  }

  if (request.method === "GET" && requestUrl.pathname === "/api/imports") {
    writeJson(response, 200, { imports: api.listImports() });
    return true;
  }

  if (
    request.method === "GET" &&
    requestUrl.pathname.startsWith("/api/imports/")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-1);

    if (!datasetId) {
      writeJson(response, 400, { error: "Dataset id is required." });
      return true;
    }

    const summary = api.getImportSummary(datasetId);

    if (!summary) {
      writeJson(response, 404, { error: "Import summary not found." });
      return true;
    }

    writeJson(response, 200, { summary });
    return true;
  }

  if (
    request.method === "GET" &&
    requestUrl.pathname.startsWith("/api/datasets/")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-1);

    if (!datasetId) {
      writeJson(response, 400, { error: "Dataset id is required." });
      return true;
    }

    const dataset = api.getSourceDataset(datasetId);

    if (!dataset) {
      writeJson(response, 404, { error: "Source dataset not found." });
      return true;
    }

    writeJson(response, 200, { dataset });
    return true;
  }

  if (request.method === "POST" && requestUrl.pathname === "/api/imports") {
    try {
      const requestBody = (await readJsonBody(
        request
      )) as WorkbookUploadRequest;
      const summary = api.importWorkbook(requestBody);

      writeJson(response, 201, { summary });
    } catch (error) {
      writeJson(response, 400, {
        error: error instanceof Error ? error.message : "Import failed.",
      });
    }

    return true;
  }

  return false;
}

export async function readJsonBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Uint8Array[] = [];

  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  const body = Buffer.concat(chunks).toString("utf8");
  return JSON.parse(body);
}

export function writeJson(
  response: ServerResponse<IncomingMessage>,
  statusCode: number,
  payload: unknown
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}
