import type { IncomingMessage, ServerResponse } from "node:http";

import type {
  NaturalLanguageQueryRequest,
  NaturalLanguageQueryResponse,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";
import { QueryApiError, type QueryApi } from "./api.js";

export async function handleQueryRequest(options: {
  api: QueryApi;
  request: IncomingMessage;
  response: ServerResponse<IncomingMessage>;
}): Promise<boolean> {
  const { api, request, response } = options;
  const requestUrl = new URL(request.url ?? "/", "http://localhost");

  if (request.method === "POST" && requestUrl.pathname === "/api/queries") {
    try {
      const requestBody = (await readJsonBody(
        request
      )) as NaturalLanguageQueryRequest;
      const queryResult = await api.runNaturalLanguageQuery(requestBody);

      writeJson(response, 201, { queryResult });
    } catch (error) {
      if (error instanceof QueryApiError) {
        writeJson(response, error.statusCode, {
          error: error.message,
          ...error.payload,
        });
        return true;
      }

      writeJson(response, 500, {
        error:
          error instanceof Error ? error.message : "Query execution failed.",
      });
    }

    return true;
  }

  if (
    request.method === "POST" &&
    requestUrl.pathname.startsWith("/api/query-logs/") &&
    requestUrl.pathname.endsWith("/import")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-2);

    if (!datasetId) {
      writeJson(response, 400, { error: "Dataset id is required." });
      return true;
    }

    try {
      const requestBody = (await readJsonBody(
        request
      )) as WorkbookUploadRequest;
      const imported = api.importQueryLogs({
        sourceDatasetId: datasetId,
        workbook: requestBody,
      });

      writeJson(response, 201, imported);
    } catch (error) {
      if (error instanceof QueryApiError) {
        writeJson(response, error.statusCode, {
          error: error.message,
        });
        return true;
      }

      writeJson(response, 400, {
        error:
          error instanceof Error ? error.message : "Query log import failed.",
      });
    }

    return true;
  }

  if (
    request.method === "GET" &&
    requestUrl.pathname.startsWith("/api/query-logs/")
  ) {
    const datasetId = requestUrl.pathname.split("/").at(-1);

    if (!datasetId) {
      writeJson(response, 400, { error: "Dataset id is required." });
      return true;
    }

    writeJson(response, 200, {
      queryLogs: api.listQueryExecutionLogs(datasetId),
    });
    return true;
  }

  return false;
}

async function readJsonBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Uint8Array[] = [];

  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  const body = Buffer.concat(chunks).toString("utf8");
  return JSON.parse(body);
}

function writeJson(
  response: ServerResponse<IncomingMessage>,
  statusCode: number,
  payload: {
    error?: string;
    generatedSqlRecord?: NaturalLanguageQueryResponse["generatedSqlRecord"];
    importedCount?: number;
    queryLog?: NaturalLanguageQueryResponse["queryLog"] | null;
    queryLogs?: NaturalLanguageQueryResponse["queryLog"][];
    queryResult?: NaturalLanguageQueryResponse;
  }
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}
