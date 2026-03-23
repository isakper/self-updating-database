import { readFileSync } from "node:fs";
import { basename } from "node:path";

import { read, utils, type WorkBook } from "xlsx";

import type {
  NaturalLanguageQueryResponse,
  WorkbookCellValue,
  WorkbookImportSummary,
  WorkbookSheetInput,
  WorkbookUploadRequest,
} from "../../../packages/shared/src/index.js";
import { parseCliArgs, renderUsage } from "./commands.js";

async function main(): Promise<void> {
  const parsed = parseCliArgs(process.argv.slice(2));

  if (parsed.error) {
    console.error(parsed.error);
    console.error("\n" + renderUsage());
    process.exitCode = 1;
    return;
  }

  const command = parsed.command;

  if (!command || command.kind === "help") {
    console.log(renderUsage());
    return;
  }

  const apiBaseUrl = parsed.options.apiBaseUrl;

  switch (command.kind) {
    case "dataset_list": {
      const response = await requestJson<{ imports: WorkbookImportSummary[] }>({
        apiBaseUrl,
        path: "/api/imports",
      });
      requireOk(response, "Failed to list datasets.");

      const imports = response.payload.imports ?? [];

      if (imports.length === 0) {
        console.log("No datasets found.");
        return;
      }

      console.table(
        imports.map((entry) => ({
          cleanDbStatus: entry.processing.cleanDatabaseStatus,
          datasetId: entry.sourceDatasetId,
          importedAt: entry.importedAt,
          pipelineStatus: entry.processing.pipelineStatus,
          sheets: entry.sheetCount,
          status: entry.status,
          workbook: entry.workbookName,
        }))
      );
      return;
    }
    case "dataset_show": {
      await printDatasetStatus({
        apiBaseUrl,
        datasetId: command.datasetId,
      });
      return;
    }
    case "upload_workbook": {
      const workbookRequest = parseWorkbookFile(command.filePath);
      const response = await requestJson<{ summary?: WorkbookImportSummary }>({
        apiBaseUrl,
        body: workbookRequest,
        method: "POST",
        path: "/api/imports",
      });
      requireOk(response, "Workbook upload failed.");

      if (!response.payload.summary) {
        throw new Error("Import summary is missing from API response.");
      }

      console.log(
        `Uploaded workbook. Dataset id: ${response.payload.summary.sourceDatasetId}`
      );
      printImportSummary(response.payload.summary);
      return;
    }
    case "upload_query_logs": {
      const workbookRequest = parseWorkbookFile(command.filePath);
      const response = await requestJson<{ importedCount?: number }>({
        apiBaseUrl,
        body: workbookRequest,
        method: "POST",
        path: `/api/query-logs/${command.datasetId}/import`,
      });
      requireOk(response, "Query-log upload failed.");

      console.log(
        `Imported ${response.payload.importedCount ?? 0} query logs into ${command.datasetId}.`
      );
      return;
    }
    case "pipeline_run": {
      const response = await requestJson<{
        accepted?: boolean;
        message?: string;
      }>({
        apiBaseUrl,
        method: "POST",
        path: `/api/imports/${command.datasetId}/pipeline-rerun`,
      });
      requireOk(response, "Failed to trigger pipeline rerun.");

      console.log(response.payload.message ?? "Pipeline rerun request sent.");
      return;
    }
    case "optimization_run": {
      const response = await requestJson<{
        accepted?: boolean;
        message?: string;
      }>({
        apiBaseUrl,
        body: command.basePipelineVersionId
          ? {
              basePipelineVersionId: command.basePipelineVersionId,
            }
          : undefined,
        method: "POST",
        path: `/api/optimization-runs/${command.datasetId}`,
      });
      requireOk(response, "Failed to trigger optimization run.");

      console.log(response.payload.message ?? "Optimization run request sent.");
      return;
    }
    case "optimization_retry_latest_failed": {
      const response = await requestJson<{
        accepted?: boolean;
        message?: string;
      }>({
        apiBaseUrl,
        method: "POST",
        path: `/api/optimization-retries/${command.datasetId}`,
      });
      requireOk(response, "Failed to retry latest failed optimization.");

      console.log(
        response.payload.message ?? "Optimization retry request sent."
      );
      return;
    }
    case "status": {
      if (!command.watch) {
        await printDatasetStatus({
          apiBaseUrl,
          datasetId: command.datasetId,
        });
        return;
      }

      console.log(
        `Watching status for ${command.datasetId} every ${command.intervalMs} ms (Ctrl+C to stop).`
      );

      for (;;) {
        await printDatasetStatus({
          apiBaseUrl,
          datasetId: command.datasetId,
          terse: true,
        });
        await delay(command.intervalMs);
      }
    }
    case "events": {
      await streamCodexEvents({
        apiBaseUrl,
        datasetId: command.datasetId,
      });
      return;
    }
    case "query": {
      const response = await requestJson<{
        queryResult?: NaturalLanguageQueryResponse;
      }>({
        apiBaseUrl,
        body: {
          prompt: command.prompt,
          sourceDatasetId: command.datasetId,
        },
        method: "POST",
        path: "/api/queries",
      });
      requireOk(response, "Query failed.");

      const queryResult = response.payload.queryResult;

      if (!queryResult) {
        throw new Error("Query result is missing from API response.");
      }

      console.log("Generated SQL:\n");
      console.log(queryResult.generatedSqlRecord?.sqlText ?? "<none>");

      if (queryResult.result) {
        const queryResultRows = queryResult.result;

        console.log("\nRows:");
        console.table(
          queryResultRows.rows.map((row) =>
            Object.fromEntries(
              queryResultRows.columnNames.map((columnName, index) => [
                columnName,
                row[index] ?? null,
              ])
            )
          )
        );
      }

      return;
    }
    default: {
      const unreachable: never = command;
      throw new Error(`Unhandled command: ${JSON.stringify(unreachable)}`);
    }
  }
}

function parseWorkbookFile(filePath: string): WorkbookUploadRequest {
  const fileBuffer = readFileSync(filePath);
  const workbook: WorkBook = read(fileBuffer, {
    type: "buffer",
  });

  const sheets: WorkbookSheetInput[] = workbook.SheetNames.map((sheetName) => {
    const worksheet = workbook.Sheets[sheetName];

    if (!worksheet) {
      return {
        name: sheetName,
        rows: [],
      };
    }

    const rows = utils
      .sheet_to_json<Record<string, unknown>>(worksheet, {
        defval: null,
      })
      .map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([key, value]) => [
            key,
            normalizeCellValue(value),
          ])
        )
      );

    return {
      name: sheetName,
      rows,
    };
  });

  return {
    workbookName: basename(filePath),
    sheets,
  };
}

function normalizeCellValue(value: unknown): WorkbookCellValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  return JSON.stringify(value);
}

async function printDatasetStatus(options: {
  apiBaseUrl: string;
  datasetId: string;
  terse?: boolean;
}): Promise<void> {
  const response = await requestJson<{ summary?: WorkbookImportSummary }>({
    apiBaseUrl: options.apiBaseUrl,
    path: `/api/imports/${options.datasetId}`,
  });
  requireOk(response, `Failed to fetch dataset ${options.datasetId}.`);

  const summary = response.payload.summary;

  if (!summary) {
    throw new Error("Import summary is missing from API response.");
  }

  if (options.terse) {
    console.log(
      [
        new Date().toISOString(),
        summary.sourceDatasetId,
        `status=${summary.status}`,
        `pipeline=${summary.processing.pipelineStatus}`,
        `clean_db=${summary.processing.cleanDatabaseStatus}`,
      ].join(" ")
    );
    return;
  }

  printImportSummary(summary);
}

function printImportSummary(summary: WorkbookImportSummary): void {
  console.log(`datasetId: ${summary.sourceDatasetId}`);
  console.log(`workbook: ${summary.workbookName}`);
  console.log(`status: ${summary.status}`);
  console.log(`pipelineStatus: ${summary.processing.pipelineStatus}`);
  console.log(`cleanDatabaseStatus: ${summary.processing.cleanDatabaseStatus}`);
  console.log(`sheetCount: ${summary.sheetCount}`);
  console.log(`totalRowCount: ${summary.totalRowCount}`);
}

async function streamCodexEvents(options: {
  apiBaseUrl: string;
  datasetId: string;
}): Promise<void> {
  const response = await fetch(
    `${options.apiBaseUrl}/api/stream/${encodeURIComponent(options.datasetId)}`
  );

  if (!response.ok || !response.body) {
    throw new Error(
      `Failed to stream events (${response.status}). ${await readErrorBody(response)}`
    );
  }

  console.log(`Streaming events for ${options.datasetId} (Ctrl+C to stop).`);

  const decoder = new TextDecoder();

  for await (const chunk of response.body) {
    const bufferChunk =
      chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    const text = decoder.decode(bufferChunk, { stream: true });
    process.stdout.write(text);
  }
}

async function requestJson<TPayload>(options: {
  apiBaseUrl: string;
  body?: unknown;
  method?: "GET" | "POST";
  path: string;
}): Promise<{ payload: TPayload; statusCode: number }> {
  const requestInit: RequestInit = {
    method: options.method ?? "GET",
  };

  if (options.body !== undefined) {
    requestInit.body = JSON.stringify(options.body);
    requestInit.headers = {
      "content-type": "application/json; charset=utf-8",
    };
  }

  const response = await fetch(
    `${options.apiBaseUrl}${options.path}`,
    requestInit
  );

  const payload = (await response.json()) as TPayload;

  return {
    payload,
    statusCode: response.status,
  };
}

function requireOk(
  response: { payload: unknown; statusCode: number },
  message: string
): void {
  if (response.statusCode >= 200 && response.statusCode < 300) {
    return;
  }

  const payload = response.payload as { error?: string; message?: string };
  const reason = payload.error ?? payload.message;

  throw new Error(reason ? `${message} ${reason}` : message);
}

async function readErrorBody(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

void main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exitCode = 1;
});
