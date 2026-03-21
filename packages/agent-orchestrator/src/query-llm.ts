import { readFile } from "node:fs/promises";

import initSqlJs, { type Database } from "sql.js";

import type {
  OptimizationHint,
  PipelineColumnDescription,
} from "../../shared/src/index.js";

export interface GenerateQueryTextOptions {
  cleanDatabaseId: string;
  cleanDatabasePath: string;
  columnDescriptions?: PipelineColumnDescription[];
  optimizationHints?: OptimizationHint[];
  onDelta?: (chunk: string) => void;
  prompt: string;
  sourceDatasetId: string;
}

export interface GeneratedQueryText {
  model: string;
  prompt: string;
  sqlText: string;
}

export interface SqlQueryGenerator {
  generateSql(options: GenerateQueryTextOptions): Promise<GeneratedQueryText>;
}

export interface OpenAiSqlQueryGeneratorOptions {
  apiKey?: string;
  baseUrl?: string;
  fetchImplementation?: typeof fetch;
  model?: string;
}

interface CleanDatabaseContext {
  schemaDescription: string;
}

export function createOpenAiSqlQueryGenerator(
  options: OpenAiSqlQueryGeneratorOptions = {}
): SqlQueryGenerator {
  const baseUrl = options.baseUrl ?? "https://api.openai.com/v1/responses";
  const fetchImplementation = options.fetchImplementation ?? fetch;
  const model = options.model ?? process.env.OPENAI_QUERY_MODEL ?? "gpt-5-mini";

  return {
    async generateSql(input) {
      const apiKey = options.apiKey ?? process.env.OPENAI_API_KEY;

      if (!apiKey) {
        throw new Error(
          "OPENAI_API_KEY is required for natural-language query generation."
        );
      }

      const databaseContext = await inspectCleanDatabase(
        input.cleanDatabasePath
      );
      const prompt = buildSqlGenerationPrompt(input, databaseContext);
      const response = await fetchImplementation(baseUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json; charset=utf-8",
        },
        body: JSON.stringify({
          input: prompt,
          model,
          stream: true,
          store: false,
        }),
      });

      if (!response.ok) {
        throw new Error(
          `OpenAI query generation failed with status ${response.status}.`
        );
      }

      const sqlText = (
        await readStreamingOutputText(response, input.onDelta)
      ).trim();

      if (!sqlText) {
        throw new Error(
          "OpenAI query generation returned an empty SQL string."
        );
      }

      return {
        model,
        prompt,
        sqlText,
      };
    },
  };
}

export async function inspectCleanDatabase(
  cleanDatabasePath: string
): Promise<CleanDatabaseContext> {
  const SQL = await initSqlJs();
  const databaseBytes = await readFile(cleanDatabasePath);
  const database = new SQL.Database(databaseBytes);

  try {
    const tables = database.exec(`
      SELECT name, type, sql
      FROM sqlite_master
      WHERE type IN ('table', 'view')
        AND name NOT LIKE 'sqlite_%'
      ORDER BY type, name
    `);
    const tableRows = tables[0]?.values ?? [];

    const schemaBlocks = tableRows.map((row) => {
      const [nameValue, typeValue, sqlValue] = row;
      const name = String(nameValue);
      const type = String(typeValue);
      const createSql = String(sqlValue);
      const sampleRows = readSampleRows(database, name);

      return [
        `${type.toUpperCase()} ${name}`,
        createSql,
        `Sample rows: ${JSON.stringify(sampleRows)}`,
      ].join("\n");
    });

    return {
      schemaDescription: schemaBlocks.join("\n\n"),
    };
  } finally {
    database.close();
  }
}

export function buildSqlGenerationPrompt(
  options: GenerateQueryTextOptions,
  databaseContext: CleanDatabaseContext
): string {
  const columnDescriptionsSection =
    options.columnDescriptions && options.columnDescriptions.length > 0
      ? `\nColumn descriptions:\n${options.columnDescriptions
          .map(
            (entry) =>
              `- ${entry.tableName}.${entry.columnName}: ${entry.description}`
          )
          .join("\n")}`
      : "";
  const optimizationHints =
    options.optimizationHints && options.optimizationHints.length > 0
      ? `\nOptimization hints:\n${options.optimizationHints
          .map(
            (hint) =>
              `- ${hint.title}: ${hint.guidance} Prefer objects ${hint.preferredObjects.join(", ")} when the question matches cluster ${hint.queryClusterId}.`
          )
          .join("\n")}`
      : "";

  return `You generate one SQLite SQL query for an analytics database.

Dataset:
- source dataset id: ${options.sourceDatasetId}
- clean database id: ${options.cleanDatabaseId}

User question:
${options.prompt}

Clean database schema:
${databaseContext.schemaDescription}
${columnDescriptionsSection}
${optimizationHints}

Rules:
- Return only SQL.
- Return exactly one statement.
- The statement must start with SELECT or WITH.
- Read only from the clean database schema shown above.
- The clean database id is metadata only, not a SQL schema name.
- Use the table and view names exactly as shown in the schema description.
- Do not prefix table names with the clean database id, main., or any other database name.
- Do not use markdown fences.
- Do not explain the answer.
- Do not invent tables or columns.
- If column descriptions are provided, treat them as semantic guidance for metric definitions.
- For gross revenue or gross sales questions, treat gross as pre-return sales and exclude returned rows when a return flag exists.
- For net revenue or net sales questions, include return impact instead of excluding returned rows.
- If a return-flag column exists, use schema-appropriate values (for example, is_return = 0/1 or returnFlag = 'No'/'Yes') to model returns correctly.
- Prefer explicit column lists instead of SELECT *.
- Prefer LIMIT 200 for detail-row queries unless the user clearly asked for all rows.`;
}

function readSampleRows(
  database: Database,
  tableName: string
): Array<Record<string, null | number | string>> {
  const escapedTableName = tableName.replaceAll('"', '""');
  const [result] = database.exec(`SELECT * FROM "${escapedTableName}" LIMIT 3`);

  if (!result) {
    return [];
  }

  return result.values.map((row) =>
    Object.fromEntries(
      row.map((value, index) => [
        result.columns[index] ?? `column_${index}`,
        value === null || typeof value === "number" || typeof value === "string"
          ? value
          : String(value),
      ])
    )
  );
}

interface OpenAiResponsesPayload {
  output?: Array<{
    content?: Array<{
      text?: string;
      type?: string;
    }>;
    type?: string;
  }>;
  output_text?: string;
}

function extractOutputText(payload: OpenAiResponsesPayload): string {
  if (typeof payload.output_text === "string") {
    return payload.output_text;
  }

  return (payload.output ?? [])
    .flatMap((item) => item.content ?? [])
    .map((item) => item.text ?? "")
    .join("");
}

async function readStreamingOutputText(
  response: Response,
  onDelta?: (chunk: string) => void
): Promise<string> {
  if (!response.body) {
    throw new Error("OpenAI query generation returned no response body.");
  }

  const decoder = new TextDecoder();
  let buffer = "";
  let outputText = "";

  for await (const value of response.body as unknown as AsyncIterable<Uint8Array>) {
    buffer += decoder.decode(value, { stream: true });

    while (true) {
      const boundaryIndex = buffer.indexOf("\n\n");

      if (boundaryIndex === -1) {
        break;
      }

      const eventBlock = buffer.slice(0, boundaryIndex);
      buffer = buffer.slice(boundaryIndex + 2);

      const parsedEvent = parseServerSentEvent(eventBlock);

      if (!parsedEvent) {
        continue;
      }

      if (parsedEvent.data === "[DONE]") {
        return outputText;
      }

      let payload: OpenAiResponseStreamEvent | null = null;

      try {
        payload = JSON.parse(parsedEvent.data) as OpenAiResponseStreamEvent;
      } catch {
        continue;
      }

      const eventType = payload.type ?? parsedEvent.event;

      if (
        eventType === "response.output_text.delta" &&
        typeof payload.delta === "string"
      ) {
        outputText += payload.delta;
        onDelta?.(payload.delta);
        continue;
      }

      if (
        outputText.length === 0 &&
        eventType === "response.completed" &&
        payload.response
      ) {
        outputText = extractOutputText(payload.response);
      }

      if (
        eventType === "error" ||
        eventType === "response.failed" ||
        eventType === "response.error"
      ) {
        throw new Error(
          payload.error?.message ??
            "OpenAI query generation failed while streaming SQL."
        );
      }
    }
  }

  buffer += decoder.decode();

  if (buffer.trim()) {
    const parsedEvent = parseServerSentEvent(buffer.trim());

    if (parsedEvent?.data && parsedEvent.data !== "[DONE]") {
      try {
        const payload = JSON.parse(
          parsedEvent.data
        ) as OpenAiResponseStreamEvent;

        if (
          payload.type === "response.completed" &&
          payload.response &&
          outputText.length === 0
        ) {
          outputText = extractOutputText(payload.response);
        }
      } catch {
        // Ignore trailing non-JSON fragments from the stream.
      }
    }
  }

  return outputText;
}
function parseServerSentEvent(
  eventBlock: string
): { data: string; event?: string } | null {
  const lines = eventBlock
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);

  if (lines.length === 0) {
    return null;
  }

  const dataLines: string[] = [];
  let event: string | undefined;

  for (const line of lines) {
    if (line.startsWith(":")) {
      continue;
    }

    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
      continue;
    }

    if (line.startsWith("data:")) {
      dataLines.push(line.slice("data:".length).trimStart());
    }
  }

  if (dataLines.length === 0) {
    return null;
  }

  return {
    data: dataLines.join("\n"),
    ...(event ? { event } : {}),
  };
}

interface OpenAiResponseStreamEvent {
  delta?: string;
  error?: {
    message?: string;
  };
  response?: OpenAiResponsesPayload;
  type?: string;
}
