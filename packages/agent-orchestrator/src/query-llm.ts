import { readFile } from "node:fs/promises";

import initSqlJs, { type Database, type SqlValue } from "sql.js";

import type {
  OptimizationHint,
  PipelineColumnDescription,
  QueryReasoningMode,
} from "../../shared/src/index.js";

export interface GenerateQueryTextOptions {
  cleanDatabaseId: string;
  cleanDatabasePath: string;
  columnDescriptions?: PipelineColumnDescription[];
  optimizationHints?: OptimizationHint[];
  onDelta?: (chunk: string) => void;
  prompt: string;
  reasoningMode?: QueryReasoningMode;
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
  tableProfilesDescription: string;
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
      const reasoningMode = input.reasoningMode ?? "standard";
      const requestHeaders = {
        authorization: `Bearer ${apiKey}`,
        "content-type": "application/json; charset=utf-8",
      };
      const response = await fetchImplementation(baseUrl, {
        method: "POST",
        headers: requestHeaders,
        body: JSON.stringify({
          input: prompt,
          model,
          ...(reasoningMode === "deliberate"
            ? { reasoning: { effort: "high" } }
            : {}),
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
      tableProfilesDescription: buildTableProfilesDescription(
        database,
        tableRows
      ),
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
  const deliberateReasoningSection =
    (options.reasoningMode ?? "standard") === "deliberate"
      ? `\nReasoning mode:\n- deliberate\n- Before writing SQL, reason through these checks internally:\n  1. Match requested metric semantics (gross vs net, return handling).\n  2. Pick the correct grain for the question (detail row, daily aggregate, store/channel rollup, etc.).\n  3. Verify join keys do not multiply rows.\n  4. Confirm return-flag value semantics from observed schema values.\n  5. Re-check requested output columns and ordering.\n- Keep this reasoning internal and output SQL only.`
      : "";

  return `You generate one SQLite SQL query for an analytics database.

Dataset:
- source dataset id: ${options.sourceDatasetId}
- clean database id: ${options.cleanDatabaseId}

User question:
${options.prompt}

Clean database schema:
${databaseContext.schemaDescription}

Table profiles:
${databaseContext.tableProfilesDescription}
${columnDescriptionsSection}
${optimizationHints}
${deliberateReasoningSection}

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
- Do not synthesize missing date rows unless the user explicitly asks to include zero-sales/missing-date rows.
- Prefer explicit column lists instead of SELECT *.
- Prefer LIMIT 200 for detail-row queries unless the user clearly asked for all rows.`;
}

function buildTableProfilesDescription(
  database: Database,
  tableRows: SqlValue[][]
): string {
  const blocks: string[] = [];

  for (const row of tableRows) {
    const [nameValue, typeValue] = row;
    const tableName = String(nameValue);
    const objectType = String(typeValue).toUpperCase();
    const escapedTableName = tableName.replaceAll('"', '""');

    const rowCount = readSingleNumber(
      database,
      `SELECT COUNT(*) FROM "${escapedTableName}"`
    );
    const columns = readColumnNames(database, escapedTableName);

    const keyCandidates = inferKeyCandidates({
      columns,
      database,
      escapedTableName,
      rowCount,
    });
    const coverageNotes = inferCoverageNotes({
      columns,
      database,
      escapedTableName,
      rowCount,
    });
    const columnObservations = inferColumnObservations({
      columns,
      database,
      escapedTableName,
    });

    blocks.push(
      [
        `${objectType} ${tableName}`,
        `- row_count: ${rowCount}`,
        `- key_candidates: ${
          keyCandidates.length > 0 ? keyCandidates.join(" | ") : "unknown"
        }`,
        `- coverage_notes: ${
          coverageNotes.length > 0 ? coverageNotes.join(" ; ") : "none"
        }`,
        `- column_observations: ${
          columnObservations.length > 0
            ? columnObservations.join(" ; ")
            : "none"
        }`,
      ].join("\n")
    );
  }

  return blocks.join("\n\n");
}

function readColumnNames(database: Database, escapedTableName: string): string[] {
  const [result] = database.exec(`PRAGMA table_info("${escapedTableName}")`);
  if (!result) {
    return [];
  }

  return result.values
    .map((row) => String(row[1] ?? "").trim())
    .filter((columnName) => columnName.length > 0);
}

function inferKeyCandidates(options: {
  columns: string[];
  database: Database;
  escapedTableName: string;
  rowCount: number;
}): string[] {
  if (options.rowCount <= 0 || options.columns.length === 0) {
    return [];
  }

  const idLikeColumns = options.columns.filter((columnName) =>
    /(^id$|_id$|sku|date|timestamp|key$)/i.test(columnName)
  );
  const prioritizedColumns = dedupeStrings([
    ...idLikeColumns,
    ...options.columns.slice(0, 8),
  ]);

  const singles = prioritizedColumns.filter((columnName) =>
    isUniqueColumn({
      columnName,
      database: options.database,
      escapedTableName: options.escapedTableName,
      rowCount: options.rowCount,
    })
  );
  if (singles.length > 0) {
    return singles.map((columnName) => columnName);
  }

  const pairs: string[] = [];
  for (let index = 0; index < prioritizedColumns.length; index += 1) {
    for (
      let innerIndex = index + 1;
      innerIndex < prioritizedColumns.length;
      innerIndex += 1
    ) {
      const pair = [
        prioritizedColumns[index] ?? "",
        prioritizedColumns[innerIndex] ?? "",
      ].filter(Boolean);
      if (pair.length !== 2) {
        continue;
      }

      if (
        isUniqueColumnSet({
          columnNames: pair,
          database: options.database,
          escapedTableName: options.escapedTableName,
          rowCount: options.rowCount,
        })
      ) {
        pairs.push(pair.join(", "));
      }
      if (pairs.length >= 3) {
        return pairs;
      }
    }
  }

  return pairs;
}

function inferCoverageNotes(options: {
  columns: string[];
  database: Database;
  escapedTableName: string;
  rowCount: number;
}): string[] {
  const notes: string[] = [];
  if (options.rowCount <= 0) {
    return notes;
  }

  const dateColumns = options.columns.filter((columnName) =>
    /(date|day|timestamp)/i.test(columnName)
  );
  if (dateColumns.length === 0) {
    return notes;
  }

  for (const dateColumn of dateColumns.slice(0, 2)) {
    const escapedDateColumn = escapeIdentifier(dateColumn);
    const distinctDates = readSingleNumber(
      options.database,
      `SELECT COUNT(DISTINCT ${escapedDateColumn}) FROM "${options.escapedTableName}" WHERE ${escapedDateColumn} IS NOT NULL`
    );
    if (distinctDates <= 0) {
      continue;
    }

    const nonDateColumn = options.columns.find(
      (columnName) =>
        columnName !== dateColumn && /(_id$|sku|store|item|product|key$)/i.test(columnName)
    );
    if (!nonDateColumn) {
      continue;
    }

    const escapedEntityColumn = escapeIdentifier(nonDateColumn);
    const distinctEntities = readSingleNumber(
      options.database,
      `SELECT COUNT(DISTINCT ${escapedEntityColumn}) FROM "${options.escapedTableName}" WHERE ${escapedEntityColumn} IS NOT NULL`
    );
    if (distinctEntities <= 0) {
      continue;
    }

    const potentialDenseRows = distinctDates * distinctEntities;
    if (potentialDenseRows <= 0) {
      continue;
    }

    const density = options.rowCount / potentialDenseRows;
    if (density < 0.95) {
      notes.push(
        `sparse ${nonDateColumn} x ${dateColumn} coverage (density=${density.toFixed(3)})`
      );
    } else {
      notes.push(
        `near-dense ${nonDateColumn} x ${dateColumn} coverage (density=${density.toFixed(3)})`
      );
    }
  }

  return dedupeStrings(notes);
}

function inferColumnObservations(options: {
  columns: string[];
  database: Database;
  escapedTableName: string;
}): string[] {
  const prioritizedColumns = dedupeStrings([
    ...options.columns.filter((columnName) =>
      /(return|is_|flag|date|sku|store|supplier|payment|channel|tier|category|department|brand)/i.test(
        columnName
      )
    ),
    ...options.columns.slice(0, 6),
  ]).slice(0, 10);

  const observations: string[] = [];

  for (const columnName of prioritizedColumns) {
    const escapedColumnName = escapeIdentifier(columnName);
    const sampleValues = readDistinctSampleValues({
      columnName: escapedColumnName,
      database: options.database,
      escapedTableName: options.escapedTableName,
    });
    if (sampleValues.length === 0) {
      continue;
    }

    const inferredKinds = dedupeStrings(
      sampleValues.map((value) => {
        if (/^-?\d+(\.\d+)?$/.test(value)) {
          return "numeric-like";
        }
        if (/^(yes|no|true|false|y|n|0|1)$/i.test(value)) {
          return "boolean-like";
        }
        if (/^\d{4}-\d{2}-\d{2}/.test(value)) {
          return "date-like";
        }
        return "text-like";
      })
    );

    observations.push(
      `${columnName} [${inferredKinds.join(", ")}] samples=${sampleValues.join(" | ")}`
    );
  }

  return observations;
}

function readDistinctSampleValues(options: {
  columnName: string;
  database: Database;
  escapedTableName: string;
}): string[] {
  const [result] = options.database.exec(
    `SELECT DISTINCT CAST(${options.columnName} AS TEXT)
     FROM "${options.escapedTableName}"
     WHERE ${options.columnName} IS NOT NULL
     LIMIT 5`
  );

  if (!result) {
    return [];
  }

  return result.values
    .map((row) => String(row[0] ?? "").trim())
    .filter((value) => value.length > 0);
}

function isUniqueColumn(options: {
  columnName: string;
  database: Database;
  escapedTableName: string;
  rowCount: number;
}): boolean {
  return isUniqueColumnSet({
    columnNames: [options.columnName],
    database: options.database,
    escapedTableName: options.escapedTableName,
    rowCount: options.rowCount,
  });
}

function isUniqueColumnSet(options: {
  columnNames: string[];
  database: Database;
  escapedTableName: string;
  rowCount: number;
}): boolean {
  if (options.columnNames.length === 0) {
    return false;
  }

  const distinctExpression = options.columnNames
    .map((columnName) => `COALESCE(CAST(${escapeIdentifier(columnName)} AS TEXT), '<NULL>')`)
    .join(` || '¦' || `);
  const query = `SELECT COUNT(DISTINCT ${distinctExpression}) FROM "${options.escapedTableName}"`;
  const distinctCount = readSingleNumber(options.database, query);
  return distinctCount === options.rowCount;
}

function readSingleNumber(database: Database, sqlText: string): number {
  const [result] = database.exec(sqlText);
  const value = result?.values?.[0]?.[0];
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function escapeIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function dedupeStrings(values: string[]): string[] {
  return [...new Set(values)];
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
