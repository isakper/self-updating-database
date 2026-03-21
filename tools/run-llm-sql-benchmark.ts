import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import initSqlJs, { type Database, type SqlJsStatic } from "sql.js";

interface BenchmarkQuestion {
  expectedResult: Array<Record<string, unknown>>;
  id: string;
  question: string;
  sql?: string;
}

interface BenchmarkScenario {
  columnDescriptions: PipelineColumnDescription[];
  databasePath: string;
  description: string;
  key: "scenario_1_raw" | "scenario_2_clean" | "scenario_3a_optimized";
  name: string;
}

interface BenchmarkResultRow {
  actualRowCount: number;
  actualRowsJson: string;
  datasetFile: string;
  error: string | null;
  evaluationCorrect: boolean | null;
  evaluationPrompt: string;
  evaluationReason: string;
  evaluationModel: string;
  executionMs: number | null;
  expectedRowCount: number;
  expectedRowsJson: string;
  generatedSql: string | null;
  groundTruthSql: string;
  inferenceModel: string;
  llmAllRowsMatched: boolean;
  llmRowAccuracy: number | null;
  llmRowsMatched: number;
  llmRowsTotal: number;
  question: string;
  questionId: string;
  relaxedCorrect: boolean;
  rowEvaluationPromptsJson: string;
  scenario: BenchmarkScenario["key"];
  sqlGenerationPrompt: string;
  sqlValid: boolean;
  strictCorrect: boolean;
}

interface ScriptOptions {
  datasetId: string;
  evaluationModel: string;
  inferenceModel: string;
  outputCsvPath: string;
  outputJsonPath: string;
  questionPaths: string[];
  sourceDatabasePath: string;
}

type QueryRow = Record<string, boolean | null | number | string>;

interface PipelineColumnDescription {
  columnName: string;
  description: string;
  tableName: string;
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

interface LlmRowEvaluationResponse {
  confidence: "high" | "low" | "medium";
  matchedActualRowIndexes: number[];
  matchedExpectedRow: boolean;
  reason: string;
}

interface LlmRowEvaluationRun {
  evaluations: LlmRowEvaluationResponse[];
  prompts: string[];
}

interface LlmQuestionEvaluationResponse {
  isCorrect: boolean;
  reason: string;
}

const DEFAULT_QUESTION_PATHS = [
  "apps/web/fixtures/demo-workbooks/retailer-transactions-random-questions-dataset-1.json",
  "apps/web/fixtures/demo-workbooks/retailer-transactions-log-inspired-dataset-2.json",
];

const DEFAULT_INFERENCE_MODEL =
  process.env.EVAL_OPENAI_QUERY_MODEL ??
  process.env.OPENAI_QUERY_MODEL ??
  "gpt-5.4";
const DEFAULT_EVALUATION_MODEL = process.env.OPENAI_QUERY_MODEL ?? "gpt-5.4";
const DEFAULT_SOURCE_DATABASE_PATH =
  process.env.SOURCE_DATABASE_PATH ?? ".data/source-datasets.sqlite";

const FORBIDDEN_QUERY_PATTERN =
  /\b(update|delete|alter|attach|detach|pragma|vacuum|reindex|begin|commit|rollback|drop|create|insert|replace)\b/i;
const ALLOWED_QUERY_PATTERNS = [/^select\s+/i, /^with\s+/i];

const options = parseOptions(process.argv.slice(2));

if (!process.env.OPENAI_API_KEY) {
  throw new Error("OPENAI_API_KEY is required.");
}

const SQL = await initSqlJs();

const sourceDatabase = openDatabase(SQL, options.sourceDatabasePath);

try {
  const sourceSheetTables = readSourceSheetTables(
    sourceDatabase,
    options.datasetId
  );

  const latestState = readLatestDatasetState(sourceDatabase, options.datasetId);
  const latestSucceededOptimizationRevision =
    readLatestSucceededOptimizationRevision(sourceDatabase, options.datasetId);

  const basePipelineVersionId =
    latestSucceededOptimizationRevision?.basePipelineVersionId ??
    latestState.pipelineVersionId;
  const optimizedPipelineVersionId =
    latestSucceededOptimizationRevision?.candidatePipelineVersionId ??
    latestState.pipelineVersionId;

  if (!basePipelineVersionId || !optimizedPipelineVersionId) {
    throw new Error(
      `Could not resolve clean/optimized pipeline versions for dataset ${options.datasetId}.`
    );
  }

  const cleanDatabasePath = resolve(
    `.data/clean-databases/${options.datasetId}-${basePipelineVersionId}.sqlite`
  );
  const optimizedDatabasePath = resolve(
    `.data/clean-databases/${options.datasetId}-${optimizedPipelineVersionId}.sqlite`
  );
  const rawScenarioPath = resolve(
    `.data/benchmarks/${options.datasetId}-scenario-1-raw.sqlite`
  );

  buildRawScenarioDatabase({
    SQL,
    sourceDatabase,
    sourceSheetTables,
    targetPath: rawScenarioPath,
  });

  const scenarios: BenchmarkScenario[] = [
    {
      columnDescriptions: [],
      databasePath: rawScenarioPath,
      description: "Scenario 1: original uncleaned workbook schema.",
      key: "scenario_1_raw",
      name: "Scenario 1 (Raw)",
    },
    {
      columnDescriptions: readPipelineColumnDescriptions(
        sourceDatabase,
        basePipelineVersionId
      ),
      databasePath: cleanDatabasePath,
      description: "Scenario 2: cleaned canonical schema.",
      key: "scenario_2_clean",
      name: "Scenario 2 (Clean)",
    },
    {
      columnDescriptions: readPipelineColumnDescriptions(
        sourceDatabase,
        optimizedPipelineVersionId
      ),
      databasePath: optimizedDatabasePath,
      description:
        "Scenario 3a: optimized schema built on top of cleaned data.",
      key: "scenario_3a_optimized",
      name: "Scenario 3a (Optimized)",
    },
  ];

  const benchmarkQuestions = readBenchmarkQuestions(options.questionPaths);
  const benchmarkResults: BenchmarkResultRow[] = [];

  for (const scenario of scenarios) {
    const db = openDatabase(SQL, scenario.databasePath);

    try {
      const schema = inspectDatabaseSchema(db);

      for (const question of benchmarkQuestions) {
        console.log(`[${scenario.key}] running ${question.id}`);
        const generationPrompt = buildSqlGenerationPrompt({
          columnDescriptions: scenario.columnDescriptions,
          question: question.question,
          scenarioDescription: scenario.description,
          schemaDescription: schema.schemaDescription,
        });

        let generatedSql: string | null = null;
        let sqlValid = false;
        let executionMs: number | null = null;
        let actualRows: QueryRow[] = [];
        let error: string | null = null;

        try {
          generatedSql = await generateSqlFromOpenAi({
            apiKey: process.env.OPENAI_API_KEY,
            model: options.inferenceModel,
            prompt: generationPrompt,
          });

          const validation = validateQuerySql(generatedSql);
          sqlValid = validation.isValid;

          if (!validation.isValid) {
            throw new Error(validation.errors.join(" "));
          }

          const startedAt = process.hrtime.bigint();
          actualRows = executeQuery(db, generatedSql);
          const endedAt = process.hrtime.bigint();
          executionMs = Number(endedAt - startedAt) / 1_000_000;
        } catch (caughtError) {
          error =
            caughtError instanceof Error
              ? caughtError.message
              : "Unknown benchmark error.";
        }

        const strictCorrect =
          error === null &&
          compareRowsStrict(actualRows, toQueryRows(question.expectedResult));
        const relaxedCorrect =
          error === null &&
          compareRowsRelaxed(actualRows, toQueryRows(question.expectedResult));

        let llmRowsMatched = 0;
        let llmRowsTotal = 0;
        let llmRowAccuracy: number | null = null;
        let llmAllRowsMatched = false;
        let rowEvaluationPrompts: string[] = [];
        const expectedRows = toQueryRows(question.expectedResult);
        let evaluationCorrect: boolean | null = null;
        let evaluationReason = "";
        let evaluationPrompt = "";

        if (error === null) {
          try {
            const rowEvaluationRun = await evaluateRowsWithLlm({
              actualRows,
              apiKey: process.env.OPENAI_API_KEY,
              evaluationModel: options.evaluationModel,
              expectedRows,
              question: question.question,
              scenarioName: scenario.name,
            });
            const rowEvaluations = rowEvaluationRun.evaluations;
            rowEvaluationPrompts = rowEvaluationRun.prompts;

            llmRowsTotal = rowEvaluations.length;
            llmRowsMatched = rowEvaluations.filter(
              (evaluation) => evaluation.matchedExpectedRow
            ).length;
            llmRowAccuracy =
              llmRowsTotal > 0
                ? Number((llmRowsMatched / llmRowsTotal).toFixed(6))
                : 1;
            llmAllRowsMatched = llmRowsMatched === llmRowsTotal;
          } catch (caughtError) {
            error =
              caughtError instanceof Error
                ? `Row evaluation failed: ${caughtError.message}`
                : "Row evaluation failed.";
          }
        }

        try {
          const finalEvaluation = await evaluateQuestionWithLlm({
            actualRows,
            apiKey: process.env.OPENAI_API_KEY,
            error,
            evaluationModel: options.evaluationModel,
            expectedRows,
            question: question.question,
          });
          evaluationCorrect = finalEvaluation.isCorrect;
          evaluationReason = finalEvaluation.reason;
          evaluationPrompt = finalEvaluation.prompt;
        } catch (caughtError) {
          evaluationReason =
            caughtError instanceof Error
              ? `Final evaluation failed: ${caughtError.message}`
              : "Final evaluation failed.";
        }

        benchmarkResults.push({
          actualRowCount: actualRows.length,
          actualRowsJson: JSON.stringify(actualRows),
          datasetFile: question.datasetFile,
          error,
          evaluationCorrect,
          evaluationPrompt,
          evaluationReason,
          evaluationModel: options.evaluationModel,
          executionMs,
          expectedRowCount: question.expectedResult.length,
          expectedRowsJson: JSON.stringify(expectedRows),
          generatedSql,
          groundTruthSql: question.sql ?? "",
          inferenceModel: options.inferenceModel,
          llmAllRowsMatched,
          llmRowAccuracy,
          llmRowsMatched,
          llmRowsTotal,
          question: question.question,
          questionId: question.id,
          relaxedCorrect,
          rowEvaluationPromptsJson: JSON.stringify(rowEvaluationPrompts),
          scenario: scenario.key,
          sqlGenerationPrompt: generationPrompt,
          sqlValid,
          strictCorrect,
        });
      }
    } finally {
      db.close();
    }
  }

  const summary = summarizeBenchmark(benchmarkResults);

  mkdirSync(dirname(options.outputJsonPath), { recursive: true });
  mkdirSync(dirname(options.outputCsvPath), { recursive: true });

  writeFileSync(
    options.outputJsonPath,
    `${JSON.stringify(
      {
        datasetId: options.datasetId,
        evaluationModel: options.evaluationModel,
        generatedAt: new Date().toISOString(),
        inferenceModel: options.inferenceModel,
        questions: options.questionPaths.map((value) => resolve(value)),
        scenarios,
        summary,
        results: benchmarkResults,
      },
      null,
      2
    )}\n`,
    "utf8"
  );

  writeBenchmarkCsv(options.outputCsvPath, benchmarkResults);

  console.log(`Wrote JSON report: ${options.outputJsonPath}`);
  console.log(`Wrote CSV report: ${options.outputCsvPath}`);
  console.log(JSON.stringify(summary, null, 2));
} finally {
  sourceDatabase.close();
}

function parseOptions(args: string[]): ScriptOptions {
  const options = new Map<string, string[]>();

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];

    if (!token) {
      continue;
    }

    if (!token.startsWith("--")) {
      continue;
    }

    const key = token.slice(2);
    const nextToken = args[index + 1];

    if (!nextToken || nextToken.startsWith("--")) {
      options.set(key, [...(options.get(key) ?? []), "true"]);
      continue;
    }

    options.set(key, [...(options.get(key) ?? []), nextToken]);
    index += 1;
  }

  const datasetId = firstOption(options, "dataset-id");

  if (!datasetId) {
    throw new Error("Missing required argument --dataset-id <dataset_id>.");
  }

  const questionPathsRaw = options.get("questions") ?? [];
  const questionPaths =
    questionPathsRaw.length > 0
      ? questionPathsRaw.flatMap((value) =>
          value
            .split(",")
            .map((part) => part.trim())
            .filter((part) => part.length > 0)
        )
      : DEFAULT_QUESTION_PATHS;

  const timestamp = new Date().toISOString().replaceAll(":", "-");

  const outputJsonPath = resolve(
    firstOption(options, "out-json") ??
      `output/benchmarks/sql-benchmark-${datasetId}-${timestamp}.json`
  );
  const outputCsvPath = resolve(
    firstOption(options, "out-csv") ??
      `output/benchmarks/sql-benchmark-${datasetId}-${timestamp}.csv`
  );

  return {
    datasetId,
    evaluationModel:
      firstOption(options, "evaluation-model") ?? DEFAULT_EVALUATION_MODEL,
    inferenceModel:
      firstOption(options, "inference-model") ??
      firstOption(options, "model") ??
      DEFAULT_INFERENCE_MODEL,
    outputCsvPath,
    outputJsonPath,
    questionPaths,
    sourceDatabasePath: resolve(
      firstOption(options, "source-db") ?? DEFAULT_SOURCE_DATABASE_PATH
    ),
  };
}

function firstOption(
  options: Map<string, string[]>,
  key: string
): string | null {
  const values = options.get(key);
  if (!values || values.length === 0) {
    return null;
  }

  return values[0] ?? null;
}

function openDatabase(SQLModule: SqlJsStatic, path: string): Database {
  const bytes = readFileSync(path);
  return new SQLModule.Database(bytes);
}

function readSourceSheetTables(
  sourceDatabase: Database,
  datasetId: string
): { items: string; stores: string; transactions: string } {
  const rows = executeQuery(
    sourceDatabase,
    `SELECT name, source_table_name
     FROM source_sheets
     WHERE dataset_id = '${escapeSqlLiteral(datasetId)}'
     ORDER BY sheet_order ASC;`
  );

  const tableBySheetName = new Map(
    rows.map((row) => [String(row.name), String(row.source_table_name)])
  );

  const transactions = tableBySheetName.get("Transactions");
  const items = tableBySheetName.get("Items");
  const stores = tableBySheetName.get("Stores");

  if (!transactions || !items || !stores) {
    throw new Error(
      `Could not resolve Transactions/Items/Stores source tables for dataset ${datasetId}.`
    );
  }

  return {
    items,
    stores,
    transactions,
  };
}

function readLatestDatasetState(
  sourceDatabase: Database,
  datasetId: string
): {
  pipelineVersionId: string | null;
} {
  const rows = executeQuery(
    sourceDatabase,
    `SELECT pipeline_version_id
     FROM import_processing_state
     WHERE dataset_id = '${escapeSqlLiteral(datasetId)}'
     ORDER BY rowid DESC
     LIMIT 1;`
  );

  if (rows.length === 0) {
    throw new Error(
      `No import_processing_state found for dataset ${datasetId}.`
    );
  }

  return {
    pipelineVersionId:
      rows[0]?.pipeline_version_id === null
        ? null
        : String(rows[0]?.pipeline_version_id),
  };
}

function readLatestSucceededOptimizationRevision(
  sourceDatabase: Database,
  datasetId: string
): {
  basePipelineVersionId: string;
  candidatePipelineVersionId: string;
} | null {
  const rows = executeQuery(
    sourceDatabase,
    `SELECT base_pipeline_version_id, candidate_pipeline_version_id
     FROM optimization_revisions
     WHERE source_dataset_id = '${escapeSqlLiteral(datasetId)}'
       AND status = 'succeeded'
       AND candidate_pipeline_version_id IS NOT NULL
     ORDER BY updated_at DESC
     LIMIT 1;`
  );

  if (rows.length === 0) {
    return null;
  }

  return {
    basePipelineVersionId: String(rows[0]?.base_pipeline_version_id),
    candidatePipelineVersionId: String(rows[0]?.candidate_pipeline_version_id),
  };
}

function readPipelineColumnDescriptions(
  sourceDatabase: Database,
  pipelineVersionId: string
): PipelineColumnDescription[] {
  const rows = executeQuery(
    sourceDatabase,
    `SELECT analysis_json
     FROM pipeline_versions
     WHERE pipeline_version_id = '${escapeSqlLiteral(pipelineVersionId)}'
     LIMIT 1;`
  );

  const rawAnalysis = rows[0]?.analysis_json;
  if (typeof rawAnalysis !== "string" || rawAnalysis.trim().length === 0) {
    return [];
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(rawAnalysis);
  } catch {
    return [];
  }

  if (!parsed || typeof parsed !== "object") {
    return [];
  }

  const candidate = parsed as { columnDescriptions?: unknown };
  if (!Array.isArray(candidate.columnDescriptions)) {
    return [];
  }

  return candidate.columnDescriptions.flatMap((entry) => {
    if (!entry || typeof entry !== "object") {
      return [];
    }

    const record = entry as {
      columnName?: unknown;
      description?: unknown;
      tableName?: unknown;
    };

    if (
      typeof record.tableName !== "string" ||
      typeof record.columnName !== "string" ||
      typeof record.description !== "string"
    ) {
      return [];
    }

    return [
      {
        columnName: record.columnName,
        description: record.description,
        tableName: record.tableName,
      },
    ];
  });
}

function buildRawScenarioDatabase(options: {
  SQL: SqlJsStatic;
  sourceDatabase: Database;
  sourceSheetTables: { items: string; stores: string; transactions: string };
  targetPath: string;
}): void {
  const benchmarkDatabase = new options.SQL.Database();

  try {
    const tableMappings = [
      {
        sourceTable: options.sourceSheetTables.transactions,
        targetTable: "transactions",
      },
      {
        sourceTable: options.sourceSheetTables.items,
        targetTable: "items",
      },
      {
        sourceTable: options.sourceSheetTables.stores,
        targetTable: "stores",
      },
    ];

    for (const mapping of tableMappings) {
      const rows = executeQuery(
        options.sourceDatabase,
        `SELECT * FROM "${mapping.sourceTable.replaceAll('"', '""')}";`
      );
      createTableFromRows(benchmarkDatabase, mapping.targetTable, rows);
    }

    mkdirSync(dirname(options.targetPath), { recursive: true });
    const bytes = benchmarkDatabase.export();
    writeFileSync(options.targetPath, bytes);
  } finally {
    benchmarkDatabase.close();
  }
}

function createTableFromRows(
  database: Database,
  tableName: string,
  rows: QueryRow[]
): void {
  if (rows.length === 0) {
    throw new Error(`Expected non-empty source rows for table ${tableName}.`);
  }

  const columns = Object.keys(rows[0] ?? {});

  const schema = columns
    .map((column) => {
      const values = rows.map((row) => row[column] ?? null);
      return `"${column.replaceAll('"', '""')}" ${inferSqlType(values)}`;
    })
    .join(", ");

  database.exec(
    `CREATE TABLE "${tableName.replaceAll('"', '""')}" (${schema});`
  );

  const placeholders = columns.map(() => "?").join(", ");
  const insert = database.prepare(
    `INSERT INTO "${tableName.replaceAll('"', '""')}" (${columns
      .map((column) => `"${column.replaceAll('"', '""')}"`)
      .join(", ")}) VALUES (${placeholders});`
  );

  database.exec("BEGIN;");

  for (const row of rows) {
    insert.run(
      columns.map((column) => {
        const value = row[column] ?? null;
        return typeof value === "boolean" ? (value ? 1 : 0) : value;
      })
    );
  }

  insert.free();
  database.exec("COMMIT;");
}

function inferSqlType(values: Array<unknown>): "REAL" | "TEXT" {
  let sawNumber = false;
  let sawNonNumber = false;

  for (const value of values) {
    if (value === null || value === undefined || value === "") {
      continue;
    }

    if (typeof value === "number") {
      sawNumber = true;
      continue;
    }

    sawNonNumber = true;
  }

  return sawNumber && !sawNonNumber ? "REAL" : "TEXT";
}

function readBenchmarkQuestions(
  questionPaths: string[]
): Array<BenchmarkQuestion & { datasetFile: string }> {
  return questionPaths.flatMap((questionPath) => {
    const absolutePath = resolve(questionPath);
    const parsed = JSON.parse(
      readFileSync(absolutePath, "utf8")
    ) as BenchmarkQuestion[];

    return parsed.map((question) => ({
      ...question,
      datasetFile: absolutePath,
    }));
  });
}

function inspectDatabaseSchema(database: Database): {
  schemaDescription: string;
} {
  const objects = executeQuery(
    database,
    `SELECT name, type, sql
     FROM sqlite_master
     WHERE type IN ('table', 'view')
       AND name NOT LIKE 'sqlite_%'
     ORDER BY type, name;`
  );

  const schemaBlocks = objects.map((row) => {
    const name = String(row.name);
    const type = String(row.type).toUpperCase();
    const createSql = String(row.sql ?? "");

    return [`${type} ${name}`, createSql].join("\n");
  });

  return {
    schemaDescription: schemaBlocks.join("\n\n"),
  };
}

function buildSqlGenerationPrompt(options: {
  columnDescriptions: PipelineColumnDescription[];
  question: string;
  scenarioDescription: string;
  schemaDescription: string;
}): string {
  const columnDescriptionsSection =
    options.columnDescriptions.length > 0
      ? `\nColumn descriptions:\n${options.columnDescriptions
          .map(
            (entry) =>
              `- ${entry.tableName}.${entry.columnName}: ${entry.description}`
          )
          .join("\n")}`
      : "";

  return `You generate exactly one SQLite SQL statement.

${options.scenarioDescription}

User question:
${options.question}

Available schema:
${options.schemaDescription}
${columnDescriptionsSection}

Rules:
- Return SQL only.
- Exactly one statement.
- Statement must start with SELECT or WITH.
- Use only tables/views shown in Available schema.
- Do not use ATTACH, PRAGMA, DDL, or write operations.
- Do not invent tables or columns.
- If column descriptions are provided, treat them as semantic guidance for metric definitions.
- For gross revenue or gross sales questions, treat gross as pre-return sales and exclude returned rows when a return flag exists.
- For net revenue or net sales questions, include return impact instead of excluding returned rows.
- If a return-flag column exists, use schema-appropriate values (for example, is_return = 0/1 or returnFlag = 'No'/'Yes') to model returns correctly.
- Prefer explicit selected columns over SELECT *.
- If joins are needed, join explicitly on matching keys.
- Include ORDER BY when deterministic ordering is relevant to the question.`;
}

async function generateSqlFromOpenAi(options: {
  apiKey: string;
  model: string;
  prompt: string;
}): Promise<string> {
  return await callOpenAiText(options);
}

function extractResponseText(payload: OpenAiResponsesPayload): string {
  if (typeof payload.output_text === "string") {
    return payload.output_text;
  }

  return (payload.output ?? [])
    .flatMap((item) => item.content ?? [])
    .map((item) => item.text ?? "")
    .join("");
}

function stripMarkdownSqlFences(text: string): string {
  const fencedMatch = text.match(/```(?:sql)?\s*([\s\S]*?)```/i);
  if (fencedMatch) {
    return fencedMatch[1] ?? "";
  }

  return text;
}

async function evaluateRowsWithLlm(options: {
  actualRows: QueryRow[];
  apiKey: string;
  evaluationModel: string;
  expectedRows: QueryRow[];
  question: string;
  scenarioName: string;
}): Promise<LlmRowEvaluationRun> {
  const evaluations: LlmRowEvaluationResponse[] = [];
  const prompts: string[] = [];

  for (let index = 0; index < options.expectedRows.length; index += 1) {
    const expectedRow = options.expectedRows[index];
    if (!expectedRow) {
      continue;
    }
    const prompt = buildRowEvaluationPrompt({
      actualRows: options.actualRows,
      expectedRow,
      question: options.question,
      rowIndex: index,
      scenarioName: options.scenarioName,
    });
    prompts.push(prompt);

    const responseText = await callOpenAiText({
      apiKey: options.apiKey,
      model: options.evaluationModel,
      prompt,
    });

    const parsed = parseRowEvaluationResponse(responseText);
    evaluations.push(parsed);
  }

  return {
    evaluations,
    prompts,
  };
}

function buildRowEvaluationPrompt(options: {
  actualRows: QueryRow[];
  expectedRow: QueryRow;
  question: string;
  rowIndex: number;
  scenarioName: string;
}): string {
  return `You are evaluating SQL result correctness for one expected ground-truth row.

Scenario:
${options.scenarioName}

Question:
${options.question}

Expected row index:
${options.rowIndex}

Expected row JSON:
${JSON.stringify(options.expectedRow)}

Actual result rows JSON:
${JSON.stringify(options.actualRows)}

Task:
- Decide whether the expected row is matched by one or more actual rows.
- Treat numeric values as matching if absolute difference <= 0.01.
- Column names may differ; focus on row content and meaning.
- If multiple actual rows match, include all their indexes.

Return ONLY valid JSON with this exact shape:
{
  "matchedExpectedRow": true or false,
  "matchedActualRowIndexes": [0-based indexes],
  "confidence": "low" | "medium" | "high",
  "reason": "short reason"
}`;
}

async function evaluateQuestionWithLlm(options: {
  actualRows: QueryRow[];
  apiKey: string;
  error: string | null;
  evaluationModel: string;
  expectedRows: QueryRow[];
  question: string;
}): Promise<LlmQuestionEvaluationResponse & { prompt: string }> {
  const prompt = buildQuestionEvaluationPrompt({
    actualRows: options.actualRows,
    error: options.error,
    expectedRows: options.expectedRows,
    question: options.question,
  });

  const responseText = await callOpenAiText({
    apiKey: options.apiKey,
    model: options.evaluationModel,
    prompt,
  });

  const parsed = parseQuestionEvaluationResponse(responseText);
  return {
    ...parsed,
    prompt,
  };
}

function buildQuestionEvaluationPrompt(options: {
  actualRows: QueryRow[];
  error: string | null;
  expectedRows: QueryRow[];
  question: string;
}): string {
  return `You are evaluating whether a SQL answer is correct for a user question.

Question:
${options.question}

Ground truth expected rows JSON:
${JSON.stringify(options.expectedRows)}

Model generated rows JSON:
${JSON.stringify(options.actualRows)}

Execution error (if any):
${options.error ?? "none"}

Rules:
- If execution error exists, classify false.
- Judge correctness by semantic equivalence of result content.
- Allow numeric tolerance of absolute difference <= 0.01.
- If row or column ordering differs but content is equivalent, classify true.

Return ONLY valid JSON with this exact shape:
{
  "isCorrect": true or false,
  "reason": "short explanation"
}`;
}

function parseQuestionEvaluationResponse(
  text: string
): LlmQuestionEvaluationResponse {
  let parsedJson: unknown;

  try {
    parsedJson = JSON.parse(text);
  } catch {
    const objectMatch = text.match(/\{[\s\S]*\}/);
    if (!objectMatch) {
      throw new Error("Could not parse question evaluation JSON.");
    }
    parsedJson = JSON.parse(objectMatch[0]);
  }

  if (!parsedJson || typeof parsedJson !== "object") {
    throw new Error("Question evaluation payload is not an object.");
  }

  const candidate = parsedJson as Partial<LlmQuestionEvaluationResponse>;
  return {
    isCorrect: Boolean(candidate.isCorrect),
    reason:
      typeof candidate.reason === "string"
        ? candidate.reason
        : "No reason provided.",
  };
}

async function callOpenAiText(options: {
  apiKey: string;
  model: string;
  prompt: string;
}): Promise<string> {
  const maxAttempts = 4;
  let lastError: unknown = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch("https://api.openai.com/v1/responses", {
        signal: AbortSignal.timeout(60_000),
        body: JSON.stringify({
          input: options.prompt,
          model: options.model,
          store: false,
          temperature: 0,
        }),
        headers: {
          authorization: `Bearer ${options.apiKey}`,
          "content-type": "application/json; charset=utf-8",
        },
        method: "POST",
      });

      if (!response.ok) {
        const isRetryable = response.status >= 500 || response.status === 429;
        if (isRetryable) {
          throw new Error(`OpenAI retryable status ${response.status}.`);
        }
        throw new Error(
          `OpenAI response failed with status ${response.status}.`
        );
      }

      const payload = (await response.json()) as OpenAiResponsesPayload;
      const output = extractResponseText(payload).trim();

      if (!output) {
        throw new Error("OpenAI returned empty evaluation output.");
      }

      return stripMarkdownSqlFences(output).trim();
    } catch (error) {
      lastError = error;
      if (attempt === maxAttempts) {
        break;
      }
      const backoffMs = attempt * 1500;
      await sleep(backoffMs);
    }
  }

  throw lastError instanceof Error
    ? lastError
    : new Error("OpenAI call failed after retries.");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolvePromise) => {
    setTimeout(resolvePromise, ms);
  });
}

function parseRowEvaluationResponse(text: string): LlmRowEvaluationResponse {
  let parsedJson: unknown;

  try {
    parsedJson = JSON.parse(text);
  } catch {
    const objectMatch = text.match(/\{[\s\S]*\}/);
    if (!objectMatch) {
      throw new Error("Could not parse row evaluation JSON.");
    }
    parsedJson = JSON.parse(objectMatch[0]);
  }

  if (!parsedJson || typeof parsedJson !== "object") {
    throw new Error("Row evaluation payload is not an object.");
  }

  const candidate = parsedJson as Partial<LlmRowEvaluationResponse>;
  const confidence =
    candidate.confidence === "low" ||
    candidate.confidence === "medium" ||
    candidate.confidence === "high"
      ? candidate.confidence
      : "low";
  const matchedActualRowIndexes = Array.isArray(
    candidate.matchedActualRowIndexes
  )
    ? candidate.matchedActualRowIndexes.filter(
        (value): value is number =>
          typeof value === "number" && Number.isInteger(value) && value >= 0
      )
    : [];

  return {
    confidence,
    matchedActualRowIndexes,
    matchedExpectedRow: Boolean(candidate.matchedExpectedRow),
    reason:
      typeof candidate.reason === "string"
        ? candidate.reason
        : "No reason provided.",
  };
}

function validateQuerySql(sqlText: string): {
  errors: string[];
  isValid: boolean;
} {
  const errors: string[] = [];
  const statements = sqlText
    .split(";")
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);

  if (statements.length === 0) {
    return {
      errors: ["Generated query SQL must not be empty."],
      isValid: false,
    };
  }

  if (statements.length > 1) {
    errors.push("Generated query SQL must contain exactly one statement.");
  }

  const statement = statements[0] ?? "";

  if (FORBIDDEN_QUERY_PATTERN.test(statement)) {
    errors.push("Generated query SQL contains a forbidden SQL statement.");
  }

  if (!ALLOWED_QUERY_PATTERNS.some((pattern) => pattern.test(statement))) {
    errors.push("Generated query SQL must start with SELECT or WITH.");
  }

  return {
    errors,
    isValid: errors.length === 0,
  };
}

function executeQuery(database: Database, sqlText: string): QueryRow[] {
  const [result] = database.exec(sqlText);

  if (!result) {
    return [];
  }

  return result.values.map((row) =>
    Object.fromEntries(
      result.columns.map((columnName, index) => {
        const rawValue = row[index];
        const normalizedValue =
          rawValue === null ||
          typeof rawValue === "number" ||
          typeof rawValue === "string"
            ? rawValue
            : String(rawValue);

        return [columnName, normalizedValue];
      })
    )
  );
}

function toQueryRows(rows: Array<Record<string, unknown>>): QueryRow[] {
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => [
        key,
        value === null ||
        typeof value === "number" ||
        typeof value === "string" ||
        typeof value === "boolean"
          ? value
          : (JSON.stringify(value) ?? Object.prototype.toString.call(value)),
      ])
    )
  );
}

function compareRowsStrict(
  actualRows: QueryRow[],
  expectedRows: QueryRow[]
): boolean {
  const normalizedActual = normalizeRowsStrictInOrder(actualRows);
  const normalizedExpected = normalizeRowsStrictInOrder(expectedRows);
  return (
    JSON.stringify(normalizedActual) === JSON.stringify(normalizedExpected)
  );
}

function compareRowsRelaxed(
  actualRows: QueryRow[],
  expectedRows: QueryRow[]
): boolean {
  const normalizedActual = normalizeRowsRelaxed(actualRows);
  const normalizedExpected = normalizeRowsRelaxed(expectedRows);
  return (
    JSON.stringify(normalizedActual) === JSON.stringify(normalizedExpected)
  );
}

function normalizeRowsStrictInOrder(rows: QueryRow[]): string[] {
  return rows.map((row) => {
    const normalizedEntries = Object.keys(row)
      .sort((leftKey, rightKey) => {
        if (leftKey < rightKey) {
          return -1;
        }
        if (leftKey > rightKey) {
          return 1;
        }
        return 0;
      })
      .map((key) => [key, normalizeValue(row[key] ?? null)]);
    return JSON.stringify(normalizedEntries);
  });
}

function normalizeRowsRelaxed(rows: QueryRow[]): string[] {
  return rows
    .map((row) => {
      const normalizedValues = Object.values(row)
        .map((value) => normalizeValue(value))
        .map((value) => JSON.stringify(value))
        .sort();
      return JSON.stringify(normalizedValues);
    })
    .sort();
}

function normalizeValue(
  value: boolean | null | number | string
): boolean | null | number | string {
  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return value;
    }

    return Number(value.toFixed(6));
  }

  return value;
}

function summarizeBenchmark(
  results: BenchmarkResultRow[]
): Record<string, unknown> {
  const byScenario = new Map<string, BenchmarkResultRow[]>();

  for (const row of results) {
    byScenario.set(row.scenario, [
      ...(byScenario.get(row.scenario) ?? []),
      row,
    ]);
  }

  return Object.fromEntries(
    [...byScenario.entries()].map(([scenario, rows]) => {
      const completedRows = rows.filter((row) => row.executionMs !== null);
      const avgExecutionMs =
        completedRows.length === 0
          ? null
          : Number(
              (
                completedRows.reduce(
                  (sum, row) => sum + (row.executionMs ?? 0),
                  0
                ) / completedRows.length
              ).toFixed(3)
            );

      return [
        scenario,
        {
          avgExecutionMs,
          llmAllRowsMatchedCount: rows.filter((row) => row.llmAllRowsMatched)
            .length,
          llmRowAccuracyAvg:
            rows.length === 0
              ? null
              : Number(
                  (
                    rows.reduce(
                      (sum, row) => sum + (row.llmRowAccuracy ?? 0),
                      0
                    ) / rows.length
                  ).toFixed(6)
                ),
          llmRowsMatched: rows.reduce(
            (sum, row) => sum + row.llmRowsMatched,
            0
          ),
          llmRowsTotal: rows.reduce((sum, row) => sum + row.llmRowsTotal, 0),
          questions: rows.length,
          relaxedCorrectCount: rows.filter((row) => row.relaxedCorrect).length,
          sqlErrors: rows.filter((row) => row.error !== null).length,
          strictCorrectCount: rows.filter((row) => row.strictCorrect).length,
        },
      ];
    })
  );
}

function writeBenchmarkCsv(path: string, rows: BenchmarkResultRow[]): void {
  const headers: Array<keyof BenchmarkResultRow> = [
    "datasetFile",
    "questionId",
    "scenario",
    "question",
    "groundTruthSql",
    "generatedSql",
    "expectedRowsJson",
    "actualRowsJson",
    "inferenceModel",
    "evaluationModel",
    "evaluationCorrect",
    "evaluationReason",
    "sqlValid",
    "strictCorrect",
    "relaxedCorrect",
    "llmRowsMatched",
    "llmRowsTotal",
    "llmRowAccuracy",
    "llmAllRowsMatched",
    "executionMs",
    "expectedRowCount",
    "actualRowCount",
    "error",
    "sqlGenerationPrompt",
    "evaluationPrompt",
    "rowEvaluationPromptsJson",
  ];

  const csvLines = [headers.join(",")];

  for (const row of rows) {
    const line = headers
      .map((header) => escapeCsv(String(row[header] ?? "")))
      .join(",");
    csvLines.push(line);
  }

  writeFileSync(path, `${csvLines.join("\n")}\n`, "utf8");
}

function escapeCsv(value: string): string {
  if (!/[",\n]/.test(value)) {
    return value;
  }

  return `"${value.replaceAll('"', '""')}"`;
}

function escapeSqlLiteral(value: string): string {
  return value.replaceAll("'", "''");
}
