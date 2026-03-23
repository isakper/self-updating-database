import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import initSqlJs, { type Database } from "sql.js";
import * as XLSX from "xlsx";

type DatasetQuestion = {
  expectedResult: Array<Record<string, unknown>>;
  id: string;
  question: string;
  sql: string;
};

type WorkbookRow = Record<string, unknown>;
type SqlCellValue = number | string | Uint8Array | null;

const workbookPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-demo.xlsx"
);
const datasetJsonPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-log-inspired-dataset-2.json"
);
const datasetCsvPath = resolve(
  "apps/web/fixtures/demo-workbooks/retailer-transactions-log-inspired-dataset-2.csv"
);

const LAUNCH_DATE_QUESTION_SUFFIX =
  "Include every date in the range for each SKU only from its launch date onward (exclude pre-launch dates), and return 0 when there were no sales on eligible dates.";
const QUESTION_SUFFIX_REPLACEMENT =
  "Include every date in the range for each SKU, and return 0 when there were no sales on those dates.";

const SQL_LAUNCH_FILTER_PATTERN =
  /FROM sku_scope\s+JOIN items\s+ON items\.itemSku = sku_scope\.itemSku\s+CROSS JOIN date_spine\s+WHERE date_spine\.businessDate >= COALESCE\(NULLIF\(items\.launchDate, ''\), DATE\('0001-01-01'\)\)\s+AND date_spine\.businessDate <= COALESCE\(NULLIF\(items\.discontinuedDate, ''\), DATE\('9999-12-31'\)\)/g;
const SQL_LAUNCH_FILTER_REPLACEMENT =
  "FROM sku_scope\n  CROSS JOIN date_spine";

const SQL = await initSqlJs();
const database = new SQL.Database();

try {
  loadWorkbookIntoDatabase({
    database,
    workbookPath,
  });

  const sourceQuestions = JSON.parse(
    readFileSync(datasetJsonPath, "utf8")
  ) as DatasetQuestion[];
  const updatedQuestions = sourceQuestions.map((question) =>
    rewriteQuestion({
      database,
      question,
    })
  );

  mkdirSync(dirname(datasetJsonPath), { recursive: true });
  writeFileSync(
    datasetJsonPath,
    `${JSON.stringify(updatedQuestions, null, 2)}\n`,
    "utf8"
  );
  writeFileSync(datasetCsvPath, toCsv(updatedQuestions), "utf8");

  console.log(
    `Regenerated ${updatedQuestions.length} questions in ${datasetJsonPath} and ${datasetCsvPath}`
  );
} finally {
  database.close();
}

function rewriteQuestion(options: {
  database: Database;
  question: DatasetQuestion;
}): DatasetQuestion {
  const rewrittenQuestion = options.question.question.replace(
    LAUNCH_DATE_QUESTION_SUFFIX,
    QUESTION_SUFFIX_REPLACEMENT
  );
  const rewrittenSql = options.question.sql.replace(
    SQL_LAUNCH_FILTER_PATTERN,
    SQL_LAUNCH_FILTER_REPLACEMENT
  );
  const expectedResult = executeSql(options.database, rewrittenSql);

  return {
    ...options.question,
    expectedResult,
    question: rewrittenQuestion,
    sql: rewrittenSql,
  };
}

function executeSql(
  database: Database,
  sqlText: string
): Array<Record<string, unknown>> {
  const results = database.exec(sqlText);
  const firstResult = results[0];

  if (!firstResult) {
    return [];
  }

  return firstResult.values.map((row) => {
    const mapped: Record<string, unknown> = {};
    firstResult.columns.forEach((columnName, index) => {
      mapped[columnName] = row[index] ?? null;
    });
    return mapped;
  });
}

function loadWorkbookIntoDatabase(options: {
  database: Database;
  workbookPath: string;
}): void {
  const workbook = XLSX.read(readFileSync(options.workbookPath), {
    type: "buffer",
  });
  const sheetMap: Array<{ sheetName: string; tableName: string }> = [
    { sheetName: "Transactions", tableName: "transactions" },
    { sheetName: "Items", tableName: "items" },
    { sheetName: "Stores", tableName: "stores" },
  ];

  for (const { sheetName, tableName } of sheetMap) {
    const sheet = workbook.Sheets[sheetName];
    if (!sheet) {
      throw new Error(`Missing ${sheetName} sheet in ${options.workbookPath}.`);
    }

    const rows = XLSX.utils.sheet_to_json<WorkbookRow>(sheet, {
      defval: null,
      raw: true,
    });
    createTableFromRows(options.database, tableName, rows);
  }
}

function createTableFromRows(
  database: Database,
  tableName: string,
  rows: WorkbookRow[]
): void {
  if (rows.length === 0) {
    throw new Error(`Cannot create table ${tableName} from empty rows.`);
  }

  const columnNames = Object.keys(rows[0] ?? {});
  if (columnNames.length === 0) {
    throw new Error(`Cannot infer columns for table ${tableName}.`);
  }

  const columnDefinitions = columnNames
    .map((columnName) => {
      const type = inferSqliteType(rows, columnName);
      return `${columnName} ${type}`;
    })
    .join(", ");

  database.exec(`DROP TABLE IF EXISTS ${tableName};`);
  database.exec(`CREATE TABLE ${tableName} (${columnDefinitions});`);

  const placeholders = columnNames.map(() => "?").join(", ");
  const insertStatement = database.prepare(
    `INSERT INTO ${tableName} (${columnNames.join(", ")}) VALUES (${placeholders});`
  );

  try {
    for (const row of rows) {
      const values = columnNames.map((columnName) =>
        toSqlCellValue(row[columnName])
      );
      insertStatement.run(values);
    }
  } finally {
    insertStatement.free();
  }
}

function inferSqliteType(rows: WorkbookRow[], columnName: string): string {
  for (const row of rows) {
    const value = row[columnName];
    if (value === null || value === undefined || value === "") {
      continue;
    }
    if (typeof value === "number") {
      return "REAL";
    }
    return "TEXT";
  }
  return "TEXT";
}

function toSqlCellValue(value: unknown): SqlCellValue {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (typeof value === "number" || typeof value === "string") {
    return value;
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  return JSON.stringify(value);
}

function toCsv(questions: DatasetQuestion[]): string {
  const header = ["id", "question", "sql", "expectedResult"];
  const lines = [header.join(",")];

  for (const question of questions) {
    const row = [
      question.id,
      question.question,
      question.sql,
      JSON.stringify(question.expectedResult),
    ];
    lines.push(row.map(csvEscape).join(","));
  }

  return `${lines.join("\n")}\n`;
}

function csvEscape(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}
