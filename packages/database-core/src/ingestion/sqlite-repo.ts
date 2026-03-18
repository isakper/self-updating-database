import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

import initSqlJs, {
  type BindParams,
  type Database,
  type SqlValue,
} from "sql.js";

import type {
  CleanDatabaseSummary,
  ImportProcessingState,
  PipelineRunRecord,
  PipelineVersionRecord,
} from "../../../shared/src/index.js";
import type { IngestionRepository } from "./repo.js";
import type { SourceDataset, SourceRow, SourceSheet } from "./types.js";

export interface OpenSourceDatabaseOptions {
  databaseFilePath: string;
}

export interface SourceDatabaseConnection {
  close(): void;
  database: Database;
  databaseFilePath: string;
  persist(): void;
}

export async function openSourceDatabase(
  options: OpenSourceDatabaseOptions
): Promise<SourceDatabaseConnection> {
  mkdirSync(dirname(options.databaseFilePath), { recursive: true });

  const SQL = await initSqlJs();
  const database = existsSync(options.databaseFilePath)
    ? new SQL.Database(readFileSync(options.databaseFilePath))
    : new SQL.Database();

  initializeSourceDatabase(database);

  return {
    database,
    databaseFilePath: options.databaseFilePath,
    close() {
      database.close();
    },
    persist() {
      writeFileSync(options.databaseFilePath, Buffer.from(database.export()));
    },
  };
}

export class SqliteSourceDatasetRepository implements IngestionRepository {
  readonly #connection: SourceDatabaseConnection;

  constructor(options: { connection: SourceDatabaseConnection }) {
    this.#connection = options.connection;
    initializeSourceDatabase(this.#connection.database);
  }

  save(dataset: SourceDataset): void {
    const database = this.#connection.database;
    database.run("BEGIN");

    try {
      database.run(
        `
          INSERT INTO source_datasets (id, workbook_name, imported_at)
          VALUES ($id, $workbookName, $importedAt)
        `,
        {
          $id: dataset.id,
          $importedAt: dataset.importedAt,
          $workbookName: dataset.workbookName,
        }
      );

      dataset.sheets.forEach((sheet, sheetOrder) => {
        database.run(
          `
            INSERT INTO source_sheets (
              sheet_id,
              dataset_id,
              name,
              source_table_name,
              column_names_json,
              sheet_order
            )
            VALUES (
              $sheetId,
              $datasetId,
              $name,
              $sourceTableName,
              $columnNamesJson,
              $sheetOrder
            )
          `,
          {
            $columnNamesJson: JSON.stringify(sheet.columns),
            $datasetId: dataset.id,
            $name: sheet.name,
            $sheetId: sheet.sheetId,
            $sheetOrder: sheetOrder,
            $sourceTableName: sheet.sourceTableName,
          }
        );

        createSourceSheetTable(database, sheet);

        sheet.rows.forEach((row) => {
          database.run(
            `
              INSERT INTO source_rows (row_id, sheet_id, source_row_number, values_json)
              VALUES ($rowId, $sheetId, $sourceRowNumber, $valuesJson)
            `,
            {
              $rowId: row.rowId,
              $sheetId: sheet.sheetId,
              $sourceRowNumber: row.sourceRowNumber,
              $valuesJson: JSON.stringify(row.values),
            }
          );

          insertSourceSheetRow(database, sheet, row);
        });
      });

      database.run("COMMIT");
      this.#connection.persist();
    } catch (error) {
      database.run("ROLLBACK");
      throw error;
    }
  }

  getById(datasetId: string): SourceDataset | undefined {
    const datasetRow = readRows(
      this.#connection.database,
      `
        SELECT id, workbook_name, imported_at
        FROM source_datasets
        WHERE id = $datasetId
      `,
      { $datasetId: datasetId }
    )[0];

    if (!datasetRow) {
      return undefined;
    }

    const sheets = readRows(
      this.#connection.database,
      `
        SELECT sheet_id, name, source_table_name, column_names_json
        FROM source_sheets
        WHERE dataset_id = $datasetId
        ORDER BY sheet_order ASC
      `,
      { $datasetId: datasetId }
    ).map((sheetRow) => {
      const sheetId = readString(sheetRow, "sheet_id");

      return {
        sheetId,
        name: readString(sheetRow, "name"),
        columns: readStringArray(sheetRow, "column_names_json"),
        sourceTableName: readString(sheetRow, "source_table_name"),
        rows: readRows(
          this.#connection.database,
          `
            SELECT row_id, source_row_number, values_json
            FROM source_rows
            WHERE sheet_id = $sheetId
            ORDER BY source_row_number ASC
          `,
          { $sheetId: sheetId }
        ).map(parseSourceRow),
      } satisfies SourceSheet;
    });

    return {
      id: readString(datasetRow, "id"),
      importedAt: readString(datasetRow, "imported_at"),
      workbookName: readString(datasetRow, "workbook_name"),
      sheets,
    };
  }

  list(): SourceDataset[] {
    return readRows(
      this.#connection.database,
      `
        SELECT id
        FROM source_datasets
        ORDER BY imported_at DESC, id DESC
      `
    ).flatMap((datasetRow) => {
      const dataset = this.getById(readString(datasetRow, "id"));
      return dataset ? [dataset] : [];
    });
  }

  saveImportProcessingState(
    datasetId: string,
    processingState: ImportProcessingState
  ): void {
    this.#connection.database.run(
      `
        INSERT INTO import_processing_state (
          dataset_id,
          pipeline_status,
          pipeline_version_id,
          pipeline_retry_count,
          clean_database_status,
          clean_database_id,
          clean_database_path,
          clean_database_built_at,
          last_pipeline_error,
          next_retry_at,
          pipeline_run_id
        )
        VALUES (
          $datasetId,
          $pipelineStatus,
          $pipelineVersionId,
          $pipelineRetryCount,
          $cleanDatabaseStatus,
          $cleanDatabaseId,
          $cleanDatabasePath,
          $cleanDatabaseBuiltAt,
          $lastPipelineError,
          $nextRetryAt,
          $pipelineRunId
        )
        ON CONFLICT(dataset_id) DO UPDATE SET
          pipeline_status = excluded.pipeline_status,
          pipeline_version_id = excluded.pipeline_version_id,
          pipeline_retry_count = excluded.pipeline_retry_count,
          clean_database_status = excluded.clean_database_status,
          clean_database_id = excluded.clean_database_id,
          clean_database_path = excluded.clean_database_path,
          clean_database_built_at = excluded.clean_database_built_at,
          last_pipeline_error = excluded.last_pipeline_error,
          next_retry_at = excluded.next_retry_at,
          pipeline_run_id = excluded.pipeline_run_id
      `,
      {
        $cleanDatabaseBuiltAt: processingState.cleanDatabase?.builtAt ?? null,
        $cleanDatabaseId:
          processingState.cleanDatabase?.cleanDatabaseId ?? null,
        $cleanDatabasePath:
          processingState.cleanDatabase?.databaseFilePath ?? null,
        $cleanDatabaseStatus: processingState.cleanDatabaseStatus,
        $datasetId: datasetId,
        $lastPipelineError: processingState.lastPipelineError,
        $nextRetryAt: processingState.nextRetryAt,
        $pipelineRetryCount: processingState.pipelineRetryCount,
        $pipelineRunId: processingState.pipelineRun?.runId ?? null,
        $pipelineStatus: processingState.pipelineStatus,
        $pipelineVersionId:
          processingState.pipelineVersion?.pipelineVersionId ?? null,
      }
    );
    this.#connection.persist();
  }

  getImportProcessingState(
    datasetId: string
  ): ImportProcessingState | undefined {
    const stateRow = readRows(
      this.#connection.database,
      `
        SELECT
          pipeline_status,
          pipeline_version_id,
          pipeline_retry_count,
          clean_database_status,
          clean_database_id,
          clean_database_path,
          clean_database_built_at,
          last_pipeline_error,
          next_retry_at,
          pipeline_run_id
        FROM import_processing_state
        WHERE dataset_id = $datasetId
      `,
      { $datasetId: datasetId }
    )[0];

    if (!stateRow) {
      return undefined;
    }

    return {
      cleanDatabase: parseCleanDatabaseSummary(stateRow),
      cleanDatabaseStatus: readStatus(stateRow, "clean_database_status"),
      lastPipelineError: readNullableString(stateRow, "last_pipeline_error"),
      nextRetryAt: readNullableString(stateRow, "next_retry_at"),
      pipelineRetryCount: readNumber(stateRow, "pipeline_retry_count"),
      pipelineRun: this.getLatestPipelineRun(datasetId) ?? null,
      pipelineStatus: readStatus(stateRow, "pipeline_status"),
      pipelineVersion: this.getLatestPipelineVersion(datasetId) ?? null,
    };
  }

  savePipelineVersion(versionRecord: PipelineVersionRecord): void {
    this.#connection.database.run(
      `
        INSERT INTO pipeline_versions (
          pipeline_id,
          pipeline_version_id,
          source_dataset_id,
          sql_text,
          analysis_json,
          summary_markdown,
          created_at,
          created_by
        )
        VALUES (
          $pipelineId,
          $pipelineVersionId,
          $sourceDatasetId,
          $sqlText,
          $analysisJson,
          $summaryMarkdown,
          $createdAt,
          $createdBy
        )
      `,
      {
        $analysisJson: JSON.stringify(versionRecord.analysisJson),
        $createdAt: versionRecord.createdAt,
        $createdBy: versionRecord.createdBy,
        $pipelineId: versionRecord.pipelineId,
        $pipelineVersionId: versionRecord.pipelineVersionId,
        $sourceDatasetId: versionRecord.sourceDatasetId,
        $sqlText: versionRecord.sqlText,
        $summaryMarkdown: versionRecord.summaryMarkdown,
      }
    );
    this.#connection.persist();
  }

  getLatestPipelineVersion(
    datasetId: string
  ): PipelineVersionRecord | undefined {
    const versionRow = readRows(
      this.#connection.database,
      `
        SELECT
          pipeline_id,
          pipeline_version_id,
          source_dataset_id,
          sql_text,
          analysis_json,
          summary_markdown,
          created_at,
          created_by
        FROM pipeline_versions
        WHERE source_dataset_id = $datasetId
        ORDER BY created_at DESC, pipeline_version_id DESC
        LIMIT 1
      `,
      { $datasetId: datasetId }
    )[0];

    if (!versionRow) {
      return undefined;
    }

    return parsePipelineVersionRecord(versionRow);
  }

  savePipelineRun(runRecord: PipelineRunRecord): void {
    this.#connection.database.run(
      `
        INSERT INTO pipeline_runs (
          run_id,
          pipeline_version_id,
          source_dataset_id,
          status,
          run_started_at,
          run_finished_at,
          retry_count,
          run_error
        )
        VALUES (
          $runId,
          $pipelineVersionId,
          $sourceDatasetId,
          $status,
          $runStartedAt,
          $runFinishedAt,
          $retryCount,
          $runError
        )
        ON CONFLICT(run_id) DO UPDATE SET
          status = excluded.status,
          run_finished_at = excluded.run_finished_at,
          retry_count = excluded.retry_count,
          run_error = excluded.run_error
      `,
      {
        $pipelineVersionId: runRecord.pipelineVersionId,
        $retryCount: runRecord.retryCount,
        $runError: runRecord.runError,
        $runFinishedAt: runRecord.runFinishedAt,
        $runId: runRecord.runId,
        $runStartedAt: runRecord.runStartedAt,
        $sourceDatasetId: runRecord.sourceDatasetId,
        $status: runRecord.status,
      }
    );
    this.#connection.persist();
  }

  getLatestPipelineRun(datasetId: string): PipelineRunRecord | undefined {
    const runRow = readRows(
      this.#connection.database,
      `
        SELECT
          run_id,
          pipeline_version_id,
          source_dataset_id,
          status,
          run_started_at,
          run_finished_at,
          retry_count,
          run_error
        FROM pipeline_runs
        WHERE source_dataset_id = $datasetId
        ORDER BY run_started_at DESC, run_id DESC
        LIMIT 1
      `,
      { $datasetId: datasetId }
    )[0];

    if (!runRow) {
      return undefined;
    }

    return parsePipelineRunRecord(runRow);
  }

  listRetryableDatasetIds(nowIso: string): string[] {
    return readRows(
      this.#connection.database,
      `
        SELECT dataset_id
        FROM import_processing_state
        WHERE pipeline_status != 'succeeded'
          AND pipeline_retry_count < 5
          AND (next_retry_at IS NULL OR next_retry_at <= $nowIso)
      `,
      { $nowIso: nowIso }
    ).map((row) => readString(row, "dataset_id"));
  }
}

function initializeSourceDatabase(database: Database): void {
  database.run(`
    CREATE TABLE IF NOT EXISTS source_datasets (
      id TEXT PRIMARY KEY,
      workbook_name TEXT NOT NULL,
      imported_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS source_sheets (
      sheet_id TEXT PRIMARY KEY,
      dataset_id TEXT NOT NULL,
      name TEXT NOT NULL,
      source_table_name TEXT NOT NULL,
      column_names_json TEXT NOT NULL,
      sheet_order INTEGER NOT NULL,
      FOREIGN KEY (dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS source_rows (
      row_id TEXT PRIMARY KEY,
      sheet_id TEXT NOT NULL,
      source_row_number INTEGER NOT NULL,
      values_json TEXT NOT NULL,
      FOREIGN KEY (sheet_id) REFERENCES source_sheets(sheet_id)
    );

    CREATE TABLE IF NOT EXISTS import_processing_state (
      dataset_id TEXT PRIMARY KEY,
      pipeline_status TEXT NOT NULL,
      pipeline_version_id TEXT,
      pipeline_retry_count INTEGER NOT NULL,
      clean_database_status TEXT NOT NULL,
      clean_database_id TEXT,
      clean_database_path TEXT,
      clean_database_built_at TEXT,
      last_pipeline_error TEXT,
      next_retry_at TEXT,
      pipeline_run_id TEXT,
      FOREIGN KEY (dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_versions (
      pipeline_version_id TEXT PRIMARY KEY,
      pipeline_id TEXT NOT NULL,
      source_dataset_id TEXT NOT NULL,
      sql_text TEXT NOT NULL,
      analysis_json TEXT NOT NULL,
      summary_markdown TEXT NOT NULL,
      created_at TEXT NOT NULL,
      created_by TEXT NOT NULL,
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_id TEXT PRIMARY KEY,
      pipeline_version_id TEXT NOT NULL,
      source_dataset_id TEXT NOT NULL,
      status TEXT NOT NULL,
      run_started_at TEXT NOT NULL,
      run_finished_at TEXT,
      retry_count INTEGER NOT NULL,
      run_error TEXT,
      FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id),
      FOREIGN KEY (source_dataset_id) REFERENCES source_datasets(id)
    );

    CREATE INDEX IF NOT EXISTS idx_source_sheets_dataset_id
      ON source_sheets(dataset_id, sheet_order);

    CREATE INDEX IF NOT EXISTS idx_source_rows_sheet_id
      ON source_rows(sheet_id, source_row_number);

    CREATE INDEX IF NOT EXISTS idx_pipeline_versions_dataset_id
      ON pipeline_versions(source_dataset_id, created_at);

    CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dataset_id
      ON pipeline_runs(source_dataset_id, run_started_at);
  `);
}

function createSourceSheetTable(database: Database, sheet: SourceSheet): void {
  const columnDefinitions = [
    `"__source_row_number" INTEGER NOT NULL`,
    ...sheet.columns.map((columnName) => `${escapeIdentifier(columnName)}`),
  ].join(", ");

  database.run(
    `CREATE TABLE ${escapeIdentifier(sheet.sourceTableName)} (${columnDefinitions})`
  );
}

function insertSourceSheetRow(
  database: Database,
  sheet: SourceSheet,
  row: SourceRow
): void {
  const insertColumns = [
    `"__source_row_number"`,
    ...sheet.columns.map((columnName) => escapeIdentifier(columnName)),
  ].join(", ");
  const insertValues = [
    `$sourceRowNumber`,
    ...sheet.columns.map((_, index) => `$column_${index}`),
  ].join(", ");
  const params: Record<string, SqlValue> = {
    $sourceRowNumber: row.sourceRowNumber,
  };

  sheet.columns.forEach((columnName, index) => {
    params[`$column_${index}`] = toSqlValue(row.values[columnName] ?? null);
  });

  database.run(
    `INSERT INTO ${escapeIdentifier(sheet.sourceTableName)} (${insertColumns}) VALUES (${insertValues})`,
    params
  );
}

function readRows(
  database: Database,
  sql: string,
  params?: BindParams
): Array<Record<string, SqlValue>> {
  const [result] = database.exec(sql, params);

  if (!result) {
    return [];
  }

  return result.values.map((rowValues) =>
    Object.fromEntries(
      result.columns.map((columnName, index) => [
        columnName,
        rowValues[index] ?? null,
      ])
    )
  );
}

function parseSourceRow(row: Record<string, SqlValue>): SourceRow {
  return {
    rowId: readString(row, "row_id"),
    sourceRowNumber: readNumber(row, "source_row_number"),
    values: readJsonRecord(row, "values_json"),
  };
}

function parsePipelineVersionRecord(
  row: Record<string, SqlValue>
): PipelineVersionRecord {
  return {
    analysisJson: readJsonValue(
      row,
      "analysis_json"
    ) as PipelineVersionRecord["analysisJson"],
    createdAt: readString(row, "created_at"),
    createdBy: "codex_cli",
    pipelineId: readString(row, "pipeline_id"),
    pipelineVersionId: readString(row, "pipeline_version_id"),
    sourceDatasetId: readString(row, "source_dataset_id"),
    sqlText: readString(row, "sql_text"),
    summaryMarkdown: readString(row, "summary_markdown"),
  };
}

function parsePipelineRunRecord(
  row: Record<string, SqlValue>
): PipelineRunRecord {
  return {
    pipelineVersionId: readString(row, "pipeline_version_id"),
    retryCount: readNumber(row, "retry_count"),
    runError: readNullableString(row, "run_error"),
    runFinishedAt: readNullableString(row, "run_finished_at"),
    runId: readString(row, "run_id"),
    runStartedAt: readString(row, "run_started_at"),
    sourceDatasetId: readString(row, "source_dataset_id"),
    status: readStatus(row, "status"),
  };
}

function parseCleanDatabaseSummary(
  row: Record<string, SqlValue>
): CleanDatabaseSummary | null {
  const cleanDatabaseId = readNullableString(row, "clean_database_id");
  const databaseFilePath = readNullableString(row, "clean_database_path");
  const builtAt = readNullableString(row, "clean_database_built_at");

  if (!cleanDatabaseId || !databaseFilePath || !builtAt) {
    return null;
  }

  return {
    builtAt,
    cleanDatabaseId,
    databaseFilePath,
  };
}

function readString(row: Record<string, SqlValue>, key: string): string {
  const value = row[key];

  if (typeof value !== "string") {
    throw new Error(`Expected ${key} to be a string.`);
  }

  return value;
}

function readNullableString(
  row: Record<string, SqlValue>,
  key: string
): string | null {
  const value = row[key];

  if (value === null) {
    return null;
  }

  if (typeof value !== "string") {
    throw new Error(`Expected ${key} to be a nullable string.`);
  }

  return value;
}

function readNumber(row: Record<string, SqlValue>, key: string): number {
  const value = row[key];

  if (typeof value !== "number") {
    throw new Error(`Expected ${key} to be a number.`);
  }

  return value;
}

function readStatus(row: Record<string, SqlValue>, key: string) {
  return readString(row, key) as
    | ImportProcessingState["pipelineStatus"]
    | ImportProcessingState["cleanDatabaseStatus"];
}

function readStringArray(row: Record<string, SqlValue>, key: string): string[] {
  const value = readJsonValue(row, key);

  if (
    !Array.isArray(value) ||
    value.some((entry) => typeof entry !== "string")
  ) {
    throw new Error(`Expected ${key} to decode to a string array.`);
  }

  return value as string[];
}

function readJsonRecord(
  row: Record<string, SqlValue>,
  key: string
): Record<string, boolean | number | string | null> {
  const value = readJsonValue(row, key);

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Expected ${key} to decode to an object.`);
  }

  const valueRecord = value as Record<string, unknown>;
  const record: Record<string, boolean | number | string | null> = {};

  Object.entries(valueRecord).forEach(([entryKey, entryValue]) => {
    if (
      entryValue === null ||
      typeof entryValue === "boolean" ||
      typeof entryValue === "number" ||
      typeof entryValue === "string"
    ) {
      record[entryKey] = entryValue;
      return;
    }

    throw new Error(
      `Expected ${key}.${entryKey} to be a scalar workbook cell value.`
    );
  });

  return record;
}

function readJsonValue(row: Record<string, SqlValue>, key: string): unknown {
  return JSON.parse(readString(row, key)) as unknown;
}

function escapeIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function toSqlValue(value: SourceRow["values"][string]): SqlValue {
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  return value;
}
