import { readFile } from "node:fs/promises";

import initSqlJs from "sql.js";

import type { QueryExecutionResult } from "../../shared/src/index.js";

export interface ExecuteQueryOptions {
  cleanDatabasePath: string;
  sqlText: string;
}

export interface QueryExecutor {
  executeQuery(options: ExecuteQueryOptions): Promise<QueryExecutionResult>;
}

export function createSqliteQueryExecutor(): QueryExecutor {
  return {
    async executeQuery(options) {
      const SQL = await initSqlJs();
      const databaseBytes = await readFile(options.cleanDatabasePath);
      const database = new SQL.Database(databaseBytes);

      try {
        const [result] = database.exec(options.sqlText);

        if (!result) {
          return {
            columnNames: [],
            rows: [],
          };
        }

        return {
          columnNames: result.columns,
          rows: result.values.map((row) =>
            row.map((value) =>
              typeof value === "number" ||
              typeof value === "string" ||
              value === null
                ? value
                : String(value)
            )
          ),
        };
      } finally {
        database.close();
      }
    },
  };
}
