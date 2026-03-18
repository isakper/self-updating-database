import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import initSqlJs from "sql.js";
import { describe, expect, it } from "vitest";

import { createSqliteQueryExecutor } from "./query-executor.js";

describe("createSqliteQueryExecutor", () => {
  it("executes a read-only query against a clean sqlite database", async () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), "query-executor-"));

    try {
      const databasePath = join(tempDirectory, "clean.sqlite");
      const SQL = await initSqlJs();
      const database = new SQL.Database();

      database.run(`
        CREATE TABLE clean_orders (
          region TEXT NOT NULL,
          amount NUMERIC NOT NULL
        );

        INSERT INTO clean_orders (region, amount)
        VALUES ('North', 25), ('South', 30);
      `);
      writeFileSync(databasePath, Buffer.from(database.export()));
      database.close();

      const executor = createSqliteQueryExecutor();
      const result = await executor.executeQuery({
        cleanDatabasePath: databasePath,
        sqlText:
          "SELECT region, amount FROM clean_orders ORDER BY amount DESC;",
      });

      expect(result).toStrictEqual({
        columnNames: ["region", "amount"],
        rows: [
          ["South", 30],
          ["North", 25],
        ],
      });
    } finally {
      rmSync(tempDirectory, { force: true, recursive: true });
    }
  });
});
