import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

import type { CleanDatabaseSummary } from "../../shared/src/index.js";

const execFileAsync = promisify(execFile);

export interface BuildCleanDatabaseOptions {
  builtAt: string;
  cleanDatabaseId: string;
  cleanDatabasePath: string;
  sourceDatabasePath: string;
  sqlText: string;
}

export interface CleanDatabaseBuilder {
  buildCleanDatabase(
    options: BuildCleanDatabaseOptions
  ): Promise<CleanDatabaseSummary>;
}

export function createSqliteCleanDatabaseBuilder(): CleanDatabaseBuilder {
  return {
    async buildCleanDatabase(options) {
      const cleanDatabasePath = resolve(options.cleanDatabasePath);
      await mkdir(dirname(cleanDatabasePath), { recursive: true });

      const sqliteScriptPath = `${cleanDatabasePath}.pipeline.sql`;
      const sqliteScript = [
        `ATTACH DATABASE '${escapeSqlLiteral(resolve(options.sourceDatabasePath))}' AS source;`,
        options.sqlText.trim(),
      ].join("\n\n");

      await writeFile(sqliteScriptPath, sqliteScript, "utf8");
      await execFileAsync(
        "sqlite3",
        [cleanDatabasePath, `.read ${sqliteScriptPath}`],
        {
          maxBuffer: 10 * 1024 * 1024,
        }
      );

      await readFile(cleanDatabasePath);

      return {
        builtAt: options.builtAt,
        cleanDatabaseId: options.cleanDatabaseId,
        databaseFilePath: cleanDatabasePath,
      };
    },
  };
}

function escapeSqlLiteral(value: string): string {
  return value.replaceAll("'", "''");
}
