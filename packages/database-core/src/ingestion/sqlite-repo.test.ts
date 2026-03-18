import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import type { SourceDataset } from "./types.js";
import {
  openSourceDatabase,
  SqliteSourceDatasetRepository,
} from "./sqlite-repo.js";

describe("SqliteSourceDatasetRepository", () => {
  const tempDirectories: string[] = [];

  afterEach(() => {
    tempDirectories.splice(0).forEach((directoryPath) => {
      rmSync(directoryPath, { force: true, recursive: true });
    });
  });

  it("persists source datasets across repository instances", async () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), "source-db-"));
    tempDirectories.push(tempDirectory);

    const databaseFilePath = join(tempDirectory, "source-datasets.sqlite");
    const firstDatabase = await openSourceDatabase({ databaseFilePath });
    const firstRepository = new SqliteSourceDatasetRepository({
      connection: firstDatabase,
    });

    const dataset: SourceDataset = {
      id: "dataset_1",
      workbookName: "sales.xlsx",
      importedAt: "2026-03-18T00:00:00.000Z",
      sheets: [
        {
          sheetId: "sheet_1",
          name: "Orders",
          columns: ["OrderId", "Amount"],
          sourceTableName: "source_sheet_sheet_1",
          rows: [
            {
              rowId: "row_1",
              sourceRowNumber: 1,
              values: {
                Amount: 25,
                OrderId: "A-1",
              },
            },
          ],
        },
      ],
    };

    firstRepository.save(dataset);
    firstDatabase.close();

    const secondDatabase = await openSourceDatabase({ databaseFilePath });
    const secondRepository = new SqliteSourceDatasetRepository({
      connection: secondDatabase,
    });

    expect(secondRepository.getById("dataset_1")).toStrictEqual(dataset);
    expect(secondRepository.list()).toStrictEqual([dataset]);

    secondDatabase.close();
  });
});
