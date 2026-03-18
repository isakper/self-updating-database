import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildCodexPipelinePrompt,
  createCodexCliPipelineGenerator,
} from "./codex-cli.js";

const tempDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    tempDirectories
      .splice(0)
      .map((directoryPath) =>
        rm(directoryPath, { force: true, recursive: true })
      )
  );
});

describe("buildCodexPipelinePrompt", () => {
  it("describes the source database path, tables, and required artifacts", () => {
    const prompt = buildCodexPipelinePrompt({
      sourceDatabasePath: ".data/source-datasets.sqlite",
      sourceDatasetId: "dataset_1",
      sourceProfile: {
        sheetProfiles: [
          {
            columnProfiles: [
              {
                columnName: "Order ID",
                inferredType: "string",
                nonNullCount: 2,
                nullCount: 0,
                sampleValues: ["A-1", "A-2"],
              },
            ],
            rowCount: 2,
            sampleRows: [{ "Order ID": "A-1" }],
            sheetName: "Orders",
            sourceTableName: "source_sheet_sheet_1",
          },
        ],
        sourceDatasetId: "dataset_1",
        totalRowCount: 2,
        workbookName: "sales.xlsx",
      },
      sourceSheets: [
        {
          sheetName: "Orders",
          columnNames: ["Order ID", "Order Date"],
          sourceTableName: "source_sheet_sheet_1",
          rowCount: 2,
        },
      ],
      workbookName: "sales.xlsx",
    });

    expect(prompt).toContain("source dataset id: dataset_1");
    expect(prompt).toContain("total imported rows: 2");
    expect(prompt).toContain("table source_sheet_sheet_1");
    expect(prompt).toContain("source-profile.json");
    expect(prompt).toContain("samples/source_sheet_sheet_1.json");
    expect(prompt).toContain("Write pipeline.sql");
    expect(prompt).toContain("analysis.json contract");
    expect(prompt).toContain(
      'The runtime will ATTACH the source database as schema "source"'
    );
  });

  it("returns once required artifacts exist even if the codex process lingers", async () => {
    const tempDirectory = await mkdtemp(join(tmpdir(), "codex-cli-test-"));
    tempDirectories.push(tempDirectory);

    const fakeCodexPath = join(tempDirectory, "fake-codex.mjs");
    await writeFile(
      fakeCodexPath,
      `#!/usr/bin/env node
import { writeFileSync } from "node:fs";
import { join } from "node:path";

const cwdIndex = process.argv.indexOf("--cd");
const cwd = cwdIndex >= 0 ? process.argv[cwdIndex + 1] : process.cwd();

writeFileSync(join(cwd, "pipeline.sql"), "DROP TABLE IF EXISTS clean_orders;\\nCREATE TABLE clean_orders AS SELECT 1 AS ok;");
writeFileSync(join(cwd, "analysis.json"), JSON.stringify({
  sourceDatasetId: "dataset_1",
  summary: "Generated from fake codex",
  findings: [],
}));
writeFileSync(join(cwd, "summary.md"), "# Fake summary\\n");
setInterval(() => {}, 1000);
`,
      "utf8"
    );
    await chmod(fakeCodexPath, 0o755);

    const generator = createCodexCliPipelineGenerator({
      artifactPollIntervalMs: 25,
      codexCommand: fakeCodexPath,
      commandTimeoutMs: 2_000,
      processExitGracePeriodMs: 25,
    });

    const result = await generator.generatePipelineArtifacts({
      sourceDatabasePath: ".data/source-datasets.sqlite",
      sourceDatasetId: "dataset_1",
      sourceProfile: {
        sheetProfiles: [
          {
            columnProfiles: [
              {
                columnName: "Order ID",
                inferredType: "string",
                nonNullCount: 1,
                nullCount: 0,
                sampleValues: ["A-1"],
              },
            ],
            rowCount: 1,
            sampleRows: [{ "Order ID": "A-1" }],
            sheetName: "Orders",
            sourceTableName: "source_sheet_sheet_1",
          },
        ],
        sourceDatasetId: "dataset_1",
        totalRowCount: 1,
        workbookName: "sales.xlsx",
      },
      sourceSheets: [
        {
          sheetName: "Orders",
          columnNames: ["Order ID"],
          sourceTableName: "source_sheet_sheet_1",
          rowCount: 1,
        },
      ],
      workbookName: "sales.xlsx",
    });

    expect(result.sqlText).toContain("CREATE TABLE clean_orders");
    expect(result.summaryMarkdown).toContain("Fake summary");
    expect(result.analysisJson.sourceDatasetId).toBe("dataset_1");
  });
});
