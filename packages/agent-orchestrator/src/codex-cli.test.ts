import { describe, expect, it } from "vitest";

import { buildCodexPipelinePrompt } from "./codex-cli.js";

describe("buildCodexPipelinePrompt", () => {
  it("describes the source database path, tables, and required artifacts", () => {
    const prompt = buildCodexPipelinePrompt({
      sourceDatabasePath: ".data/source-datasets.sqlite",
      sourceDatasetId: "dataset_1",
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
    expect(prompt).toContain("source sqlite database path:");
    expect(prompt).toContain("table source_sheet_sheet_1");
    expect(prompt).toContain("Write pipeline.sql");
    expect(prompt).toContain("analysis.json contract");
    expect(prompt).toContain(
      'The runtime will ATTACH the source database as schema "source"'
    );
  });
});
