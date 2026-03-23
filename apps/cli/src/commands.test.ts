import { describe, expect, it } from "vitest";

import { parseCliArgs } from "./commands.js";

describe("parseCliArgs", () => {
  it("parses workbook upload", () => {
    const parsed = parseCliArgs(["upload", "workbook", "./demo.xlsx"]);

    expect(parsed.error).toBeUndefined();
    expect(parsed.command).toEqual({
      filePath: "./demo.xlsx",
      kind: "upload_workbook",
    });
  });

  it("parses query-log upload", () => {
    const parsed = parseCliArgs([
      "upload",
      "query-logs",
      "dataset_1",
      "./logs.xlsx",
    ]);

    expect(parsed.error).toBeUndefined();
    expect(parsed.command).toEqual({
      datasetId: "dataset_1",
      filePath: "./logs.xlsx",
      kind: "upload_query_logs",
    });
  });

  it("parses status watch flags", () => {
    const parsed = parseCliArgs([
      "--watch",
      "--interval-ms",
      "3000",
      "status",
      "dataset_2",
    ]);

    expect(parsed.error).toBeUndefined();
    expect(parsed.command).toEqual({
      datasetId: "dataset_2",
      intervalMs: 3000,
      kind: "status",
      watch: true,
    });
  });

  it("requires query prompt", () => {
    const parsed = parseCliArgs(["query", "dataset_3"]);

    expect(parsed.error).toBe("Query prompt is required.");
  });

  it("parses query command", () => {
    const parsed = parseCliArgs([
      "query",
      "dataset_4",
      "show",
      "daily",
      "revenue",
    ]);

    expect(parsed.error).toBeUndefined();
    expect(parsed.command).toEqual({
      datasetId: "dataset_4",
      kind: "query",
      prompt: "show daily revenue",
    });
  });

  it("parses explicit api base url", () => {
    const parsed = parseCliArgs([
      "--api-base-url",
      "http://127.0.0.1:4010",
      "dataset",
      "list",
    ]);

    expect(parsed.options.apiBaseUrl).toBe("http://127.0.0.1:4010");
    expect(parsed.command).toEqual({ kind: "dataset_list" });
  });

  it("parses optimization run with pinned base pipeline version", () => {
    const parsed = parseCliArgs([
      "optimization",
      "run",
      "dataset_9",
      "--base-pipeline-version-id",
      "pipeline_version_123",
    ]);

    expect(parsed.error).toBeUndefined();
    expect(parsed.command).toEqual({
      basePipelineVersionId: "pipeline_version_123",
      datasetId: "dataset_9",
      kind: "optimization_run",
    });
  });
});
