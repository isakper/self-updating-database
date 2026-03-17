import { describe, expect, it } from "vitest";

import type { WorkbookUploadRequest } from "../../../../packages/shared/src/index.js";
import { createInMemoryIngestionApi } from "./api.js";
import { createWorkbookImportJob } from "./jobs.js";

describe("ingestion api", () => {
  it("stores imported workbooks and exposes import summaries", () => {
    const api = createInMemoryIngestionApi({
      now: new Date("2026-03-17T12:00:00.000Z"),
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
    });

    const workbook: WorkbookUploadRequest = {
      workbookName: "operations.xlsx",
      sheets: [
        {
          name: "Shipments",
          rows: [{ ShipmentId: "S-1", Status: "Delivered" }],
        },
      ],
    };

    const summary = api.importWorkbook(workbook);

    expect(summary.status).toBe("succeeded");
    expect(api.listImports()).toStrictEqual([summary]);
    expect(api.getSourceDataset(summary.sourceDatasetId)).toMatchObject({
      workbookName: "operations.xlsx",
    });
  });

  it("creates queued import jobs before execution starts", () => {
    const job = createWorkbookImportJob({
      workbookName: "finance.xlsx",
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
    });

    expect(job).toStrictEqual({
      jobId: "job_1",
      workbookName: "finance.xlsx",
      status: "queued",
    });
  });
});
