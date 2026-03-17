import { describe, expect, it } from "vitest";

import type { WorkbookUploadRequest } from "../../../shared/src/index.js";
import { InMemorySourceDatasetRepository } from "./repo.js";
import { ingestWorkbook } from "./service.js";

describe("ingestion service", () => {
  it("creates an immutable source dataset with provenance-rich sheet snapshots", () => {
    const repository = new InMemorySourceDatasetRepository();
    const workbook: WorkbookUploadRequest = {
      workbookName: "finance.xlsx",
      sheets: [
        {
          name: "Orders",
          rows: [
            { OrderId: "A-1", Amount: 25, Region: "North" },
            { OrderId: "A-2", Amount: 30, Region: "South" },
          ],
        },
        {
          name: "Customers",
          rows: [{ CustomerId: "C-1", Segment: "Enterprise" }],
        },
      ],
    };

    const result = ingestWorkbook({
      repository,
      request: workbook,
      now: new Date("2026-03-17T10:00:00.000Z"),
      createId: (() => {
        let counter = 0;
        return (prefix: string) => `${prefix}_${++counter}`;
      })(),
    });

    expect(result.summary).toStrictEqual({
      sourceDatasetId: "dataset_1",
      workbookName: "finance.xlsx",
      status: "succeeded",
      sheetCount: 2,
      totalRowCount: 3,
      sheets: [
        {
          sheetName: "Orders",
          columnNames: ["OrderId", "Amount", "Region"],
          rowCount: 2,
        },
        {
          sheetName: "Customers",
          columnNames: ["CustomerId", "Segment"],
          rowCount: 1,
        },
      ],
      importedAt: "2026-03-17T10:00:00.000Z",
    });

    expect(result.dataset.sheets[0]).toMatchObject({
      sheetId: "sheet_2",
      name: "Orders",
      columns: ["OrderId", "Amount", "Region"],
    });
    expect(result.dataset.sheets[0]?.rows[0]).toStrictEqual({
      rowId: "row_3",
      sourceRowNumber: 1,
      values: {
        Amount: 25,
        OrderId: "A-1",
        Region: "North",
      },
    });

    expect(repository.getById("dataset_1")).toStrictEqual(result.dataset);
  });

  it("rejects workbook uploads without sheets", () => {
    const repository = new InMemorySourceDatasetRepository();

    expect(() =>
      ingestWorkbook({
        repository,
        request: {
          workbookName: "empty.xlsx",
          sheets: [],
        },
      })
    ).toThrowError("Workbook upload must include at least one sheet.");
  });
});
