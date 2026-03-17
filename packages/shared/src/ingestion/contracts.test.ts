import { describe, expect, it } from "vitest";

import type { WorkbookUploadRequest } from "./contracts.js";

describe("ingestion contracts", () => {
  it("defines the workbook upload request shape", () => {
    const request: WorkbookUploadRequest = {
      workbookName: "sales.xlsx",
      sheets: [
        {
          name: "Orders",
          rows: [{ OrderId: "A-1", Amount: 25 }],
        },
      ],
    };

    expect(request.sheets[0]?.rows[0]).toStrictEqual({
      Amount: 25,
      OrderId: "A-1",
    });
  });
});
