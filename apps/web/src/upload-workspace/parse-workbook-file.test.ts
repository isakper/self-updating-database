import { describe, expect, it } from "vitest";
import { utils, write, type WorkBook } from "xlsx";

import { parseWorkbookFile } from "./parse-workbook-file.js";

describe("parseWorkbookFile", () => {
  it("turns an excel workbook into the shared upload contract", () => {
    const workbook: WorkBook = utils.book_new();
    const ordersSheet = utils.json_to_sheet([
      { OrderId: "A-1", Amount: 25, Region: "North" },
      { OrderId: "A-2", Amount: 30, Region: "South" },
    ]);
    const customersSheet = utils.json_to_sheet([
      { CustomerId: "C-1", Segment: "Enterprise" },
    ]);

    utils.book_append_sheet(workbook, ordersSheet, "Orders");
    utils.book_append_sheet(workbook, customersSheet, "Customers");

    const fileBuffer = Buffer.from(
      write(workbook, {
        type: "buffer",
        bookType: "xlsx",
      }) as Uint8Array
    );

    expect(
      parseWorkbookFile({
        fileBuffer,
        fileName: "sales.xlsx",
      })
    ).toStrictEqual({
      workbookName: "sales.xlsx",
      sheets: [
        {
          name: "Orders",
          rows: [
            { Amount: 25, OrderId: "A-1", Region: "North" },
            { Amount: 30, OrderId: "A-2", Region: "South" },
          ],
        },
        {
          name: "Customers",
          rows: [{ CustomerId: "C-1", Segment: "Enterprise" }],
        },
      ],
    });
  });
});
