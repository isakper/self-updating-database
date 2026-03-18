import { describe, expect, it } from "vitest";

import { validatePipelineSql } from "./sql-validator.js";

describe("validatePipelineSql", () => {
  it("accepts a create-table cleaning pipeline that reads from source", () => {
    const result = validatePipelineSql(`
      DROP TABLE IF EXISTS cleaned_orders;
      CREATE TABLE cleaned_orders AS
      SELECT trim("Order ID") AS order_id
      FROM source.source_sheet_sheet_1;
    `);

    expect(result).toStrictEqual({
      errors: [],
      isValid: true,
    });
  });

  it("rejects forbidden statements and writes to source tables", () => {
    const result = validatePipelineSql(`
      UPDATE source.source_sheet_sheet_1
      SET "Order ID" = trim("Order ID");
    `);

    expect(result.isValid).toBe(false);
    expect(result.errors.join(" ")).toContain("forbidden");
  });
});
