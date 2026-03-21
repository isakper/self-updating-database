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

  it("accepts indexes on derived clean-database objects", () => {
    const result = validatePipelineSql(`
      DROP TABLE IF EXISTS cleaned_orders;
      CREATE TABLE cleaned_orders AS
      SELECT trim("Order ID") AS order_id
      FROM source.source_sheet_sheet_1;
      CREATE INDEX IF NOT EXISTS idx_cleaned_orders_order_id
        ON cleaned_orders(order_id);
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

  it("rejects indexes on source tables", () => {
    const result = validatePipelineSql(`
      CREATE INDEX idx_source_orders_order_id
        ON source.source_sheet_sheet_1("Order ID");
    `);

    expect(result.isValid).toBe(false);
    expect(result.errors.join(" ")).toContain(
      "must not create indexes on source"
    );
  });

  it("rejects ROUND(...) to preserve numeric precision", () => {
    const result = validatePipelineSql(`
      DROP TABLE IF EXISTS cleaned_orders;
      CREATE TABLE cleaned_orders AS
      SELECT ROUND(amount, 2) AS amount_rounded
      FROM source.source_sheet_sheet_1;
    `);

    expect(result.isValid).toBe(false);
    expect(result.errors.join(" ")).toContain("must not use ROUND");
  });
});
