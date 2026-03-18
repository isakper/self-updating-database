import { describe, expect, it } from "vitest";

import { validateQuerySql } from "./query-sql-validator.js";

describe("validateQuerySql", () => {
  it("accepts a single select statement", () => {
    expect(
      validateQuerySql(
        "SELECT region, SUM(amount) AS total_amount FROM clean_orders GROUP BY region;"
      )
    ).toStrictEqual({
      errors: [],
      isValid: true,
    });
  });

  it("rejects multiple or mutating statements", () => {
    const result = validateQuerySql(
      "SELECT * FROM clean_orders; DROP TABLE clean_orders;"
    );

    expect(result.isValid).toBe(false);
    expect(result.errors).toContain(
      "Generated query SQL must contain exactly one statement."
    );
  });
});
