import { describe, expect, it } from "vitest";

import {
  detectUsedOptimizationObjects,
  extractQueryPattern,
} from "./query-patterns.js";

describe("extractQueryPattern", () => {
  it("normalizes equivalent aggregate queries into the same fingerprint", () => {
    const first = extractQueryPattern({
      cleanDatabaseId: "clean_db_1",
      sqlText: `
        SELECT o.region, SUM(o.amount) AS total_amount
        FROM clean_orders o
        WHERE o.country = 'SE'
        GROUP BY o.region
        ORDER BY o.region;
      `,
    });
    const second = extractQueryPattern({
      cleanDatabaseId: "clean_db_1",
      sqlText: `
        select region, sum(amount)
        from clean_orders
        where country = 'US'
        group by region
        order by region asc
        limit 200;
      `,
    });

    expect(first.patternSummary.patternFingerprint).toBe(
      second.patternSummary.patternFingerprint
    );
    expect(first.patternSummary.filters).toStrictEqual([
      "clean_orders.country = ?",
    ]);
    expect(first.patternSummary.groupBy).toStrictEqual(["clean_orders.region"]);
    expect(first.patternSummary.aggregates).toStrictEqual([
      "sum(clean_orders.amount)",
    ]);
  });

  it("separates patterns when the grouping grain or aggregate changes", () => {
    const byRegion = extractQueryPattern({
      cleanDatabaseId: "clean_db_1",
      sqlText: "SELECT region, SUM(amount) FROM clean_orders GROUP BY region;",
    });
    const byRegionAndProduct = extractQueryPattern({
      cleanDatabaseId: "clean_db_1",
      sqlText:
        "SELECT region, product, SUM(amount) FROM clean_orders GROUP BY region, product;",
    });
    const avgByRegion = extractQueryPattern({
      cleanDatabaseId: "clean_db_1",
      sqlText: "SELECT region, AVG(amount) FROM clean_orders GROUP BY region;",
    });

    expect(byRegion.patternSummary.patternFingerprint).not.toBe(
      byRegionAndProduct.patternSummary.patternFingerprint
    );
    expect(byRegion.patternSummary.patternFingerprint).not.toBe(
      avgByRegion.patternSummary.patternFingerprint
    );
  });

  it("marks unsupported query shapes as ineligible", () => {
    const pattern = extractQueryPattern({
      cleanDatabaseId: "clean_db_1",
      sqlText: `
        SELECT region, SUM(amount)
        FROM clean_orders
        WHERE country = 'SE' OR country = 'US'
        GROUP BY region;
      `,
    });

    expect(pattern.patternSummary.optimizationEligible).toBe(false);
  });
});

describe("detectUsedOptimizationObjects", () => {
  it("records referenced optimized objects from hints", () => {
    expect(
      detectUsedOptimizationObjects({
        optimizationHints: [
          {
            guidance: "Prefer the helper table.",
            preferredObjects: ["agg_orders_by_region", "agg_unused"],
            queryClusterId: "query_cluster_1",
            title: "By region",
          },
        ],
        sqlText: "SELECT region, total_amount FROM agg_orders_by_region;",
      })
    ).toStrictEqual(["agg_orders_by_region"]);
  });
});
