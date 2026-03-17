import { describe, expect, it } from "vitest";

import type { NaturalLanguageQueryRequest } from "./contracts.js";

describe("query contracts", () => {
  it("defines the initial natural language query request shape", () => {
    const request: NaturalLanguageQueryRequest = {
      prompt: "Show total revenue by region",
      sourceDatasetId: "dataset_123",
    };

    expect(request).toStrictEqual({
      prompt: "Show total revenue by region",
      sourceDatasetId: "dataset_123",
    });
  });
});
