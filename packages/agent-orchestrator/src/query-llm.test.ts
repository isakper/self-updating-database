import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import initSqlJs from "sql.js";
import { describe, expect, it } from "vitest";

import {
  buildSqlGenerationPrompt,
  createOpenAiSqlQueryGenerator,
} from "./query-llm.js";

describe("buildSqlGenerationPrompt", () => {
  it("describes the clean schema and asks for SQL only", () => {
    const prompt = buildSqlGenerationPrompt(
      {
        cleanDatabaseId: "clean_db_1",
        cleanDatabasePath: ".data/clean.sqlite",
        columnDescriptions: [
          {
            columnName: "units_signed",
            description:
              "Net units where returns are represented as negative quantities.",
            tableName: "daily_sales",
          },
        ],
        optimizationHints: [
          {
            guidance: "Prefer agg_orders_by_region.",
            preferredObjects: ["agg_orders_by_region"],
            queryClusterId: "query_cluster_1",
            title: "Regional totals",
          },
        ],
        prompt: "Show total revenue by region",
        sourceDatasetId: "dataset_1",
      },
      {
        schemaDescription:
          "TABLE orders\nCREATE TABLE orders(order_id, amount, region)\nSample rows: []",
      }
    );

    expect(prompt).toContain("clean database id: clean_db_1");
    expect(prompt).toContain("User question:");
    expect(prompt).toContain("Return only SQL.");
    expect(prompt).toContain("TABLE orders");
    expect(prompt).toContain("Optimization hints:");
    expect(prompt).toContain("agg_orders_by_region");
    expect(prompt).toContain("Column descriptions:");
    expect(prompt).toContain("daily_sales.units_signed");
    expect(prompt).toContain("must start with SELECT or WITH");
    expect(prompt).toContain("not a SQL schema name");
    expect(prompt).toContain(
      "For gross revenue or gross sales questions, treat gross as pre-return sales"
    );
    expect(prompt).toContain(
      "For net revenue or net sales questions, include return impact"
    );
  });

  it("streams SQL deltas from the responses API", async () => {
    const chunks: string[] = [];
    const cleanDatabasePath = await createCleanDatabaseFixture();
    const generator = createOpenAiSqlQueryGenerator({
      apiKey: "test-key",
      fetchImplementation: (_input, init) => {
        if (typeof init?.body !== "string") {
          throw new Error("Expected request body to be a JSON string.");
        }

        const payload = JSON.parse(init.body) as { stream?: boolean };
        expect(payload.stream).toBe(true);

        return Promise.resolve(
          new Response(
            new ReadableStream({
              start(controller) {
                controller.enqueue(
                  new TextEncoder().encode(
                    [
                      "event: response.output_text.delta",
                      'data: {"type":"response.output_text.delta","delta":"SELECT order_id"}',
                      "",
                      "event: response.output_text.delta",
                      'data: {"type":"response.output_text.delta","delta":" FROM orders LIMIT 1;"}',
                      "",
                      "event: response.completed",
                      'data: {"type":"response.completed","response":{"output_text":"SELECT order_id FROM orders LIMIT 1;"}}',
                      "",
                      "data: [DONE]",
                      "",
                    ].join("\n")
                  )
                );
                controller.close();
              },
            }),
            {
              headers: {
                "content-type": "text/event-stream; charset=utf-8",
              },
              status: 200,
            }
          )
        );
      },
    });

    const result = await generator.generateSql({
      cleanDatabaseId: "clean_db_1",
      cleanDatabasePath,
      onDelta(chunk) {
        chunks.push(chunk);
      },
      prompt: "Fetch first row",
      sourceDatasetId: "dataset_1",
    });

    expect(chunks).toStrictEqual(["SELECT order_id", " FROM orders LIMIT 1;"]);
    expect(result.sqlText).toBe("SELECT order_id FROM orders LIMIT 1;");
  });
});

async function createCleanDatabaseFixture(): Promise<string> {
  const SQL = await initSqlJs();
  const database = new SQL.Database();
  database.run(
    "CREATE TABLE orders(order_id TEXT, amount NUMERIC, region TEXT);"
  );
  database.run(
    "INSERT INTO orders(order_id, amount, region) VALUES ('A-1', 25, 'North');"
  );

  const directoryPath = await mkdtemp(join(tmpdir(), "query-llm-test-"));
  const databasePath = join(directoryPath, "clean.sqlite");
  await writeFile(databasePath, Buffer.from(database.export()));
  database.close();

  return databasePath;
}
