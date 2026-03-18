import { describe, expect, it } from "vitest";

import { parseEnvFile } from "./load-env.js";

describe("parseEnvFile", () => {
  it("parses key-value lines and strips matching quotes", () => {
    expect(
      parseEnvFile(`
# comment
OPENAI_API_KEY="sk-test"
OPENAI_QUERY_MODEL='gpt-5-mini'
API_PORT=3401
INVALID_LINE
`)
    ).toStrictEqual({
      API_PORT: "3401",
      OPENAI_API_KEY: "sk-test",
      OPENAI_QUERY_MODEL: "gpt-5-mini",
    });
  });
});
