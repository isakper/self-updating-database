import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const DEFAULT_ENV_FILES = [".env", ".env.local"] as const;

export function loadLocalEnvironment(
  envFileNames: readonly string[] = DEFAULT_ENV_FILES
): void {
  envFileNames.forEach((envFileName) => {
    const envFilePath = resolve(envFileName);

    if (!existsSync(envFilePath)) {
      return;
    }

    const fileContents = readFileSync(envFilePath, "utf8");
    const parsedValues = parseEnvFile(fileContents);

    Object.entries(parsedValues).forEach(([key, value]) => {
      if (process.env[key] === undefined) {
        process.env[key] = value;
      }
    });
  });
}

export function parseEnvFile(fileContents: string): Record<string, string> {
  const result: Record<string, string> = {};

  fileContents.split(/\r?\n/u).forEach((line) => {
    const trimmedLine = line.trim();

    if (!trimmedLine || trimmedLine.startsWith("#")) {
      return;
    }

    const separatorIndex = trimmedLine.indexOf("=");

    if (separatorIndex <= 0) {
      return;
    }

    const key = trimmedLine.slice(0, separatorIndex).trim();
    const rawValue = trimmedLine.slice(separatorIndex + 1).trim();

    if (!key) {
      return;
    }

    result[key] = stripMatchingQuotes(rawValue);
  });

  return result;
}

function stripMatchingQuotes(value: string): string {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  return value;
}
