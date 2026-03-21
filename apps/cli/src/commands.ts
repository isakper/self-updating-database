export type CliCommand =
  | { kind: "help" }
  | { kind: "dataset_list" }
  | { datasetId: string; kind: "dataset_show" }
  | { filePath: string; kind: "upload_workbook" }
  | { datasetId: string; filePath: string; kind: "upload_query_logs" }
  | { datasetId: string; kind: "pipeline_run" }
  | { datasetId: string; kind: "optimization_run" }
  | { datasetId: string; kind: "optimization_retry_latest_failed" }
  | {
      datasetId: string;
      intervalMs: number;
      kind: "status";
      watch: boolean;
    }
  | { datasetId: string; kind: "events" }
  | { datasetId: string; kind: "query"; prompt: string };

export interface CliOptions {
  apiBaseUrl: string;
}

export interface ParseResult {
  command?: CliCommand;
  error?: string;
  options: CliOptions;
}

const DEFAULT_API_BASE_URL = "http://127.0.0.1:3001";
const DEFAULT_STATUS_INTERVAL_MS = 2000;

export function parseCliArgs(argv: string[]): ParseResult {
  const options: CliOptions = {
    apiBaseUrl: process.env.API_BASE_URL ?? DEFAULT_API_BASE_URL,
  };

  const { args, error, intervalMs, watch } = parseGlobalFlags(argv, options);

  if (error) {
    return {
      error,
      options,
    };
  }

  if (args.length === 0 || args[0] === "help" || args[0] === "--help") {
    return {
      command: { kind: "help" },
      options,
    };
  }

  const [group, action, ...rest] = args;

  if (group === "dataset" && action === "list" && rest.length === 0) {
    return {
      command: { kind: "dataset_list" },
      options,
    };
  }

  if (group === "dataset" && action === "show" && rest.length === 1) {
    const datasetId = rest[0];

    if (!datasetId) {
      return {
        error: "Dataset id is required.",
        options,
      };
    }

    return {
      command: {
        datasetId,
        kind: "dataset_show",
      },
      options,
    };
  }

  if (group === "upload" && action === "workbook" && rest.length === 1) {
    const filePath = rest[0];

    if (!filePath) {
      return {
        error: "Workbook path is required.",
        options,
      };
    }

    return {
      command: {
        filePath,
        kind: "upload_workbook",
      },
      options,
    };
  }

  if (group === "upload" && action === "query-logs" && rest.length === 2) {
    const datasetId = rest[0];
    const filePath = rest[1];

    if (!datasetId || !filePath) {
      return {
        error: "Dataset id and workbook path are required.",
        options,
      };
    }

    return {
      command: {
        datasetId,
        filePath,
        kind: "upload_query_logs",
      },
      options,
    };
  }

  if (group === "pipeline" && action === "run" && rest.length === 1) {
    const datasetId = rest[0];

    if (!datasetId) {
      return {
        error: "Dataset id is required.",
        options,
      };
    }

    return {
      command: {
        datasetId,
        kind: "pipeline_run",
      },
      options,
    };
  }

  if (group === "optimization" && action === "run" && rest.length === 1) {
    const datasetId = rest[0];

    if (!datasetId) {
      return {
        error: "Dataset id is required.",
        options,
      };
    }

    return {
      command: {
        datasetId,
        kind: "optimization_run",
      },
      options,
    };
  }

  if (
    group === "optimization" &&
    action === "retry-latest-failed" &&
    rest.length === 1
  ) {
    const datasetId = rest[0];

    if (!datasetId) {
      return {
        error: "Dataset id is required.",
        options,
      };
    }

    return {
      command: {
        datasetId,
        kind: "optimization_retry_latest_failed",
      },
      options,
    };
  }

  if (group === "status" && action && rest.length === 0) {
    return {
      command: {
        datasetId: action,
        intervalMs,
        kind: "status",
        watch,
      },
      options,
    };
  }

  if (group === "events" && action && rest.length === 0) {
    return {
      command: {
        datasetId: action,
        kind: "events",
      },
      options,
    };
  }

  if (group === "query" && action) {
    const datasetId = action;
    const prompt = rest.join(" ").trim();

    if (prompt.length === 0) {
      return {
        error: "Query prompt is required.",
        options,
      };
    }

    return {
      command: {
        datasetId,
        kind: "query",
        prompt,
      },
      options,
    };
  }

  return {
    error: `Unknown command: ${args.join(" ")}`,
    options,
  };
}

function parseGlobalFlags(
  argv: string[],
  options: CliOptions
): {
  args: string[];
  error?: string;
  intervalMs: number;
  watch: boolean;
} {
  const args: string[] = [];
  let watch = false;
  let intervalMs = DEFAULT_STATUS_INTERVAL_MS;

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (!arg) {
      continue;
    }

    if (arg === "--watch") {
      watch = true;
      continue;
    }

    if (arg === "--api-base-url") {
      const value = argv[index + 1];

      if (!value) {
        return {
          args,
          error: "--api-base-url requires a value.",
          intervalMs,
          watch,
        };
      }

      options.apiBaseUrl = value;
      index += 1;
      continue;
    }

    if (arg.startsWith("--interval-ms=")) {
      const value = arg.slice("--interval-ms=".length);
      const parsed = Number(value);

      if (!Number.isInteger(parsed) || parsed <= 0) {
        return {
          args,
          error: "--interval-ms must be a positive integer.",
          intervalMs,
          watch,
        };
      }

      intervalMs = parsed;
      continue;
    }

    if (arg === "--interval-ms") {
      const value = argv[index + 1];

      if (!value) {
        return {
          args,
          error: "--interval-ms requires a value.",
          intervalMs,
          watch,
        };
      }

      const parsed = Number(value);

      if (!Number.isInteger(parsed) || parsed <= 0) {
        return {
          args,
          error: "--interval-ms must be a positive integer.",
          intervalMs,
          watch,
        };
      }

      intervalMs = parsed;
      index += 1;
      continue;
    }

    args.push(arg);
  }

  return {
    args,
    intervalMs,
    watch,
  };
}

export function renderUsage(): string {
  return [
    "CLI-first workflow for self-updating-database",
    "",
    "Usage:",
    "  pnpm cli [--api-base-url <url>] <command>",
    "",
    "Commands:",
    "  dataset list",
    "  dataset show <datasetId>",
    "  upload workbook <workbook.xlsx>",
    "  upload query-logs <datasetId> <query-logs.xlsx>",
    "  pipeline run <datasetId>",
    "  optimization run <datasetId>",
    "  optimization retry-latest-failed <datasetId>",
    "  status <datasetId> [--watch] [--interval-ms <n>]",
    "  events <datasetId>",
    "  query <datasetId> <prompt>",
  ].join("\n");
}
