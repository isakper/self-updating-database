import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import {
  readRequiredArtifact,
  runCodexCommand,
} from "./codex-command-runner.js";

export interface GenerateQueryArtifactsOptions {
  cleanDatabaseId: string;
  cleanDatabasePath: string;
  onRunEvent?: (runEvent: {
    message: string;
    stream: "stderr" | "stdout";
  }) => void;
  prompt: string;
  sourceDatasetId: string;
}

export interface GeneratedQueryArtifacts {
  prompt: string;
  sqlText: string;
  summaryMarkdown: string;
  workspacePath: string;
}

export interface CodexQueryGenerator {
  generateQueryArtifacts(
    options: GenerateQueryArtifactsOptions
  ): Promise<GeneratedQueryArtifacts>;
}

export interface CodexCliQueryGeneratorOptions {
  artifactPollIntervalMs?: number;
  codexCommand?: string;
  commandTimeoutMs?: number;
  model?: string;
  playwrightMcpStartupTimeoutSec?: number;
  processExitGracePeriodMs?: number;
}

export function createCodexCliQueryGenerator(
  options: CodexCliQueryGeneratorOptions = {}
): CodexQueryGenerator {
  const codexCommand = options.codexCommand ?? "codex";
  const artifactPollIntervalMs = options.artifactPollIntervalMs ?? 200;
  const commandTimeoutMs = options.commandTimeoutMs ?? 120_000;
  const playwrightMcpStartupTimeoutSec =
    options.playwrightMcpStartupTimeoutSec ?? 1;
  const processExitGracePeriodMs = options.processExitGracePeriodMs ?? 1_000;

  return {
    async generateQueryArtifacts(input) {
      const workspacePath = await mkdtemp(join(tmpdir(), "codex-query-"));
      const codexPrompt = buildCodexQueryPrompt(input);
      const promptPath = join(workspacePath, "prompt.md");
      const lastMessagePath = join(workspacePath, "codex-last-message.md");

      await mkdir(dirname(lastMessagePath), { recursive: true });
      await writeFile(promptPath, codexPrompt, "utf8");

      try {
        await runCodexCommand(
          codexCommand,
          [
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "workspace-write",
            "-c",
            `mcp_servers.playwright.startup_timeout_sec=${playwrightMcpStartupTimeoutSec}`,
            "--add-dir",
            dirname(resolve(input.cleanDatabasePath)),
            "--cd",
            workspacePath,
            "--output-last-message",
            lastMessagePath,
            ...(options.model ? ["--model", options.model] : []),
            "-",
          ],
          codexPrompt,
          workspacePath,
          [join(workspacePath, "query.sql"), join(workspacePath, "summary.md")],
          {
            artifactPollIntervalMs,
            commandTimeoutMs,
            ...(input.onRunEvent
              ? {
                  onStderrChunk: (chunk: string) => {
                    input.onRunEvent?.({
                      message: chunk,
                      stream: "stderr",
                    });
                  },
                  onStdoutChunk: (chunk: string) => {
                    input.onRunEvent?.({
                      message: chunk,
                      stream: "stdout",
                    });
                  },
                }
              : {}),
            processExitGracePeriodMs,
          }
        );

        return {
          prompt: codexPrompt,
          sqlText: await readRequiredArtifact(join(workspacePath, "query.sql")),
          summaryMarkdown: await readRequiredArtifact(
            join(workspacePath, "summary.md")
          ),
          workspacePath,
        };
      } catch (error) {
        await rm(workspacePath, { force: true, recursive: true });
        throw error;
      }
    },
  };
}

export function buildCodexQueryPrompt(
  options: GenerateQueryArtifactsOptions
): string {
  return `You are generating one read-only SQL query for a clean SQLite analytics database.

Dataset:
- source dataset id: ${options.sourceDatasetId}
- clean database id: ${options.cleanDatabaseId}
- clean sqlite database path: ${resolve(options.cleanDatabasePath)}

User question:
${options.prompt}

Inspect the clean database directly and generate the best SQL answer to the user's question.

Output contract:
1. Write query.sql in the current working directory.
2. Write summary.md in the current working directory.

SQL contract for query.sql:
- exactly one SQL statement
- must start with SELECT or WITH
- read-only only
- forbidden:
  - UPDATE
  - DELETE
  - INSERT
  - ALTER
  - CREATE
  - DROP
  - ATTACH
  - DETACH
  - PRAGMA
- use explicit aliases when it improves readability
- prefer a LIMIT 200 for detail-row queries unless the user clearly asks for all rows

summary.md contract:
- short human summary of how the SQL answers the question
- mention key tables or columns used

Important:
- answer only from the clean database
- do not invent columns or tables
- produce SQL that can run directly in SQLite`;
}
