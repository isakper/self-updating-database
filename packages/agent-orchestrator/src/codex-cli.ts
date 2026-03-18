import {
  mkdtemp,
  mkdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";

import type {
  CodexAnalysisArtifact,
  SourceSheetSummary,
} from "../../shared/src/index.js";

export interface GeneratePipelineArtifactsOptions {
  sourceDatabasePath: string;
  sourceDatasetId: string;
  sourceSheets: SourceSheetSummary[];
  workbookName: string;
}

export interface GeneratedPipelineArtifacts {
  analysisJson: CodexAnalysisArtifact;
  prompt: string;
  sqlText: string;
  summaryMarkdown: string;
  workspacePath: string;
}

export interface CodexPipelineGenerator {
  generatePipelineArtifacts(
    options: GeneratePipelineArtifactsOptions
  ): Promise<GeneratedPipelineArtifacts>;
}

export interface CodexCliPipelineGeneratorOptions {
  artifactPollIntervalMs?: number;
  codexCommand?: string;
  commandTimeoutMs?: number;
  model?: string;
  playwrightMcpStartupTimeoutSec?: number;
  processExitGracePeriodMs?: number;
}

export function createCodexCliPipelineGenerator(
  options: CodexCliPipelineGeneratorOptions = {}
): CodexPipelineGenerator {
  const codexCommand = options.codexCommand ?? "codex";
  const artifactPollIntervalMs = options.artifactPollIntervalMs ?? 200;
  const commandTimeoutMs = options.commandTimeoutMs ?? 120_000;
  const playwrightMcpStartupTimeoutSec =
    options.playwrightMcpStartupTimeoutSec ?? 1;
  const processExitGracePeriodMs = options.processExitGracePeriodMs ?? 1_000;

  return {
    async generatePipelineArtifacts(input) {
      const workspacePath = await mkdtemp(join(tmpdir(), "codex-pipeline-"));
      const prompt = buildCodexPipelinePrompt(input);
      const promptPath = join(workspacePath, "prompt.md");
      const lastMessagePath = join(workspacePath, "codex-last-message.md");

      await mkdir(dirname(lastMessagePath), { recursive: true });
      await writeFile(promptPath, prompt, "utf8");

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
            dirname(resolve(input.sourceDatabasePath)),
            "--cd",
            workspacePath,
            "--output-last-message",
            lastMessagePath,
            ...(options.model ? ["--model", options.model] : []),
            "-",
          ],
          prompt,
          workspacePath,
          [
            join(workspacePath, "pipeline.sql"),
            join(workspacePath, "analysis.json"),
            join(workspacePath, "summary.md"),
          ],
          {
            artifactPollIntervalMs,
            commandTimeoutMs,
            processExitGracePeriodMs,
          }
        );

        const sqlText = await readRequiredArtifact(
          join(workspacePath, "pipeline.sql")
        );
        const summaryMarkdown = await readRequiredArtifact(
          join(workspacePath, "summary.md")
        );
        const analysisJsonText = await readRequiredArtifact(
          join(workspacePath, "analysis.json")
        );
        const analysisJson = parseAnalysisArtifact(analysisJsonText);

        return {
          analysisJson,
          prompt,
          sqlText,
          summaryMarkdown,
          workspacePath,
        };
      } catch (error) {
        await rm(workspacePath, { force: true, recursive: true });
        throw error;
      }
    },
  };
}

async function runCodexCommand(
  codexCommand: string,
  args: string[],
  prompt: string,
  cwd: string,
  requiredArtifacts: string[],
  options: {
    artifactPollIntervalMs: number;
    commandTimeoutMs: number;
    processExitGracePeriodMs: number;
  }
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(codexCommand, args, {
      cwd,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let settled = false;
    let stderr = "";
    let stdout = "";

    const artifactPoll = setInterval(() => {
      void checkArtifacts();
    }, options.artifactPollIntervalMs);
    const commandTimeout = setTimeout(() => {
      finishWithError(
        new Error(
          [
            `Codex CLI timed out after ${options.commandTimeoutMs}ms.`,
            stderr && `stderr: ${stderr.trim()}`,
            stdout && `stdout: ${stdout.trim()}`,
          ]
            .filter(Boolean)
            .join(" ")
        )
      );
    }, options.commandTimeoutMs);

    function cleanup(): void {
      clearInterval(artifactPoll);
      clearTimeout(commandTimeout);
    }

    function finishWithSuccess(): void {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      void stopChildProcess(child, options.processExitGracePeriodMs);
      resolve();
    }

    function finishWithError(error: Error): void {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      void stopChildProcess(child, options.processExitGracePeriodMs);
      reject(error);
    }

    async function checkArtifacts(): Promise<void> {
      if (settled) {
        return;
      }

      if (await areArtifactsReady(requiredArtifacts)) {
        finishWithSuccess();
      }
    }

    child.stderr.on("data", (chunk: Buffer | string) => {
      stderr += chunk.toString();
    });

    child.stdout.on("data", (chunk: Buffer | string) => {
      stdout += chunk.toString();
    });

    child.on("error", (error) => {
      finishWithError(error);
    });

    child.on("close", (code) => {
      if (settled) {
        return;
      }

      if (code === 0) {
        void checkArtifacts().then(() => {
          if (!settled) {
            finishWithError(
              new Error(
                "Codex CLI exited successfully without writing the required artifacts."
              )
            );
          }
        });
        return;
      }

      finishWithError(
        new Error(stderr || `Codex CLI exited with code ${code ?? "unknown"}.`)
      );
    });

    child.stdin.write(prompt);
    child.stdin.end();
    void checkArtifacts();
  });
}

async function areArtifactsReady(filePaths: string[]): Promise<boolean> {
  const statuses = await Promise.all(
    filePaths.map(async (filePath) => {
      try {
        const fileStat = await stat(filePath);
        return fileStat.isFile() && fileStat.size > 0;
      } catch {
        return false;
      }
    })
  );

  return statuses.every(Boolean);
}

async function stopChildProcess(
  child: ChildProcessWithoutNullStreams,
  processExitGracePeriodMs: number
): Promise<void> {
  if (child.killed || child.exitCode !== null) {
    return;
  }

  child.kill("SIGTERM");

  await new Promise<void>((resolve) => {
    const forceKillTimeout = setTimeout(() => {
      if (!child.killed && child.exitCode === null) {
        child.kill("SIGKILL");
      }

      resolve();
    }, processExitGracePeriodMs);

    child.once("close", () => {
      clearTimeout(forceKillTimeout);
      resolve();
    });
  });
}

export function buildCodexPipelinePrompt(
  options: GeneratePipelineArtifactsOptions
): string {
  const sheetSummaries = options.sourceSheets
    .map(
      (sheet) =>
        `- ${sheet.sheetName} -> table ${sheet.sourceTableName} (${sheet.rowCount} rows, columns: ${sheet.columnNames.join(", ")})`
    )
    .join("\n");

  return `You are generating a SQL-only cleaning pipeline for a self-updating database prototype.

Dataset:
- source dataset id: ${options.sourceDatasetId}
- workbook name: ${options.workbookName}
- source sqlite database path: ${resolve(options.sourceDatabasePath)}

Source tables for this dataset:
${sheetSummaries}

Inspect the source database directly and identify:
- bad or inconsistent column names
- misspelled or inconsistent categorical values
- mixed data types
- inconsistent date formats
- whitespace and casing cleanup opportunities
- other obvious improvements that make querying easier

Cleaning scope:
- balanced and conservative enough to avoid destructive semantic rewrites
- allow high-confidence spelling/value normalization when justified by observed data
- do not invent new business meaning
- do not drop rows unless absolutely required and explicitly justified

Output contract:
1. Write pipeline.sql in the current working directory.
2. Write analysis.json in the current working directory.
3. Write summary.md in the current working directory.

SQL contract for pipeline.sql:
- The runtime will ATTACH the source database as schema "source".
- Read only from source tables.
- Write only to the main clean database.
- Allowed statements:
  - CREATE TABLE ... AS SELECT ...
  - CREATE VIEW ... AS SELECT ...
  - INSERT INTO ... SELECT ...
  - DROP TABLE IF EXISTS ...
  - DROP VIEW IF EXISTS ...
  - WITH ... SELECT ...
- Forbidden statements:
  - UPDATE
  - DELETE
  - ALTER
  - ATTACH
  - DETACH
  - PRAGMA
  - writes to source.*
  - filesystem or shell side effects

analysis.json contract:
- valid JSON object
- fields:
  - sourceDatasetId: string
  - summary: string
  - findings: array of objects with:
    - kind: string
    - target: string
    - message: string
    - proposedFix: string
    - confidence: "low" | "medium" | "high"

summary.md contract:
- short human summary of what the cleaning pipeline does
- mention the highest-impact fixes

Important:
- use only the dataset tables listed above
- generate SQL that can be rerun from scratch
- prefer stable table names in the clean database
- ensure source data remains immutable`;
}

async function readRequiredArtifact(filePath: string): Promise<string> {
  const fileStat = await stat(filePath);

  if (!fileStat.isFile() || fileStat.size === 0) {
    throw new Error(
      `Expected Codex to write a non-empty artifact at ${filePath}.`
    );
  }

  return await readFile(filePath, "utf8");
}

function parseAnalysisArtifact(rawJson: string): CodexAnalysisArtifact {
  const candidate = JSON.parse(rawJson) as unknown;

  if (!candidate || typeof candidate !== "object") {
    throw new Error("analysis.json must be a JSON object.");
  }

  const analysis = candidate as Record<string, unknown>;

  if (
    typeof analysis.sourceDatasetId !== "string" ||
    typeof analysis.summary !== "string" ||
    !Array.isArray(analysis.findings)
  ) {
    throw new Error("analysis.json does not match the required contract.");
  }

  return {
    findings: analysis.findings.map((finding) => {
      if (!finding || typeof finding !== "object") {
        throw new Error("analysis.json contains an invalid finding.");
      }

      const record = finding as Record<string, unknown>;

      if (
        typeof record.kind !== "string" ||
        typeof record.target !== "string" ||
        typeof record.message !== "string" ||
        typeof record.proposedFix !== "string" ||
        (record.confidence !== "low" &&
          record.confidence !== "medium" &&
          record.confidence !== "high")
      ) {
        throw new Error("analysis.json contains a malformed finding.");
      }

      return {
        confidence: record.confidence,
        kind: record.kind,
        message: record.message,
        proposedFix: record.proposedFix,
        target: record.target,
      };
    }),
    sourceDatasetId: analysis.sourceDatasetId,
    summary: analysis.summary,
  };
}
