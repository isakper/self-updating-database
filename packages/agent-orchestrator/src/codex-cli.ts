import { existsSync } from "node:fs";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import type {
  CodexRunEvent,
  CodexAnalysisArtifact,
  SourceSheetSummary,
} from "../../shared/src/index.js";
import {
  readRequiredArtifact,
  runCodexCommand,
} from "./codex-command-runner.js";

export interface GeneratePipelineArtifactsOptions {
  onRunEvent?: (runEvent: Pick<CodexRunEvent, "message" | "stream">) => void;
  sourceDatabasePath: string;
  sourceDatasetId: string;
  sourceProfile: SourceDatasetProfile;
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

export interface SourceDatasetProfile {
  sheetProfiles: SourceSheetProfile[];
  sourceDatasetId: string;
  totalRowCount: number;
  workbookName: string;
}

export interface SourceSheetProfile {
  columnProfiles: Array<{
    columnName: string;
    inferredType:
      | "boolean"
      | "date-like"
      | "empty"
      | "mixed"
      | "number"
      | "string";
    nonNullCount: number;
    nullCount: number;
    sampleValues: string[];
  }>;
  rowCount: number;
  sampleRows: Array<Record<string, boolean | number | string | null>>;
  sheetName: string;
  sourceTableName: string;
}

export function createCodexCliPipelineGenerator(
  options: CodexCliPipelineGeneratorOptions = {}
): CodexPipelineGenerator {
  const codexCommand =
    options.codexCommand ??
    process.env.CODEX_COMMAND ??
    resolveDefaultCodexCommand();
  const artifactPollIntervalMs = options.artifactPollIntervalMs ?? 200;
  const commandTimeoutMs = options.commandTimeoutMs ?? 120_000;
  const playwrightMcpStartupTimeoutSec =
    options.playwrightMcpStartupTimeoutSec ?? 10;
  const processExitGracePeriodMs = options.processExitGracePeriodMs ?? 1_000;

  return {
    async generatePipelineArtifacts(input) {
      const workspacePath = await mkdtemp(join(tmpdir(), "codex-pipeline-"));
      const prompt = buildCodexPipelinePrompt(input);
      const promptPath = join(workspacePath, "prompt.md");
      const lastMessagePath = join(workspacePath, "codex-last-message.md");
      const sourceProfilePath = join(workspacePath, "source-profile.json");
      const samplesDirectoryPath = join(workspacePath, "samples");

      await mkdir(dirname(lastMessagePath), { recursive: true });
      await mkdir(samplesDirectoryPath, { recursive: true });
      await writeFile(promptPath, prompt, "utf8");
      await writeFile(
        sourceProfilePath,
        JSON.stringify(input.sourceProfile, null, 2),
        "utf8"
      );
      await Promise.all(
        input.sourceProfile.sheetProfiles.map((sheetProfile) =>
          writeFile(
            join(samplesDirectoryPath, `${sheetProfile.sourceTableName}.json`),
            JSON.stringify(sheetProfile.sampleRows, null, 2),
            "utf8"
          )
        )
      );

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

function resolveDefaultCodexCommand(): string {
  const installedCodexPath = "/Applications/Codex.app/Contents/Resources/codex";

  if (existsSync(installedCodexPath)) {
    return installedCodexPath;
  }

  return "codex";
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
  const sampleFileList = options.sourceProfile.sheetProfiles
    .map(
      (sheetProfile) =>
        `- samples/${sheetProfile.sourceTableName}.json (${sheetProfile.sampleRows.length} sampled rows from ${sheetProfile.sheetName})`
    )
    .join("\n");

  return `You are generating a SQL-only cleaning pipeline for a self-updating database prototype.

Dataset:
- source dataset id: ${options.sourceDatasetId}
- workbook name: ${options.workbookName}
- total imported rows: ${options.sourceProfile.totalRowCount}

Source tables for this dataset:
${sheetSummaries}

Primary inspection artifacts in the current workspace:
- source-profile.json
${sampleFileList}

Use the profile and sampled-row artifacts as your primary inspection input. They are intentionally bounded so the pipeline can still be generated reliably for larger datasets.

Identify:
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
- optimize for a clean-database schema that is easy for an LLM to understand and query
- prefer a small number of clear, well-named tables over many narrow or redundant tables
- do not add new tables or columns unless they meaningfully simplify likely queries
- avoid schema changes that make the database more fragmented or harder for an LLM to navigate

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
  - CREATE INDEX ... ON clean-database objects
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
  - indexes on source.*
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
