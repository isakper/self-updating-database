import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import type {
  CodexRunEvent,
  OptimizationCandidateSet,
  OptimizationHint,
  OptimizationRevisionDecision,
} from "../../shared/src/index.js";
import { parseAnalysisArtifact } from "./codex-cli.js";
import { inspectCleanDatabase } from "./query-llm.js";
import {
  type RequiredArtifact,
  readRequiredArtifact,
  runCodexCommand,
} from "./codex-command-runner.js";

export interface GenerateOptimizationArtifactsOptions {
  candidateSet: OptimizationCandidateSet;
  cleanDatabasePath: string;
  currentPipelineSql: string;
  onRunEvent?: (runEvent: Pick<CodexRunEvent, "message" | "stream">) => void;
  sourceDatasetId: string;
}

export interface GeneratedOptimizationArtifacts {
  analysisJson: Record<string, unknown>;
  decision: OptimizationRevisionDecision;
  optimizationHints: OptimizationHint[];
  prompt: string;
  sqlText: string;
  summaryMarkdown: string;
  workspacePath: string;
}

export interface CodexOptimizationGenerator {
  generateOptimizationArtifacts(
    options: GenerateOptimizationArtifactsOptions
  ): Promise<GeneratedOptimizationArtifacts>;
}

export interface CodexCliOptimizationGeneratorOptions {
  artifactPollIntervalMs?: number;
  codexCommand?: string;
  commandTimeoutMs?: number;
  model?: string;
  playwrightMcpStartupTimeoutSec?: number;
  processExitGracePeriodMs?: number;
  retainWorkspaceOnSuccess?: boolean;
}

export function createCodexCliOptimizationGenerator(
  options: CodexCliOptimizationGeneratorOptions = {}
): CodexOptimizationGenerator {
  const codexCommand = options.codexCommand ?? "codex";
  const artifactPollIntervalMs = options.artifactPollIntervalMs ?? 200;
  const commandTimeoutMs = options.commandTimeoutMs ?? 120_000;
  const playwrightMcpStartupTimeoutSec =
    options.playwrightMcpStartupTimeoutSec ?? 10;
  const processExitGracePeriodMs = options.processExitGracePeriodMs ?? 1_000;
  const retainWorkspaceOnSuccess = options.retainWorkspaceOnSuccess ?? false;

  return {
    async generateOptimizationArtifacts(input) {
      const workspacePath = await mkdtemp(
        join(tmpdir(), "codex-optimization-")
      );
      const schemaContext = await inspectCleanDatabase(input.cleanDatabasePath);
      const prompt = buildCodexOptimizationPrompt({
        candidateSet: input.candidateSet,
        cleanDatabaseSchemaDescription: schemaContext.schemaDescription,
        currentPipelineSql: input.currentPipelineSql,
        sourceDatasetId: input.sourceDatasetId,
      });

      await mkdir(dirname(join(workspacePath, "prompt.md")), {
        recursive: true,
      });
      await Promise.all([
        writeFile(join(workspacePath, "prompt.md"), prompt, "utf8"),
        writeFile(
          join(workspacePath, "candidate-set.json"),
          JSON.stringify(input.candidateSet, null, 2),
          "utf8"
        ),
        writeFile(
          join(workspacePath, "current-pipeline.sql"),
          input.currentPipelineSql,
          "utf8"
        ),
      ]);

      try {
        const requiredArtifacts: RequiredArtifact[] = [
          {
            filePath: join(workspacePath, "decision.json"),
            validateContents(contents) {
              parseDecisionArtifact(contents);
            },
          },
          {
            filePath: join(workspacePath, "pipeline.sql"),
          },
          {
            filePath: join(workspacePath, "analysis.json"),
            validateContents(contents) {
              const analysis = parseAnalysisArtifact(contents);
              if (analysis.sourceDatasetId !== input.sourceDatasetId) {
                throw new Error(
                  `analysis.json sourceDatasetId mismatch: expected ${input.sourceDatasetId}, received ${analysis.sourceDatasetId}.`
                );
              }
            },
          },
          {
            filePath: join(workspacePath, "summary.md"),
            validateContents(contents) {
              validateMarkdownArtifact(contents, "summary.md");
            },
          },
        ];

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
            ...(options.model ? ["--model", options.model] : []),
            "-",
          ],
          prompt,
          workspacePath,
          requiredArtifacts,
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

        const decisionText = await readRequiredArtifact(
          join(workspacePath, "decision.json")
        );
        const pipelineSql = await readRequiredArtifact(
          join(workspacePath, "pipeline.sql")
        );
        const analysisJsonText = await readRequiredArtifact(
          join(workspacePath, "analysis.json")
        );
        const summaryMarkdown = await readRequiredArtifact(
          join(workspacePath, "summary.md")
        );
        const decision = parseDecisionArtifact(decisionText);
        const parsedAnalysis = parseAnalysisArtifact(analysisJsonText);
        if (parsedAnalysis.sourceDatasetId !== input.sourceDatasetId) {
          throw new Error(
            `analysis.json sourceDatasetId mismatch: expected ${input.sourceDatasetId}, received ${parsedAnalysis.sourceDatasetId}.`
          );
        }

        const result: GeneratedOptimizationArtifacts = {
          analysisJson: parsedAnalysis as unknown as Record<string, unknown>,
          decision: decision.decision,
          optimizationHints: decision.optimizationHints,
          prompt,
          sqlText: pipelineSql,
          summaryMarkdown,
          workspacePath,
        };

        if (!retainWorkspaceOnSuccess) {
          await rm(workspacePath, { force: true, recursive: true });
        }

        return result;
      } catch (error) {
        await rm(workspacePath, { force: true, recursive: true });
        throw error;
      }
    },
  };
}

function validateMarkdownArtifact(
  contents: string,
  artifactLabel: string
): void {
  if (contents.trim().length === 0) {
    throw new Error(`${artifactLabel} must be non-empty markdown text.`);
  }
}

export function buildCodexOptimizationPrompt(options: {
  candidateSet: OptimizationCandidateSet;
  cleanDatabaseSchemaDescription: string;
  currentPipelineSql: string;
  sourceDatasetId: string;
}): string {
  const clusterSummary = options.candidateSet.queryClusters
    .map(
      (cluster, index) =>
        `Candidate ${index + 1}
- query cluster id: ${cluster.queryClusterId}
- query count: ${cluster.queryCount}
- cumulative execution latency ms: ${cluster.cumulativeExecutionLatencyMs}
- average execution latency ms: ${cluster.averageExecutionLatencyMs}
- latest seen at: ${cluster.latestSeenAt}
- relations: ${cluster.patternSummary.relations.join(", ") || "(none)"}
- joins: ${cluster.patternSummary.joins.join(", ") || "(none)"}
- filters: ${cluster.patternSummary.filters.join(", ") || "(none)"}
- group by: ${cluster.patternSummary.groupBy.join(", ") || "(none)"}
- aggregates: ${cluster.patternSummary.aggregates.join(", ") || "(none)"}
- order by: ${cluster.patternSummary.orderBy.join(", ") || "(none)"}`
    )
    .join("\n\n");

  return `You are deciding whether to revise a SQL-only transformation pipeline for a self-updating database prototype.

Dataset:
- source dataset id: ${options.sourceDatasetId}
- base clean database id: ${options.candidateSet.baseCleanDatabaseId}
- base pipeline version id: ${options.candidateSet.basePipelineVersionId}

The workspace contains:
- candidate-set.json
- current-pipeline.sql

Current clean database schema:
${options.cleanDatabaseSchemaDescription}

Top repeated query groups for this optimization cycle:
${clusterSummary}

Current pipeline SQL:
${options.currentPipelineSql}

Your job:
- decide whether these top repeated query groups justify a structural change to the derived clean database
- you may decide no change is better
- if a change is worthwhile, you may reshape tables, add helper tables or views, precompute access paths, improve joins, or otherwise revise the derived clean-database design
- do not mutate the source database
- do not assume you must add only one aggregate table
- optimize for a clean-database schema that is easy for an LLM to understand and query
- prefer a smaller number of clear, well-named tables and columns over a more fragmented schema
- do not add new tables or new columns unless they materially simplify repeated queries or make the schema easier for an LLM to use
- if a proposed helper object adds complexity without clear query simplification, prefer no change

Output contract:
1. Write decision.json in the current working directory.
2. Write pipeline.sql in the current working directory.
3. Write analysis.json in the current working directory.
4. Write summary.md in the current working directory.

decision.json contract:
- valid JSON object
- fields:
  - decision: "no_change" | "pipeline_revision"
  - optimizationHints: array of objects with:
    - queryClusterId: string
    - title: string
    - guidance: string
    - preferredObjects: string[]

pipeline.sql contract:
- always write a complete pipeline
- if decision is "no_change", write the current pipeline unchanged
- if decision is "pipeline_revision", write the full revised pipeline

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
- explain why no change or change is appropriate
- include the candidate cluster ids you considered in the findings or summary

summary.md contract:
- short human summary of your decision
- mention the main reasoning

Important:
- preserve the source-data immutability rule
- keep the pipeline rerunnable from scratch
- the runtime allows CREATE INDEX statements on derived clean-database objects
- do not create indexes on source.* objects`;
}

function parseDecisionArtifact(rawJson: string): {
  decision: OptimizationRevisionDecision;
  optimizationHints: OptimizationHint[];
} {
  const candidate = JSON.parse(rawJson) as unknown;

  if (!candidate || typeof candidate !== "object") {
    throw new Error("decision.json must be a JSON object.");
  }

  const record = candidate as Record<string, unknown>;
  const decision = record.decision;
  const optimizationHints = record.optimizationHints;

  if (decision !== "no_change" && decision !== "pipeline_revision") {
    throw new Error("decision.json contains an invalid decision.");
  }

  if (!Array.isArray(optimizationHints)) {
    throw new Error("decision.json must contain optimizationHints.");
  }

  return {
    decision,
    optimizationHints: optimizationHints.map((hint) => {
      if (!hint || typeof hint !== "object") {
        throw new Error("decision.json contains an invalid optimization hint.");
      }

      const hintRecord = hint as Record<string, unknown>;

      if (
        typeof hintRecord.queryClusterId !== "string" ||
        typeof hintRecord.title !== "string" ||
        typeof hintRecord.guidance !== "string" ||
        !Array.isArray(hintRecord.preferredObjects) ||
        hintRecord.preferredObjects.some((value) => typeof value !== "string")
      ) {
        throw new Error(
          "decision.json contains a malformed optimization hint."
        );
      }

      return {
        guidance: hintRecord.guidance,
        preferredObjects: hintRecord.preferredObjects as string[],
        queryClusterId: hintRecord.queryClusterId,
        title: hintRecord.title,
      };
    }),
  };
}
