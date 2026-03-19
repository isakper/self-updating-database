import { mkdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import {
  CodexCommandError,
  type CodexPipelineGenerator,
} from "../../../../packages/agent-orchestrator/src/index.js";
import {
  createQueuedImportProcessingState,
  createWorkbookImportSummary,
  type IngestionRepository,
} from "../../../../packages/database-core/src/index.js";
import type {
  CleanDatabaseSummary,
  CodexRunFailureReasonCode,
  CodexRunEvent,
  PipelineRunRecord,
  PipelineVersionRecord,
} from "../../../../packages/shared/src/index.js";
import type {
  CleanDatabaseBuilder,
  PipelineSqlValidationResult,
} from "../../../../packages/pipeline-sdk/src/index.js";
import { buildSourceDatasetProfile } from "./source-profile.js";

export interface PipelineSqlValidator {
  validate(sqlText: string): PipelineSqlValidationResult;
}

export interface PipelineRetryScheduler {
  drain(): Promise<void>;
  resumePendingWork(): void;
  schedule(datasetId: string): void;
}

export interface CreatePipelineRetrySchedulerOptions {
  cleanDatabaseDirectoryPath: string;
  cleanDatabaseBuilder: CleanDatabaseBuilder;
  codexPipelineGenerator: CodexPipelineGenerator;
  createId?: (prefix: string) => string;
  now?: () => Date;
  onRunEvent?: (runEvent: CodexRunEvent) => void;
  repository: IngestionRepository;
  retryDelayMs?: number;
  sourceDatabasePath: string;
  sqlValidator: PipelineSqlValidator;
}

export function createPipelineRetryScheduler(
  options: CreatePipelineRetrySchedulerOptions
): PipelineRetryScheduler {
  const retryDelayMs = options.retryDelayMs ?? 25;
  const createId =
    options.createId ??
    ((prefix: string) =>
      `${prefix}_${Math.random().toString(36).slice(2, 10)}`);
  const now = options.now ?? (() => new Date());
  const inFlight = new Map<string, Promise<void>>();
  const pendingReschedule = new Set<string>();

  function recordRunEvent(
    runEvent: Omit<CodexRunEvent, "createdAt" | "eventId">
  ) {
    const persistedRunEvent: CodexRunEvent = {
      createdAt: now().toISOString(),
      eventId: createId("codex_run_event"),
      ...runEvent,
    };

    options.repository.saveCodexRunEvent(persistedRunEvent);
    options.onRunEvent?.(persistedRunEvent);
  }

  async function processDataset(datasetId: string): Promise<void> {
    const dataset = options.repository.getById(datasetId);

    if (!dataset) {
      return;
    }

    const previousState =
      options.repository.getImportProcessingState(datasetId) ??
      createQueuedImportProcessingState();
    const startedAt = now().toISOString();

    options.repository.saveImportProcessingState(datasetId, {
      ...previousState,
      cleanDatabaseStatus: "running",
      lastPipelineError: null,
      nextRetryAt: null,
      pipelineStatus: "running",
    });
    recordRunEvent({
      message: `Starting pipeline generation for ${dataset.workbookName}.`,
      queryLogId: null,
      scope: "pipeline",
      sourceDatasetId: datasetId,
      stream: "system",
    });

    let versionRecord: PipelineVersionRecord | null = null;
    let runRecord: PipelineRunRecord | null = null;
    const startedAtMs = Date.parse(startedAt);

    try {
      const generated =
        await options.codexPipelineGenerator.generatePipelineArtifacts({
          onRunEvent: (runEvent) => {
            recordRunEvent({
              message: runEvent.message,
              queryLogId: null,
              scope: "pipeline",
              sourceDatasetId: datasetId,
              stream: runEvent.stream,
            });
          },
          sourceDatabasePath: options.sourceDatabasePath,
          sourceDatasetId: datasetId,
          sourceProfile: buildSourceDatasetProfile(dataset),
          sourceSheets: createWorkbookImportSummary(dataset, previousState)
            .sheets,
          workbookName: dataset.workbookName,
        });

      versionRecord = {
        analysisJson: generated.analysisJson,
        createdAt: startedAt,
        createdBy: "codex_cli",
        pipelineId: `pipeline_${datasetId}`,
        pipelineVersionId: createId("pipeline_version"),
        promptMarkdown: generated.prompt,
        sourceDatasetId: datasetId,
        sqlText: generated.sqlText,
        summaryMarkdown: generated.summaryMarkdown,
      };
      options.repository.savePipelineVersion(versionRecord);

      runRecord = {
        pipelineVersionId: versionRecord.pipelineVersionId,
        retryCount: previousState.pipelineRetryCount,
        runError: null,
        runFinishedAt: null,
        runId: createId("pipeline_run"),
        runStartedAt: startedAt,
        sourceDatasetId: datasetId,
        status: "running",
      };
      options.repository.savePipelineRun(runRecord);

      const validation = options.sqlValidator.validate(generated.sqlText);

      if (!validation.isValid) {
        throw new PipelineSqlValidationError(validation.errors.join(" "));
      }
      recordRunEvent({
        elapsedMs: Date.now() - startedAtMs,
        message: "Generated pipeline SQL passed validation.",
        queryLogId: null,
        scope: "pipeline",
        sourceDatasetId: datasetId,
        stream: "system",
      });

      const builtAt = now().toISOString();
      const cleanDatabase = await buildCleanDatabase({
        builder: options.cleanDatabaseBuilder,
        builtAt,
        cleanDatabaseDirectoryPath: options.cleanDatabaseDirectoryPath,
        cleanDatabaseId: createId("clean_db"),
        pipelineVersionId: versionRecord.pipelineVersionId,
        sourceDatasetId: datasetId,
        sourceDatabasePath: options.sourceDatabasePath,
        sqlText: generated.sqlText,
      });

      const completedRun: PipelineRunRecord = {
        ...runRecord,
        runFinishedAt: builtAt,
        status: "succeeded",
      };
      options.repository.savePipelineRun(completedRun);
      options.repository.saveImportProcessingState(datasetId, {
        cleanDatabase,
        cleanDatabaseStatus: "succeeded",
        lastPipelineError: null,
        nextRetryAt: null,
        pipelineRetryCount: previousState.pipelineRetryCount,
        pipelineRun: completedRun,
        pipelineStatus: "succeeded",
        pipelineVersion: versionRecord,
      });
      recordRunEvent({
        message: `Clean database build completed: ${cleanDatabase.cleanDatabaseId}.`,
        queryLogId: null,
        scope: "pipeline",
        sourceDatasetId: datasetId,
        stream: "system",
      });
    } catch (error) {
      const failure = classifyPipelineFailure(error, startedAtMs);
      const errorMessage = failure.message;
      const retryCount = previousState.pipelineRetryCount + 1;
      const nextRetryAt =
        retryCount < 5
          ? new Date(now().getTime() + retryDelayMs).toISOString()
          : null;
      const failedRun =
        runRecord === null
          ? null
          : {
              ...runRecord,
              retryCount,
              runError: errorMessage,
              runFinishedAt: now().toISOString(),
              status: "failed" as const,
            };

      if (failedRun) {
        options.repository.savePipelineRun(failedRun);
      }

      options.repository.saveImportProcessingState(datasetId, {
        cleanDatabase: null,
        cleanDatabaseStatus: "failed",
        lastPipelineError: errorMessage,
        nextRetryAt,
        pipelineRetryCount: retryCount,
        pipelineRun: failedRun,
        pipelineStatus: "failed",
        pipelineVersion: versionRecord,
      });
      recordRunEvent({
        ...(failure.elapsedMs !== null ? { elapsedMs: failure.elapsedMs } : {}),
        message: errorMessage,
        queryLogId: null,
        ...(failure.reasonCode !== null
          ? { reasonCode: failure.reasonCode }
          : {}),
        scope: "pipeline",
        sourceDatasetId: datasetId,
        stream: "system",
      });

      if (retryCount < 5) {
        setTimeout(() => {
          schedule(datasetId);
        }, retryDelayMs);
      }
    }
  }

  function schedule(datasetId: string): void {
    if (inFlight.has(datasetId)) {
      pendingReschedule.add(datasetId);
      return;
    }

    const task = processDataset(datasetId).finally(() => {
      inFlight.delete(datasetId);
      if (pendingReschedule.delete(datasetId)) {
        schedule(datasetId);
      }
    });

    inFlight.set(datasetId, task);
  }

  return {
    async drain() {
      while (inFlight.size > 0) {
        await Promise.all([...inFlight.values()]);
      }
    },
    resumePendingWork() {
      const nowIso = now().toISOString();

      options.repository
        .listRetryableDatasetIds(nowIso)
        .forEach((datasetId) => {
          schedule(datasetId);
        });
    },
    schedule,
  };
}

class PipelineSqlValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineSqlValidationError";
  }
}

function classifyPipelineFailure(
  error: unknown,
  startedAtMs: number
): {
  elapsedMs: number | null;
  message: string;
  reasonCode: CodexRunFailureReasonCode | null;
} {
  if (error instanceof CodexCommandError) {
    return {
      elapsedMs: error.elapsedMs,
      message: error.message,
      reasonCode: error.code,
    };
  }

  if (error instanceof PipelineSqlValidationError) {
    return {
      elapsedMs: Date.now() - startedAtMs,
      message: error.message,
      reasonCode: "sql_validation",
    };
  }

  if (
    error instanceof Error &&
    (error.message.includes("analysis.json") ||
      error.message.includes("summary.md"))
  ) {
    return {
      elapsedMs: Date.now() - startedAtMs,
      message: error.message,
      reasonCode: "artifact_contract",
    };
  }

  if (error instanceof Error) {
    return {
      elapsedMs: Date.now() - startedAtMs,
      message: error.message,
      reasonCode: "runtime_error",
    };
  }

  return {
    elapsedMs: Date.now() - startedAtMs,
    message: "Pipeline generation failed.",
    reasonCode: "runtime_error",
  };
}

async function buildCleanDatabase(options: {
  builder: CleanDatabaseBuilder;
  builtAt: string;
  cleanDatabaseDirectoryPath: string;
  cleanDatabaseId: string;
  pipelineVersionId: string;
  sourceDatasetId: string;
  sourceDatabasePath: string;
  sqlText: string;
}): Promise<CleanDatabaseSummary> {
  const cleanDatabasePath = resolve(
    join(
      options.cleanDatabaseDirectoryPath,
      `${options.sourceDatasetId}-${options.pipelineVersionId}.sqlite`
    )
  );

  await mkdir(dirname(cleanDatabasePath), { recursive: true });

  return await options.builder.buildCleanDatabase({
    builtAt: options.builtAt,
    cleanDatabaseId: options.cleanDatabaseId,
    cleanDatabasePath,
    sourceDatabasePath: options.sourceDatabasePath,
    sqlText: options.sqlText,
  });
}
