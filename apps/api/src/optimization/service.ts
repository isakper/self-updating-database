import { createHash } from "node:crypto";
import { access, mkdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { CodexCommandError } from "../../../../packages/agent-orchestrator/src/index.js";
import type {
  CodexOptimizationGenerator,
  GeneratedOptimizationArtifacts,
  SqlQueryGenerator,
} from "../../../../packages/agent-orchestrator/src/index.js";
import {
  buildPatternMetadataUpdate,
  type IngestionRepository,
} from "../../../../packages/database-core/src/index.js";
import type {
  CleanDatabaseSummary,
  CodexRunEvent,
  OptimizationCandidateSet,
  OptimizationFailureReasonCode,
  OptimizationRevision,
  PipelineRunRecord,
  PipelineVersionRecord,
  QueryCluster,
  QueryExecutionLog,
} from "../../../../packages/shared/src/index.js";
import type {
  CleanDatabaseBuilder,
  PipelineSqlValidationResult,
  QueryExecutor,
} from "../../../../packages/pipeline-sdk/src/index.js";

export interface OptimizationSqlValidator {
  validate(sqlText: string): PipelineSqlValidationResult;
}

export interface OptimizationQuerySqlValidator {
  validate(sqlText: string): {
    errors: string[];
    isValid: boolean;
  };
}

export interface QueryLearningLoop {
  drain(): Promise<void>;
  schedule(sourceDatasetId: string): void;
  triggerRun(
    sourceDatasetId: string,
    options?: { basePipelineVersionId?: string }
  ): { accepted: boolean; message: string };
  retryLatestFailedRevision(sourceDatasetId: string): {
    accepted: boolean;
    message: string;
  };
}

export interface CreateQueryLearningLoopOptions {
  cleanDatabaseBuilder: CleanDatabaseBuilder;
  cleanDatabaseDirectoryPath: string;
  createId?: (prefix: string) => string;
  codexOptimizationGenerator: CodexOptimizationGenerator;
  now?: () => Date;
  onRunEvent?: (runEvent: CodexRunEvent) => void;
  optimizationRetryBackoffMs?: number;
  optimizationRetryLimitPerCandidate?: number;
  optimizationParityMaxAttempts?: number;
  optimizationParityMinPassRatio?: number;
  optimizationValidationFullResultMaxCells?: number;
  optimizationValidationFullResultMaxRows?: number;
  optimizationValidationMaxLogs?: number;
  optimizationValidationPerLogTimeoutMs?: number;
  queryGenerator?: SqlQueryGenerator;
  querySqlValidator?: OptimizationQuerySqlValidator;
  queryExecutor?: QueryExecutor;
  repository: IngestionRepository;
  sourceDatabasePath: string;
  sqlValidator: OptimizationSqlValidator;
}

interface OptimizationRunRequest {
  allowFailedCandidateRetry: boolean;
  basePipelineVersionId?: string;
  mode: "auto" | "manual" | "retry_latest_failed";
  preferredCandidateFingerprint?: string;
}

interface PipelineSqlInput {
  sqlText: string;
  sourceLabel: string;
}

const AUTO_OPTIMIZATION_REQUEST: OptimizationRunRequest = {
  allowFailedCandidateRetry: false,
  mode: "auto",
};

const MANUAL_OPTIMIZATION_REQUEST: OptimizationRunRequest = {
  allowFailedCandidateRetry: true,
  mode: "manual",
};

export function createQueryLearningLoop(
  options: CreateQueryLearningLoopOptions
): QueryLearningLoop {
  const createId =
    options.createId ??
    ((prefix: string) =>
      `${prefix}_${Math.random().toString(36).slice(2, 10)}`);
  const now = options.now ?? (() => new Date());
  const optimizationRetryBackoffMs =
    options.optimizationRetryBackoffMs ?? 5_000;
  const optimizationRetryLimitPerCandidate =
    options.optimizationRetryLimitPerCandidate ?? 3;
  const optimizationParityMaxAttempts = Math.max(
    1,
    options.optimizationParityMaxAttempts ?? 3
  );
  const optimizationParityMinPassRatio = clampPassRatio(
    options.optimizationParityMinPassRatio ?? 1
  );
  const optimizationValidationFullResultMaxRows =
    options.optimizationValidationFullResultMaxRows ?? 2_000;
  const optimizationValidationFullResultMaxCells =
    options.optimizationValidationFullResultMaxCells ?? 20_000;
  const optimizationValidationMaxLogs = options.optimizationValidationMaxLogs ?? 200;
  const optimizationValidationPerLogTimeoutMs =
    options.optimizationValidationPerLogTimeoutMs ?? 120_000;
  const inFlight = new Map<string, Promise<void>>();
  const pendingRequests = new Map<string, OptimizationRunRequest>();

  function recordRunEvent(
    runEvent: Omit<CodexRunEvent, "createdAt" | "eventId">
  ): void {
    const persistedRunEvent: CodexRunEvent = {
      createdAt: now().toISOString(),
      eventId: createId("codex_run_event"),
      ...runEvent,
    };

    options.repository.saveCodexRunEvent(persistedRunEvent);
    options.onRunEvent?.(persistedRunEvent);
  }

  async function processDataset(
    sourceDatasetId: string,
    request: OptimizationRunRequest
  ): Promise<void> {
    const processingState =
      options.repository.getImportProcessingState(sourceDatasetId);
    const cleanDatabase = processingState?.cleanDatabase;
    const pipelineVersion = processingState?.pipelineVersion;

    if (
      !processingState ||
      processingState.cleanDatabaseStatus !== "succeeded" ||
      !cleanDatabase ||
      !pipelineVersion
    ) {
      return;
    }

    if (
      request.basePipelineVersionId &&
      request.basePipelineVersionId !== pipelineVersion.pipelineVersionId
    ) {
      recordRunEvent({
        message:
          `Optimization run skipped: requested base pipeline version ` +
          `${request.basePipelineVersionId} is not the active version ` +
          `${pipelineVersion.pipelineVersionId} for ${sourceDatasetId}.`,
        queryLogId: null,
        reasonCode: "runtime_error",
        scope: "optimization",
        sourceDatasetId,
        stream: "system",
      });
      return;
    }

    const queryLogs = options.repository.listQueryExecutionLogs(
      sourceDatasetId,
      500
    );
    const hydratedLogs = hydratePatternMetadata(queryLogs, options.repository);
    const benchmarkHydratedLogs = hydratedLogs
      .filter((log) => log.isBenchmarkLog === true)
      .sort((left, right) =>
        left.generationStartedAt.localeCompare(right.generationStartedAt)
      )
      .slice(0, 20);
    const parityHydratedLogs =
      benchmarkHydratedLogs.length > 0
        ? benchmarkHydratedLogs
        : hydratedLogs
            .slice()
            .sort((left, right) =>
              left.generationStartedAt.localeCompare(right.generationStartedAt)
            )
            .slice(0, 20);
    const currentClusters = rebuildQueryClusters({
      cleanDatabaseId: cleanDatabase.cleanDatabaseId,
      existingClusters: options.repository.listQueryClusters(
        sourceDatasetId,
        500
      ),
      queryLogs: hydratedLogs,
      sourceDatasetId,
    });

    currentClusters.forEach((cluster) => {
      options.repository.upsertQueryCluster(cluster);
    });

    const topClusters = rankCandidateClusters(currentClusters).slice(0, 2);

    const revisions = options.repository.listOptimizationRevisions(
      sourceDatasetId,
      200
    );
    const candidateSelection = selectCandidateSetForRequest({
      request,
      currentClusters,
      revisions,
      topClusters,
      cleanDatabase,
      pipelineVersion,
      sourceDatasetId,
    });

    if (!candidateSelection) {
      return;
    }

    const candidateSet = candidateSelection.candidateSet;
    const candidateClusters = candidateSelection.queryClusters;
    const matchingRevisions = revisions.filter((revision) =>
      isSameCandidateSet(revision, candidateSet)
    );
    const blockingRevision = matchingRevisions.find(
      (revision) =>
        revision.status === "queued" ||
        revision.status === "running" ||
        revision.status === "succeeded"
    );

    if (blockingRevision) {
      // Allow explicit manual reruns even when the same candidate already succeeded,
      // so operators can re-evaluate parity after prompt/log updates.
      // Also allow manual/retry runs to bypass stale persisted queued/running
      // revisions left behind by process restarts.
      if (
        (request.mode === "manual" ||
          request.mode === "retry_latest_failed") &&
        (blockingRevision.status === "succeeded" ||
          blockingRevision.status === "queued" ||
          blockingRevision.status === "running")
      ) {
        // Continue and create a new revision.
      } else {
        return;
      }
    }

    const failedRevisions = matchingRevisions
      .filter((revision) => revision.status === "failed")
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
    const latestFailedRevision = failedRevisions.at(0);

    const isExplicitRunRequest =
      request.mode === "manual" || request.mode === "retry_latest_failed";
    if (failedRevisions.length >= optimizationRetryLimitPerCandidate) {
      if (!isExplicitRunRequest) {
        recordRunEvent({
          message: `Skipping optimization for ${sourceDatasetId}: retry limit reached for candidate fingerprint ${candidateSet.candidateSetFingerprint}.`,
          queryLogId: null,
          reasonCode: "retry_exhausted",
          scope: "optimization",
          sourceDatasetId,
          stream: "system",
        });
        return;
      }

      recordRunEvent({
        message: `Retry limit reached for candidate fingerprint ${candidateSet.candidateSetFingerprint}, but proceeding due to explicit ${request.mode} request.`,
        queryLogId: null,
        scope: "optimization",
        sourceDatasetId,
        stream: "system",
      });
    }

    if (!request.allowFailedCandidateRetry && failedRevisions.length > 0) {
      if (!latestFailedRevision) {
        return;
      }

      const elapsedSinceLatestFailureMs =
        Date.now() - Date.parse(latestFailedRevision.updatedAt);
      if (elapsedSinceLatestFailureMs < optimizationRetryBackoffMs) {
        return;
      }
    }

    const revisionId = createId("optimization_revision");
    const startedAt = now().toISOString();
    const startedAtMs = Date.parse(startedAt);
    let revision = createOptimizationRevision({
      analysisJson: {
        candidateSetFingerprint: candidateSet.candidateSetFingerprint,
      },
      candidateSet,
      createdAt: startedAt,
      decision: "no_change",
      optimizationRevisionId: revisionId,
      promptMarkdown: "",
      sourceDatasetId,
      status: "queued",
      summaryMarkdown: "",
    });
    options.repository.saveOptimizationRevision(revision);
    recordRunEvent({
      message: `Starting optimization evaluation for ${sourceDatasetId}.`,
      queryLogId: null,
      scope: "optimization",
      sourceDatasetId,
      stream: "system",
    });

    try {
      revision = {
        ...revision,
        failureReasonCode: null,
        status: "running",
        updatedAt: now().toISOString(),
      };
      options.repository.saveOptimizationRevision(revision);

      const diagnosticContextMarkdown = buildDiagnosticContext(
        latestFailedRevision
      );
      const retryPipelineSqlInput = resolveRetryPipelineSqlInput({
        latestFailedRevision,
        repository: options.repository,
      });
      const pipelineSqlInput: PipelineSqlInput =
        request.mode === "retry_latest_failed" && retryPipelineSqlInput
          ? retryPipelineSqlInput
          : {
              sourceLabel: `active pipeline ${pipelineVersion.pipelineVersionId}`,
              sqlText: pipelineVersion.sqlText,
            };
      const validationEvidenceJson = await buildValidationEvidenceJson({
        baseCleanDatabase: cleanDatabase,
        candidateSet,
        fullResultMaxCells: optimizationValidationFullResultMaxCells,
        fullResultMaxRows: optimizationValidationFullResultMaxRows,
        maxLogs: optimizationValidationMaxLogs,
        perLogTimeoutMs: optimizationValidationPerLogTimeoutMs,
        ...(options.queryGenerator ? { queryGenerator: options.queryGenerator } : {}),
        ...(options.queryExecutor ? { queryExecutor: options.queryExecutor } : {}),
        queryLogs: parityHydratedLogs,
      });
      const validationEvidenceSummary =
        summarizeValidationEvidence(validationEvidenceJson);
      const baselinePipelineSqlFingerprint = normalizeSqlForComparison(
        pipelineSqlInput.sqlText
      );
      let attempt = 1;
      let attemptPipelineSqlInput = pipelineSqlInput;
      let iterationDiagnosticContext = diagnosticContextMarkdown;
      let lastParityFailureMessage =
        validationEvidenceSummary.comparedRecords === 0 &&
        validationEvidenceSummary.skippedRecords > 0
          ? `Optimization validation evidence is non-comparable: 0 comparable records and ${validationEvidenceSummary.skippedRecords} skipped records.`
          : undefined;

      for (; attempt <= optimizationParityMaxAttempts; attempt += 1) {
        const generated =
          await options.codexOptimizationGenerator.generateOptimizationArtifacts({
            candidateSet,
            cleanDatabasePath: cleanDatabase.databaseFilePath,
            currentPipelineSql: attemptPipelineSqlInput.sqlText,
            ...(iterationDiagnosticContext
              ? { diagnosticContextMarkdown: iterationDiagnosticContext }
              : {}),
            validationEvidenceJson,
            onRunEvent: (runEvent) => {
              recordRunEvent({
                message: runEvent.message,
                queryLogId: null,
                scope: "optimization",
                sourceDatasetId,
                stream: runEvent.stream,
              });
            },
            pipelineSqlSourceLabel: attemptPipelineSqlInput.sourceLabel,
            sourceDatasetId,
          });

        revision = {
          ...revision,
          analysisJson: generated.analysisJson,
          decision: generated.decision,
          optimizationHints: generated.optimizationHints,
          promptMarkdown: generated.prompt,
          summaryMarkdown: generated.summaryMarkdown,
          updatedAt: now().toISOString(),
        };

        const candidateSqlFingerprint = normalizeSqlForComparison(
          generated.sqlText
        );
        if (
          candidateClusters.length > 0 &&
          candidateSqlFingerprint === baselinePipelineSqlFingerprint
        ) {
          lastParityFailureMessage =
            "Candidate pipeline is identical to the baseline pipeline. " +
            "Optimization must make a material pipeline change guided by repeated query clusters before parity validation.";
          recordRunEvent({
            message:
              `Optimization iteration ${attempt}/${optimizationParityMaxAttempts} ` +
              `produced no material pipeline change; retrying candidate generation.`,
            queryLogId: null,
            scope: "optimization",
            sourceDatasetId,
            stream: "system",
          });
          attemptPipelineSqlInput = {
            sourceLabel:
              `baseline pipeline ${pipelineVersion.pipelineVersionId} ` +
              `(material-change retry ${attempt}/${optimizationParityMaxAttempts})`,
            sqlText: generated.sqlText,
          };
          iterationDiagnosticContext = [
            diagnosticContextMarkdown,
            lastParityFailureMessage,
          ]
            .filter((part) => Boolean(part && part.trim().length > 0))
            .join("\n\n");
          continue;
        }

        const validation = options.sqlValidator.validate(generated.sqlText);
        if (!validation.isValid) {
          throw new OptimizationSqlValidationError(validation.errors.join(" "));
        }

        const candidatePipelineVersion: PipelineVersionRecord = {
          analysisJson:
            generated.analysisJson as unknown as PipelineVersionRecord["analysisJson"],
          createdAt: now().toISOString(),
          createdBy: "codex_cli",
          pipelineId: pipelineVersion.pipelineId,
          pipelineVersionId: createId("pipeline_version"),
          promptMarkdown: generated.prompt,
          sourceDatasetId,
          sqlText: generated.sqlText,
          summaryMarkdown: generated.summaryMarkdown,
        };
        options.repository.savePipelineVersion(candidatePipelineVersion);
        revision = {
          ...revision,
          candidatePipelineVersionId: candidatePipelineVersion.pipelineVersionId,
        };

        const attemptStartedAt = now().toISOString();
        const pipelineRun: PipelineRunRecord = {
          pipelineVersionId: candidatePipelineVersion.pipelineVersionId,
          retryCount: 0,
          runError: null,
          runFinishedAt: null,
          runId: createId("pipeline_run"),
          runStartedAt: attemptStartedAt,
          sourceDatasetId,
          status: "running",
        };
        options.repository.savePipelineRun(pipelineRun);

        const candidateCleanDatabase = await buildCleanDatabase({
          builder: options.cleanDatabaseBuilder,
          builtAt: now().toISOString(),
          cleanDatabaseDirectoryPath: options.cleanDatabaseDirectoryPath,
          cleanDatabaseId: createId("clean_db"),
          pipelineVersionId: candidatePipelineVersion.pipelineVersionId,
          sourceDatasetId,
          sourceDatabasePath: options.sourceDatabasePath,
          sqlText: generated.sqlText,
        });

        const completedRun: PipelineRunRecord = {
          ...pipelineRun,
          runFinishedAt: candidateCleanDatabase.builtAt,
          status: "succeeded",
        };
        options.repository.savePipelineRun(completedRun);

        const parityCheck = await validateParityAgainstHistoricalLogs({
          baseCleanDatabase: cleanDatabase,
          candidateCleanDatabase,
          fullResultMaxCells: optimizationValidationFullResultMaxCells,
          fullResultMaxRows: optimizationValidationFullResultMaxRows,
          maxLogs: optimizationValidationMaxLogs,
          perLogTimeoutMs: optimizationValidationPerLogTimeoutMs,
          ...(options.queryGenerator
            ? { queryGenerator: options.queryGenerator }
            : {}),
          ...(options.querySqlValidator
            ? { querySqlValidator: options.querySqlValidator }
            : {}),
          ...(options.queryExecutor ? { queryExecutor: options.queryExecutor } : {}),
          onProgress: (event) => {
            recordRunEvent({
              message: `Optimization parity validation: checked ${event.checkedLogs}/${event.totalLogs} log prompts (${event.latestQueryLogId}).`,
              queryLogId: event.latestQueryLogId,
              scope: "optimization",
              sourceDatasetId,
              stream: "system",
            });
          },
          queryLogs: parityHydratedLogs,
        });

        if (
          !parityCheck.skippedReason &&
          meetsStrictPassRatio({
            comparedCount: parityCheck.comparedLogs,
            mismatchCount: parityCheck.mismatches.length,
            minPassRatio: optimizationParityMinPassRatio,
          })
        ) {
          recordRunEvent({
            message:
              `Optimization parity validation passed on iteration ${attempt}/${optimizationParityMaxAttempts} ` +
              `for ${parityCheck.checkedLogs} historical log query${
                parityCheck.checkedLogs === 1 ? "" : "ies"
              } ` +
              `(full output compared: ${parityCheck.comparedLogs}; skipped: ${parityCheck.skippedLogs}).`,
            queryLogId: null,
            scope: "optimization",
            sourceDatasetId,
            stream: "system",
          });

          options.repository.saveImportProcessingState(sourceDatasetId, {
            cleanDatabase: candidateCleanDatabase,
            cleanDatabaseStatus: "succeeded",
            lastPipelineError: null,
            nextRetryAt: null,
            pipelineRetryCount: processingState.pipelineRetryCount,
            pipelineRun: completedRun,
            pipelineStatus: "succeeded",
            pipelineVersion: candidatePipelineVersion,
          });

          const completedRevision: OptimizationRevision = {
            ...revision,
            analysisJson: generated.analysisJson,
            appliedCleanDatabaseId: candidateCleanDatabase.cleanDatabaseId,
            candidatePipelineVersionId: candidatePipelineVersion.pipelineVersionId,
            decision: "pipeline_revision",
            failureReasonCode: null,
            optimizationHints: generated.optimizationHints,
            promptMarkdown: generated.prompt,
            status: "succeeded",
            summaryMarkdown: generated.summaryMarkdown,
            updatedAt: now().toISOString(),
          };
          options.repository.saveOptimizationRevision(completedRevision);
          persistClusterDecision(
            candidateClusters,
            completedRevision,
            options.repository
          );
          recordRunEvent({
            elapsedMs: Date.now() - startedAtMs,
            message: `Optimization applied with pipeline ${candidatePipelineVersion.pipelineVersionId}.`,
            queryLogId: null,
            scope: "optimization",
            sourceDatasetId,
            stream: "system",
          });
          return;
        }

        lastParityFailureMessage = parityCheck.skippedReason
          ? `Skipped optimization parity validation: ${parityCheck.skippedReason}.`
          : buildParityMismatchMessage({
              comparedLogs: parityCheck.comparedLogs,
              checkedLogs: parityCheck.checkedLogs,
              mismatches: parityCheck.mismatches,
              skippedLogs: parityCheck.skippedLogs,
            });
        attemptPipelineSqlInput = {
          sourceLabel:
            `candidate pipeline ${candidatePipelineVersion.pipelineVersionId} ` +
            `(iteration ${attempt}/${optimizationParityMaxAttempts})`,
          sqlText: generated.sqlText,
        };
        iterationDiagnosticContext = [
          diagnosticContextMarkdown,
          lastParityFailureMessage,
        ]
          .filter((part) => Boolean(part && part.trim().length > 0))
          .join("\n\n");
      }

      throw new OptimizationParityMismatchError(
        lastParityFailureMessage ??
          `Optimization parity iteration failed after ${optimizationParityMaxAttempts} attempt(s).`
      );
    } catch (error) {
      const failure = classifyOptimizationFailure(error, startedAtMs);
      const failedRevision: OptimizationRevision = {
        ...revision,
        errorMessage: failure.message,
        failureReasonCode: failure.reasonCode,
        status: "failed",
        updatedAt: now().toISOString(),
      };
      options.repository.saveOptimizationRevision(failedRevision);
      recordRunEvent({
        ...(failure.elapsedMs !== null ? { elapsedMs: failure.elapsedMs } : {}),
        message: failure.message,
        queryLogId: null,
        ...(failure.reasonCode ? { reasonCode: failure.reasonCode } : {}),
        scope: "optimization",
        sourceDatasetId,
        stream: "system",
      });

      if (
        failure.reasonCode === "runtime_error" &&
        (failure.message.startsWith("Optimization parity check failed:") ||
          failure.message.startsWith(
            "Optimization validation evidence is non-comparable:"
          )) &&
        request.allowFailedCandidateRetry
      ) {
        const totalFailedForCandidate = failedRevisions.length + 1;
        if (totalFailedForCandidate < optimizationRetryLimitPerCandidate) {
          pendingRequests.set(sourceDatasetId, {
            allowFailedCandidateRetry: true,
            mode: "retry_latest_failed",
            preferredCandidateFingerprint: candidateSet.candidateSetFingerprint,
          });
          recordRunEvent({
            message:
              `Queued automatic optimization retry ${totalFailedForCandidate + 1}/` +
              `${optimizationRetryLimitPerCandidate} for candidate fingerprint ` +
              `${candidateSet.candidateSetFingerprint}.`,
            queryLogId: null,
            scope: "optimization",
            sourceDatasetId,
            stream: "system",
          });
        }
      }
    }
  }

  function enqueue(
    sourceDatasetId: string,
    request: OptimizationRunRequest
  ): { accepted: boolean; message: string } {
    if (inFlight.has(sourceDatasetId)) {
      const mergedRequest = mergeOptimizationRunRequest(
        pendingRequests.get(sourceDatasetId) ?? AUTO_OPTIMIZATION_REQUEST,
        request
      );
      pendingRequests.set(sourceDatasetId, mergedRequest);
      return {
        accepted: true,
        message: `Optimization already running for ${sourceDatasetId}; queued next run.`,
      };
    }

    const task = processDataset(sourceDatasetId, request).finally(() => {
      inFlight.delete(sourceDatasetId);
      const pendingRequest = pendingRequests.get(sourceDatasetId);
      if (pendingRequest) {
        pendingRequests.delete(sourceDatasetId);
        void enqueue(sourceDatasetId, pendingRequest);
      }
    });
    inFlight.set(sourceDatasetId, task);

    return {
      accepted: true,
      message: `Optimization run scheduled for ${sourceDatasetId}.`,
    };
  }

  return {
    async drain() {
      while (inFlight.size > 0) {
        await Promise.all([...inFlight.values()]);
      }
    },
    schedule(sourceDatasetId) {
      void enqueue(sourceDatasetId, AUTO_OPTIMIZATION_REQUEST);
    },
    triggerRun(sourceDatasetId, triggerOptions) {
      if (triggerOptions?.basePipelineVersionId) {
        const processingState =
          options.repository.getImportProcessingState(sourceDatasetId);
        const activePipelineVersionId =
          processingState?.pipelineVersion?.pipelineVersionId;

        if (!activePipelineVersionId) {
          return {
            accepted: false,
            message:
              `Cannot pin optimization run for ${sourceDatasetId}: ` +
              `no active pipeline version is available.`,
          };
        }

        if (activePipelineVersionId !== triggerOptions.basePipelineVersionId) {
          return {
            accepted: false,
            message:
              `Requested base pipeline version ${triggerOptions.basePipelineVersionId} ` +
              `does not match active pipeline version ${activePipelineVersionId} ` +
              `for ${sourceDatasetId}.`,
          };
        }
      }

      return enqueue(
        sourceDatasetId,
        triggerOptions?.basePipelineVersionId === undefined
          ? MANUAL_OPTIMIZATION_REQUEST
          : {
              ...MANUAL_OPTIMIZATION_REQUEST,
              basePipelineVersionId: triggerOptions.basePipelineVersionId,
            }
      );
    },
    retryLatestFailedRevision(sourceDatasetId) {
      const latestFailedRevision = options.repository
        .listOptimizationRevisions(sourceDatasetId, 200)
        .find((revision) => revision.status === "failed");

      if (!latestFailedRevision) {
        return {
          accepted: false,
          message: `No failed optimization revision available for ${sourceDatasetId}.`,
        };
      }

      return enqueue(sourceDatasetId, {
        allowFailedCandidateRetry: true,
        mode: "retry_latest_failed",
        preferredCandidateFingerprint:
          latestFailedRevision.candidateSet.candidateSetFingerprint,
      });
    },
  };
}

class OptimizationSqlValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OptimizationSqlValidationError";
  }
}

class OptimizationParityMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OptimizationParityMismatchError";
  }
}

function selectCandidateSetForRequest(options: {
  cleanDatabase: CleanDatabaseSummary;
  currentClusters: QueryCluster[];
  pipelineVersion: PipelineVersionRecord;
  request: OptimizationRunRequest;
  revisions: OptimizationRevision[];
  sourceDatasetId: string;
  topClusters: QueryCluster[];
}): {
  candidateSet: OptimizationCandidateSet;
  queryClusters: QueryCluster[];
} | null {
  if (options.request.preferredCandidateFingerprint) {
    const preferredRevision = options.revisions.find(
      (revision) =>
        revision.status === "failed" &&
        revision.candidateSet.candidateSetFingerprint ===
          options.request.preferredCandidateFingerprint
    );

    if (!preferredRevision) {
      return null;
    }

    if (
      preferredRevision.candidateSet.baseCleanDatabaseId !==
        options.cleanDatabase.cleanDatabaseId ||
      preferredRevision.candidateSet.basePipelineVersionId !==
        options.pipelineVersion.pipelineVersionId
    ) {
      return null;
    }

    return {
      candidateSet: preferredRevision.candidateSet,
      queryClusters: preferredRevision.candidateSet.queryClusters,
    };
  }

  if (options.topClusters.length < 2) {
    if (options.request.mode !== "manual") {
      return null;
    }

    // Manual triggers should always create a revision row, even when
    // optimization-eligible clusters are sparse. Use best available signals.
    const manualFallbackClusters = options.currentClusters
      .slice()
      .sort((left, right) => {
        const latencyDelta =
          right.cumulativeExecutionLatencyMs - left.cumulativeExecutionLatencyMs;

        if (latencyDelta !== 0) {
          return latencyDelta;
        }

        const countDelta = right.queryCount - left.queryCount;
        if (countDelta !== 0) {
          return countDelta;
        }

        return right.latestSeenAt.localeCompare(left.latestSeenAt);
      })
      .slice(0, 2);

    return {
      candidateSet: buildOptimizationCandidateSet({
        cleanDatabase: options.cleanDatabase,
        pipelineVersion: options.pipelineVersion,
        queryClusters: manualFallbackClusters,
        sourceDatasetId: options.sourceDatasetId,
      }),
      queryClusters: manualFallbackClusters,
    };
  }

  return {
    candidateSet: buildOptimizationCandidateSet({
      cleanDatabase: options.cleanDatabase,
      pipelineVersion: options.pipelineVersion,
      queryClusters: options.topClusters,
      sourceDatasetId: options.sourceDatasetId,
    }),
    queryClusters: options.topClusters,
  };
}

function mergeOptimizationRunRequest(
  current: OptimizationRunRequest,
  incoming: OptimizationRunRequest
): OptimizationRunRequest {
  if (incoming.mode === "retry_latest_failed") {
    return incoming;
  }

  if (current.mode === "retry_latest_failed") {
    return current;
  }

  if (incoming.mode === "manual") {
    return incoming;
  }

  if (current.mode === "manual") {
    return current;
  }

  return current;
}

function isSameCandidateSet(
  revision: OptimizationRevision,
  candidateSet: OptimizationCandidateSet
): boolean {
  return (
    revision.candidateSet.candidateSetFingerprint ===
      candidateSet.candidateSetFingerprint &&
    revision.baseCleanDatabaseId === candidateSet.baseCleanDatabaseId &&
    revision.basePipelineVersionId === candidateSet.basePipelineVersionId
  );
}

function classifyOptimizationFailure(
  error: unknown,
  startedAtMs: number
): {
  elapsedMs: number | null;
  message: string;
  reasonCode: OptimizationFailureReasonCode;
} {
  if (error instanceof CodexCommandError) {
    return {
      elapsedMs: error.elapsedMs,
      message: error.message,
      reasonCode: error.code as OptimizationFailureReasonCode,
    };
  }

  if (error instanceof OptimizationSqlValidationError) {
    return {
      elapsedMs: Date.now() - startedAtMs,
      message: error.message,
      reasonCode: "sql_validation",
    };
  }

  if (error instanceof OptimizationParityMismatchError) {
    return {
      elapsedMs: Date.now() - startedAtMs,
      message: error.message,
      reasonCode: "runtime_error",
    };
  }

  if (
    error instanceof Error &&
    (error.message.includes("decision.json") ||
      error.message.includes("analysis.json") ||
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
    message: "Optimization evaluation failed.",
    reasonCode: "runtime_error",
  };
}

function buildDiagnosticContext(
  latestFailedRevision: OptimizationRevision | undefined
): string | undefined {
  if (!latestFailedRevision?.errorMessage) {
    return undefined;
  }

  return `Latest failed revision id: ${latestFailedRevision.optimizationRevisionId}
Failure reason code: ${latestFailedRevision.failureReasonCode ?? "unknown"}
Failure message:
${latestFailedRevision.errorMessage}`;
}

function resolveRetryPipelineSqlInput(options: {
  latestFailedRevision: OptimizationRevision | undefined;
  repository: IngestionRepository;
}): PipelineSqlInput | undefined {
  const candidatePipelineVersionId =
    options.latestFailedRevision?.candidatePipelineVersionId;
  if (!candidatePipelineVersionId) {
    return undefined;
  }

  const candidatePipelineVersion =
    options.repository.getPipelineVersionById(candidatePipelineVersionId);
  if (!candidatePipelineVersion) {
    return undefined;
  }

  return {
    sourceLabel:
      `failed candidate pipeline ${candidatePipelineVersion.pipelineVersionId} ` +
      `(from revision ${options.latestFailedRevision?.optimizationRevisionId ?? "unknown"})`,
    sqlText: candidatePipelineVersion.sqlText,
  };
}

async function validateParityAgainstHistoricalLogs(options: {
  baseCleanDatabase: CleanDatabaseSummary;
  candidateCleanDatabase: CleanDatabaseSummary;
  fullResultMaxCells: number;
  fullResultMaxRows: number;
  maxLogs: number;
  onProgress?: (event: {
    checkedLogs: number;
    latestQueryLogId: string;
    totalLogs: number;
  }) => void;
  perLogTimeoutMs: number;
  queryGenerator?: SqlQueryGenerator;
  querySqlValidator?: OptimizationQuerySqlValidator;
  queryExecutor?: QueryExecutor;
  queryLogs: QueryExecutionLog[];
}): Promise<{
  checkedLogs: number;
  comparedLogs: number;
  mismatches: Array<{ detail: string; queryLogId: string }>;
  skippedLogs: number;
  skippedReason?: string;
}> {
  if (!options.queryExecutor) {
    return {
      checkedLogs: 0,
      comparedLogs: 0,
      mismatches: [],
      skippedLogs: 0,
      skippedReason: "query executor unavailable",
    };
  }
  if (!options.queryGenerator) {
    return {
      checkedLogs: 0,
      comparedLogs: 0,
      mismatches: [],
      skippedLogs: 0,
      skippedReason: "query generator unavailable",
    };
  }
  const basePath = options.baseCleanDatabase.databaseFilePath;
  const candidatePath = options.candidateCleanDatabase.databaseFilePath;

  try {
    await Promise.all([access(basePath), access(candidatePath)]);
  } catch {
    return {
      checkedLogs: 0,
      comparedLogs: 0,
      mismatches: [],
      skippedLogs: 0,
      skippedReason: "database files unavailable",
    };
  }

  const eligibleLogs = options.queryLogs
    .filter(
      (log) =>
        log.cleanDatabaseId === options.baseCleanDatabase.cleanDatabaseId &&
        log.status === "succeeded" &&
        log.prompt.trim().length > 0
    )
    .slice(0, options.maxLogs);

  const mismatches: Array<{ detail: string; queryLogId: string }> = [];
  let checkedLogs = 0;
  let comparedLogs = 0;
  let skippedLogs = 0;

  for (const queryLog of eligibleLogs) {
    try {
      const generated = await withTimeout(
        options.queryGenerator.generateSql({
          cleanDatabaseId: options.candidateCleanDatabase.cleanDatabaseId,
          cleanDatabasePath: candidatePath,
          prompt: queryLog.prompt,
          sourceDatasetId: queryLog.sourceDatasetId,
        }),
        options.perLogTimeoutMs,
        `query SQL generation timed out after ${options.perLogTimeoutMs}ms`
      );
      const generatedSql = generated.sqlText.trim();
      if (!generatedSql) {
        mismatches.push({
          detail: "generated SQL is empty",
          queryLogId: queryLog.queryLogId,
        });
        continue;
      }

      if (options.querySqlValidator) {
        const validation = options.querySqlValidator.validate(generatedSql);
        if (!validation.isValid) {
          mismatches.push({
            detail: `generated SQL failed validation: ${validation.errors.join(" ")}`,
            queryLogId: queryLog.queryLogId,
          });
          continue;
        }
      }

      const candidateResult = await withTimeout(
        options.queryExecutor.executeQuery({
          cleanDatabasePath: candidatePath,
          sqlText: generatedSql,
        }),
        options.perLogTimeoutMs,
        `candidate SQL execution timed out after ${options.perLogTimeoutMs}ms`
      );

      const expectedSql = queryLog.generatedSql?.trim() ?? "";
      if (!expectedSql) {
        skippedLogs += 1;
        continue;
      }

      if (
        shouldSkipFullResultComparison({
          columnCountHint: queryLog.resultColumnNames.length,
          maxCells: options.fullResultMaxCells,
          maxRows: options.fullResultMaxRows,
          rowCountHint: queryLog.rowCount,
        })
      ) {
        skippedLogs += 1;
        continue;
      }

      const expectedResult = await withTimeout(
        options.queryExecutor.executeQuery({
          cleanDatabasePath: basePath,
          sqlText: expectedSql,
        }),
        options.perLogTimeoutMs,
        `expected SQL execution timed out after ${options.perLogTimeoutMs}ms`
      );

      if (
        shouldSkipFullResultComparison({
          columnCountHint: expectedResult.columnNames.length,
          maxCells: options.fullResultMaxCells,
          maxRows: options.fullResultMaxRows,
          rowCountHint: expectedResult.rows.length,
        }) ||
        shouldSkipFullResultComparison({
          columnCountHint: candidateResult.columnNames.length,
          maxCells: options.fullResultMaxCells,
          maxRows: options.fullResultMaxRows,
          rowCountHint: candidateResult.rows.length,
        })
      ) {
        skippedLogs += 1;
        continue;
      }

      comparedLogs += 1;
      if (
        !areQueryResultsSemanticallyEquivalent({
          actualColumnNames: candidateResult.columnNames,
          actualRows: candidateResult.rows,
          expectedColumnNames: expectedResult.columnNames,
          expectedRows: expectedResult.rows,
        })
      ) {
        mismatches.push({
          detail: `prompt-generated result mismatch for prompt "${queryLog.prompt}"`,
          queryLogId: queryLog.queryLogId,
        });
      }
    } catch (error) {
      mismatches.push({
        detail:
          error instanceof Error
            ? `execution mismatch: ${error.message}`
            : "execution mismatch",
        queryLogId: queryLog.queryLogId,
      });
    }

    checkedLogs += 1;
    options.onProgress?.({
      checkedLogs,
      latestQueryLogId: queryLog.queryLogId,
      totalLogs: eligibleLogs.length,
    });
  }

  return {
    checkedLogs,
    comparedLogs,
    mismatches,
    skippedLogs,
  };
}

function shouldSkipFullResultComparison(options: {
  columnCountHint: number;
  maxCells: number;
  maxRows: number;
  rowCountHint: number | null;
}): boolean {
  if (options.rowCountHint === null) {
    return false;
  }

  if (options.rowCountHint > options.maxRows) {
    return true;
  }

  const columnCount = Math.max(1, options.columnCountHint);
  return options.rowCountHint * columnCount > options.maxCells;
}

function areQueryResultsSemanticallyEquivalent(options: {
  actualColumnNames: string[];
  actualRows: Array<Array<boolean | number | string | null>>;
  expectedColumnNames: string[];
  expectedRows: Array<Array<boolean | number | string | null>>;
}): boolean {
  if (
    options.actualColumnNames.length === options.expectedColumnNames.length &&
    areResultRowsSemanticallyEquivalent({
      actualRows: options.actualRows,
      expectedRows: options.expectedRows,
    })
  ) {
    return true;
  }

  const projectedActualRows = projectRowsToExpectedColumns({
    actualColumnNames: options.actualColumnNames,
    actualRows: options.actualRows,
    expectedColumnNames: options.expectedColumnNames,
  });

  if (!projectedActualRows) {
    return false;
  }

  if (
    areResultRowsSemanticallyEquivalent({
      actualRows: projectedActualRows,
      expectedRows: options.expectedRows,
    })
  ) {
    return true;
  }

  const aggregatedComparison = aggregateRowsForSemanticComparison({
    actualRows: projectedActualRows,
    expectedRows: options.expectedRows,
  });

  if (!aggregatedComparison) {
    return false;
  }

  return areResultRowsSemanticallyEquivalent(aggregatedComparison);
}

function areResultRowsSemanticallyEquivalent(options: {
  actualRows: Array<Array<boolean | number | string | null>>;
  expectedRows: Array<Array<boolean | number | string | null>>;
}): boolean {
  if (options.actualRows.length !== options.expectedRows.length) {
    return false;
  }

  const actualCounts = new Map<string, number>();

  options.actualRows.forEach((row) => {
    const key = canonicalizeRow(row);
    actualCounts.set(key, (actualCounts.get(key) ?? 0) + 1);
  });

  for (const expectedRow of options.expectedRows) {
    const key = canonicalizeRow(expectedRow);
    const count = actualCounts.get(key) ?? 0;
    if (count <= 0) {
      return false;
    }
    actualCounts.set(key, count - 1);
  }

  return true;
}

function canonicalizeRow(
  row: Array<boolean | number | string | null>
): string {
  return row
    .map((cell) => canonicalizeCell(cell))
    .sort()
    .join("\u001f");
}

function canonicalizeCell(cell: boolean | number | string | null): string {
  if (cell === null) {
    return "null";
  }
  if (typeof cell === "number") {
    if (!Number.isFinite(cell)) {
      return `num:${String(cell)}`;
    }
    const normalized = Math.abs(cell) < 1e-9 ? 0 : Number(cell.toFixed(9));
    return `num:${normalized}`;
  }
  if (typeof cell === "boolean") {
    return `bool:${cell ? "1" : "0"}`;
  }
  return `str:${cell.trim()}`;
}

function aggregateRowsForSemanticComparison(options: {
  actualRows: Array<Array<boolean | number | string | null>>;
  expectedRows: Array<Array<boolean | number | string | null>>;
}):
  | {
      actualRows: Array<Array<boolean | number | string | null>>;
      expectedRows: Array<Array<boolean | number | string | null>>;
    }
  | null {
  const expectedWidth = options.expectedRows[0]?.length ?? 0;
  if (expectedWidth === 0) {
    return null;
  }

  const numericColumnIndexes = detectNumericColumnIndexes(
    options.expectedRows,
    expectedWidth
  );
  const keyColumnIndexes = Array.from({ length: expectedWidth }, (_, index) =>
    index
  ).filter((index) => !numericColumnIndexes.has(index));

  if (numericColumnIndexes.size === 0 || keyColumnIndexes.length === 0) {
    return null;
  }

  return {
    actualRows: aggregateRowsByKey({
      keyColumnIndexes,
      numericColumnIndexes,
      rows: options.actualRows,
      width: expectedWidth,
    }),
    expectedRows: aggregateRowsByKey({
      keyColumnIndexes,
      numericColumnIndexes,
      rows: options.expectedRows,
      width: expectedWidth,
    }),
  };
}

function detectNumericColumnIndexes(
  rows: Array<Array<boolean | number | string | null>>,
  width: number
): Set<number> {
  const numericIndexes = new Set<number>();

  for (let index = 0; index < width; index += 1) {
    let hasValue = false;
    let allNumeric = true;

    for (const row of rows) {
      const cell = row[index] ?? null;
      if (cell === null) {
        continue;
      }
      hasValue = true;
      if (typeof cell !== "number") {
        allNumeric = false;
        break;
      }
    }

    if (hasValue && allNumeric) {
      numericIndexes.add(index);
    }
  }

  return numericIndexes;
}

function aggregateRowsByKey(options: {
  keyColumnIndexes: number[];
  numericColumnIndexes: Set<number>;
  rows: Array<Array<boolean | number | string | null>>;
  width: number;
}): Array<Array<boolean | number | string | null>> {
  const grouped = new Map<string, Array<boolean | number | string | null>>();

  for (const row of options.rows) {
    const key = options.keyColumnIndexes
      .map((index) => canonicalizeCell(row[index] ?? null))
      .join("\u001e");
    const existing = grouped.get(key);

    if (!existing) {
      const seeded = Array.from({ length: options.width }, (_, index) => {
        if (options.numericColumnIndexes.has(index)) {
          const raw = row[index];
          return typeof raw === "number" ? raw : 0;
        }
        return row[index] ?? null;
      });
      grouped.set(key, seeded);
      continue;
    }

    for (let index = 0; index < options.width; index += 1) {
      if (!options.numericColumnIndexes.has(index)) {
        continue;
      }
      const raw = row[index];
      const value = typeof raw === "number" ? raw : 0;
      const current = existing[index];
      existing[index] = (typeof current === "number" ? current : 0) + value;
    }
  }

  return [...grouped.values()];
}

function projectRowsToExpectedColumns(options: {
  actualColumnNames: string[];
  actualRows: Array<Array<boolean | number | string | null>>;
  expectedColumnNames: string[];
}): Array<Array<boolean | number | string | null>> | null {
  const usedIndices = new Set<number>();
  const projection: number[] = [];

  for (const expectedColumnName of options.expectedColumnNames) {
    const matchedIndex = findMatchingColumnIndex({
      actualColumnNames: options.actualColumnNames,
      expectedColumnName,
      usedIndices,
    });

    if (matchedIndex === null) {
      return null;
    }

    usedIndices.add(matchedIndex);
    projection.push(matchedIndex);
  }

  return options.actualRows.map((row) =>
    projection.map((columnIndex) => row[columnIndex] ?? null)
  );
}

function findMatchingColumnIndex(options: {
  actualColumnNames: string[];
  expectedColumnName: string;
  usedIndices: Set<number>;
}): number | null {
  const expectedNormalized = normalizeColumnName(options.expectedColumnName);
  const aliases = columnNameAliases(expectedNormalized);

  for (let index = 0; index < options.actualColumnNames.length; index += 1) {
    if (options.usedIndices.has(index)) {
      continue;
    }

    const actualNormalized = normalizeColumnName(
      options.actualColumnNames[index] ?? ""
    );

    if (actualNormalized === expectedNormalized) {
      return index;
    }
  }

  for (let index = 0; index < options.actualColumnNames.length; index += 1) {
    if (options.usedIndices.has(index)) {
      continue;
    }

    const actualNormalized = normalizeColumnName(
      options.actualColumnNames[index] ?? ""
    );

    if (aliases.has(actualNormalized)) {
      return index;
    }
  }

  return null;
}

function normalizeColumnName(columnName: string): string {
  return columnName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function columnNameAliases(expectedNormalized: string): Set<string> {
  const aliases = new Set<string>([expectedNormalized]);
  const additional = COLUMN_NAME_ALIAS_MAP[expectedNormalized] ?? [];
  for (const alias of additional) {
    aliases.add(alias);
  }
  return aliases;
}

const COLUMN_NAME_ALIAS_MAP: Record<string, string[]> = {
  business_date: ["date", "sales_date"],
  cost_ex_vat: ["cogs_ex_vat", "cogs_ex_vat_raw", "cogs_ex_vat_net"],
  revenue_incl_vat: ["gross_sales_incl_vat", "gross_sales_incl_vat_gross"],
  units_sold: ["units", "units_gross", "units_net"],
};

async function withTimeout<T>(
  work: Promise<T>,
  timeoutMs: number,
  timeoutMessage: string
): Promise<T> {
  let timeoutHandle: NodeJS.Timeout | null = null;

  try {
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(() => {
        reject(new Error(timeoutMessage));
      }, timeoutMs);
    });

    return await Promise.race([work, timeoutPromise]);
  } finally {
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
  }
}

function buildParityMismatchMessage(options: {
  comparedLogs: number;
  checkedLogs: number;
  mismatches: Array<{ detail: string; queryLogId: string }>;
  skippedLogs: number;
}): string {
  const preview = options.mismatches
    .slice(0, 5)
    .map(
      (mismatch, index) =>
        `${index + 1}. queryLogId=${mismatch.queryLogId}; ${mismatch.detail}`
    )
    .join("\n");

  return `Optimization parity check failed: ${options.mismatches.length} of ${options.checkedLogs} historical log prompts produced incompatible results in the candidate clean database (full output compared: ${options.comparedLogs}; skipped: ${options.skippedLogs}).

Sample mismatches:
${preview}`;
}

function meetsStrictPassRatio(options: {
  comparedCount: number;
  mismatchCount: number;
  minPassRatio: number;
}): boolean {
  if (options.comparedCount <= 0) {
    return false;
  }
  const matchedCount = options.comparedCount - options.mismatchCount;
  const passRatio = matchedCount / options.comparedCount;
  return passRatio > options.minPassRatio;
}

function clampPassRatio(value: number): number {
  if (!Number.isFinite(value)) {
    return 1;
  }
  if (value < 0) {
    return 0;
  }
  if (value > 1) {
    return 1;
  }
  return value;
}

function normalizeSqlForComparison(sqlText: string): string {
  return sqlText
    .replaceAll(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function summarizeValidationEvidence(evidenceJson: string): {
  comparedRecords: number;
  mismatchRecords: Array<{ prompt: string; queryLogId: string }>;
  skippedRecords: number;
} {
  try {
    const parsed = JSON.parse(evidenceJson) as {
      fullResultComparison?: {
        comparedRecords?: unknown;
        skippedRecords?: unknown;
      };
      records?: Array<{
        fullResultComparison?: {
          expected?: {
            columnNames?: string[];
            rows?: Array<Array<boolean | number | string | null>>;
          };
          generated?: {
            columnNames?: string[];
            rows?: Array<Array<boolean | number | string | null>>;
          };
          status?: string;
        };
        prompt?: string;
        queryLogId?: string;
      }>;
    };
    const comparedRecords = Number(
      parsed.fullResultComparison?.comparedRecords ?? 0
    );
    const skippedRecords = Number(
      parsed.fullResultComparison?.skippedRecords ?? 0
    );
    const mismatchRecords =
      parsed.records
        ?.filter(
          (record) =>
            record.fullResultComparison?.status === "available" &&
            Array.isArray(record.fullResultComparison.expected?.columnNames) &&
            Array.isArray(record.fullResultComparison.expected?.rows) &&
            Array.isArray(record.fullResultComparison.generated?.columnNames) &&
            Array.isArray(record.fullResultComparison.generated?.rows)
        )
        .filter(
          (record) =>
            !areQueryResultsSemanticallyEquivalent({
              actualColumnNames:
                record.fullResultComparison?.generated?.columnNames ?? [],
              actualRows: record.fullResultComparison?.generated?.rows ?? [],
              expectedColumnNames:
                record.fullResultComparison?.expected?.columnNames ?? [],
              expectedRows: record.fullResultComparison?.expected?.rows ?? [],
            })
        )
        .map((record) => ({
          prompt: record.prompt ?? "",
          queryLogId: record.queryLogId ?? "unknown_query_log",
        })) ?? [];
    return {
      comparedRecords: Number.isFinite(comparedRecords) ? comparedRecords : 0,
      mismatchRecords,
      skippedRecords: Number.isFinite(skippedRecords) ? skippedRecords : 0,
    };
  } catch {
    return {
      comparedRecords: 0,
      mismatchRecords: [],
      skippedRecords: 0,
    };
  }
}

async function buildValidationEvidenceJson(options: {
  baseCleanDatabase: CleanDatabaseSummary;
  candidateSet: OptimizationCandidateSet;
  fullResultMaxCells: number;
  fullResultMaxRows: number;
  maxLogs: number;
  perLogTimeoutMs: number;
  queryExecutor?: QueryExecutor;
  queryGenerator?: SqlQueryGenerator;
  queryLogs: QueryExecutionLog[];
}): Promise<string> {
  const clusterByFingerprint = new Map(
    options.candidateSet.queryClusters.map((cluster) => [
      cluster.patternFingerprint,
      cluster,
    ])
  );
  const evidenceMaxLogs = Math.min(options.maxLogs, 20);
  const basePath = options.baseCleanDatabase.databaseFilePath;
  const queryExecutor = options.queryExecutor;
  const queryGenerator = options.queryGenerator;

  const relevantLogs = options.queryLogs
    .filter(
      (queryLog) =>
        queryLog.cleanDatabaseId === options.candidateSet.baseCleanDatabaseId &&
        queryLog.status === "succeeded" &&
        queryLog.prompt.trim().length > 0 &&
        queryLog.patternFingerprint !== null &&
        clusterByFingerprint.has(queryLog.patternFingerprint)
    )
    .sort((left, right) => right.totalLatencyMs - left.totalLatencyMs)
    .slice(0, evidenceMaxLogs);

  const records: Array<Record<string, unknown>> = [];
  let fullComparedCount = 0;
  let fullSkippedCount = 0;

  for (const queryLog of relevantLogs) {
    const cluster = queryLog.patternFingerprint
      ? clusterByFingerprint.get(queryLog.patternFingerprint)
      : undefined;
    const record: Record<string, unknown> = {
      cleanDatabaseId: queryLog.cleanDatabaseId,
      expected: {
        columnNames: queryLog.resultColumnNames,
        rowCount: queryLog.rowCount,
        rowsSample: queryLog.resultRowsSample ?? [],
      },
      generatedSql: queryLog.generatedSql,
      prompt: queryLog.prompt,
      queryClusterId: cluster?.queryClusterId ?? queryLog.matchedClusterId,
      queryKind: queryLog.queryKind,
      queryLogId: queryLog.queryLogId,
    };

    if (!queryExecutor || !queryGenerator) {
      fullSkippedCount += 1;
      record.fullResultComparison = {
        reason: "query generator or query executor unavailable",
        status: "skipped",
      };
      records.push(record);
      continue;
    }

    const expectedSql = queryLog.generatedSql?.trim() ?? "";
    if (!expectedSql) {
      fullSkippedCount += 1;
      record.fullResultComparison = {
        reason: "expected SQL from query log is empty",
        status: "skipped",
      };
      records.push(record);
      continue;
    }

    if (
      shouldSkipFullResultComparison({
        columnCountHint: queryLog.resultColumnNames.length,
        maxCells: options.fullResultMaxCells,
        maxRows: options.fullResultMaxRows,
        rowCountHint: queryLog.rowCount,
      })
    ) {
      fullSkippedCount += 1;
      record.fullResultComparison = {
        reason: "result size hint exceeds full comparison limits",
        status: "skipped",
      };
      records.push(record);
      continue;
    }

    try {
      const expectedResult = await withTimeout(
        queryExecutor.executeQuery({
          cleanDatabasePath: basePath,
          sqlText: expectedSql,
        }),
        options.perLogTimeoutMs,
        `expected SQL execution timed out after ${options.perLogTimeoutMs}ms`
      );

      if (
        shouldSkipFullResultComparison({
          columnCountHint: expectedResult.columnNames.length,
          maxCells: options.fullResultMaxCells,
          maxRows: options.fullResultMaxRows,
          rowCountHint: expectedResult.rows.length,
        })
      ) {
        fullSkippedCount += 1;
        record.fullResultComparison = {
          reason: "expected result exceeds full comparison limits",
          status: "skipped",
        };
        records.push(record);
        continue;
      }

      const generated = await withTimeout(
        queryGenerator.generateSql({
          cleanDatabaseId: options.baseCleanDatabase.cleanDatabaseId,
          cleanDatabasePath: basePath,
          prompt: queryLog.prompt,
          sourceDatasetId: queryLog.sourceDatasetId,
        }),
        options.perLogTimeoutMs,
        `validation SQL generation timed out after ${options.perLogTimeoutMs}ms`
      );
      const generatedSql = generated.sqlText.trim();

      if (!generatedSql) {
        fullSkippedCount += 1;
        record.fullResultComparison = {
          reason: "generated SQL is empty",
          status: "skipped",
        };
        records.push(record);
        continue;
      }

      const generatedResult = await withTimeout(
        queryExecutor.executeQuery({
          cleanDatabasePath: basePath,
          sqlText: generatedSql,
        }),
        options.perLogTimeoutMs,
        `generated SQL execution timed out after ${options.perLogTimeoutMs}ms`
      );

      if (
        shouldSkipFullResultComparison({
          columnCountHint: generatedResult.columnNames.length,
          maxCells: options.fullResultMaxCells,
          maxRows: options.fullResultMaxRows,
          rowCountHint: generatedResult.rows.length,
        })
      ) {
        fullSkippedCount += 1;
        record.fullResultComparison = {
          reason: "generated result exceeds full comparison limits",
          status: "skipped",
        };
        records.push(record);
        continue;
      }

      fullComparedCount += 1;
      record.fullResultComparison = {
        expected: {
          columnNames: expectedResult.columnNames,
          rowCount: expectedResult.rows.length,
          rows: expectedResult.rows,
        },
        generated: {
          columnNames: generatedResult.columnNames,
          rowCount: generatedResult.rows.length,
          rows: generatedResult.rows,
          sqlText: generatedSql,
        },
        status: "available",
      };
    } catch (error) {
      fullSkippedCount += 1;
      record.fullResultComparison = {
        reason:
          error instanceof Error ? error.message : "full comparison failed",
        status: "skipped",
      };
    }

    records.push(record);
  }

  return JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      note: "For records with fullResultComparison.status=available, expected and generated full rows are provided for manual semantic comparison by Codex.",
      sourceDatasetId: options.candidateSet.sourceDatasetId,
      baseCleanDatabaseId: options.candidateSet.baseCleanDatabaseId,
      fullResultComparison: {
        comparedRecords: fullComparedCount,
        maxCellsPerResult: options.fullResultMaxCells,
        maxRowsPerResult: options.fullResultMaxRows,
        skippedRecords: fullSkippedCount,
      },
      queryClusters: options.candidateSet.queryClusters.map((cluster) => ({
        queryClusterId: cluster.queryClusterId,
        patternFingerprint: cluster.patternFingerprint,
        queryCount: cluster.queryCount,
      })),
      records,
    },
    null,
    2
  );
}

function hydratePatternMetadata(
  queryLogs: QueryExecutionLog[],
  repository: IngestionRepository
): QueryExecutionLog[] {
  return queryLogs.map((queryLog) => {
    const patternMetadata = buildPatternMetadataUpdate({ queryLog });

    if (patternMetadata === null) {
      return queryLog;
    }

    if (
      queryLog.patternFingerprint === patternMetadata.patternFingerprint &&
      queryLog.patternSummaryJson !== null &&
      queryLog.queryKind !== null
    ) {
      return queryLog;
    }

    repository.updateQueryExecutionLogPatternMetadata(patternMetadata);
    return {
      ...queryLog,
      matchedClusterId: patternMetadata.matchedClusterId,
      optimizationEligible: patternMetadata.optimizationEligible,
      patternFingerprint: patternMetadata.patternFingerprint,
      patternSummaryJson: patternMetadata.patternSummaryJson,
      patternVersion: patternMetadata.patternVersion,
      queryKind: patternMetadata.queryKind,
      usedOptimizationObjects:
        patternMetadata.usedOptimizationObjects ??
        queryLog.usedOptimizationObjects,
    };
  });
}

function rebuildQueryClusters(options: {
  cleanDatabaseId: string;
  existingClusters: QueryCluster[];
  queryLogs: QueryExecutionLog[];
  sourceDatasetId: string;
}): QueryCluster[] {
  const existingById = new Map(
    options.existingClusters.map((cluster) => [cluster.queryClusterId, cluster])
  );
  const groupedLogs = new Map<string, QueryExecutionLog[]>();

  options.queryLogs.forEach((queryLog) => {
    if (
      queryLog.cleanDatabaseId !== options.cleanDatabaseId ||
      queryLog.patternFingerprint === null ||
      queryLog.patternSummaryJson === null ||
      queryLog.matchedClusterId === null
    ) {
      return;
    }

    const currentLogs = groupedLogs.get(queryLog.matchedClusterId) ?? [];
    currentLogs.push(queryLog);
    groupedLogs.set(queryLog.matchedClusterId, currentLogs);
  });

  return [...groupedLogs.entries()].map(([clusterId, logs]) => {
    const sortedLogs = [...logs].sort((left, right) => {
      const latencyDelta =
        (right.executionLatencyMs ?? 0) - (left.executionLatencyMs ?? 0);
      if (latencyDelta !== 0) {
        return latencyDelta;
      }

      return right.generationStartedAt.localeCompare(left.generationStartedAt);
    });
    const latestLog = [...logs].sort((left, right) =>
      right.generationStartedAt.localeCompare(left.generationStartedAt)
    )[0];
    const cumulativeExecutionLatencyMs = logs.reduce(
      (sum, log) => sum + (log.executionLatencyMs ?? 0),
      0
    );
    const existingCluster = existingById.get(clusterId);

    if (
      !latestLog ||
      !latestLog.patternSummaryJson ||
      !latestLog.patternFingerprint ||
      latestLog.patternVersion === null
    ) {
      throw new Error("Expected clustered logs to contain pattern metadata.");
    }

    return {
      averageExecutionLatencyMs: Math.round(
        cumulativeExecutionLatencyMs / Math.max(logs.length, 1)
      ),
      cleanDatabaseId: latestLog.cleanDatabaseId,
      cumulativeExecutionLatencyMs,
      latestOptimizationDecision:
        existingCluster?.latestOptimizationDecision ?? null,
      latestOptimizationRevisionId:
        existingCluster?.latestOptimizationRevisionId ?? null,
      latestQueryLogId: latestLog.queryLogId,
      latestSeenAt: latestLog.generationStartedAt,
      patternFingerprint: latestLog.patternFingerprint,
      patternSummary: latestLog.patternSummaryJson,
      patternVersion: latestLog.patternVersion,
      queryClusterId: clusterId,
      queryCount: logs.length,
      representativeQueryLogIds: sortedLogs
        .slice(0, 3)
        .map((log) => log.queryLogId),
      sourceDatasetId: options.sourceDatasetId,
    } satisfies QueryCluster;
  });
}

function rankCandidateClusters(queryClusters: QueryCluster[]): QueryCluster[] {
  return queryClusters
    .filter(
      (cluster) =>
        cluster.patternSummary.optimizationEligible && cluster.queryCount > 1
    )
    .sort((left, right) => {
      const latencyDelta =
        right.cumulativeExecutionLatencyMs - left.cumulativeExecutionLatencyMs;

      if (latencyDelta !== 0) {
        return latencyDelta;
      }

      const countDelta = right.queryCount - left.queryCount;

      if (countDelta !== 0) {
        return countDelta;
      }

      return right.latestSeenAt.localeCompare(left.latestSeenAt);
    });
}

function buildOptimizationCandidateSet(options: {
  cleanDatabase: CleanDatabaseSummary;
  pipelineVersion: PipelineVersionRecord;
  queryClusters: QueryCluster[];
  sourceDatasetId: string;
}): OptimizationCandidateSet {
  const candidateSetFingerprint = createHash("sha256")
    .update(
      JSON.stringify({
        baseCleanDatabaseId: options.cleanDatabase.cleanDatabaseId,
        basePipelineVersionId: options.pipelineVersion.pipelineVersionId,
        queryClusterIds: options.queryClusters.map(
          (cluster) => cluster.queryClusterId
        ),
      })
    )
    .digest("hex");

  return {
    baseCleanDatabaseId: options.cleanDatabase.cleanDatabaseId,
    basePipelineVersionId: options.pipelineVersion.pipelineVersionId,
    candidateSetFingerprint,
    queryClusters: options.queryClusters,
    sourceDatasetId: options.sourceDatasetId,
  };
}

function createOptimizationRevision(options: {
  analysisJson: OptimizationRevision["analysisJson"];
  candidateSet: OptimizationCandidateSet;
  createdAt: string;
  decision: OptimizationRevision["decision"];
  optimizationRevisionId: string;
  promptMarkdown: string;
  sourceDatasetId: string;
  status: OptimizationRevision["status"];
  summaryMarkdown: string;
}): OptimizationRevision {
  return {
    analysisJson: options.analysisJson,
    appliedCleanDatabaseId: null,
    baseCleanDatabaseId: options.candidateSet.baseCleanDatabaseId,
    basePipelineVersionId: options.candidateSet.basePipelineVersionId,
    candidatePipelineVersionId: null,
    candidateSet: options.candidateSet,
    createdAt: options.createdAt,
    decision: options.decision,
    errorMessage: null,
    failureReasonCode: null,
    optimizationHints: [],
    optimizationRevisionId: options.optimizationRevisionId,
    promptMarkdown: options.promptMarkdown,
    sourceDatasetId: options.sourceDatasetId,
    status: options.status,
    summaryMarkdown: options.summaryMarkdown,
    updatedAt: options.createdAt,
  };
}

function persistClusterDecision(
  queryClusters: QueryCluster[],
  revision: OptimizationRevision,
  repository: IngestionRepository
): void {
  queryClusters.forEach((cluster) => {
    repository.upsertQueryCluster({
      ...cluster,
      latestOptimizationDecision: revision.decision,
      latestOptimizationRevisionId: revision.optimizationRevisionId,
    });
  });
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

export function createOptimizationRevisionFromArtifacts(options: {
  artifacts: GeneratedOptimizationArtifacts;
  candidateSet: OptimizationCandidateSet;
  createdAt: string;
  optimizationRevisionId: string;
  sourceDatasetId: string;
}): OptimizationRevision {
  return createOptimizationRevision({
    analysisJson: options.artifacts.analysisJson,
    candidateSet: options.candidateSet,
    createdAt: options.createdAt,
    decision: options.artifacts.decision,
    optimizationRevisionId: options.optimizationRevisionId,
    promptMarkdown: options.artifacts.prompt,
    sourceDatasetId: options.sourceDatasetId,
    status: "succeeded",
    summaryMarkdown: options.artifacts.summaryMarkdown,
  });
}
