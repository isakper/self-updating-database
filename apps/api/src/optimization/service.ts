import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { CodexCommandError } from "../../../../packages/agent-orchestrator/src/index.js";
import type {
  CodexOptimizationGenerator,
  GeneratedOptimizationArtifacts,
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
} from "../../../../packages/pipeline-sdk/src/index.js";

export interface OptimizationSqlValidator {
  validate(sqlText: string): PipelineSqlValidationResult;
}

export interface QueryLearningLoop {
  drain(): Promise<void>;
  schedule(sourceDatasetId: string): void;
  triggerRun(sourceDatasetId: string): { accepted: boolean; message: string };
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
  repository: IngestionRepository;
  sourceDatabasePath: string;
  sqlValidator: OptimizationSqlValidator;
}

interface OptimizationRunRequest {
  allowFailedCandidateRetry: boolean;
  mode: "auto" | "manual" | "retry_latest_failed";
  preferredCandidateFingerprint?: string;
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

    const queryLogs = options.repository.listQueryExecutionLogs(
      sourceDatasetId,
      500
    );
    const hydratedLogs = hydratePatternMetadata(queryLogs, options.repository);
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
      return;
    }

    const failedRevisions = matchingRevisions
      .filter((revision) => revision.status === "failed")
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));

    if (failedRevisions.length >= optimizationRetryLimitPerCandidate) {
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

    if (!request.allowFailedCandidateRetry && failedRevisions.length > 0) {
      const latestFailedRevision = failedRevisions.at(0);

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

      const generated =
        await options.codexOptimizationGenerator.generateOptimizationArtifacts({
          candidateSet,
          cleanDatabasePath: cleanDatabase.databaseFilePath,
          currentPipelineSql: pipelineVersion.sqlText,
          onRunEvent: (runEvent) => {
            recordRunEvent({
              message: runEvent.message,
              queryLogId: null,
              scope: "optimization",
              sourceDatasetId,
              stream: runEvent.stream,
            });
          },
          sourceDatasetId,
        });

      if (generated.decision === "no_change") {
        const completedRevision: OptimizationRevision = {
          ...revision,
          analysisJson: generated.analysisJson,
          decision: "no_change",
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
          message: "Optimization evaluation completed with no pipeline change.",
          queryLogId: null,
          scope: "optimization",
          sourceDatasetId,
          stream: "system",
        });
        return;
      }

      const validation = options.sqlValidator.validate(generated.sqlText);

      if (!validation.isValid) {
        throw new OptimizationSqlValidationError(validation.errors.join(" "));
      }

      const candidatePipelineVersion: PipelineVersionRecord = {
        analysisJson:
          generated.analysisJson as unknown as PipelineVersionRecord["analysisJson"],
        createdAt: startedAt,
        createdBy: "codex_cli",
        pipelineId: pipelineVersion.pipelineId,
        pipelineVersionId: createId("pipeline_version"),
        promptMarkdown: generated.prompt,
        sourceDatasetId,
        sqlText: generated.sqlText,
        summaryMarkdown: generated.summaryMarkdown,
      };
      options.repository.savePipelineVersion(candidatePipelineVersion);

      const pipelineRun: PipelineRunRecord = {
        pipelineVersionId: candidatePipelineVersion.pipelineVersionId,
        retryCount: 0,
        runError: null,
        runFinishedAt: null,
        runId: createId("pipeline_run"),
        runStartedAt: startedAt,
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
    triggerRun(sourceDatasetId) {
      return enqueue(sourceDatasetId, MANUAL_OPTIMIZATION_REQUEST);
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

function selectCandidateSetForRequest(options: {
  cleanDatabase: CleanDatabaseSummary;
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
    return null;
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
