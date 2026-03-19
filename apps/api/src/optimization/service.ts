import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

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
}

export interface CreateQueryLearningLoopOptions {
  cleanDatabaseBuilder: CleanDatabaseBuilder;
  cleanDatabaseDirectoryPath: string;
  createId?: (prefix: string) => string;
  codexOptimizationGenerator: CodexOptimizationGenerator;
  now?: () => Date;
  onRunEvent?: (runEvent: CodexRunEvent) => void;
  repository: IngestionRepository;
  sourceDatabasePath: string;
  sqlValidator: OptimizationSqlValidator;
}

export function createQueryLearningLoop(
  options: CreateQueryLearningLoopOptions
): QueryLearningLoop {
  const createId =
    options.createId ??
    ((prefix: string) =>
      `${prefix}_${Math.random().toString(36).slice(2, 10)}`);
  const now = options.now ?? (() => new Date());
  const inFlight = new Map<string, Promise<void>>();

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

  async function processDataset(sourceDatasetId: string): Promise<void> {
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

    if (topClusters.length < 2) {
      return;
    }

    const candidateSet = buildOptimizationCandidateSet({
      cleanDatabase,
      pipelineVersion,
      queryClusters: topClusters,
      sourceDatasetId,
    });
    const existingRevision = options.repository
      .listOptimizationRevisions(sourceDatasetId, 100)
      .find(
        (revision) =>
          revision.candidateSet.candidateSetFingerprint ===
            candidateSet.candidateSetFingerprint &&
          revision.baseCleanDatabaseId === candidateSet.baseCleanDatabaseId &&
          revision.basePipelineVersionId === candidateSet.basePipelineVersionId
      );

    if (existingRevision) {
      return;
    }

    const revisionId = createId("optimization_revision");
    const startedAt = now().toISOString();
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
          optimizationHints: generated.optimizationHints,
          promptMarkdown: generated.prompt,
          status: "succeeded",
          summaryMarkdown: generated.summaryMarkdown,
          updatedAt: now().toISOString(),
        };
        options.repository.saveOptimizationRevision(completedRevision);
        persistClusterDecision(
          topClusters,
          completedRevision,
          options.repository
        );
        recordRunEvent({
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
        throw new Error(validation.errors.join(" "));
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
        optimizationHints: generated.optimizationHints,
        promptMarkdown: generated.prompt,
        status: "succeeded",
        summaryMarkdown: generated.summaryMarkdown,
        updatedAt: now().toISOString(),
      };
      options.repository.saveOptimizationRevision(completedRevision);
      persistClusterDecision(
        topClusters,
        completedRevision,
        options.repository
      );
      recordRunEvent({
        message: `Optimization applied with pipeline ${candidatePipelineVersion.pipelineVersionId}.`,
        queryLogId: null,
        scope: "optimization",
        sourceDatasetId,
        stream: "system",
      });
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Optimization evaluation failed.";
      const failedRevision: OptimizationRevision = {
        ...revision,
        errorMessage: message,
        status: "failed",
        updatedAt: now().toISOString(),
      };
      options.repository.saveOptimizationRevision(failedRevision);
      recordRunEvent({
        message,
        queryLogId: null,
        scope: "optimization",
        sourceDatasetId,
        stream: "system",
      });
    }
  }

  return {
    drain() {
      return Promise.all([...inFlight.values()]).then(() => undefined);
    },
    schedule(sourceDatasetId) {
      if (inFlight.has(sourceDatasetId)) {
        return;
      }

      const task = processDataset(sourceDatasetId).finally(() => {
        inFlight.delete(sourceDatasetId);
      });
      inFlight.set(sourceDatasetId, task);
    },
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
