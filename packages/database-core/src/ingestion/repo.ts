import type {
  CodexRunEvent,
  ImportProcessingState,
  OptimizationHint,
  OptimizationRevision,
  PipelineRunRecord,
  PipelineVersionRecord,
  QueryCluster,
  QueryExecutionLog,
} from "../../../shared/src/index.js";
import type { SourceDataset } from "./types.js";

export interface SourceDatasetRepository {
  save(dataset: SourceDataset): void;
  getById(datasetId: string): SourceDataset | undefined;
  list(): SourceDataset[];
}

export interface IngestionRepository extends SourceDatasetRepository {
  getPipelineVersionById(
    pipelineVersionId: string
  ): PipelineVersionRecord | undefined;
  listActiveOptimizationHints(sourceDatasetId: string): OptimizationHint[];
  listCodexRunEvents(sourceDatasetId: string, limit?: number): CodexRunEvent[];
  getImportProcessingState(
    datasetId: string
  ): ImportProcessingState | undefined;
  getLatestPipelineRun(datasetId: string): PipelineRunRecord | undefined;
  getLatestPipelineVersion(
    datasetId: string
  ): PipelineVersionRecord | undefined;
  listOptimizationRevisions(
    sourceDatasetId: string,
    limit?: number
  ): OptimizationRevision[];
  listQueryClusters(sourceDatasetId: string, limit?: number): QueryCluster[];
  listRetryableDatasetIds(nowIso: string): string[];
  listQueryExecutionLogs(
    sourceDatasetId: string,
    limit?: number
  ): QueryExecutionLog[];
  saveImportProcessingState(
    datasetId: string,
    processingState: ImportProcessingState
  ): void;
  saveOptimizationRevision(revision: OptimizationRevision): void;
  saveCodexRunEvent(runEvent: CodexRunEvent): void;
  saveQueryExecutionLog(queryLog: QueryExecutionLog): void;
  savePipelineRun(runRecord: PipelineRunRecord): void;
  savePipelineVersion(versionRecord: PipelineVersionRecord): void;
  updateQueryExecutionLogPatternMetadata(options: {
    matchedClusterId: string | null;
    optimizationEligible: boolean;
    patternFingerprint: string;
    patternSummaryJson: QueryExecutionLog["patternSummaryJson"];
    patternVersion: number;
    queryKind: QueryExecutionLog["queryKind"];
    queryLogId: string;
    usedOptimizationObjects?: string[];
  }): void;
  upsertQueryCluster(cluster: QueryCluster): void;
}

export class InMemorySourceDatasetRepository implements IngestionRepository {
  readonly #datasets = new Map<string, SourceDataset>();
  readonly #codexRunEvents = new Map<string, CodexRunEvent[]>();
  readonly #optimizationRevisions = new Map<string, OptimizationRevision[]>();
  readonly #processingStates = new Map<string, ImportProcessingState>();
  readonly #queryClusters = new Map<string, QueryCluster[]>();
  readonly #queryLogs = new Map<string, QueryExecutionLog[]>();
  readonly #pipelineRuns = new Map<string, PipelineRunRecord>();
  readonly #pipelineVersions = new Map<string, PipelineVersionRecord>();
  readonly #pipelineVersionsById = new Map<string, PipelineVersionRecord>();

  save(dataset: SourceDataset): void {
    this.#datasets.set(dataset.id, dataset);
  }

  getById(datasetId: string): SourceDataset | undefined {
    return this.#datasets.get(datasetId);
  }

  list(): SourceDataset[] {
    return [...this.#datasets.values()];
  }

  saveImportProcessingState(
    datasetId: string,
    processingState: ImportProcessingState
  ): void {
    this.#processingStates.set(datasetId, processingState);
  }

  getImportProcessingState(
    datasetId: string
  ): ImportProcessingState | undefined {
    return this.#processingStates.get(datasetId);
  }

  savePipelineVersion(versionRecord: PipelineVersionRecord): void {
    this.#pipelineVersions.set(versionRecord.sourceDatasetId, versionRecord);
    this.#pipelineVersionsById.set(versionRecord.pipelineVersionId, versionRecord);
  }

  getPipelineVersionById(
    pipelineVersionId: string
  ): PipelineVersionRecord | undefined {
    return this.#pipelineVersionsById.get(pipelineVersionId);
  }

  getLatestPipelineVersion(
    datasetId: string
  ): PipelineVersionRecord | undefined {
    return this.#pipelineVersions.get(datasetId);
  }

  savePipelineRun(runRecord: PipelineRunRecord): void {
    this.#pipelineRuns.set(runRecord.sourceDatasetId, runRecord);
  }

  getLatestPipelineRun(datasetId: string): PipelineRunRecord | undefined {
    return this.#pipelineRuns.get(datasetId);
  }

  listRetryableDatasetIds(nowIso: string): string[] {
    return [...this.#processingStates.entries()]
      .filter(([, processingState]) => {
        if (processingState.pipelineStatus === "succeeded") {
          return false;
        }

        if (processingState.pipelineRetryCount >= 5) {
          return false;
        }

        if (!processingState.nextRetryAt) {
          return processingState.pipelineStatus === "queued";
        }

        return processingState.nextRetryAt <= nowIso;
      })
      .map(([datasetId]) => datasetId);
  }

  saveCodexRunEvent(runEvent: CodexRunEvent): void {
    const currentEvents =
      this.#codexRunEvents.get(runEvent.sourceDatasetId) ?? [];
    this.#codexRunEvents.set(runEvent.sourceDatasetId, [
      ...currentEvents,
      runEvent,
    ]);
  }

  listCodexRunEvents(sourceDatasetId: string, limit = 200): CodexRunEvent[] {
    const currentEvents = this.#codexRunEvents.get(sourceDatasetId) ?? [];
    return currentEvents.slice(-limit);
  }

  saveOptimizationRevision(revision: OptimizationRevision): void {
    const currentRevisions =
      this.#optimizationRevisions.get(revision.sourceDatasetId) ?? [];
    const nextRevisions = currentRevisions.filter(
      (entry) =>
        entry.optimizationRevisionId !== revision.optimizationRevisionId
    );
    nextRevisions.unshift(revision);
    this.#optimizationRevisions.set(revision.sourceDatasetId, nextRevisions);
  }

  listOptimizationRevisions(
    sourceDatasetId: string,
    limit = 20
  ): OptimizationRevision[] {
    return (this.#optimizationRevisions.get(sourceDatasetId) ?? []).slice(
      0,
      limit
    );
  }

  upsertQueryCluster(cluster: QueryCluster): void {
    const currentClusters =
      this.#queryClusters.get(cluster.sourceDatasetId) ?? [];
    const nextClusters = currentClusters.filter(
      (entry) => entry.queryClusterId !== cluster.queryClusterId
    );
    nextClusters.unshift(cluster);
    this.#queryClusters.set(cluster.sourceDatasetId, nextClusters);
  }

  listQueryClusters(sourceDatasetId: string, limit = 20): QueryCluster[] {
    return (this.#queryClusters.get(sourceDatasetId) ?? []).slice(0, limit);
  }

  saveQueryExecutionLog(queryLog: QueryExecutionLog): void {
    const currentLogs = this.#queryLogs.get(queryLog.sourceDatasetId) ?? [];
    this.#queryLogs.set(queryLog.sourceDatasetId, [queryLog, ...currentLogs]);
  }

  listQueryExecutionLogs(
    sourceDatasetId: string,
    limit = 20
  ): QueryExecutionLog[] {
    return (this.#queryLogs.get(sourceDatasetId) ?? []).slice(0, limit);
  }

  updateQueryExecutionLogPatternMetadata(options: {
    matchedClusterId: string | null;
    optimizationEligible: boolean;
    patternFingerprint: string;
    patternSummaryJson: QueryExecutionLog["patternSummaryJson"];
    patternVersion: number;
    queryKind: QueryExecutionLog["queryKind"];
    queryLogId: string;
    usedOptimizationObjects?: string[];
  }): void {
    for (const [datasetId, logs] of this.#queryLogs.entries()) {
      const nextLogs = logs.map((log) =>
        log.queryLogId === options.queryLogId
          ? {
              ...log,
              matchedClusterId: options.matchedClusterId,
              optimizationEligible: options.optimizationEligible,
              patternFingerprint: options.patternFingerprint,
              patternSummaryJson: options.patternSummaryJson,
              patternVersion: options.patternVersion,
              queryKind: options.queryKind,
              usedOptimizationObjects:
                options.usedOptimizationObjects ?? log.usedOptimizationObjects,
            }
          : log
      );
      this.#queryLogs.set(datasetId, nextLogs);
    }
  }

  listActiveOptimizationHints(sourceDatasetId: string): OptimizationHint[] {
    return (
      (this.#optimizationRevisions.get(sourceDatasetId) ?? []).find(
        (revision) =>
          revision.status === "succeeded" &&
          revision.decision === "pipeline_revision" &&
          revision.appliedCleanDatabaseId !== null
      )?.optimizationHints ?? []
    );
  }
}
