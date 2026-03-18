import type {
  ImportProcessingState,
  PipelineRunRecord,
  PipelineVersionRecord,
} from "../../../shared/src/index.js";
import type { SourceDataset } from "./types.js";

export interface SourceDatasetRepository {
  save(dataset: SourceDataset): void;
  getById(datasetId: string): SourceDataset | undefined;
  list(): SourceDataset[];
}

export interface IngestionRepository extends SourceDatasetRepository {
  getImportProcessingState(
    datasetId: string
  ): ImportProcessingState | undefined;
  getLatestPipelineRun(datasetId: string): PipelineRunRecord | undefined;
  getLatestPipelineVersion(
    datasetId: string
  ): PipelineVersionRecord | undefined;
  listRetryableDatasetIds(nowIso: string): string[];
  saveImportProcessingState(
    datasetId: string,
    processingState: ImportProcessingState
  ): void;
  savePipelineRun(runRecord: PipelineRunRecord): void;
  savePipelineVersion(versionRecord: PipelineVersionRecord): void;
}

export class InMemorySourceDatasetRepository implements IngestionRepository {
  readonly #datasets = new Map<string, SourceDataset>();
  readonly #processingStates = new Map<string, ImportProcessingState>();
  readonly #pipelineRuns = new Map<string, PipelineRunRecord>();
  readonly #pipelineVersions = new Map<string, PipelineVersionRecord>();

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
}
