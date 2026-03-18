import {
  createWorkbookImportSummary,
  createQueuedImportProcessingState,
  InMemorySourceDatasetRepository,
  ingestWorkbook,
  type IngestionRepository,
  type SourceDataset,
} from "../../../../packages/database-core/src/index.js";
import type {
  WorkbookImportSummary,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";
import type { PipelineRetryScheduler } from "./pipeline.js";

export interface IngestionApi {
  importWorkbook(request: WorkbookUploadRequest): WorkbookImportSummary;
  getImportSummary(datasetId: string): WorkbookImportSummary | undefined;
  getSourceDataset(datasetId: string): SourceDataset | undefined;
  listImports(): WorkbookImportSummary[];
}

export interface CreateIngestionApiOptions {
  pipelineRetryScheduler?: PipelineRetryScheduler;
  repository: IngestionRepository;
  now?: Date;
  createId?: (prefix: string) => string;
}

export function createIngestionApi(
  options: CreateIngestionApiOptions
): IngestionApi {
  const repository = options.repository;

  return {
    importWorkbook(request) {
      const result = ingestWorkbook({
        repository,
        request,
        ...(options.now ? { now: options.now } : {}),
        ...(options.createId ? { createId: options.createId } : {}),
      });

      repository.saveImportProcessingState(
        result.dataset.id,
        createQueuedImportProcessingState()
      );
      options.pipelineRetryScheduler?.schedule(result.dataset.id);

      return (
        getImportSummaryFromRepository(repository, result.dataset.id) ??
        result.summary
      );
    },
    getImportSummary(datasetId) {
      return getImportSummaryFromRepository(repository, datasetId);
    },
    getSourceDataset(datasetId) {
      return repository.getById(datasetId);
    },
    listImports() {
      return repository.list().flatMap((dataset) => {
        const summary = getImportSummaryFromRepository(repository, dataset.id);
        return summary ? [summary] : [];
      });
    },
  };
}

export interface CreateInMemoryIngestionApiOptions {
  now?: Date;
  createId?: (prefix: string) => string;
}

export function createInMemoryIngestionApi(
  options: CreateInMemoryIngestionApiOptions = {}
): IngestionApi {
  return createIngestionApi({
    repository: new InMemorySourceDatasetRepository(),
    ...(options.now ? { now: options.now } : {}),
    ...(options.createId ? { createId: options.createId } : {}),
  });
}

function getImportSummaryFromRepository(
  repository: IngestionRepository,
  datasetId: string
): WorkbookImportSummary | undefined {
  const dataset = repository.getById(datasetId);
  const processingState = repository.getImportProcessingState(datasetId);

  if (!dataset || !processingState) {
    return undefined;
  }

  return createWorkbookImportSummary(dataset, processingState);
}
