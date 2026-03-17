import {
  InMemorySourceDatasetRepository,
  ingestWorkbook,
  type SourceDataset,
} from "../../../../packages/database-core/src/index.js";
import type {
  WorkbookImportSummary,
  WorkbookUploadRequest,
} from "../../../../packages/shared/src/index.js";

export interface InMemoryIngestionApi {
  importWorkbook(request: WorkbookUploadRequest): WorkbookImportSummary;
  getSourceDataset(datasetId: string): SourceDataset | undefined;
  listImports(): WorkbookImportSummary[];
}

export interface CreateInMemoryIngestionApiOptions {
  now?: Date;
  createId?: (prefix: string) => string;
}

export function createInMemoryIngestionApi(
  options: CreateInMemoryIngestionApiOptions = {}
): InMemoryIngestionApi {
  const repository = new InMemorySourceDatasetRepository();
  const imports: WorkbookImportSummary[] = [];

  return {
    importWorkbook(request) {
      const result = ingestWorkbook({
        repository,
        request,
        ...(options.now ? { now: options.now } : {}),
        ...(options.createId ? { createId: options.createId } : {}),
      });

      imports.push(result.summary);
      return result.summary;
    },
    getSourceDataset(datasetId) {
      return repository.getById(datasetId);
    },
    listImports() {
      return [...imports];
    },
  };
}
