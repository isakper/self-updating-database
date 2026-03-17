import type { SourceDataset } from "./types.js";

export interface SourceDatasetRepository {
  save(dataset: SourceDataset): void;
  getById(datasetId: string): SourceDataset | undefined;
  list(): SourceDataset[];
}

export class InMemorySourceDatasetRepository implements SourceDatasetRepository {
  readonly #datasets = new Map<string, SourceDataset>();

  save(dataset: SourceDataset): void {
    this.#datasets.set(dataset.id, dataset);
  }

  getById(datasetId: string): SourceDataset | undefined {
    return this.#datasets.get(datasetId);
  }

  list(): SourceDataset[] {
    return [...this.#datasets.values()];
  }
}
