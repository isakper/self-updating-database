import type { IngestionRepository } from "../../../../packages/database-core/src/index.js";
import type {
  OptimizationRevision,
  QueryCluster,
} from "../../../../packages/shared/src/index.js";

export interface OptimizationInsightsResponse {
  queryClusters: QueryCluster[];
  optimizationRevisions: OptimizationRevision[];
}

export interface OptimizationApi {
  getInsights(sourceDatasetId: string): OptimizationInsightsResponse;
}

export function createOptimizationApi(options: {
  repository: IngestionRepository;
}): OptimizationApi {
  return {
    getInsights(sourceDatasetId) {
      return {
        optimizationRevisions: options.repository.listOptimizationRevisions(
          sourceDatasetId,
          20
        ),
        queryClusters: options.repository.listQueryClusters(
          sourceDatasetId,
          20
        ),
      };
    },
  };
}
