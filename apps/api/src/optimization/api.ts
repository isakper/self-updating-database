import type { IngestionRepository } from "../../../../packages/database-core/src/index.js";
import type {
  OptimizationRevision,
  QueryCluster,
} from "../../../../packages/shared/src/index.js";
import type { QueryLearningLoop } from "./service.js";

export interface OptimizationInsightsResponse {
  queryClusters: QueryCluster[];
  optimizationRevisions: OptimizationRevision[];
}

export interface OptimizationApi {
  getInsights(sourceDatasetId: string): OptimizationInsightsResponse;
  retryLatestFailedRevision(sourceDatasetId: string): OptimizationRunResponse;
  triggerRun(
    sourceDatasetId: string,
    options?: { basePipelineVersionId?: string }
  ): OptimizationRunResponse;
}

export interface OptimizationRunResponse {
  accepted: boolean;
  message: string;
}

export function createOptimizationApi(options: {
  queryLearningLoop: QueryLearningLoop;
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
    retryLatestFailedRevision(sourceDatasetId) {
      return options.queryLearningLoop.retryLatestFailedRevision(
        sourceDatasetId
      );
    },
    triggerRun(sourceDatasetId, triggerOptions) {
      return options.queryLearningLoop.triggerRun(sourceDatasetId, triggerOptions);
    },
  };
}
