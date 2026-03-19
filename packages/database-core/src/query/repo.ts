import type {
  OptimizationHint,
  OptimizationRevision,
  QueryCluster,
  QueryExecutionLog,
} from "../../../shared/src/index.js";

export interface QueryLogRepository {
  listActiveOptimizationHints(sourceDatasetId: string): OptimizationHint[];
  listOptimizationRevisions(
    sourceDatasetId: string,
    limit?: number
  ): OptimizationRevision[];
  listQueryClusters(sourceDatasetId: string, limit?: number): QueryCluster[];
  listQueryExecutionLogs(
    sourceDatasetId: string,
    limit?: number
  ): QueryExecutionLog[];
  saveOptimizationRevision(revision: OptimizationRevision): void;
  saveQueryExecutionLog(queryLog: QueryExecutionLog): void;
  upsertQueryCluster(cluster: QueryCluster): void;
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
}
