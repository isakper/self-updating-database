import type { QueryExecutionLog } from "../../../shared/src/index.js";

export interface QueryLogRepository {
  listQueryExecutionLogs(
    sourceDatasetId: string,
    limit?: number
  ): QueryExecutionLog[];
  saveQueryExecutionLog(queryLog: QueryExecutionLog): void;
}
