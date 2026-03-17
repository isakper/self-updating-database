import type { WorkbookImportStatus } from "../../../../packages/shared/src/index.js";

export interface WorkbookImportJob {
  jobId: string;
  workbookName: string;
  status: Extract<WorkbookImportStatus, "queued">;
}

export function createWorkbookImportJob(options: {
  workbookName: string;
  createId?: (prefix: string) => string;
}): WorkbookImportJob {
  const createId =
    options.createId ?? ((prefix: string) => `${prefix}_${Date.now()}`);

  return {
    jobId: createId("job"),
    workbookName: options.workbookName,
    status: "queued",
  };
}
