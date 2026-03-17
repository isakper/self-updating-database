import Busboy from "busboy";
import type { IncomingHttpHeaders, IncomingMessage } from "node:http";
import type { FileInfo } from "busboy";
import type { Readable } from "node:stream";

export interface UploadedWorkbookFile {
  fileBuffer: Buffer;
  fileName: string;
}

export interface MultipartRequest {
  headers: IncomingHttpHeaders;
  pipe: IncomingMessage["pipe"];
}

export async function readWorkbookUpload(
  request: MultipartRequest
): Promise<UploadedWorkbookFile> {
  return await new Promise<UploadedWorkbookFile>((resolve, reject) => {
    const busboy = Busboy({
      headers: request.headers,
    });

    let workbookFile: UploadedWorkbookFile | undefined;

    busboy.on("file", (fieldName: string, file: Readable, info: FileInfo) => {
      if (fieldName !== "workbookFile") {
        file.resume();
        return;
      }

      const chunks: Uint8Array[] = [];

      file.on("data", (chunk: Buffer) => {
        chunks.push(chunk);
      });

      file.on("end", () => {
        workbookFile = {
          fileBuffer: Buffer.concat(chunks),
          fileName: info.filename || "uploaded-workbook.xlsx",
        };
      });
    });

    busboy.on("error", reject);

    busboy.on("finish", () => {
      if (!workbookFile) {
        reject(new Error("Please choose an Excel workbook to import."));
        return;
      }

      resolve(workbookFile);
    });

    request.pipe(busboy);
  });
}
