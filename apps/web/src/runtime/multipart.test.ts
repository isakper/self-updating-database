import { describe, expect, it } from "vitest";
import { Readable } from "node:stream";

import { readWorkbookUpload } from "./multipart.js";

describe("readWorkbookUpload", () => {
  it("extracts the uploaded workbook file from multipart form data", async () => {
    const boundary = "test-boundary";
    const requestBody = [
      `--${boundary}\r\n`,
      'Content-Disposition: form-data; name="workbookFile"; filename="sales.xlsx"\r\n',
      "Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n",
      "fake-binary-content",
      `\r\n--${boundary}--\r\n`,
    ].join("");

    const request = Readable.from([Buffer.from(requestBody)]) as Readable & {
      headers: Record<string, string>;
    };
    request.headers = {
      "content-type": `multipart/form-data; boundary=${boundary}`,
    };

    await expect(readWorkbookUpload(request)).resolves.toMatchObject({
      fileName: "sales.xlsx",
    });
  });
});
