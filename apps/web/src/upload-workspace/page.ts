import type { WorkbookImportSummary } from "../../../../packages/shared/src/index.js";
import { buildUploadWorkspaceModel } from "./model.js";

export function renderUploadWorkspacePage(options?: {
  importSummary?: WorkbookImportSummary;
  errorMessage?: string;
}): string {
  const model = options?.importSummary
    ? buildUploadWorkspaceModel(options.importSummary)
    : undefined;

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    ${
      model?.shouldAutoRefresh
        ? '<meta http-equiv="refresh" content="2" />'
        : ""
    }
    <title>Workbook Ingestion Workspace</title>
    <style>
      :root {
        color-scheme: light;
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
        background: #f4f1ea;
        color: #1c1a17;
      }
      body {
        margin: 0;
        background:
          radial-gradient(circle at top, rgba(214, 177, 91, 0.22), transparent 28%),
          linear-gradient(180deg, #f6f1e7 0%, #efe7d8 100%);
      }
      main {
        max-width: 960px;
        margin: 0 auto;
        padding: 48px 24px 80px;
      }
      .hero {
        margin-bottom: 32px;
      }
      .eyebrow {
        text-transform: uppercase;
        letter-spacing: 0.16em;
        font-size: 0.75rem;
        color: #8f5b14;
        font-weight: 700;
      }
      h1 {
        font-size: clamp(2.4rem, 5vw, 4rem);
        line-height: 0.95;
        margin: 12px 0 16px;
        max-width: 12ch;
      }
      p {
        max-width: 64ch;
        line-height: 1.6;
      }
      .grid {
        display: grid;
        gap: 24px;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      }
      .panel {
        background: rgba(255, 255, 255, 0.86);
        border: 1px solid rgba(28, 26, 23, 0.08);
        border-radius: 24px;
        padding: 24px;
        box-shadow: 0 18px 40px rgba(55, 38, 8, 0.08);
      }
      input[type="file"] {
        width: 100%;
        border-radius: 18px;
        border: 1px solid rgba(28, 26, 23, 0.12);
        padding: 16px;
        font: 0.95rem/1.5 "IBM Plex Sans", "Segoe UI", sans-serif;
        box-sizing: border-box;
        background: #fcfaf5;
      }
      .hint {
        margin-top: 12px;
        font-size: 0.9rem;
        color: #5f5548;
      }
      button {
        margin-top: 16px;
        border: 0;
        border-radius: 999px;
        background: #1c1a17;
        color: #fffdf8;
        padding: 12px 20px;
        font: inherit;
        font-weight: 700;
        cursor: pointer;
      }
      .badge {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        background: #eef7eb;
        color: #245c1d;
        font-size: 0.8rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }
      .error {
        background: #fff1ee;
        color: #932f1d;
        padding: 14px 16px;
        border-radius: 16px;
        margin-bottom: 16px;
        border: 1px solid rgba(147, 47, 29, 0.15);
      }
      ul {
        padding-left: 18px;
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <div class="eyebrow">Workbook ingestion</div>
        <h1>Load a workbook into the immutable source database.</h1>
        <p>
          Upload an Excel workbook, let the server normalize its sheets, and inspect the immutable source dataset summary that will seed the rest of the system.
        </p>
      </section>
      <section class="grid">
        <form class="panel" method="post" action="/imports" enctype="multipart/form-data">
          <h2>Excel workbook</h2>
          ${
            options?.errorMessage
              ? `<div class="error" role="alert">${escapeHtml(options.errorMessage)}</div>`
              : ""
          }
          <input
            id="workbookFile"
            name="workbookFile"
            type="file"
            accept=".xlsx,.xls"
            aria-label="Workbook file"
            required
          />
          <p class="hint">Upload a multi-sheet Excel workbook in xlsx or xls format. The original data will be preserved as the immutable source dataset.</p>
          <button type="submit">Import workbook</button>
        </form>
        <section class="panel" aria-live="polite">
          <h2>Import result</h2>
          ${
            model
              ? `
                <div class="badge">${escapeHtml(model.statusBadge)}</div>
                <h3>${escapeHtml(model.headline)}</h3>
                <p>${escapeHtml(model.datasetLabel)}</p>
                <p>${escapeHtml(model.totalRowsLabel)}</p>
                <p><strong>Pipeline:</strong> ${escapeHtml(model.pipelineStatusBadge)}</p>
                <p>${escapeHtml(model.pipelineVersionLabel)}</p>
                <p><strong>Clean database:</strong> ${escapeHtml(model.cleanDatabaseStatusBadge)}</p>
                <p>${escapeHtml(model.cleanDatabaseLabel)}</p>
                ${
                  model.nextRetryLabel
                    ? `<p>${escapeHtml(model.nextRetryLabel)}</p>`
                    : ""
                }
                ${
                  model.lastPipelineError
                    ? `<div class="error">${escapeHtml(model.lastPipelineError)}</div>`
                    : ""
                }
                <ul>
                  ${model.sheetBreakdown
                    .map((sheet) => `<li>${escapeHtml(sheet)}</li>`)
                    .join("")}
                </ul>
              `
              : "<p>No import has been run yet.</p>"
          }
        </section>
      </section>
    </main>
  </body>
</html>`;
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
