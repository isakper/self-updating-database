import type {
  NaturalLanguageQueryResponse,
  QueryExecutionLog,
  WorkbookImportSummary,
} from "../../../../packages/shared/src/index.js";
import { buildUploadWorkspaceModel } from "./model.js";
import { buildQueryWorkspaceModel } from "../query-workspace/model.js";

export function renderUploadWorkspacePage(options?: {
  importSummary?: WorkbookImportSummary;
  queryLogs?: QueryExecutionLog[];
  errorMessage?: string;
  queryErrorMessage?: string;
  queryPrompt?: string;
  queryResponse?: NaturalLanguageQueryResponse;
}): string {
  const sourceDatasetId = options?.importSummary?.sourceDatasetId ?? null;
  const fragments = renderWorkspaceFragments(options);

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
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
      textarea {
        width: 100%;
        min-height: 128px;
        border-radius: 18px;
        border: 1px solid rgba(28, 26, 23, 0.12);
        padding: 16px;
        font: 0.95rem/1.5 "IBM Plex Sans", "Segoe UI", sans-serif;
        box-sizing: border-box;
        background: #fcfaf5;
        resize: vertical;
      }
      pre {
        overflow-x: auto;
        background: #1e1a14;
        color: #f8f1e3;
        border-radius: 18px;
        padding: 16px;
        font: 0.9rem/1.5 "IBM Plex Mono", "SFMono-Regular", monospace;
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      th, td {
        text-align: left;
        border-bottom: 1px solid rgba(28, 26, 23, 0.08);
        padding: 10px 12px;
        vertical-align: top;
      }
      th {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #6e5f48;
      }
      details {
        margin-top: 16px;
      }
      summary {
        cursor: pointer;
        font-weight: 700;
      }
      .log-list {
        display: grid;
        gap: 12px;
        margin-top: 16px;
      }
      .log-card {
        border: 1px solid rgba(28, 26, 23, 0.08);
        border-radius: 16px;
        padding: 14px 16px;
        background: #fcfaf5;
      }
      .stream-panel {
        margin-top: 24px;
      }
      .stream-output {
        min-height: 120px;
        max-height: 320px;
        overflow: auto;
        white-space: pre-wrap;
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
          <div id="import-result-root">${fragments.importResultHtml}</div>
        </section>
      </section>
      <div id="query-workspace-root">${fragments.queryWorkspaceHtml}</div>
      <div id="query-logs-root">${fragments.queryLogsHtml}</div>
    </main>
    ${
      sourceDatasetId
        ? `<script>
            (() => {
              const datasetId = ${JSON.stringify(sourceDatasetId)};
              let refreshTimer = null;
              let refreshInFlight = false;
              let queryStreamActive = false;

              function getPipelineOutput() {
                return document.getElementById("pipeline-stream-output");
              }

              function getQueryOutput() {
                return document.getElementById("query-stream-output");
              }

              function appendOutput(target, message) {
                if (!target || !message) {
                  return;
                }

                const normalizedMessage = String(message);

                if (target.textContent === "Waiting for pipeline output..." || target.textContent === "Waiting for query output...") {
                  target.textContent = "";
                }

                target.textContent += normalizedMessage;
                target.scrollTop = target.scrollHeight;
              }

              function replaceFragment(rootId, html, streamId) {
                const root = document.getElementById(rootId);

                if (!root) {
                  return;
                }

                const previousStreamText = streamId
                  ? document.getElementById(streamId)?.textContent ?? ""
                  : "";

                root.innerHTML = html;

                if (!streamId) {
                  return;
                }

                const nextStreamOutput = document.getElementById(streamId);

                if (
                  nextStreamOutput &&
                  previousStreamText &&
                  previousStreamText !== "Waiting for pipeline output..." &&
                  previousStreamText !== "Waiting for query output..."
                ) {
                  nextStreamOutput.textContent = previousStreamText;
                }
              }

              function shouldRefreshQueryWorkspace(html) {
                const queryWorkspaceRoot = document.getElementById('query-workspace-root');

                if (!queryWorkspaceRoot) {
                  return false;
                }

                if (!queryWorkspaceRoot.innerHTML.trim()) {
                  return true;
                }

                if (!html.trim()) {
                  return false;
                }

                return !queryWorkspaceRoot.textContent?.includes('Generated SQL');
              }

              async function refreshViewState() {
                if (refreshInFlight) {
                  return;
                }

                refreshInFlight = true;

                try {
                  const response = await fetch('/imports/' + datasetId + '/view-state', {
                    headers: {
                      accept: 'application/json',
                    },
                  });

                  if (!response.ok) {
                    return;
                  }

                  const payload = await response.json();
                  replaceFragment(
                    'import-result-root',
                    payload.importResultHtml,
                    'pipeline-stream-output'
                  );
                  if (shouldRefreshQueryWorkspace(payload.queryWorkspaceHtml)) {
                    replaceFragment(
                      'query-workspace-root',
                      payload.queryWorkspaceHtml,
                      'query-stream-output'
                    );
                  }
                  replaceFragment('query-logs-root', payload.queryLogsHtml);
                } finally {
                  refreshInFlight = false;
                }
              }

              function scheduleRefreshViewState() {
                if (refreshTimer !== null) {
                  return;
                }

                refreshTimer = window.setTimeout(() => {
                  refreshTimer = null;
                  void refreshViewState();
                }, 250);
              }

              const eventSource = new EventSource('/events/' + datasetId);
              eventSource.addEventListener('codex-run', (event) => {
                const payload = JSON.parse(event.data);
                const target =
                  payload.scope === 'pipeline'
                    ? getPipelineOutput()
                    : payload.scope === 'query' && queryStreamActive
                      ? getQueryOutput()
                      : null;
                appendOutput(target, payload.message);

                if (payload.stream === 'system') {
                  scheduleRefreshViewState();
                }
              });

              document.addEventListener('submit', async (event) => {
                const queryForm =
                  event.target instanceof HTMLFormElement &&
                  event.target.id === 'query-form'
                    ? event.target
                    : null;

                if (!queryForm) {
                  return;
                }

                event.preventDefault();
                const queryTextarea = queryForm.querySelector('textarea[name="prompt"]');

                if (!queryTextarea?.value.trim()) {
                  return;
                }

                const queryButton = queryForm.querySelector('button[type="submit"]');

                if (queryButton) {
                  queryButton.disabled = true;
                  queryButton.textContent = 'Running query...';
                }

                const queryOutput = getQueryOutput();

                if (queryOutput) {
                  queryOutput.textContent = 'Generating SQL...\\n';
                }
                queryStreamActive = true;

                try {
                  const formData = new URLSearchParams(new FormData(queryForm));
                  const response = await fetch('/imports/' + datasetId + '/query-partial', {
                    body: formData,
                    headers: {
                      'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
                    },
                    method: 'POST',
                  });
                  const payload = await response.json();

                  replaceFragment(
                    'query-workspace-root',
                    payload.queryWorkspaceHtml,
                    'query-stream-output'
                  );
                  replaceFragment('query-logs-root', payload.queryLogsHtml);
                  scheduleRefreshViewState();
                } finally {
                  queryStreamActive = false;
                }
              });
            })();
          </script>`
        : ""
    }
  </body>
</html>`;
}

export function renderWorkspaceFragments(options?: {
  importSummary?: WorkbookImportSummary;
  queryLogs?: QueryExecutionLog[];
  queryErrorMessage?: string;
  queryPrompt?: string;
  queryResponse?: NaturalLanguageQueryResponse;
}): {
  importResultHtml: string;
  queryLogsHtml: string;
  queryWorkspaceHtml: string;
} {
  const queryWorkspaceOptions = {
    ...(options?.importSummary ? { importSummary: options.importSummary } : {}),
    ...(options?.queryErrorMessage !== undefined
      ? { queryErrorMessage: options.queryErrorMessage }
      : {}),
    ...(options?.queryPrompt !== undefined
      ? { queryPrompt: options.queryPrompt }
      : {}),
    ...(options?.queryResponse ? { queryResponse: options.queryResponse } : {}),
  };

  return {
    importResultHtml: renderImportResultHtml(
      options?.importSummary,
      options?.queryLogs ?? []
    ),
    queryLogsHtml: renderQueryLogsHtml(
      options?.importSummary,
      options?.queryLogs ?? []
    ),
    queryWorkspaceHtml: renderQueryWorkspaceHtml(queryWorkspaceOptions),
  };
}

function renderImportResultHtml(
  importSummary: WorkbookImportSummary | undefined,
  queryLogs: QueryExecutionLog[]
): string {
  const model = importSummary
    ? buildUploadWorkspaceModel(importSummary, queryLogs)
    : undefined;

  if (!model) {
    return "<p>No import has been run yet.</p>";
  }

  if (!importSummary) {
    return "<p>No import has been run yet.</p>";
  }

  const populatedImportSummary = importSummary;

  return `
    <div class="badge">${escapeHtml(model.statusBadge)}</div>
    <h3>${escapeHtml(model.headline)}</h3>
    <p>${escapeHtml(model.datasetLabel)}</p>
    <p>${escapeHtml(model.totalRowsLabel)}</p>
    <p><strong>Pipeline:</strong> ${escapeHtml(model.pipelineStatusBadge)}</p>
    <p>${escapeHtml(model.pipelineVersionLabel)}</p>
    <p><strong>Clean database:</strong> ${escapeHtml(model.cleanDatabaseStatusBadge)}</p>
    <p>${escapeHtml(model.cleanDatabaseLabel)}</p>
    ${model.nextRetryLabel ? `<p>${escapeHtml(model.nextRetryLabel)}</p>` : ""}
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
    ${
      populatedImportSummary.processing.pipelineVersion
        ? `
          <details>
            <summary>Codex pipeline summary</summary>
            <p>${escapeHtml(populatedImportSummary.processing.pipelineVersion.summaryMarkdown)}</p>
          </details>
          <details>
            <summary>Codex findings (${populatedImportSummary.processing.pipelineVersion.analysisJson.findings.length})</summary>
            <ul>
              ${populatedImportSummary.processing.pipelineVersion.analysisJson.findings
                .map(
                  (finding) =>
                    `<li>${escapeHtml(`${finding.kind}: ${finding.message} -> ${finding.proposedFix} (${finding.confidence})`)}</li>`
                )
                .join("")}
            </ul>
          </details>
          <details>
            <summary>Codex prompt</summary>
            <pre>${escapeHtml(populatedImportSummary.processing.pipelineVersion.promptMarkdown)}</pre>
          </details>
          <details>
            <summary>Pipeline SQL</summary>
            <pre>${escapeHtml(populatedImportSummary.processing.pipelineVersion.sqlText)}</pre>
          </details>
        `
        : ""
    }
    <details class="stream-panel" open>
      <summary>Live pipeline CLI output</summary>
      <pre id="pipeline-stream-output" class="stream-output">Waiting for pipeline output...</pre>
    </details>
  `;
}

function renderQueryWorkspaceHtml(options: {
  importSummary?: WorkbookImportSummary;
  queryErrorMessage?: string;
  queryPrompt?: string;
  queryResponse?: NaturalLanguageQueryResponse;
}): string {
  const importSummary = options.importSummary;

  if (importSummary?.processing.cleanDatabaseStatus !== "succeeded") {
    return "";
  }

  const readyImportSummary = importSummary;

  const queryModel =
    options.queryPrompt || options.queryResponse || options.queryErrorMessage
      ? buildQueryWorkspaceModel({
          prompt: options.queryPrompt ?? "",
          ...(options.queryErrorMessage !== undefined
            ? { queryErrorMessage: options.queryErrorMessage }
            : {}),
          ...(options.queryResponse
            ? { queryResponse: options.queryResponse }
            : {}),
        })
      : null;

  return `
    <section class="grid" style="margin-top: 24px;">
      <form
        id="query-form"
        class="panel"
        method="post"
        action="/imports/${escapeHtml(readyImportSummary.sourceDatasetId)}/query"
      >
        <h2>Natural-language query</h2>
        <label for="queryPrompt">Ask one question against the clean database.</label>
        <textarea
          id="queryPrompt"
          name="prompt"
          aria-label="Query prompt"
          placeholder="Show total revenue by region"
          required
        >${escapeHtml(options.queryPrompt ?? "")}</textarea>
        ${
          options.queryErrorMessage && !queryModel?.errorMessage
            ? `<div class="error" role="alert">${escapeHtml(options.queryErrorMessage)}</div>`
            : ""
        }
        <button type="submit">Run query</button>
        <details class="stream-panel" open>
          <summary>Live SQL generation</summary>
          <pre id="query-stream-output" class="stream-output">Waiting for query output...</pre>
        </details>
      </form>
      <section class="panel" aria-live="polite">
        <h2>Query result</h2>
        ${
          queryModel
            ? `
                <p>${escapeHtml(queryModel.queryLogLabel)}</p>
                <p>${escapeHtml(queryModel.timingLabel)}</p>
                <p>${escapeHtml(queryModel.rowCountLabel)}</p>
                ${
                  queryModel.errorMessage
                    ? `<div class="error" role="alert">${escapeHtml(queryModel.errorMessage)}</div>`
                    : ""
                }
                ${
                  queryModel.summaryMarkdown
                    ? `<p>${escapeHtml(queryModel.summaryMarkdown)}</p>`
                    : ""
                }
                ${
                  queryModel.generatedSql
                    ? `<h3>Generated SQL</h3><pre>${escapeHtml(queryModel.generatedSql)}</pre>`
                    : "<p>No query has been run yet.</p>"
                }
                ${
                  queryModel.resultColumnNames.length > 0
                    ? renderQueryResultTable(
                        queryModel.resultColumnNames,
                        queryModel.resultRows
                      )
                    : ""
                }
              `
            : "<p>No query has been run yet.</p>"
        }
      </section>
    </section>
  `;
}

function renderQueryLogsHtml(
  importSummary: WorkbookImportSummary | undefined,
  queryLogs: QueryExecutionLog[]
): string {
  const model = importSummary
    ? buildUploadWorkspaceModel(importSummary, queryLogs)
    : undefined;

  if (!model || model.latestQueryLogs.length === 0) {
    return "";
  }

  return `
    <section class="panel" style="margin-top: 24px;">
      <h2>Recent query logs</h2>
      <div class="log-list">
        ${model.latestQueryLogs
          .map(
            (queryLog) => `
              <div class="log-card">
                <strong>${escapeHtml(queryLog.queryLogLabel)}</strong>
                <p>${escapeHtml(queryLog.prompt)}</p>
                <p>${escapeHtml(queryLog.rowCountLabel)} · ${escapeHtml(queryLog.timingLabel)}</p>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderQueryResultTable(
  columnNames: string[],
  rows: string[][]
): string {
  return `
    <h3>Result rows</h3>
    <table>
      <thead>
        <tr>
          ${columnNames.map((columnName) => `<th>${escapeHtml(columnName)}</th>`).join("")}
        </tr>
      </thead>
      <tbody>
        ${
          rows.length > 0
            ? rows
                .map(
                  (row) =>
                    `<tr>${row.map((value) => `<td>${escapeHtml(value)}</td>`).join("")}</tr>`
                )
                .join("")
            : `<tr><td colspan="${columnNames.length}">Query returned no rows.</td></tr>`
        }
      </tbody>
    </table>
  `;
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
