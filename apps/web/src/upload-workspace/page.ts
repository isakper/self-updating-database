import type {
  NaturalLanguageQueryResponse,
  QueryExecutionLog,
  WorkbookImportSummary,
} from "../../../../packages/shared/src/index.js";
import { buildUploadWorkspaceModel } from "./model.js";
import { buildQueryWorkspaceModel } from "../query-workspace/model.js";

type WorkspaceTabName = "upload" | "query" | "logs";

export function renderUploadWorkspacePage(options?: {
  activeTab?: WorkspaceTabName;
  importSummary?: WorkbookImportSummary;
  queryLogs?: QueryExecutionLog[];
  errorMessage?: string;
  queryErrorMessage?: string;
  queryLogsErrorMessage?: string;
  queryPrompt?: string;
  queryResponse?: NaturalLanguageQueryResponse;
}): string {
  const sourceDatasetId = options?.importSummary?.sourceDatasetId ?? null;
  const activeTab = options?.activeTab ?? "upload";
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
        min-height: 100vh;
        background:
          radial-gradient(circle at top, rgba(214, 177, 91, 0.22), transparent 28%),
          linear-gradient(180deg, #f6f1e7 0%, #efe7d8 100%);
      }
      main {
        max-width: none;
        margin: 0 auto;
        padding: 0;
      }
      .hero {
        display: none;
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
      .workspace-shell {
        display: grid;
        gap: 0;
        grid-template-columns: 320px minmax(0, 1fr);
        min-height: 100vh;
      }
      .workspace-nav {
        min-height: 100vh;
        padding: 32px 24px;
        background: linear-gradient(180deg, rgba(28, 26, 23, 0.96) 0%, rgba(42, 35, 24, 0.96) 100%);
        color: #fff8eb;
      }
      .nav-card {
        position: sticky;
        top: 24px;
        display: grid;
        gap: 18px;
      }
      .nav-card h2 {
        margin: 0;
        font-size: 2.3rem;
        line-height: 0.95;
      }
      .nav-card p {
        margin: 0;
        max-width: 20ch;
        color: rgba(255, 248, 235, 0.72);
      }
      .nav-list {
        display: grid;
        gap: 12px;
      }
      .nav-tab {
        width: 100%;
        margin: 0;
        text-align: left;
        border-radius: 22px;
        padding: 18px;
        background: rgba(255, 248, 235, 0.08);
        color: #fff8eb;
        border: 1px solid rgba(255, 248, 235, 0.12);
      }
      .nav-tab[data-active="true"] {
        background: #fff4da;
        color: #231d15;
        border-color: transparent;
      }
      .nav-tab-label {
        display: block;
        font-size: 1.05rem;
      }
      .nav-tab-detail {
        display: block;
        margin-top: 6px;
        font-size: 0.85rem;
        font-weight: 500;
        opacity: 0.86;
      }
      .workspace-content {
        min-width: 0;
        padding: 40px 32px 80px;
      }
      .tab-panel {
        display: none;
      }
      .tab-panel[data-active="true"] {
        display: block;
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
      .tab-intro {
        margin-bottom: 20px;
      }
      .tab-intro h2 {
        margin-bottom: 8px;
      }
      .stack {
        display: grid;
        gap: 24px;
      }
      .logs-table-wrap {
        overflow-x: auto;
      }
      .logs-table td pre {
        margin: 0;
        min-width: 260px;
        max-width: 420px;
        max-height: 180px;
      }
      @media (max-width: 900px) {
        .workspace-shell {
          grid-template-columns: 1fr;
        }
        .workspace-nav {
          min-height: auto;
        }
        .workspace-content {
          padding: 24px 20px 64px;
        }
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
      <section class="workspace-shell">
        <aside class="workspace-nav">
          <div class="nav-card">
            <h2>Workspace</h2>
            <p>Move through the product story step by step instead of reading one giant page.</p>
            <div class="nav-list" role="tablist" aria-label="Workspace tabs">
              <button
                class="nav-tab"
                data-active="${String(activeTab === "upload")}"
                data-tab-button="upload"
                id="tab-button-upload"
                role="tab"
                aria-selected="${String(activeTab === "upload")}"
                aria-controls="tab-panel-upload"
              >
                <span class="nav-tab-label">Upload Excel + Build Clean Database</span>
                <span class="nav-tab-detail">Import the workbook, inspect the first Codex cleanup step, and watch the clean database build.</span>
              </button>
              <button
                class="nav-tab"
                data-active="${String(activeTab === "query")}"
                data-tab-button="query"
                id="tab-button-query"
                role="tab"
                aria-selected="${String(activeTab === "query")}"
                aria-controls="tab-panel-query"
              >
                <span class="nav-tab-label">Ask Questions in Plain English</span>
                <span class="nav-tab-detail">Type a question, stream the generated SQL, and review the returned table.</span>
              </button>
              <button
                class="nav-tab"
                data-active="${String(activeTab === "logs")}"
                data-tab-button="logs"
                id="tab-button-logs"
                role="tab"
                aria-selected="${String(activeTab === "logs")}"
                aria-controls="tab-panel-logs"
              >
                <span class="nav-tab-label">Query History + SQL Logs</span>
                <span class="nav-tab-detail">Inspect previous prompts, generated SQL, timing, and execution outcome in one place.</span>
              </button>
            </div>
          </div>
        </aside>
        <div class="workspace-content">
          <section
            class="tab-panel"
            data-active="${String(activeTab === "upload")}"
            data-tab-panel="upload"
            id="tab-panel-upload"
            role="tabpanel"
            aria-labelledby="tab-button-upload"
          >
            <section class="stack">
              <form class="panel" method="post" action="/imports" enctype="multipart/form-data">
                <h2>Upload Excel as DB</h2>
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
                <button type="submit">Import workbook</button>
              </form>
              <section class="panel">
                <h2>Live Codex CLI output</h2>
                <pre id="pipeline-stream-output" class="stream-output">Waiting for pipeline output...</pre>
              </section>
              <section class="panel" aria-live="polite">
                <h2>Generated Pipeline</h2>
                <div id="import-result-root">${fragments.importResultHtml}</div>
              </section>
            </section>
          </section>
          <section
            class="tab-panel"
            data-active="${String(activeTab === "query")}"
            data-tab-panel="query"
            id="tab-panel-query"
            role="tabpanel"
            aria-labelledby="tab-button-query"
          >
            <div class="tab-intro">
              <h2>Ask questions against the clean database</h2>
              <p>Use the cleaned schema as the querying surface, stream the generated SQL, and inspect the result rows before you decide what to ask next.</p>
            </div>
            <div id="query-workspace-root">${fragments.queryWorkspaceHtml}</div>
          </section>
          <section
            class="tab-panel"
            data-active="${String(activeTab === "logs")}"
            data-tab-panel="logs"
            id="tab-panel-logs"
            role="tabpanel"
            aria-labelledby="tab-button-logs"
          >
            <div class="tab-intro">
              <h2>Inspect query history and generated SQL</h2>
              <p>Use this view during demos to show what has already been asked, what SQL was generated, how long it took, and whether the query succeeded.</p>
            </div>
            <div id="query-logs-root">${fragments.queryLogsHtml}</div>
          </section>
        </div>
      </section>
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

              function setActiveTab(tabName) {
                document.querySelectorAll('[data-tab-button]').forEach((button) => {
                  const isActive = button.getAttribute('data-tab-button') === tabName;
                  button.setAttribute('data-active', String(isActive));
                  button.setAttribute('aria-selected', String(isActive));
                });

                document.querySelectorAll('[data-tab-panel]').forEach((panel) => {
                  const isActive = panel.getAttribute('data-tab-panel') === tabName;
                  panel.setAttribute('data-active', String(isActive));
                });
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

              document.querySelectorAll('[data-tab-button]').forEach((button) => {
                button.addEventListener('click', () => {
                  const tabName = button.getAttribute('data-tab-button');

                  if (tabName) {
                    setActiveTab(tabName);
                  }
                });
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

                setActiveTab('query');
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
  queryLogsErrorMessage?: string;
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
      options?.queryLogs ?? [],
      options?.queryLogsErrorMessage
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
  const pipelineVersion = populatedImportSummary.processing.pipelineVersion;

  return `
    <div class="badge">${escapeHtml(model.pipelineStatusBadge)}</div>
    <p><strong>Workbook:</strong> ${escapeHtml(populatedImportSummary.workbookName)}</p>
    <p><strong>Pipeline status:</strong> ${escapeHtml(model.pipelineStatusBadge)}</p>
    ${pipelineVersion ? `<p><strong>Pipeline version:</strong> ${escapeHtml(pipelineVersion.pipelineVersionId)}</p>` : "<p>Pipeline version pending.</p>"}
    ${
      model.lastPipelineError
        ? `<div class="error">${escapeHtml(model.lastPipelineError)}</div>`
        : ""
    }
    ${
      pipelineVersion
        ? `
          <details>
            <summary>Pipeline SQL</summary>
            <pre>${escapeHtml(pipelineVersion.sqlText)}</pre>
          </details>
          <details open>
            <summary>Codex findings (${pipelineVersion.analysisJson.findings.length})</summary>
            <ul>
              ${pipelineVersion.analysisJson.findings
                .map(
                  (finding) =>
                    `<li>${escapeHtml(`${finding.kind}: ${finding.message} -> ${finding.proposedFix} (${finding.confidence})`)}</li>`
                )
                .join("")}
            </ul>
          </details>
        `
        : "<p>Generated SQL and Codex findings will appear here when the cleanup pipeline is ready.</p>"
    }
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
    <section class="stack" style="margin-top: 24px;">
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
      </form>
      <section class="panel">
        <h2>Live SQL generation</h2>
        <pre id="query-stream-output" class="stream-output">Waiting for query output...</pre>
      </section>
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
  queryLogs: QueryExecutionLog[],
  queryLogsErrorMessage?: string
): string {
  if (!importSummary) {
    return "";
  }

  return `
    <section class="panel">
      <h2>Recent query history</h2>
      ${
        importSummary.processing.cleanDatabaseStatus === "succeeded"
          ? `
            <form
              method="post"
              action="/imports/${escapeHtml(importSummary.sourceDatasetId)}/query-logs/import"
              enctype="multipart/form-data"
            >
              ${
                queryLogsErrorMessage
                  ? `<div class="error" role="alert">${escapeHtml(queryLogsErrorMessage)}</div>`
                  : ""
              }
              <label for="queryLogsWorkbookFile">Upload mock query-log workbook.</label>
              <input
                id="queryLogsWorkbookFile"
                name="workbookFile"
                type="file"
                accept=".xlsx,.xls"
                aria-label="Mock query log workbook"
                required
              />
              <p class="hint">Use the companion demo file with the repeated SKU/day queries to seed clustering and optimization.</p>
              <button type="submit">Upload mock query logs</button>
            </form>
          `
          : '<p class="hint">Query-log imports are available after the clean database is ready.</p>'
      }
      ${
        queryLogs.length === 0
          ? "<p>No query logs have been uploaded or generated yet.</p>"
          : ""
      }
      ${
        queryLogs.length > 0
          ? `
      <div class="logs-table-wrap">
        <table class="logs-table">
          <thead>
            <tr>
              <th>Query Log</th>
              <th>Prompt</th>
              <th>Generated SQL</th>
              <th>Status</th>
              <th>Rows</th>
              <th>Timing</th>
            </tr>
          </thead>
          <tbody>
            ${queryLogs
              .map(
                (queryLog) => `
                  <tr>
                    <td>${escapeHtml(queryLog.queryLogId)}</td>
                    <td>${escapeHtml(queryLog.prompt)}</td>
                    <td>${
                      queryLog.generatedSql
                        ? `<pre>${escapeHtml(queryLog.generatedSql)}</pre>`
                        : `<span>${escapeHtml(queryLog.errorMessage ?? "No SQL generated")}</span>`
                    }</td>
                    <td>${escapeHtml(queryLog.status)}</td>
                    <td>${escapeHtml(
                      queryLog.rowCount === null
                        ? "No rows"
                        : `${queryLog.rowCount} row${queryLog.rowCount === 1 ? "" : "s"}`
                    )}</td>
                    <td>${escapeHtml(`${queryLog.totalLatencyMs}ms total`)}</td>
                  </tr>
                `
              )
              .join("")}
          </tbody>
        </table>
      </div>
      `
          : ""
      }
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
