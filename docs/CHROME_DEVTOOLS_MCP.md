# Chrome DevTools MCP

This doc defines how to make the UI legible to agents by driving the app via Chrome DevTools Protocol (CDP) through an MCP integration.

## Goals
- Allow an agent to deterministically launch the app and attach CDP tooling.
- Capture UI validation artifacts on demand: screenshot, DOM snapshot, console excerpt, and network failures.
- Keep instructions stack-agnostic while still executable.

## Boot Per Worktree (Required Strategy)
Use per-worktree ports and data directories so parallel worktrees can run side-by-side without conflicts.

Recommended defaults:
- `PORT`: derived from a stable offset per worktree (see below).
- `DATA_DIR`: under the worktree (for local app data, caches, or sqlite).
- `LOG_DIR`: under the worktree (to keep console logs local).

Example strategy:
1. Derive a `WT_ID` from the worktree folder name (e.g. `wt-chrome-devtools-mcp`).
2. Map `WT_ID` to a port offset (manual mapping or a small script).
3. Export the environment before starting the app.

Example (manual mapping):
- `wt-chrome-devtools-mcp` -> `PORT=4101`
- `wt-signup-flow` -> `PORT=4102`

If the repo later adds tooling, prefer adding a small helper script (e.g. `scripts/dev-env.sh`) to compute these values and keep this doc updated.

## Deterministic Target URL
Choose a deterministic target route for validation and document it here. For this template, use:
- `TARGET_URL`: `http://localhost:$PORT/`

If your app requires auth or seed data, document the exact steps in `docs/UI_VALIDATION.md`.

## Start The App
1. Open a fresh worktree (per `AGENTS.md`).
2. Export the per-worktree environment.
3. Start the app using the canonical command in `docs/CHECKS.md`.

Example:
```bash
export PORT=4101
export DATA_DIR="$PWD/.data"
export LOG_DIR="$PWD/.logs"

# Replace with the real command from docs/CHECKS.md
<APP_RUN_COMMAND>
```

## Launch Chrome For CDP
Start a Chrome instance with remote debugging enabled.

Example:
```bash
# Choose a free port for CDP
export CDP_PORT=9222

/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=$CDP_PORT \
  --user-data-dir="$PWD/.chrome" \
  --no-first-run \
  --no-default-browser-check
```

## Connect MCP To CDP
Use your MCP Chrome DevTools integration to attach to the running Chrome instance.

Required inputs:
- `cdpUrl`: `http://127.0.0.1:$CDP_PORT`
- `targetUrl`: `http://127.0.0.1:$PORT/`

Once connected, open `TARGET_URL` in the controlled browser session.

## Capture Validation Artifacts
When validating a UI change, capture the following from the MCP-controlled session:
- Screenshot (full page or viewport, whichever is standard for your team).
- DOM snapshot or HTML dump of the target page.
- Console log excerpt covering the interaction.
- Network failures (failed requests + status codes).

Store artifacts under the worktree in a predictable location (example: `.artifacts/ui/`).

## Troubleshooting
- Port already in use: pick a new `PORT` or `CDP_PORT` and restart the app/Chrome.
- Empty or blank page: verify the app is running and `TARGET_URL` is reachable in a normal browser.
- CDP attach fails: ensure Chrome is launched with `--remote-debugging-port` and not managed by another automation tool.
- Missing DOM snapshot: ensure the target page has fully loaded before capture.

## References
- UI validation loop: `docs/UI_VALIDATION.md`
- Local run commands: `docs/CHECKS.md`
