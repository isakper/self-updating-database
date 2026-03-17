# Chrome DevTools MCP

This doc defines how to make the TypeScript web app legible to agents through Chrome DevTools Protocol.

## Goals

- Allow an agent to deterministically launch the web app and attach DevTools tooling.
- Capture screenshots, DOM snapshots, console excerpts, and network failures for upload and query flows.
- Keep validation isolated per worktree.

## Boot per worktree

Use per-worktree ports and local directories so multiple worktrees can run side by side.

Recommended environment:

- `WEB_PORT`
- `API_PORT`
- `DATA_DIR`
- `LOG_DIR`

## Deterministic target routes

- Home or upload route: `http://127.0.0.1:$WEB_PORT/`
- Query workspace route: `http://127.0.0.1:$WEB_PORT/query`

## Start the app

1. Open a fresh worktree.
2. Export per-worktree environment.
3. Start the TypeScript workspace command documented in [docs/CHECKS.md](CHECKS.md).

## Connect DevTools tooling

- Launch Chrome with a remote debugging port.
- Attach your MCP or DevTools integration.
- Open the target route and capture validation artifacts.

## Capture expectations

- Upload state before and after import.
- Query state before and after execution.
- Generated SQL visibility.
- Network failures for API requests.

## References

- [docs/UI_VALIDATION.md](UI_VALIDATION.md)
- [docs/CHECKS.md](CHECKS.md)
