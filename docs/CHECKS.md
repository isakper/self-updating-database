# Checks

Last reviewed: 2026-02-23


This doc defines the standard local validation loop before opening a PR.

## Canonical commands (fill these in per repo)
Document the *actual* commands for this repo here, then keep them stable.

- Format:
- Lint: `scripts/lint-architecture`
- Typecheck:
- Unit tests:
- Build:
- Run locally:

## Suggested sequence (before commit / PR)
1. Format
2. Lint + typecheck
3. Unit tests
4. Build
5. Run locally + manual checks (below)

## Run locally
Add the standard “run it” command and required env vars/secrets setup.

- Command:
- Required env:
- Seed data (if any):
- Common troubleshooting:

## Manual checks (examples)
Keep this short and focused on high-signal flows.

- App starts cleanly (no obvious errors in logs/console)
- UI validation loop completed (capture screenshot, DOM snapshot, console excerpt, network failures). See `docs/UI_VALIDATION.md`.
- One primary user flow end-to-end
- One negative path (invalid input / permission denied / missing resource)
- Performance sanity (page/API responds; no accidental N+1 obviousness)
- Observability sanity (key logs/metrics emitted; no secret leakage)
- Observability: confirm a key metric moves during a core flow
- Observability: confirm a trace exists for a core flow

## Optional: single entrypoint script
If you want a one-liner, keep it as a convenience (not the source of truth) and ensure it matches this doc.
