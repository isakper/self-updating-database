# Execution Plan: Chrome DevTools MCP (Agent UI Legibility)

Updated: 2026-02-17

## Objective
Make UI behavior directly legible to agents by documenting and standardizing how to drive the app via Chrome DevTools Protocol (CDP) using an MCP integration.

## Non-goals
- Building a full E2E test framework for every repo by default.
- Locking in a single frontend stack (this template must remain adaptable).

## Scope
- Add a dedicated doc that explains:
  - How to start the app in a way that an agent can drive it deterministically
  - How to connect to a Chrome instance via CDP (through MCP)
  - How to capture DOM snapshots/screenshots and record runtime events
  - A standard “validation loop” agents should follow (before/after snapshots, reproduce, fix, re-run)
- Make the app bootable per git worktree (if applicable to the stack) so multiple parallel instances can run.
- Define minimal invariants for UI legibility (what must exist to support agent QA).

## Proposed doc(s)
- `docs/CHROME_DEVTOOLS_MCP.md` (how to connect, what to capture, troubleshooting)
- Optionally: `docs/UI_VALIDATION.md` (validation loop and “clean” definition)

## Proposed invariants (v1)
- The app can be launched from a fresh worktree without global state conflicts (ports, DB, caches).
- A deterministic “target URL” exists for validation (e.g. home page / a specific route).
- A small set of “critical user journeys” is listed in docs (so prompts can reference them).
- UI validation artifacts are easy to produce on demand:
  - Screenshot
  - DOM snapshot (or HTML dump)
  - Console log excerpt
  - Network failures (if applicable)

## Milestones
1. Decide the “boot per worktree” strategy (ports, data, env, storage).
2. Write `docs/CHROME_DEVTOOLS_MCP.md` with step-by-step instructions and troubleshooting.
3. Define the UI validation loop (what gets snapshotted; what “clean” means).
4. Add a minimal checklist in `docs/CHECKS.md` that references the UI validation loop (manual, not scripted).

## Validation
- A new contributor can follow docs to:
  - Launch the app locally
  - Attach CDP tooling
  - Capture a screenshot + DOM snapshot
  - Reproduce a known UI bug and verify the fix

## Rollout
- Land docs first (no gating).
- Add optional CI smoke/E2E later only if it is high-signal and stable.

## Risks
- CDP/MCP setup becomes stack/tool specific: keep instructions modular with “choose one” options.
- Flaky UI validation: keep the loop deterministic, limit reliance on timing.

## Decision log
- 2026-02-16: Add a plan to document Chrome DevTools MCP workflow to increase UI legibility for agents.

## Progress log
- 2026-02-16: Plan created.
- 2026-02-23: Added Chrome DevTools MCP docs, UI validation loop, and updated manual checks.
