# UI Validation Loop

This doc defines the standard UI validation loop and what “clean” means for agent-driven validation.

## When To Use
- Any UI-facing change.
- Any bug reproduction or verification via CDP/MCP.

## Required Artifacts
Capture and store these for each validation run:
- Screenshot (full page or viewport).
- DOM snapshot (or HTML dump).
- Console log excerpt for the interaction window.
- Network failures (failed requests + status codes).

Recommended storage location: `.artifacts/ui/` within the active worktree.

## Validation Loop
1. Start the app with per-worktree ports and data dirs (see `docs/CHROME_DEVTOOLS_MCP.md`).
2. Connect MCP to CDP and open `TARGET_URL`.
3. Capture a **before** snapshot set.
4. Reproduce the issue or exercise the target flow.
5. Capture an **after** snapshot set.
6. Apply the fix.
7. Re-run steps 2-5 and verify the issue is resolved.

## Definition Of “Clean”
A run is considered clean when all of the following are true:
- No new console errors were introduced by the change.
- No new failed network requests appear.
- The UI matches expected state in the screenshots.
- The DOM snapshot reflects the intended structure (no missing critical nodes).

## Critical User Journeys (Template)
List 3-5 critical flows that agents can reference. Replace these with real flows when the app exists.

- Launch app and load home page.
- Navigate to the primary feature route.
- Perform the core write action (create/update/save).
- Perform a negative path (invalid input / permission denied / missing resource).
- Sign out / session expiration (if applicable).
