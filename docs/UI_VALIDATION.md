# UI Validation Loop

This doc defines the standard UI validation loop for the TypeScript web app.

## When to use
- Any upload, query workspace, history, or optimization insights UI change.
- Any bug reproduction or verification via DevTools or MCP tooling.

## Required artifacts
- Screenshot
- DOM snapshot or HTML dump
- Console log excerpt
- Network failures

Recommended storage location: `.artifacts/ui/` within the active worktree.

## Validation loop
1. Start the web app and API for the current worktree.
2. Open the deterministic route for the target workflow.
3. Capture a before snapshot set.
4. Run the target flow, such as workbook upload or natural-language query execution.
5. Capture an after snapshot set.
6. Verify query results, status states, and diagnostics surfaces.

## Definition of clean
- No new console errors were introduced.
- No new failed network requests appear.
- The UI reflects the intended upload or query state.
- Generated SQL and execution metadata appear when expected.

## Critical user journeys
- Upload a workbook and verify sheet discovery.
- Wait for optimized query database readiness.
- Run a natural-language query and inspect generated SQL.
- View query history for the just-executed query.
- Inspect optimization insights after cluster data exists.
