# Testing

Last reviewed: 2026-02-23


## Default stance
- Write the test first when practical, especially for bugs and boundary behavior.
- Prefer deterministic tests that run fast.
- Test behavior and invariants, not implementation details.

## Coverage
- Target: 100% coverage for both Python and TypeScript.
- If a line is truly untestable, document the reason inline and in the PR description.

## Test taxonomy
- Unit tests: pure logic, no network, no real DB.
- Integration tests: real DB or external services (local containers or fakes).
- E2E/UI tests: optional, only for critical user journeys when stable.

## Where tests go
- Python: `tests/` mirroring the package/module structure.
- TypeScript: `src/**/__tests__` or `tests/` (choose one and keep it consistent).

## Tooling (recommended for agents)
- Python: `pytest`
- TypeScript: `vitest` (fast, watch mode, good TS ergonomics)

## How to run tests
Document the canonical commands for this repo here and keep them stable.

Examples (pick the actual ones and replace these):
- Python: `pytest`
- TypeScript: `npm test` or `pnpm test`
- Coverage:
  - Python: `pytest --cov --cov-report=term-missing`
  - TypeScript: `vitest run --coverage`

## What to test (checklist)
- Happy path behavior
- Boundary conditions (empty/0/null/large)
- Error handling and user-visible messages
- Security-sensitive checks (authz, input validation) when relevant

## Boundary validation
- Boundary parsing should be exercised in tests, not just implicitly by runtime paths.
- Add explicit tests for invalid inputs, missing fields, and type mismatches at boundaries.
- If runtime validation is automatic (e.g., schema parsing), still add tests that prove the validator is wired correctly.

## Manual UI validation (Chrome DevTools)
Use Chrome DevTools to manually validate UI changes when relevant:
- Open the app in a fresh worktree instance.
- Use DevTools to inspect DOM, console, and network behavior.
- Capture a “before” snapshot (DOM + screenshot) for the critical flow.
- Trigger the UI path and capture an “after” snapshot.
- Verify there are no console errors and no failed network requests.
