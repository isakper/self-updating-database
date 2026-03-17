# Tech Debt Tracker

Track ongoing cleanup work as small, well-scoped items.

## Current items
- Item: Replace transitional Python repo-maintenance scripts with TypeScript-native workspace tooling.
  Owner: unassigned
  Impact: aligns maintenance workflows with the intended product stack.
  Proposed fix: migrate doc checks and architecture checks into the TypeScript workspace.
  Success criteria: primary repo validation runs through workspace commands.
  Notes: preserve current doc-lint behavior while migrating.

- Item: Define the first concrete SQL safety and validation layer for generated queries.
  Owner: unassigned
  Impact: reduces risk in the first NL-to-SQL release.
  Proposed fix: add query validation rules and execution guardrails before broadening dataset support.
  Success criteria: unsafe or ambiguous generated SQL is blocked or clearly surfaced.
  Notes: should land before the optimization loop becomes autonomous.
