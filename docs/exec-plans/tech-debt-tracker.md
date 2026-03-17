# Tech Debt Tracker

Track ongoing cleanup work as small, well-scoped items.

## Current items
- Item: Define the first concrete SQL safety and validation layer for generated queries.
  Owner: unassigned
  Impact: reduces risk in the first NL-to-SQL release.
  Proposed fix: add query validation rules and execution guardrails before broadening dataset support.
  Success criteria: unsafe or ambiguous generated SQL is blocked or clearly surfaced.
  Notes: should land before the optimization loop becomes autonomous.
