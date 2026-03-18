# Tech Debt Tracker

Track ongoing cleanup work as small, well-scoped items.

## Current items

- Item: Expose pipeline artifacts and retry controls in the UI.
  Owner: unassigned
  Impact: makes Codex-generated cleanup behavior easier to inspect and operate after import.
  Proposed fix: add an operator view for `pipeline.sql`, `analysis.json`, `summary.md`, and a manual rerun control for failed or outdated pipeline versions.
  Success criteria: an operator can inspect the generated pipeline and trigger a rerun without going through the API or database directly.
  Notes: should land before broader rollout of automatic cleanup to real user data.

- Item: Expand clean-pipeline validation coverage beyond the happy path.
  Owner: unassigned
  Impact: reduces risk as workbook complexity increases.
  Proposed fix: add fixtures and manual checks for inconsistent dates, mixed types, spelling normalization, and larger multi-sheet datasets.
  Success criteria: representative messy workbooks produce explainable findings and valid clean databases.
  Notes: should happen before query execution begins to rely on cleaned schemas as the default target.
