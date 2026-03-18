# Tech Debt Tracker

Track ongoing cleanup work as small, well-scoped items.

## Current items

- Item: Separate live query/pipeline events from historical run output.
  Owner: unassigned
  Impact: makes the demo UI easier to trust during live walkthroughs.
  Proposed fix: split active-run streaming panels from persisted event history so a new query only shows its own SQL generation and outcome.
  Success criteria: an operator can distinguish the current live run from older events without reading timestamps closely.
  Notes: should land before polishing the demo UI.

- Item: Add manual rerun controls and clearer operator recovery flows.
  Owner: unassigned
  Impact: makes Codex-generated cleanup behavior easier to inspect and operate after import.
  Proposed fix: add a manual rerun control for failed or outdated pipeline versions and surface recovery guidance directly in the dataset page.
  Success criteria: an operator can trigger a rerun without going through the API or database directly.
  Notes: should land before broader rollout of automatic cleanup to real user data.

- Item: Expand clean-pipeline validation coverage beyond the happy path.
  Owner: unassigned
  Impact: reduces risk as workbook complexity increases.
  Proposed fix: add fixtures and manual checks for inconsistent dates, mixed types, spelling normalization, and larger multi-sheet datasets.
  Success criteria: representative messy workbooks produce explainable findings and valid clean databases.
  Notes: should happen before query execution begins to rely on cleaned schemas as the default target.
