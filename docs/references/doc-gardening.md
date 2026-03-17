# Doc Gardening Process

Last reviewed: 2026-02-23

## Goal
Keep high-drift docs fresh and cross-links accurate with a recurring, low-friction maintenance loop.

## Signals
A doc is considered stale when any of the following is true:
- Its `Last reviewed` date is older than the threshold in `docs/references/doc-integrity.md`.
- It is missing a required freshness marker.
- It contains broken relative links (reported by doc-lint).

## Output
Doc-gardening produces a report file:
- `docs/reports/doc-gardening-report.md`

## PR flow
- A scheduled workflow runs `scripts/doc_gardening.py`.
- If the report changes, the workflow opens a PR labeled `doc-gardening`.
- The PR should include:
  - Updated report file
  - Suggested fixes or assignments in the report

## Review expectations
- Validate that the stale findings are accurate.
- Convert high-priority items into actionable issues or doc updates.
- Close the PR once follow-up owners are assigned (or fix inline).
