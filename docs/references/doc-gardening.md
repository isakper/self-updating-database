# Doc Gardening Process

Last reviewed: 2026-03-17

## Goal

Keep product docs fresh and aligned with the evolving TypeScript monorepo and self-updating database workflow.

## Signals

A doc is considered stale when any of the following is true:

- Its `Last reviewed` date is older than the threshold in [doc-integrity.md](doc-integrity.md).
- It is missing a required freshness marker.
- It uses outdated product terms or template wording.
- It contains broken relative links.

## Output

Doc-gardening produces a report file:

- `docs/reports/doc-gardening-report.md`

## Review expectations

- Confirm that docs still describe the immutable source database and derived optimized query database accurately.
- Confirm TypeScript remains the stated implementation language for frontend and backend product code.
- Convert high-priority drift into doc updates or implementation follow-ups.
