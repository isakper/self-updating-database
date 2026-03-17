# Product Sense

Last reviewed: 2026-03-17

How we reason about shipping the self-updating database product.

## North star
Help users get answers from messy spreadsheet data with less manual SQL work over time.

## Product bets
- Spreadsheet-shaped data is valuable but difficult to query once relationships span many sheets.
- Users will trust an adaptive system if the original data stays intact and every optimization is inspectable.
- Query logs can be turned into a durable learning signal for improving derived database structure.

## POC priorities
- End-to-end workbook upload through query execution.
- Visible generated SQL for natural-language requests.
- Query logging with latency and cost-oriented metadata.
- Query clustering that identifies expensive, frequent patterns.
- Pipeline revision loop driven by Codex CLI and applied to the optimized database only.

## What we optimize for
- Time from upload to first useful query.
- Reduction in repeated expensive query patterns after optimization.
- User trust in generated SQL and optimization behavior.
- Clear operational debugging for ingestion, query execution, and rebuild failures.

## Scope discipline
- Keep the source database immutable.
- Prefer transparent derived structures over opaque magic behavior.
- Start with one primary agent engine: Codex CLI.
- Avoid adding broad automation or approval workflows until the core learning loop is observable.

## Not yet in scope
- Multi-agent orchestration across many model providers.
- Fully autonomous production optimization without operator review.
- Rich dashboarding beyond what is needed to validate the learning loop.
