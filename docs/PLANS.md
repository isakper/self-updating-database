# Plans

Last reviewed: 2026-03-17

How execution plans are written and tracked for the self-updating database.

## Where things live

- Active plans: `docs/exec-plans/active/`
- Completed plans: `docs/exec-plans/completed/`
- Tech debt: `docs/exec-plans/tech-debt-tracker.md`

## Plan template

```markdown
# <Plan Title>

Updated: YYYY-MM-DD

## Objective

<exact deliverable and success criteria>

## Non-goals

<explicit guardrails for what must not change>

## Milestones

- <step 1 with clear output>
- <step 2 with clear output>

## Validation

<commands or checks to verify correctness>

## Rollout

<how to stage changes safely>

## Risks

- <likely failure modes>
- <what to do if blocked>
```

## What good plans look like here

- State whether the plan touches source DB ingestion, optimized DB rebuilds, query execution, clustering, or optimization.
- Call out invariants explicitly, especially source-database immutability and pipeline auditability.
- Make it clear whether a change is TypeScript product work or transitional repo-maintenance work.
- Include validation that proves the upload-to-query loop still works.

## Example plan themes

- Introduce workbook ingestion and provenance tracking.
- Add the first stored transformation pipeline format.
- Implement natural-language query execution with logged SQL output.
- Add query clustering and optimization trigger thresholds.
- Migrate transitional Python doc tooling into TypeScript-native workspace tooling.
