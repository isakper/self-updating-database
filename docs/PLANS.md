# Plans

Last reviewed: 2026-02-23


How execution plans are written and tracked.

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

## Agent-oriented guidance
Each plan is written to be followed by an agent without ambiguity.

- Objective: the exact deliverable and success criteria.
- Updated: last update date for the plan.
- Non-goals: explicit guardrails for what must not change.
- Milestones: small, sequential, checkable steps with clear outputs.
- Risks: likely failure modes and what to do if blocked.
- Rollout: how to stage changes safely (small diffs, flags if applicable).
- Validation: concrete commands or checks to verify correctness.
