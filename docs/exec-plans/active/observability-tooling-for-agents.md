# Execution Plan: Observability Tooling (Agent-Queryable Logs/Metrics/Traces)

Updated: 2026-02-18

## Objective
Make runtime behavior directly legible to agents by documenting a local, worktree-isolated observability workflow for logs, metrics, and traces.

## Non-goals
- Operating a production observability platform from this template.
- Mandating a single vendor or stack for every project.

## Scope
- Add docs that explain:
  - How to run the app with logs/metrics/traces enabled locally
  - How to query logs and metrics in an agent-friendly way (examples, common queries)
  - How to keep observability isolated per worktree (ephemeral, teardown on completion)
- Define performance and reliability checks that become possible with this context:
  - Startup time budgets
  - “No span in critical journeys exceeds X”
  - Error-rate sanity checks

## Proposed doc(s)
- `docs/OBSERVABILITY.md`:
  - Logs: where they go, how to query, what fields exist
  - Metrics: naming conventions, how to query, what dashboards matter
  - Traces: how to view spans, what attributes are required
  - Local stack: how to bring up and tear down (per worktree)

## Proposed invariants (v1)
- Logs are structured (JSON or key/value) and include:
  - `service`, `env`, `version` (or git SHA), and request correlation id where applicable
- Metrics expose at least:
  - request duration histogram (or equivalent)
  - error counter (by route/code where applicable)
  - process uptime / restarts (if applicable)
- Traces exist for the “critical user journeys” (even if sampling is 100% locally).
- Local observability can be started/stopped without affecting other worktrees.

## Milestones
1. Pick a “default local observability stack” option and document it as the recommended path.
2. Document LogQL/PromQL-style query examples (or the equivalent for the chosen stack).
3. Add a small “performance budgets” section (startup, key endpoints/journeys).
4. Integrate a minimal manual checklist into `docs/CHECKS.md`:
   - confirm no obvious errors in logs
   - confirm a key metric moves during a core flow
   - confirm a trace exists for a core flow

## Validation
- A new contributor can follow docs to:
  - Start the app plus local observability
  - Run one core flow
  - Query logs for errors
  - Query a latency metric
  - View a trace and verify span durations

## Rollout
- Docs first.
- Add optional CI validation later (e.g. “startup < 800ms” gate) once stable and low-noise.

## Risks
- Overly heavy local stack increases friction: keep the default lightweight and optional.
- Query language mismatch across stacks: document the recommended path, keep alternates short.

## Decision log
- 2026-02-16: Add a plan to document agent-queryable observability (logs/metrics/traces) with per-worktree isolation.

## Progress log
- 2026-02-16: Plan created.
- 2026-02-18: Added `docs/OBSERVABILITY.md` and updated `docs/CHECKS.md` manual checks.
