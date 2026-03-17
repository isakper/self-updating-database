# AGENTS.md

<INSTRUCTIONS>
## Project overview
- Purpose: This repo documents and incrementally builds a self-updating database product that learns from repeated query patterns.
- Product idea: users upload Excel workbooks, the system imports them into an immutable source database, Codex CLI writes a transformation pipeline, and the pipeline builds a separate optimized query database for natural-language querying.
- Implementation target: TypeScript for both backend and frontend in a monorepo shape.
- Operating principle: the repository is the system of record. Prefer updating docs, plans, and checks over repeating tribal knowledge in chat.
- Default branch: `main`
- Branch prefix (agent work): prefer `codex/` for branches created primarily by an agent.

## Recommended repo architecture

- `apps/web`: frontend for workbook upload, query workspace, history/diagnostics, and optimization insights.
- `apps/api`: backend for ingestion orchestration, pipeline execution, natural-language query handling, query logging, clustering jobs, and optimization triggers.
- `packages/shared`: shared TypeScript types, schemas, and contracts.
- `packages/pipeline-sdk`: pipeline step definitions and execution utilities.
- `packages/agent-orchestrator`: Codex CLI adapter boundary and audit trail handling.
- `packages/database-core`: source DB ingestion, optimized DB build logic, clustering, and optimization services.
- `docs/`: product and engineering system-of-record.

## How to create a PR (high level)

1. Always create a new worktree, then a branch:
   - Branch naming: `codex/<short-kebab-case>` (agent) or `feature|fix|chore/<short-kebab-case>`
   - Example: `git worktree add ../wt-query-clustering -b codex/query-clustering`
2. Write tests first when practical: see [docs/TESTING.md](docs/TESTING.md)
3. Write code in the documented architecture: see [ARCHITECTURE.md](ARCHITECTURE.md) and [docs/CODE_STANDARDS.md](docs/CODE_STANDARDS.md)
4. Update any relevant documents under `docs/` when behavior, architecture, or product decisions change.
5. Before committing, run local checks:
   - Format, lint, typecheck, tests, and build: see [docs/CHECKS.md](docs/CHECKS.md)
   - Run the app and validate core flows: see [docs/CHECKS.md](docs/CHECKS.md)
6. Make a commit.
7. Self-review, then open a PR.
8. After merge, remove the worktree: `git worktree remove ../wt-query-clustering`

## Docs directory map

- `ARCHITECTURE.md`: high-level system shape, package boundaries, and domain layering.
- `docs/DESIGN.md`: product and interaction design principles for the upload-to-query workflow.
- `docs/FRONTEND.md`: frontend structure, state boundaries, and UI quality expectations.
- `docs/PLANS.md`: how to write and track execution plans for this product.
- `docs/PRODUCT_SENSE.md`: scope, prioritization, and user value rules.
- `docs/QUALITY_SCORE.md`: definition of quality for this product and how we measure it.
- `docs/RELIABILITY.md`: SLOs, operational expectations, and recovery priorities.
- `docs/SECURITY.md`: threat model and baseline security requirements.
- `docs/TESTING.md`: testing philosophy and expected coverage.
- `docs/CODE_STANDARDS.md`: code standards, layering, contracts, and failure handling.
- `docs/CHECKS.md`: build, run, and validation steps before PR.
- `docs/design-docs/index.md`: index of design docs.
- `docs/design-docs/core-beliefs.md`: durable product and engineering beliefs.
- `docs/product-specs/index.md`: index of product specs.
- `docs/product-specs/new-user-onboarding.md`: POC spec for upload, querying, logging, clustering, and optimization.
- `docs/exec-plans/tech-debt-tracker.md`: current debt items blocking product momentum.
- `docs/exec-plans/active/`: active execution plans.
- `docs/exec-plans/completed/`: completed execution plans.
- `docs/generated/db-schema.md`: generated or generated-style schema reference for source DB, optimized DB, and telemetry entities.
- `docs/references/`: long-lived reference notes aimed at agents.

## House rules (defaults)

- Preserve the immutable source database. Optimization work must happen through the stored pipeline and derived optimized database.
- Prefer TypeScript-only tooling and docs across the repo.
- Add tests for new behavior when feasible; avoid untested drive-by changes.
- Do not add dependencies without explicit approval in the PR description.
- Keep diffs small and scoped; avoid reformatting unrelated files.
- When requirements are ambiguous, resolve them in docs once so future agents inherit the decision.
  </INSTRUCTIONS>
