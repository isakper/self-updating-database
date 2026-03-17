# AGENTS.md

<INSTRUCTIONS>
## Project overview
- Purpose: This repo is a template for starting new projects with a docs-first, agent-friendly workflow.
- Operating principle: The repository is the system of record. Prefer updating docs/checks over repeating tribal knowledge in chat.
- Default branch: `main`
- Branch prefix (agent work): Prefer `codex/` for branches created primarily by an agent.

## How to create a PR (high level)
1. Always create a new worktree, then a branch:
   - Branch naming: `codex/<short-kebab-case>` (agent) or `feature|fix|chore/<short-kebab-case>`
   - Example: `git worktree add ../wt-signup-flow -b codex/signup-flow`
2. Write unit tests first: see [docs/TESTING.md](docs/TESTING.md)
3. Write code (standards + invariants): see [docs/CODE_STANDARDS.md](docs/CODE_STANDARDS.md)
4. Update any relevant documents under `docs/` (new behavior, workflows, decisions, plans/specs).
5. Before committing, run local checks:
   - Format/lint/typecheck/tests/build: see [docs/CHECKS.md](docs/CHECKS.md)
   - Build + run + manual checks: see [docs/CHECKS.md](docs/CHECKS.md)
6. Make a commit.
7. Self-review, then open a PR.
8. After merge, remove the worktree: `git worktree remove ../wt-signup-flow`

## Docs directory map
- `ARCHITECTURE.md`: High-level system shape, boundaries, and invariants; link out to deeper docs.
- `docs/DESIGN.md`: Product/UX principles and interaction guidelines.
- `docs/FRONTEND.md`: Frontend conventions (structure, state, performance, testing).
- `docs/PLANS.md`: How to write and track execution plans.
- `docs/PRODUCT_SENSE.md`: How to reason about scope, tradeoffs, and user impact.
- `docs/QUALITY_SCORE.md`: Definition of quality and the dimensions we measure.
- `docs/RELIABILITY.md`: SLOs/SLIs, incident practices, and operational expectations.
- `docs/SECURITY.md`: Threat model notes and baseline security policy.
- `docs/TESTING.md`: Testing philosophy and how to write/run tests.
- `docs/CODE_STANDARDS.md`: Code standards, layering, error-handling, and “taste” rules.
- `docs/CHECKS.md`: How to build/run the code and the local validation/manual checks before PR.
- `docs/design-docs/index.md`: Index of design docs; add new decisions here.
- `docs/design-docs/core-beliefs.md`: Durable principles that guide decisions.
- `docs/product-specs/index.md`: Index of product specs; add new specs here.
- `docs/product-specs/new-user-onboarding.md`: Example spec template for onboarding flows.
- `docs/exec-plans/tech-debt-tracker.md`: Lightweight list of tech-debt items and cleanup candidates.
- `docs/exec-plans/active/`: Active execution plans (one file per initiative).
- `docs/exec-plans/completed/`: Completed execution plans (move here on done).
- `docs/generated/db-schema.md`: Generated artifacts; keep human-written notes elsewhere.
- `docs/references/`: Long-lived reference notes aimed at agents (deployment/tooling/design system).

## House rules (defaults)
- Ask when requirements are ambiguous; prefer clarifying in docs once.
- Add tests for new behavior when feasible; avoid untested “drive-by” changes.
- Do not add dependencies without explicit approval in the PR description.
- Keep diffs small and scoped; avoid reformatting unrelated code.
</INSTRUCTIONS>
