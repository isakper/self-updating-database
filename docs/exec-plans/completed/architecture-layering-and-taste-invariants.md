# Execution Plan: Architecture Layering + Taste Invariants

Updated: 2026-02-17

## Objective
Keep an agent-heavy codebase coherent by documenting and mechanically enforcing a rigid domain architecture (layering + explicit cross-cutting boundaries) plus a small set of “taste invariants”.

## Non-goals
- Picking a specific language/framework or forcing a single folder layout across all downstream repos.
- Micromanaging implementations inside a layer beyond what’s needed for correctness and legibility.

## Scope
- Add documentation under `docs/` that specifies:
  - The layered domain model: `Types → Config → Repo → Service → Runtime → UI`
  - The cross-cutting boundary: “Providers” as the single explicit interface for auth/connectors/telemetry/feature flags
  - What imports/edges are allowed vs disallowed
  - How to introduce a new domain and where code should live
- Add mechanical enforcement via:
  - Pre-commit hooks (fast, local feedback)
  - CI jobs (authoritative, required checks)
  - Structural tests / custom linters with remediation-oriented error messages

## Proposed docs
- `ARCHITECTURE.md`: System-of-record for architecture, including the layering model and allowed/disallowed edges.
- Optional (later): a dedicated “taste invariants” doc if the list grows large or needs ownership.

## Proposed invariants (v1)
- Code inside a business domain can only depend “forward” through the fixed layers:
  - `Types → Config → Repo → Service → Runtime → UI`
- Cross-cutting concerns enter the domain through a single explicit interface:
  - `Providers` (interfaces only, implemented/wired at runtime)
- No hidden global singletons for cross-cutting concerns inside domain code.
- Data at boundaries is parsed/validated (how is flexible; requirement is not).
- Structured logging is used (fields + correlation), with a baseline format documented.

## Enforcement approach (template-friendly)
Because this repo is a template, enforcement is defined in a stack-agnostic way:
- Primary: a repo-local “structure linter” that checks path/import edges against a config file.
- Secondary: language-native enforcement (e.g. ESLint rule, Python import linter, Go analysis) as an optional enhancement in downstream repos.

## Milestones
1. Update `ARCHITECTURE.md` with:
   - The layering model and allowed/disallowed edges
   - Examples for common cross-cutting flows via `Providers`
2. Implement an architecture lint entrypoint:
   - `./scripts/lint-architecture` (language chosen per downstream repo; template ships with a stub + contract)
3. Add pre-commit hooks to run architecture lint (and other fast checks):
   - `pre-commit` framework is recommended, but keep hooks runnable without it in CI
4. Add CI job(s) that run architecture lint and fail on violations.
5. Add remediation-oriented error messages that tell an agent what to do next (move file, change import, add provider, etc.).

## Validation
- Introduce a deliberately-bad import edge; pre-commit and CI both fail with a clear error and remediation steps.
- A “new domain” can be added following the doc template and passes all checks.

## Rollout
- Land docs first.
- Add linting in warn mode locally (optional) before making it CI-required.
- Once stable, make CI gating mandatory to prevent drift.

## Risks
- Overly rigid rules slow humans down: keep invariants minimal and focus on edges/boundaries.
- False positives due to language/tooling mismatch: keep a config-driven core and adapt per stack.

## Decision log
- 2026-02-16: Add a plan to document and mechanically enforce domain layering + explicit cross-cutting boundaries, plus a small set of taste invariants.

## Progress log
- 2026-02-16: Plan created.
- 2026-02-23: Updated `ARCHITECTURE.md` with layering, providers boundary, and taste invariants.
- 2026-02-23: Added architecture lint contract doc and stub entrypoint (`docs/references/architecture-lint.md`, `scripts/lint-architecture`).
- 2026-02-23: Added pre-commit hook and CI workflow for architecture lint.
