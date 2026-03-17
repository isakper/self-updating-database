# Architecture Lint (Contract)

This template ships with a Python-based entrypoint at `scripts/lint-architecture`. Downstream repos may replace it or wire it to a stack-specific linter, but should keep the contract stable.

## Contract
A compliant architecture linter must:
- Exit non-zero on rule violations.
- Print remediation-oriented errors (what to move/change).
- Support a repo-local config file (default path below).
- Run fast enough for pre-commit and CI.

## Default Config Location
- `architecture-lint.toml` (repo root)

If the config file is missing, the linter exits successfully and prints a message explaining how to enable enforcement.

## Expected Rule Coverage (v1)
- Enforce the fixed layering edges: `Types → Config → Repo → Service → Runtime → UI`.
- Enforce `Providers` as the only cross-cutting boundary.
- Reject any backward or skipped-layer imports within a domain.

## Remediation Message Guidelines
Violations should tell an agent how to fix the issue. Example structure:
- What rule was violated.
- Where it was detected (file + import).
- Suggested fix (move file, change import, add Provider interface, wire in Runtime).

## Suggested Implementation Paths
Pick the approach that fits the stack (preserve the contract and config semantics):
- TypeScript: ESLint custom rule or dependency-cruiser config.
- Python: import-linter or a small AST-based checker.
- Go: staticcheck + custom analyzer.

## Current Behavior (Template Default)
- The linter reads `architecture-lint.toml` and enforces layer + domain boundaries for Python imports.
- If the config exists but has no domains configured, the linter skips and reports the issue.
