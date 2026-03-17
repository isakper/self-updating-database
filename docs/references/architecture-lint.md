# Architecture Lint (Contract)

This repo currently ships a Python-based entrypoint at `scripts/lint-architecture`, but the intended long-term implementation for this product is TypeScript-native.

## Contract
A compliant architecture linter must:
- Exit non-zero on rule violations.
- Print remediation-oriented errors.
- Support a repo-local config file.
- Run fast enough for pre-commit and CI.

## Default config location
- `architecture-lint.toml`

## Expected rule coverage
- Enforce the backend layering edges: `types -> schemas -> repo -> service -> jobs -> api`.
- Enforce explicit provider or adapter boundaries for Codex CLI, databases, and telemetry.
- Reject backward or skipped-layer imports within a domain.
- Allow shared contract imports from `packages/shared`.

## Suggested implementation paths
- Preferred: ESLint custom rules or `dependency-cruiser`.
- Transitional: keep the current stub entrypoint while the TypeScript workspace is being created.

## Current behavior
- The current repo tooling is template-era and not yet aligned with the intended TypeScript monorepo.
- Contributors should treat this doc as the contract for future enforcement, not as evidence that full enforcement already exists.
