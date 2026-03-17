# Architecture Lint (Contract)

This repo is TypeScript-only. When architecture linting is added, it should be implemented with TypeScript-native tooling.

## Contract
A compliant architecture linter must:
- Exit non-zero on rule violations.
- Print remediation-oriented errors.
- Support a repo-local config file.
- Run fast enough for local hooks and CI.

## Expected rule coverage
- Enforce the backend layering edges: `types -> schemas -> repo -> service -> jobs -> api`.
- Enforce explicit provider or adapter boundaries for Codex CLI, databases, and telemetry.
- Reject backward or skipped-layer imports within a domain.
- Allow shared contract imports from `packages/shared`.

## Suggested implementation paths
- Preferred: ESLint custom rules or `dependency-cruiser`.

## Current behavior
- Architecture linting is not implemented yet in-repo.
- Contributors should treat this doc as the contract for future enforcement.
