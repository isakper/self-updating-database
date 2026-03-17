# Code Standards

Last reviewed: 2026-03-17

## Principles

- Optimize for legibility: clear names, small functions, explicit control flow.
- Prefer TypeScript-first implementations for both frontend and backend product code.
- Keep boundaries crisp: avoid cross-layer imports and spooky action at a distance.
- Prefer boring, dependency-light solutions when they do not hide product intent.
- Preserve the immutable source database invariant.

## Architecture rules

- Shared contracts live in `packages/shared`.
- Backend domains follow `types -> schemas -> repo -> service -> jobs -> api`.
- External systems such as Codex CLI, databases, and telemetry enter through explicit providers or adapters.
- Frontend code talks to backend contracts, not directly to database or pipeline internals.

## Change hygiene

- Keep diffs scoped to the problem.
- Avoid drive-by refactors unless they unblock the change.
- Do not reformat unrelated files.
- If docs and code diverge, update the docs as part of the same change when feasible.

## Error handling

- Make failure modes explicit and actionable.
- Include enough context for debugging without leaking secrets.
- Distinguish clearly between ingestion, query, clustering, optimization, and pipeline execution failures.

## Security and reliability

- Follow [docs/SECURITY.md](SECURITY.md) and [docs/RELIABILITY.md](RELIABILITY.md).
- Never implement an optimization path that mutates or destroys the source database.
