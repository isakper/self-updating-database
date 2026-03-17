# Quality Score

Last reviewed: 2026-03-17

Define what "good" means for the self-updating database product.

## Quality dimensions

- Correctness: imports, pipeline execution, SQL generation, and query results behave as documented.
- Safety: source data remains immutable and rebuilds are reproducible.
- Maintainability: TypeScript contracts are shared, docs stay current, and package boundaries remain clear.
- Performance: uploads, query execution, clustering, and rebuilds stay within agreed budgets.
- Observability: every major step is traceable through logs, metrics, traces, and query history.
- Security: workbook handling, query execution, and Codex CLI invocation respect the product threat model.
- Explainability: users and operators can inspect generated SQL, pipeline revisions, and failure reasons.

## Scoring prompts for reviews

- Can a user understand what happened during import, query execution, and optimization?
- Can an engineer rebuild the optimized database from source data and stored pipeline versions?
- Can an agent trace a failed query from UI request to generated SQL to execution log?
- Does the change strengthen or weaken the shared TypeScript contract between frontend and backend?
- Does the change preserve the immutable-source-database invariant?
