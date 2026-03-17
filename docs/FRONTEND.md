# Frontend

Last reviewed: 2026-03-17

The frontend is a TypeScript application in `apps/web`.

## Responsibilities
- Accept workbook uploads and show sheet-level import summaries.
- Provide the natural-language query workspace and results display.
- Surface generated SQL, latency, and failure diagnostics.
- Expose query history and optimization insights to operators.

## Recommended structure
- Route by product surface, not by generic component category.
- Keep feature code close to the routes that own it.
- Pull cross-surface contracts from `packages/shared`.
- Keep agent or database orchestration logic out of the UI; call backend APIs instead.

## Suggested feature areas
- `upload-workspace`
- `query-workspace`
- `query-history`
- `optimization-insights`
- `shared-ui`

## State management
- Server state should be fetched through a typed API client backed by shared contracts.
- Form state should remain local to the feature when possible.
- Long-running jobs such as ingestion and rebuilds should expose explicit states: queued, running, succeeded, failed, stale.

## UI patterns
- Upload flow: workbook inspection, sheet mapping summary, import status.
- Query flow: natural-language prompt, generated SQL preview, result grid, execution metadata.
- History flow: searchable list of prior queries with failure filters and trace IDs.
- Optimization flow: query cluster summaries, proposed pipeline changes, and rebuild progress.

## Performance expectations
- Initial query workspace render should prioritize time-to-interactive over heavy data loading.
- Large query results should use pagination or virtualization.
- Avoid recomputing heavy transforms in the client when the backend can return pre-shaped results.

## Testing focus
- Upload and query flows are the highest-value UI paths.
- Generated SQL visibility and failure messaging should be covered by UI tests.
- Operator flows can start with focused smoke coverage and expand as the product stabilizes.
