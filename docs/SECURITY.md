# Security

Last reviewed: 2026-03-17

Security baseline and threat model notes for the self-updating database.

## High-value assets

- Uploaded workbooks and their derived source database contents.
- Optimized query database contents.
- Stored transformation pipelines and revision history.
- Query execution logs, including generated SQL and metadata.
- Codex CLI invocation prompts, outputs, and audit records.

## Baseline security requirements

- Treat uploaded workbooks as untrusted input and validate before ingestion.
- Restrict query execution to the optimized query database boundary intended for end-user requests.
- Preserve strict separation between source data, derived data, and telemetry stores.
- Redact secrets and sensitive data from logs, prompts, and traces.
- Make Codex CLI invocations auditable and attributable.

## Threat model focus

- Malformed workbook inputs causing parser or import failures.
- Over-broad SQL generation or unsafe execution paths.
- Pipeline revisions introducing incorrect or destructive derived structures.
- Sensitive data leaking through logs, traces, or prompt payloads.
- Unauthorized access to query history or optimization controls.

## Dependency and tooling policy

- Prefer TypeScript-first dependencies for app code.
- Transitional Python tooling is acceptable for doc maintenance only until replaced.
- New dependencies should be justified in terms of product need, security posture, and operational burden.

## Security checks

- Schema validation at API and job boundaries.
- Role-aware access to operator or optimization endpoints.
- Audit records for pipeline revisions and Codex CLI actions.
- Manual review of generated SQL behavior before production autonomy increases.
