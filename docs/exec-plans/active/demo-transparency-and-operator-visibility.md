# Execution Plan: Demo Transparency and Operator Visibility

Updated: 2026-03-18

## Objective

Make the product legible during demos and debugging by exposing what the system is doing at each major step: source import, Codex analysis, pipeline generation, clean-database build, natural-language query generation, SQL execution, and later optimization work.

## Non-goals

- Final visual polish or brand-level UI design.
- Building a full observability platform with external dashboards.
- Exposing raw low-level logs that overwhelm the demo narrative.

## Milestones

- Add a clear status timeline for the upload-to-clean-database flow.
- Expose Codex artifacts in the UI, including the prompt, `analysis.json`, `summary.md`, and `pipeline.sql`.
- Add operator-facing views for clean-database readiness, retry state, and last failure reason.
- Extend the later query workspace so it shows natural-language input, generated SQL, execution latency, and result status in a clear sequence.
- Add a compact event/log feed that makes the current system state understandable during live demos.

## Validation

- Upload a representative workbook and confirm a demo viewer can understand each completed step without reading API responses directly.
- Confirm operators can inspect generated pipeline artifacts from the UI after an import.
- Confirm failure states surface enough detail to explain what broke and what the system will do next.

## Rollout

- Start with the pipeline path that already exists: import, Codex analysis, and clean-database build.
- Reuse the same visibility patterns for query execution and later optimization revisions.
- Prefer simple, reliable status surfaces before adding richer live-log behavior.

## Risks

- If transparency is added only as debug text, the product may still feel opaque in demos.
- If artifact visibility is inconsistent between steps, the system story will remain hard to follow.
- Showing too much raw detail without structure can make the product feel more confusing, not less.
