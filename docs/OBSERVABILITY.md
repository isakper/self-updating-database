# Observability

This document defines an agent-friendly local workflow for logs, metrics, and traces.

## Goals
- Make runtime behavior legible to agents.
- Keep observability isolated per worktree.
- Prefer simple, local-first tooling.

## Local workflow (per worktree)
1. Start the app for this worktree.
2. Start the local observability stack for this worktree.
3. Run a core flow.
4. Inspect logs, metrics, and traces for the flow.
5. Tear down the stack when done.

## Required fields (v1)
### Logs
- `service`
- `env`
- `version` (or git SHA)
- request correlation id (`request_id` or `trace_id`)

### Metrics
- request duration histogram (or equivalent)
- error counter (by route/status)
- process uptime (if applicable)

### Traces
- spans for critical user journeys
- `service`, `env`, `version` (or git SHA) on spans

## Default local stack (required by template)
This template follows the standard agent diagram:
- App emits logs (HTTP), OTLP metrics, and OTLP traces.
- Vector fans out locally to:
  - VictoriaLogs
  - VictoriaMetrics
  - VictoriaTraces

Downstream repos should provide a `docker-compose.yml` under the worktree with:
- Vector + VictoriaLogs + VictoriaMetrics + VictoriaTraces
- Ports assigned per worktree to avoid conflicts

## Query examples (templates)
Replace placeholders like `<service>` and `<route>`.

### Logs (VictoriaLogs / LogQL)
- Errors for a service:
  - `{service="<service>"} |~ "error|exception|fatal"`
- Requests for a route:
  - `{service="<service>"} |~ "<route>"`

### Metrics (VictoriaMetrics / PromQL)
- p95 latency:
  - `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{service="<service>"}[5m])) by (le))`
- Error rate:
  - `sum(rate(http_requests_total{service="<service>",status=~"5.."}[5m]))`

### Traces (VictoriaTraces / TraceQL)
- Filter by service:
  - `service="<service>"`
- Filter by route:
  - `http.route="<route>"`

## Performance budgets (starter)
Set budgets per repo once real data is available.
- App startup: < 800ms
- P95 latency for key route: < 300ms
- Error rate: < 1%

## Troubleshooting
- Missing logs: verify logger is structured and includes required fields.
- Missing metrics: confirm middleware/exporter wiring.
- Missing traces: confirm spans are created for the request handler and downstream calls.
