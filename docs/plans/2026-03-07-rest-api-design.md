# REST API Design

**Date:** 2026-03-07
**Status:** Accepted

## Summary

Add a read-only REST API to `pit serve`, exposing DAG configuration, run history, task instances, and declared outputs over HTTP. Builds on the SQLite metadata store. Shares the existing HTTP server and port with the webhook handler.

## Decisions

- **Read-only for v1** — query endpoints only. Manual trigger and log streaming are follow-up work.
- **Same HTTP server** — API routes (`/api/`) share the mux with webhook routes (`/webhook/`) on the same `--port` (default 9090).
- **Optional bearer token auth** — configured via `api_token` in `pit_config.toml`. If set, all `/api/` routes except `/api/health` require `Authorization: Bearer <token>`. If not set, the API is open.
- **Config + store merge** — `GET /api/dags` discovers all `pit.toml` projects and left-joins with latest run data. DAGs that have never run appear with `null` run info.
- **No pagination links** — just `?limit=N` (default 20, max 100) for v1.

## Endpoints

```
GET  /api/health              — server health check (always public)
GET  /api/dags                — list all DAGs with latest run status
GET  /api/dags/{name}         — DAG detail with task graph and recent runs
GET  /api/runs                — recent runs across all DAGs (?limit, ?dag)
GET  /api/runs/{id}           — run detail with task instances
GET  /api/outputs             — outputs registry (?dag filter)
```

All responses are `application/json`. Times are RFC3339 UTC.

## Response Shapes

### `GET /api/health`

```json
{"status": "ok"}
```

### `GET /api/dags`

```json
{
  "dags": [
    {
      "name": "claims_pipeline",
      "schedule": "0 6 * * *",
      "task_count": 3,
      "latest_run": {
        "id": "20260307_143000.000_claims_pipeline",
        "status": "success",
        "started_at": "2026-03-07T14:30:00Z",
        "ended_at": "2026-03-07T14:32:15Z",
        "trigger": "cron"
      }
    },
    {
      "name": "new_project",
      "schedule": "",
      "task_count": 2,
      "latest_run": null
    }
  ]
}
```

### `GET /api/dags/{name}`

```json
{
  "name": "claims_pipeline",
  "schedule": "0 6 * * *",
  "overlap": "skip",
  "timeout": "45m0s",
  "tasks": [
    {"name": "extract", "script": "tasks/extract.py", "depends_on": []},
    {"name": "validate", "script": "tasks/validate.py", "depends_on": ["extract"]},
    {"name": "load", "script": "tasks/load.py", "depends_on": ["validate"]}
  ],
  "recent_runs": [
    {
      "id": "20260307_143000.000_claims_pipeline",
      "status": "success",
      "started_at": "2026-03-07T14:30:00Z",
      "ended_at": "2026-03-07T14:32:15Z",
      "trigger": "cron"
    }
  ]
}
```

Returns 404 if DAG name not found in discovered configs.

### `GET /api/runs?limit=20&dag=claims_pipeline`

```json
{
  "runs": [
    {
      "id": "20260307_143000.000_claims_pipeline",
      "dag_name": "claims_pipeline",
      "status": "success",
      "started_at": "2026-03-07T14:30:00Z",
      "ended_at": "2026-03-07T14:32:15Z",
      "trigger": "cron",
      "error": null
    }
  ]
}
```

### `GET /api/runs/{id}`

```json
{
  "id": "20260307_143000.000_claims_pipeline",
  "dag_name": "claims_pipeline",
  "status": "success",
  "started_at": "2026-03-07T14:30:00Z",
  "ended_at": "2026-03-07T14:32:15Z",
  "trigger": "cron",
  "error": null,
  "tasks": [
    {
      "name": "extract",
      "status": "success",
      "started_at": "2026-03-07T14:30:00Z",
      "ended_at": "2026-03-07T14:30:45Z",
      "attempts": 1,
      "error": null
    }
  ]
}
```

Returns 404 if run ID not found.

### `GET /api/outputs?dag=claims_pipeline`

```json
{
  "outputs": [
    {
      "dag_name": "claims_pipeline",
      "name": "claims_staging",
      "type": "table",
      "location": "warehouse.staging.claims"
    }
  ]
}
```

Without `?dag`, returns outputs from all DAGs. Returns outputs from the latest successful run per DAG.

## Error Format

```json
{
  "error": "DAG \"foo\" not found"
}
```

HTTP status codes used: 200, 401, 404, 500.

## Authentication

Optional bearer token via `pit_config.toml`:

```toml
api_token = "my-secret-token"
```

When set, all `/api/` routes except `/api/health` require:

```
Authorization: Bearer my-secret-token
```

Comparison uses `crypto/subtle.ConstantTimeCompare` (same pattern as webhook auth). Missing or invalid token returns 401:

```json
{"error": "unauthorized"}
```

When `api_token` is not set or empty, all endpoints are open.

## Package Structure

```
internal/api/
├── server.go      — NewHandler(), auth middleware, router setup
├── handlers.go    — handler functions for each endpoint
└── api_test.go    — tests using httptest
```

### `NewHandler`

```go
func NewHandler(configs map[string]*config.ProjectConfig, store meta.Store, token string) http.Handler
```

- `configs` — discovered project configs (for DAG list/detail, task graph)
- `store` — metadata store (for run history, outputs)
- `token` — API bearer token (empty string = no auth)

Returns an `http.Handler` that routes `/api/` requests.

### Auth Middleware

Wraps all `/api/` routes except `/api/health`. Checks `Authorization: Bearer <token>` with constant-time comparison. Only active when `token` is non-empty.

## Integration with pit serve

The HTTP server startup in `Start()` becomes unconditional — it always starts for the API, even when no webhooks are configured. Both webhook and API routes share the same mux:

```go
mux := http.NewServeMux()
mux.Handle("/api/", apiHandler)
if len(s.webhookTokens) > 0 {
    mux.HandleFunc("/webhook/", s.webhookHandler)
}
```

### Config Changes

`PitConfig` in `internal/config/pit_config.go`:

```go
APIToken string `toml:"api_token"`
```

`serve.Options`:

```go
APIToken string
```

Passed through to `api.NewHandler`.

### CLI Changes

`internal/cli/serve.go` adds `resolveAPIToken()` and passes it to serve Options.

`internal/cli/root.go` adds `resolveAPIToken`:

```go
func resolveAPIToken() string {
    if workspaceCfg != nil {
        return workspaceCfg.APIToken
    }
    return ""
}
```

## Testing

Tests in `internal/api/api_test.go` using `net/http/httptest`. Each test creates an in-memory metadata store, seeds data, and makes HTTP requests.

| Test | Endpoint | Validates |
|------|----------|-----------|
| `TestHealth` | `GET /api/health` | 200, `{"status":"ok"}` |
| `TestListDAGs` | `GET /api/dags` | Merges configs with run data, null latest_run for new DAGs |
| `TestDAGDetail` | `GET /api/dags/{name}` | Config + recent runs |
| `TestDAGDetailNotFound` | `GET /api/dags/nope` | 404 |
| `TestListRuns` | `GET /api/runs` | Returns runs, respects `?limit` and `?dag` |
| `TestRunDetail` | `GET /api/runs/{id}` | Run + task instances |
| `TestRunDetailNotFound` | `GET /api/runs/nope` | 404 |
| `TestListOutputs` | `GET /api/outputs` | Returns outputs, respects `?dag` |
| `TestAuthRequired` | All endpoints | 401 when token set but not provided |
| `TestAuthBypassed` | `/api/health` | 200 even when token set |
| `TestNoAuthWhenEmpty` | All endpoints | 200 when token is empty |

## File Changes

| Action | Path | Description |
|--------|------|-------------|
| Create | `internal/api/server.go` | `NewHandler()`, auth middleware, router |
| Create | `internal/api/handlers.go` | Handler functions for all 6 endpoints |
| Create | `internal/api/api_test.go` | httptest-based tests |
| Modify | `internal/config/pit_config.go` | Add `APIToken` field |
| Modify | `internal/serve/server.go` | Mount API handler, unconditional HTTP server start |
| Modify | `internal/cli/serve.go` | Pass APIToken through Options |
| Modify | `internal/cli/root.go` | Add `resolveAPIToken()` |
| Modify | `README.md` | Document API endpoints, auth, config |

## Future Work (out of scope)

- `POST /api/dags/{name}/trigger` — manual trigger via API
- `GET /api/runs/{id}/tasks/{name}/logs` — stream task logs
- Pagination with cursor/links
- CORS headers for browser clients
- Rate limiting
