# Log Streaming Design

**Date:** 2026-03-07
**Status:** Accepted

## Summary

Add SSE-based log streaming to the REST API, backed by a structured logging layer using `slog`. Logs on disk stay as raw text; structured metadata is added in-flight for SSE consumers. Supports both live tailing of running DAGs and retrieval of finished run logs.

## Endpoints

```
GET /api/runs/{id}/logs?lines=N      # specific run
GET /api/dags/{name}/logs?lines=N    # latest run for that DAG
```

- `lines` query param limits to last N lines (default: all)
- Auth follows existing bearer token middleware
- DAG name variant resolves to the latest run (from metadata store or active hub)

## SSE Protocol

### Event Types

**`event: log`** — A structured log line:

```json
{
  "timestamp": "2026-03-07T14:30:22Z",
  "run_id": "20260307_143022.000_my_dag",
  "dag_name": "my_dag",
  "task_name": "extract",
  "level": "info",
  "message": "Fetching records...",
  "attempt": 1,
  "duration": "2.3s"
}
```

**`event: complete`** — Run finished, no more events:

```json
{"status": "success"}
```

### Behaviour

- **Finished run:** Send all (or last N) log lines as `log` events, then `complete` event, close.
- **Running run:** Send existing log lines, then tail for new lines. When run finishes, send `complete` and close.
- **Unknown run ID / DAG:** Return 404 JSON error (not SSE).

### Headers

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

## Architecture: Log Hub

An in-process pub/sub that bridges the executor (writes to files) with SSE clients (need live lines).

**Package:** `internal/loghub`

### Core Types

- **`Hub`** — Central registry. Holds per-run subscriber lists. Created once in `serve`, passed to both executor and API handler.
- **`Entry`** — Structured log line: `Timestamp`, `RunID`, `DAGName`, `TaskName`, `Level`, `Message`, `Attempt`, `Duration`.
- **`Subscriber`** — A channel of `Entry` values that an SSE client reads from.

### Flow

1. Executor creates a log writer that wraps each output line into an `Entry` and publishes to the hub (in addition to writing raw text to the log file).
2. SSE handler subscribes to a run ID on the hub, gets a channel back.
3. Hub fans out each published entry to all subscribers for that run.
4. When the run completes, hub publishes a sentinel and closes all subscriber channels.
5. For finished runs (no active hub entry), SSE handler reads from log files on disk, wraps lines with metadata from the metadata store, and sends as events.

The hub is only active during `pit serve`. The CLI `pit run` path keeps working exactly as today.

## Structured Log Fields

Each log entry carries:

| Field | Description |
|-------|-------------|
| `timestamp` | When the line was emitted |
| `run_id` | Run identifier |
| `dag_name` | DAG name |
| `task_name` | Task that produced this line |
| `level` | info / error / warn |
| `message` | The actual log line content |
| `attempt` | Retry attempt number (1-based) |
| `duration` | Elapsed time since task started |

## Log Files on Disk

Log files stay as raw text (human-readable). No changes to the on-disk format. The structured metadata wrapping happens in-memory when:
- Publishing to the hub for SSE consumers
- Reading finished logs from disk for the SSE endpoint

The CLI `pit logs` command continues reading raw text files unchanged.

## Integration Points

### Executor (`internal/engine`)

- `ExecuteOpts` gains optional `LogHub *loghub.Hub`
- In `executeTask`, the log writer chain becomes: raw file + optional verbose stdout + optional hub writer
- Hub writer wraps each line into an `Entry` with metadata (run ID, task name, attempt, duration since task start)
- On run completion, executor calls `hub.Complete(runID, status)`

### API (`internal/api`)

- Two new routes: `GET /api/runs/{id}/logs`, `GET /api/dags/{name}/logs`
- Handler needs: hub (live runs), runs directory (finished runs), metadata store (run lookups)
- `NewHandler` signature gains `hub *loghub.Hub` and `runsDir string`

### Serve (`internal/serve`)

- Creates `Hub` instance, passes to both `ExecuteOpts` and `api.NewHandler`

## Webhook Streaming

The existing webhook endpoint (`POST /webhook/{dag}`) triggers a DAG run and returns a JSON response with the run ID. With an opt-in query param, the caller can instead receive an SSE stream of the run's logs:

```
POST /webhook/{dag}?stream=true
```

**Without `?stream=true` (default):** Behaviour unchanged — returns JSON with `run_id` and status, connection closes immediately.

**With `?stream=true`:** The response switches to SSE (`Content-Type: text/event-stream`). The caller receives all log events for the triggered run in real-time, followed by the `complete` event with the final status. The connection closes after the run finishes.

This gives webhook callers (CI systems, external orchestrators) the option to follow a run to completion in a single HTTP request — trigger and stream.

### Implementation

- The webhook handler checks for `stream=true` query param
- If streaming, it triggers the run as usual, then delegates to the same SSE streaming logic used by `/api/runs/{id}/logs`
- The hub subscription happens immediately after the run is started, so no log lines are missed

## Testing

### `internal/loghub` (unit tests)

- Publish entries, subscriber receives them
- Multiple subscribers on same run both get all entries
- Subscriber joining mid-run gets only new entries (replay comes from disk)
- `Complete()` closes subscriber channels with final status
- Unsubscribe cleans up without affecting others

### `internal/api` (httptest)

- SSE endpoint for finished run: log events + complete event, correct JSON, `lines` limiting
- Unknown run/DAG returns 404
- DAG name resolves to latest run
- Auth applies to SSE endpoints

### Hub writer (unit test)

- Writing to hub writer publishes `Entry` with correct metadata

### Webhook streaming (httptest)

- `POST /webhook/{dag}?stream=true` returns SSE stream with log events and complete event
- `POST /webhook/{dag}` (no param) returns JSON as before (no regression)

### Live tailing (integration, `//go:build integration`)

- Start run, connect SSE mid-execution, verify lines arrive, verify complete event

## File Changes

| Action | Path | Description |
|--------|------|-------------|
| Create | `internal/loghub/hub.go` | Hub, Entry, Subscriber types and pub/sub logic |
| Create | `internal/loghub/hub_test.go` | Unit tests for hub |
| Create | `internal/loghub/writer.go` | Hub-aware io.Writer that wraps lines into Entry values |
| Create | `internal/loghub/writer_test.go` | Unit tests for hub writer |
| Modify | `internal/engine/executor.go` | Add LogHub to ExecuteOpts, wire hub writer into executeTask |
| Modify | `internal/api/server.go` | Update NewHandler signature, add SSE routes |
| Create | `internal/api/sse.go` | SSE handler: live tailing via hub, finished runs from disk |
| Modify | `internal/api/api_test.go` | Tests for SSE endpoints |
| Modify | `internal/serve/server.go` | Create Hub, pass to executor and API |
| Modify | `internal/serve/webhook.go` | Add `?stream=true` opt-in SSE streaming to webhook handler |
