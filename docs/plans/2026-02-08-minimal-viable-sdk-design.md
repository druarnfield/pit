# Minimal Viable SDK — Design

Unblock real pipelines by implementing secrets resolution, an SDK socket for task-to-orchestrator communication, and a SQL operator for MSSQL and DuckDB.

## Goal

Enable Pit to run real data pipelines that need database connections and secrets. Python tasks call `sdk.get_secret()` over a Unix socket. SQL tasks execute `.sql` files in-process against MSSQL or DuckDB. Both resolve credentials from a TOML secrets file.

## 1. Secrets File Loader

**Package:** `internal/secrets/`

The secrets file lives outside the repo at `/etc/pit/secrets.toml` (production) or a custom path via `--secrets` flag (local dev).

```toml
[global]
smtp_password = "..."

[claims_pipeline]
claims_db = "Server=...;User Id=...;Password=..."

[warehouse_transforms]
warehouse_db = "Server=...;Database=analytics;..."
```

**Resolution order:** Project-scoped section first, then `[global]` fallback. `sdk.get_secret("claims_db")` from a task in `claims_pipeline` checks `[claims_pipeline].claims_db` first, then `[global].claims_db`.

**Implementation:**
- `Store` struct holding parsed TOML as `map[string]map[string]string`
- `Load(path) (*Store, error)` — parse the file
- `Resolve(project, key) (string, error)` — project-first, then global lookup
- Load once at startup, no hot-reload

The `--secrets` flag is added as a persistent flag on the root command, defaulting to `/etc/pit/secrets.toml`.

## 2. SDK Socket Server

**Package:** `internal/sdk/`

A JSON-over-Unix-socket server that tasks connect to for secrets. The socket path is passed to tasks via the `PIT_SOCKET` environment variable.

**Protocol:** One JSON request, one JSON response per connection.

```
-> {"method": "get_secret", "params": {"key": "claims_db"}}
<- {"result": "Server=...;User Id=...;Password=...", "error": null}

-> {"method": "unknown_method", "params": {}}
<- {"result": null, "error": "unknown method: unknown_method"}
```

**Implementation:**
- `Server` struct holding a `*secrets.Store` and a `net.Listener`
- `NewServer(socketPath, store) (*Server, error)` — creates the Unix socket
- `Serve(ctx)` — accepts connections, decodes JSON, routes to handlers, encodes response. Shuts down cleanly when context is cancelled.
- `RegisterMethod(name, handler)` — extensible dispatch. v1 registers only `get_secret`.
- Socket file created in a temp directory per run (e.g., `/tmp/pit-<pid>.sock`), cleaned up on shutdown.

**Lifecycle:** The executor starts the SDK server before running any tasks and shuts it down after the run completes. The server knows the current DAG name for scoped secret resolution.

## 3. SQL Operator

**Package:** Extends `internal/runner/`

The SQL runner executes `.sql` files in-process via Go's `database/sql`. It resolves database connections from the project's `[dag.sql]` config + the secrets store.

**Connection resolution flow:**
1. Task has `.sql` extension -> runner resolves to SQL operator
2. SQL operator reads `[dag.sql].connection` from project config (e.g., `"warehouse_db"`)
3. Resolves connection string via `secrets.Resolve(dagName, connectionKey)`
4. Determines driver from URI scheme in the connection string

**Driver dispatch:**
- `mssql://...` or `sqlserver://...` -> `github.com/microsoft/go-mssqldb`
- `duckdb:///path/to/db` or file path ending in `.db`/`.duckdb` -> `github.com/duckdb/duckdb-go/v2`

**Execution model:**
1. Read the `.sql` file from the snapshot directory
2. Open a connection (one per unique connection string per run)
3. Execute the entire file content as a single `db.ExecContext(ctx, sql)`
4. Log rows affected and execution time to the task's log writer
5. Return success/failure based on query result

**Dependencies:**
- `github.com/microsoft/go-mssqldb`
- `github.com/duckdb/duckdb-go/v2`

## 4. Wiring into the Executor

**Before a run starts:**
1. Load secrets file (path from `--secrets` flag)
2. Start SDK socket server, passing it the secrets store
3. Add `PIT_SOCKET` to the task environment

**During task execution:**
- Python/shell tasks connect to the socket to call `get_secret`
- SQL tasks skip the socket — the SQL runner resolves connections directly from the secrets store in-process

**After the run completes:**
- Shut down the SDK socket server
- Clean up the socket file

**Changes to existing code:**
- `ExecuteOpts` gets a `SecretsPath` field
- `Execute()` creates the secrets store and SDK server, passes them through
- `runner.Resolve()` signature expands to accept a context struct carrying the secrets store + project config, so the SQL runner can resolve connections
- `newRunCmd()` reads the `--secrets` flag and passes it into `ExecuteOpts`
- `PIT_SOCKET` env var gets set to the actual socket path

## 5. Python SDK Client

Lives outside the Go module at `sdk/python/pit/sdk.py`. ~50 lines.

- Reads `PIT_SOCKET` from environment
- Connects to Unix socket, sends JSON request, reads JSON response
- Single function to start: `get_secret(key) -> str`
- Raises on error responses or connection failures

## 6. Testing Strategy

**`internal/secrets/`** — Pure unit tests:
- Parse valid TOML, verify scoped resolution (project-first, global fallback)
- Missing key returns error, missing file returns error
- Empty project section falls through to global

**`internal/sdk/`** — Unit tests with real Unix socket:
- Start server in goroutine, connect as client, send `get_secret`, verify response
- Unknown method returns error
- Malformed JSON returns error
- Context cancellation shuts down cleanly
- All use `t.TempDir()` for socket path

**`internal/runner/` SQL runner** — Split by build tag:
- Unit tests: driver dispatch logic, connection string parsing, error on missing `[dag.sql]` config
- Integration tests (`//go:build integration`): execute `.sql` against DuckDB in-memory. MSSQL integration tests only if real server available (skip otherwise).

**Python SDK** — Integration test that connects to a socket and verifies `get_secret` round-trips.

**End-to-end** — Integration test running a full DAG with a `.sql` task against DuckDB in-memory, verifying the secrets -> SQL operator -> execution pipeline.

## Not Yet Included

These will be added to the SDK later:
- Socket authentication (currently trusts any connection from the same user)
- Secret access audit logging
- Connector methods beyond `get_secret` (`mssql_query`, `mssql_bulk_insert`, etc.)
- Connection pooling across tasks within a run
- Encrypted secrets at rest
- Per-project secret ACLs
