# Pit

Lightweight data orchestration for small-to-medium data teams. Go orchestrator, language-agnostic tasks.

Tasks are just executables — Python scripts, shell scripts, SQL files, or anything with an exit code. No decorators, no magic, no framework lock-in. The orchestrator is a single binary.

## Installation

```bash
go install github.com/druarnfield/pit/cmd/pit@latest
```

Or build from source:

```bash
git clone https://github.com/druarnfield/pit.git
cd pit
go build ./cmd/pit
```

## Quick Start

```bash
# Scaffold a new project
pit init my_pipeline                 # Python project (full package layout)
pit init --type sql my_transforms    # SQL-only project (minimal)
pit init --type shell my_jobs        # Shell-only project

# Validate all project configs
pit validate

# Run a DAG
pit run my_pipeline                  # run entire DAG
pit run my_pipeline/extract          # run a single task
pit run my_pipeline --verbose        # stream task output to stdout

# View logs from past runs
pit logs my_pipeline                 # latest run, all tasks
pit logs my_pipeline/extract         # latest run, single task
pit logs my_pipeline --list          # list available runs
pit logs my_pipeline --run-id <id>   # specific run

# Query the outputs registry
pit outputs                          # list all declared outputs
pit outputs --project my_pipeline    # filter by project
pit outputs --type table             # filter by output type
pit outputs --location "warehouse.*" # filter by location (glob)
```

## Project Structure

Projects live under `projects/<name>/` with a `pit.toml` defining the DAG:

```
projects/
├── claims_pipeline/           # Python project
│   ├── pit.toml               # DAG definition
│   ├── pyproject.toml         # Python deps
│   ├── src/claims/            # shared package
│   └── tasks/
│       ├── extract.py
│       ├── validate.py
│       └── load.py
├── warehouse_transforms/      # SQL-only project
│   ├── pit.toml
│   └── tasks/
│       ├── staging_claims.sql
│       └── dim_providers.sql
└── nightly_maintenance/       # Shell-only project
    ├── pit.toml
    └── tasks/
        ├── vacuum_db.sh
        └── archive_logs.sh
```

## DAG Configuration

Each project's `pit.toml` declares tasks, dependencies, and outputs:

```toml
[dag]
name = "claims_pipeline"
schedule = "0 6 * * *"
overlap = "skip"
timeout = "45m"

[[tasks]]
name = "extract"
script = "tasks/extract.py"
timeout = "15m"
retries = 2
retry_delay = "30s"

[[tasks]]
name = "validate"
script = "tasks/validate.py"
depends_on = ["extract"]

[[tasks]]
name = "load"
script = "tasks/load.py"
depends_on = ["validate"]

[[outputs]]
name = "claims_staging"
type = "table"
location = "warehouse.staging.claims"
```

### Task Runners

Runner is determined by file extension, with an optional override:

| Extension | Default Runner | Execution |
|-----------|---------------|-----------|
| `.py`     | `python`      | `uv run --project {project_dir} {script}` |
| `.sh`     | `bash`        | `bash {script}` |
| `.sql`    | `sql`         | Go SQL operator against `[dag.sql]` connection |

Custom runners use the `$ prefix` syntax:

```toml
[[tasks]]
name = "transform"
script = "tasks/transform.js"
runner = "$ node"              # runs: node tasks/transform.js
```

## CLI Commands

### Implemented

| Command | Description |
|---------|-------------|
| `pit validate` | Validate all `pit.toml` files (cycles, missing deps, script paths) |
| `pit init <name>` | Scaffold a new project (`--type python\|sql\|shell`) |
| `pit run <dag>[/<task>]` | Execute a DAG or single task (`--verbose` for live output) |
| `pit logs <dag>[/<task>]` | View task logs (`--list` for runs, `--run-id` for specific run) |
| `pit outputs` | List declared outputs (`--project`, `--type`, `--location` filters) |

### Global Flags

| Flag | Description |
|------|-------------|
| `--project-dir` | Root project directory (default: `.`) |
| `--verbose` | Enable verbose output |
| `--secrets` | Path to secrets TOML file (enables SDK socket and SQL connections) |

## Run Snapshots

When a run begins, Pit copies the project directory to a snapshot. Tasks execute from the snapshot, not the source — so git pulls or edits during a run can't affect in-flight tasks.

```
runs/
└── 20240115_143022.123_claims_pipeline/
    ├── project/     # frozen copy of the project
    └── logs/        # per-task log files
        ├── extract.log
        ├── validate.log
        └── load.log
```

## Execution Model

- Tasks execute in topological order, parallelising independent branches
- Per-task retries with configurable delay
- Per-task and per-DAG timeouts via context cancellation
- Failed tasks mark all downstream tasks as `upstream_failed`
- Task states: `pending` → `running` → `success | failed | skipped | upstream_failed`

## Development

```bash
# Run all tests
go test -race ./...

# Run with verbose output
go test -v ./internal/engine

# Integration tests (requires bash, uv, etc.)
go test -tags integration ./...

# Vet
go vet ./...
```

## Secrets

Secrets are stored in a TOML file outside the repo (e.g. `/etc/pit/secrets.toml` in production). Pass the path via `--secrets`:

```bash
pit run my_pipeline --secrets ./secrets.toml
```

Secrets are organised by project scope with a `[global]` fallback:

```toml
[global]
smtp_password = "..."

[claims_pipeline]
claims_db = "sqlserver://user:pass@host/db"

[warehouse_transforms]
warehouse_db = "duckdb:///data/warehouse.duckdb"
```

Resolution order: project-scoped section first, then `[global]`.

## SDK Socket

When `--secrets` is provided, Pit starts a JSON-over-Unix-socket server. Tasks can connect via the `PIT_SOCKET` environment variable to request secrets at runtime.

Python tasks use the bundled SDK client:

```python
from pit.sdk import get_secret

conn_str = get_secret("claims_db")
```

The SDK client is at `sdk/python/pit/sdk.py`.

## SQL Execution

SQL tasks (`.sql` files) execute in-process via Go's `database/sql`. Configure the connection name in `pit.toml`:

```toml
[dag]
name = "warehouse_transforms"

[dag.sql]
connection = "warehouse_db"
```

The connection name is resolved from the secrets file. Supported drivers:

| Connection string prefix | Driver |
|-------------------------|--------|
| `sqlserver://`, `mssql://` | Microsoft SQL Server |
| `duckdb://`, `.db`/`.duckdb` file path | DuckDB |

Without `--secrets`, SQL tasks fall back to stub mode (log file contents without executing).

## Roadmap

The following features are planned but not yet implemented. See `pit-architecture.md` for full design details.

### Near-term

- **`pit sync`** — Sync Python environments. Hash `uv.lock` files, only run `uv sync` for changed lockfiles. Parallel sync across projects.
- **`pit status`** — Show DAG and task status from run history. Recent runs, success/failure counts, last run timestamps.
- **Cross-project requirements** — Temporal dependencies between DAGs (`requires = { max_age = "24h" }`). Check SQLite run history at DAG start.

### Mid-term

- **`pit serve`** — Production scheduler with REST API. Cron-based scheduling via robfig/cron, manual triggers, config hot-reload.
- **SQLite metadata store** — Persistent run history, task instance tracking, environment hashes. WAL mode for concurrent access.
- **DuckLake inter-task data** — Typed, versioned data passing between tasks via DuckDB + DuckLake. Schema-enforced, time-travel capable. Required for meaningful Python pipelines.
- **Notifications** — Email on DAG failure via SMTP connector, webhook support for Slack/Teams.
- **Go connectors** — MSSQL query/bulk insert, SMTP, HTTP, Minio/S3 — all exposed via SDK socket.

### Long-term

- **REST API** — Full API for DAG status, run history, task logs, outputs registry, manual triggers.
- **Status dashboard** — Read-only web UI for monitoring DAGs, viewing logs, browsing outputs.
- **SDK socket authentication** — Per-connection auth, secret access audit logging, per-project ACLs.
- **Log archival** — Archive completed run logs to DuckLake for historical queries.
- **Encrypted secrets** — At-rest encryption, key rotation, connection pooling across tasks.
