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
pit init --type dbt my_dbt_project   # dbt project (uvx-managed)

# Validate all project configs
pit validate

# Run a DAG
pit run my_pipeline                  # run entire DAG
pit run my_pipeline/extract          # run a single task
pit run my_pipeline --verbose        # stream task output to stdout

# Start the scheduler (cron + FTP watch triggers)
pit serve                            # runs until SIGINT/SIGTERM
pit serve --verbose                  # with live task output

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
├── nightly_maintenance/       # Shell-only project
│   ├── pit.toml
│   └── tasks/
│       ├── vacuum_db.sh
│       └── archive_logs.sh
└── analytics_dbt/             # dbt project
    ├── pit.toml               # [dag.dbt] config
    └── dbt_repo/              # dbt project root
        ├── dbt_project.yml
        ├── models/
        └── tests/
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
| n/a       | `dbt`         | `uvx dbt {command}` via `[dag.dbt]` config |

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
| `pit init <name>` | Scaffold a new project (`--type python\|sql\|shell\|dbt`) |
| `pit run <dag>[/<task>]` | Execute a DAG or single task (`--verbose` for live output) |
| `pit serve` | Run the scheduler with cron and FTP watch triggers |
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
    ├── logs/        # per-task log files
    │   ├── extract.log
    │   ├── validate.log
    │   └── load.log
    └── data/        # inter-task Parquet files
        └── extracted_claims.parquet
```

The `data/` directory is used for inter-task data passing. Tasks discover it via the `PIT_DATA_DIR` environment variable.

## Execution Model

- Tasks execute in topological order, parallelising independent branches
- Per-task retries with configurable delay
- Per-task and per-DAG timeouts via context cancellation
- Failed tasks mark all downstream tasks as `upstream_failed`
- Task states: `pending` → `running` → `success | failed | skipped | upstream_failed`

## Automated Scheduling

`pit serve` runs as a long-lived process, monitoring all projects for scheduled triggers and FTP file watches.

### Cron Triggers

Set `schedule` in `pit.toml` using standard cron syntax (5-field):

```toml
[dag]
name = "nightly_etl"
schedule = "0 6 * * *"    # 6 AM daily
overlap = "skip"           # skip if previous run still active
```

### FTP Watch Triggers

Monitor an FTP server for incoming files. When files matching the pattern are stable (unchanged size for `stable_seconds`), a DAG run is triggered with the files seeded into the run's `data/` directory.

```toml
[dag]
name = "sales_ingest"
overlap = "skip"

[dag.ftp_watch]
host = "ftp.example.com"
port = 21
user = "data_user"
password_secret = "ftp_password"    # resolved from secrets
tls = true
directory = "/incoming/sales"
pattern = "sales_*.csv"
archive_dir = "/archive/sales"      # move files here after success
poll_interval = "30s"
stable_seconds = 30                  # wait for file to stop growing
```

Both trigger types can be combined on the same DAG.

## Workspace Configuration

Create a `pit_config.toml` in the project root to set workspace-level defaults:

```toml
secrets_dir = "secrets/secrets.toml"
runs_dir = "runs"
dbt_driver = "ODBC Driver 17 for SQL Server"
keep_artifacts = ["logs", "project", "data"]
```

| Field | Default | Description |
|-------|---------|-------------|
| `secrets_dir` | (none) | Path to secrets TOML file |
| `runs_dir` | `"runs"` | Directory for run snapshots |
| `dbt_driver` | `"ODBC Driver 17 for SQL Server"` | ODBC driver for dbt profiles |
| `keep_artifacts` | `["logs", "project", "data"]` | Which run subdirs to keep after completion |

All fields are optional. Relative paths are resolved from the project root. CLI flags take precedence if both are set.

### Artifact Retention

By default, Pit keeps all run artifacts (project snapshot, logs, and data). To save disk space, configure `keep_artifacts` to retain only what you need:

```toml
# Workspace level — applies to all projects
keep_artifacts = ["logs"]
```

Per-project overrides are supported in `pit.toml`:

```toml
[dag]
name = "my_pipeline"
keep_artifacts = ["logs", "data"]   # override workspace default
```

Resolution order: per-project (if set) > workspace (if set) > default (keep all). Valid values: `logs`, `project`, `data`.

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
warehouse_db = "sqlserver://user:pass@host/warehouse"
```

Resolution order: project-scoped section first, then `[global]`.

## SDK Socket

Pit starts a JSON-over-socket server for every run (Unix domain socket on Linux/macOS, TCP localhost on Windows). Tasks connect via the `PIT_SOCKET` environment variable. When `--secrets` is provided, the server can resolve secrets and load data into databases.

Python tasks use the bundled SDK client:

```python
from pit_sdk import get_secret, read_sql, write_output, load_data

# Read secrets
conn_str = get_secret("claims_db")

# Read from database (ConnectorX — no ODBC drivers needed)
table = read_sql(conn_str, "SELECT * FROM staging.claims")

# Write inter-task data
write_output("claims", table)

# Bulk-load Parquet into database (Go-side, no ODBC)
load_data("claims.parquet", "target_table", "claims_db")
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `PIT_RUN_ID` | Current run identifier |
| `PIT_TASK_NAME` | Current task name |
| `PIT_DAG_NAME` | Current DAG name |
| `PIT_SOCKET` | SDK server address |
| `PIT_DATA_DIR` | Path to run's data directory for Parquet files |

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

Without `--secrets`, SQL tasks fall back to stub mode (log file contents without executing).

## dbt Projects

Pit can orchestrate dbt projects managed by other teams. dbt is executed via `uvx` (no global install needed), and `profiles.yml` is generated at runtime from Pit secrets.

### dbt Configuration

```toml
[dag]
name = "analytics_dbt"
schedule = "0 7 * * *"
overlap = "skip"
timeout = "2h"

[dag.dbt]
version = "1.9.1"              # dbt-core version
adapter = "dbt-sqlserver"       # pip package name for the adapter
extra_deps = ["dbt-utils"]      # additional pip packages (optional)
project_dir = "dbt_repo"        # relative path to dbt project root
profile = "analytics"           # profile name in profiles.yml (default: dag name)
target = "prod"                 # target name (default: "prod")

[[tasks]]
name = "staging"
script = "run --select staging"  # dbt command (not a file path)
runner = "dbt"

[[tasks]]
name = "marts"
script = "run --select marts"
runner = "dbt"
depends_on = ["staging"]

[[tasks]]
name = "test"
script = "test"
runner = "dbt"
depends_on = ["marts"]
```

For dbt tasks, the `script` field contains the dbt subcommand and arguments (e.g. `"run --select staging"`), not a file path. The `runner` field must be set to `"dbt"`.

### dbt Secrets

dbt connection details are resolved from the secrets file using these keys:

```toml
[analytics_dbt]
dbt_host = "sql-server.example.com"
dbt_port = "1433"
dbt_database = "analytics"
dbt_schema = "dbo"
dbt_user = "dbt_user"
dbt_password = "secret123"
```

Pit generates a `profiles.yml` in a temporary directory before each run and sets `DBT_PROFILES_DIR` so dbt picks it up automatically.

### dbt JSON Log Parsing

dbt is invoked with `--log-format json`. Pit parses the JSON output and displays key events:

- Model results: `OK stg_orders (2.5s, 1500 rows)`
- Test results: `PASS not_null_orders_id (0.3s)`
- Freshness results: `FRESH raw_orders`
- Completion: `Completed in 15.7s`

## Python SDK

The Python SDK (`sdk/python/`) provides helpers for tasks running under Pit:

| Function | Description |
|----------|-------------|
| `get_secret(key)` | Retrieve a secret from the orchestrator's secrets store |
| `read_sql(conn, query)` | Read from a database via ConnectorX (returns Arrow Table) |
| `write_output(name, data)` | Write Arrow/pandas/polars data to Parquet in the data directory |
| `read_input(name)` | Read a named Parquet file from the data directory |
| `load_data(file, table, conn)` | Trigger Go-side bulk load of Parquet into a database |

The `load_data` function accepts optional `schema` (default `"dbo"`), and `mode` parameters. Supported modes:

| Mode | Behaviour |
|------|-----------|
| `append` (default) | Insert rows into the existing table |
| `truncate_and_load` | Truncate the table, then insert rows |
| `create_or_replace` | Drop the table if it exists, recreate it from the Parquet schema, then insert rows |

Database reads use ConnectorX (Rust-native, no ODBC drivers needed). Database writes go through the Go orchestrator's bulk loader via RPC (also no ODBC).

## Roadmap

The following features are planned but not yet implemented. See `pit-architecture.md` for full design details.

### Near-term

- **`pit sync`** — Sync Python environments. Hash `uv.lock` files, only run `uv sync` for changed lockfiles. Parallel sync across projects.
- **`pit status`** — Show DAG and task status from run history. Recent runs, success/failure counts, last run timestamps.
- **Cross-project requirements** — Temporal dependencies between DAGs (`requires = { max_age = "24h" }`). Check SQLite run history at DAG start.

### Mid-term

- **SQLite metadata store** — Persistent run history, task instance tracking, environment hashes. WAL mode for concurrent access.
- **Notifications** — Email on DAG failure via SMTP connector, webhook support for Slack/Teams.
- **Additional Go connectors** — SMTP, HTTP, Minio/S3 — exposed via SDK socket.

### Long-term

- **REST API** — Full API for DAG status, run history, task logs, outputs registry, manual triggers.
- **Status dashboard** — Read-only web UI for monitoring DAGs, viewing logs, browsing outputs.
- **SDK socket authentication** — Per-connection auth, secret access audit logging, per-project ACLs.
- **Log archival** — Archive completed run logs for historical queries.
- **Encrypted secrets** — At-rest encryption, key rotation, connection pooling across tasks.
