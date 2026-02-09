# Pit — Lightweight Data Orchestration

A minimal, opinionated orchestration tool for small-to-medium data teams. Go orchestrator, language-agnostic task execution, Parquet files for inter-task data, SQLite for metadata.

---

## Philosophy

- Tasks are just executables. Python scripts, shell scripts, SQL files, or anything with an exit code. No decorators, no magic, no framework lock-in.
- Python projects are standard packages. LSP, autocomplete, and type checking work out of the box. But Python is optional — a project can be pure SQL or shell scripts.
- Go connectors are the recommended path for I/O where Go excels (MSSQL, bulk insert, SMTP), but tasks can use any library directly. It's your code.
- SQL files are first-class tasks. The Go SQL operator runs `.sql` files directly against configured database connections — no Python dependency required.
- The orchestrator is a single binary. The deployment is a git pull.
- Only build what's needed. Add complexity when someone asks for it twice.

---

## Project Structure

Everything is organised by project. Each project is a self-contained pipeline with its own DAG definition, tasks, and (optionally) dependencies and shared code. The structure adapts to what the project needs — Python projects get a full package layout, SQL-only or shell-only projects stay minimal.

```
projects/
├── claims_pipeline/           # Python project — full package layout
│   ├── pit.toml               # DAG definition
│   ├── pyproject.toml         # Python deps + package config
│   ├── uv.lock
│   ├── src/
│   │   └── claims/            # shared package for this project
│   │       ├── __init__.py
│   │       ├── sftp.py        # team's SFTP helper
│   │       ├── formatting.py  # shared data cleaning utils
│   │       └── validation.py  # domain-specific validators
│   └── tasks/
│       ├── extract.py         # from claims.sftp import upload
│       ├── validate.py        # from claims.validation import check_claims
│       └── load.py
├── warehouse_transforms/      # SQL-only project — minimal, no Python
│   ├── pit.toml
│   └── tasks/
│       ├── staging_claims.sql
│       ├── dim_providers.sql
│       └── fact_claims.sql
├── nightly_maintenance/       # Shell-only project
│   ├── pit.toml
│   └── tasks/
│       ├── vacuum_db.sh
│       └── archive_logs.sh
└── monthly_reports/           # Mixed — Python + shell
    ├── pit.toml
    ├── pyproject.toml
    ├── uv.lock
    ├── src/
    │   └── reports/
    │       ├── __init__.py
    │       └── templates.py
    └── tasks/
        ├── generate.py
        ├── email.py
        └── upload.sh
```

**Why this structure:**

- Everything about a pipeline lives together — no cross-referencing between directories
- Python projects get per-project deps via collocated `pyproject.toml`; SQL/shell projects need no Python infrastructure at all
- Shared code is a proper Python package — LSP, autocomplete, type checking all work naturally
- Git-friendly: PRs are self-contained, ownership is clear
- Local dev works naturally: `cd projects/my_pipeline && pit run extract`
- Portable: a project directory is a complete unit

**Task isolation:**

Tasks cannot import from each other. For Python projects, shared logic goes in the project's package, task-specific logic stays in the task file. The dependency direction is always one way:

```
tasks/extract.py      → imports from → claims/ (shared package)
tasks/validate.py     → imports from → claims/ (shared package)
tasks/load.py         → imports from → claims/ (shared package)
```

Never:

```
tasks/extract.py      → imports from → tasks/validate.py
```

Each task file is self-contained and readable top to bottom. For Python projects, the shared package provides utilities — connection helpers, data cleaning functions, domain validators — not orchestration logic. For SQL projects, each `.sql` file is a standalone query.

**Package configuration in `pyproject.toml`:**

```toml
[project]
name = "claims-pipeline"
version = "0.1.0"
dependencies = [
    "duckdb>=1.0",
    "pandas>=2.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

`uv sync` installs the package as editable in the project venv. Imports work everywhere — in tasks, in tests, in the REPL.

**Scaffolding:** `pit init my_pipeline` generates a Python project by default (full structure including `src/`, `__init__.py`, `pyproject.toml`, and a sample task). Use `pit init --type sql my_transforms` for a minimal SQL project (just `pit.toml` and `tasks/`), or `pit init --type shell my_jobs` for shell scripts. Nobody needs to remember the boilerplate.

---

## Core Components

### 1. CLI & Server

Pit is a single Go binary with two modes.

**CLI mode** — local development and ad-hoc execution:

```bash
pit run claims_pipeline              # run entire DAG locally
pit run claims_pipeline/extract      # run a single task
pit run warehouse_transforms         # run SQL DAG (uses [dag.sql] connection)
pit validate                         # validate all pit.toml files
pit sync                             # sync all UV environments
pit status                           # show DAG/task states
pit init my_pipeline                 # scaffold a new project (Python)
pit init --type sql my_transforms    # scaffold a SQL project
pit outputs                          # list all registered outputs
pit outputs --type table             # filter by type
pit outputs --location warehouse.*   # search by location
pit logs claims_pipeline/extract     # tail task logs
```

**Server mode** — production scheduler with API:

```bash
pit serve                            # start scheduler, API, git sync
```

Global flags: `--project-dir` (root directory), `--secrets` (path to secrets file, default `/etc/pit/secrets.toml`), `--verbose`.

Both modes use the same binary, same SDK socket, same execution path. Local runs behave identically to production runs — no conditional logic, no fallback paths, no "works on my machine" surprises.

---

### 2. Scheduler

**Runtime:** Go (robfig/cron)

- Parses cron expressions from each project's `pit.toml`
- Creates a run record in SQLite when a schedule fires
- Hands off to the DAG executor — scheduler doesn't execute anything directly
- Supports manual trigger via CLI (`pit run`) or API (`POST /api/dags/{name}/trigger`)
- Configurable overlap policy per DAG: skip, queue, or allow concurrent
- Graceful shutdown: stops accepting new runs on SIGTERM, waits for in-flight runs to complete or timeout

---

### 3. DAG Definitions

Each project contains a `pit.toml` that declares the task graph, cross-project requirements, and external outputs. Paths are relative to the project root.

**Python pipeline example:**

```toml
[dag]
name = "claims_pipeline"
schedule = "0 6 * * *"
overlap = "skip"
timeout = "45m"

# Cross-project dependencies — temporal only.
# Projects are black boxes to each other. This just checks
# "did it succeed recently?" not "what did it produce?"
[dag.requires]
raw_extracts = { max_age = "24h" }

# Tasks within the DAG
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
timeout = "5m"

[[tasks]]
name = "load"
script = "tasks/load.py"
depends_on = ["validate"]
timeout = "10m"
retries = 2

# External outputs this project is responsible for.
# Pure metadata — Pit doesn't create these, tasks do.
# Provides a searchable registry of what pipelines produce.
[[outputs]]
name = "claims_staging"
type = "table"
location = "warehouse.staging.claims"

[[outputs]]
name = "claim_lines_staging"
type = "table"
location = "warehouse.staging.claim_lines"
```

**SQL-only pipeline example (poor man's dbt):**

```toml
[dag]
name = "warehouse_transforms"
schedule = "0 7 * * *"
overlap = "skip"
timeout = "30m"

# Connection used by all SQL tasks in this project (resolved via secrets)
[dag.sql]
connection = "warehouse_db"

[[tasks]]
name = "staging_claims"
script = "tasks/staging_claims.sql"
timeout = "10m"

[[tasks]]
name = "dim_providers"
script = "tasks/dim_providers.sql"
depends_on = ["staging_claims"]
timeout = "5m"

[[tasks]]
name = "fact_claims"
script = "tasks/fact_claims.sql"
depends_on = ["staging_claims", "dim_providers"]
timeout = "10m"
```

No Python, no UV, no venv. The Go SQL operator reads the `.sql` file and executes it against the configured connection. Dependencies between SQL tasks work exactly like any other task — defined in `depends_on`, executed in topological order.

**Task runner dispatch:**

The runner is determined by convention (file extension), with an optional override:

| Extension | Default runner | What happens |
|-----------|---------------|--------------|
| `.py` | `python` | `uv run --project {project_dir} {script}` |
| `.sh` | `bash` | `bash {script}` |
| `.sql` | `sql` | Go SQL operator executes against configured connection |

Override with the `runner` field on any task:

```toml
[[tasks]]
name = "transform"
script = "tasks/transform.js"
runner = "$ node"              # custom: runs `node tasks/transform.js`

[[tasks]]
name = "analysis"
script = "tasks/analysis.R"
runner = "$ Rscript"           # custom: runs `Rscript tasks/analysis.R`

[[tasks]]
name = "forced_python"
script = "tasks/run.sh"
runner = "python"              # override: treat .sh file as python
```

Predefined runners (`python`, `bash`, `sql`) use their built-in execution strategy. The `$` prefix is an escape hatch — the value after `$` is used as a shell command prefix before the script path. This means pit can run anything without needing built-in support for every language.

**Loading and validation:**

- On startup, scan `projects/*/pit.toml`
- Parse and validate: check for cycles, missing dependencies, unknown scripts
- Build in-memory adjacency graph per DAG (topological sort)
- Reload configs on SIGHUP or via API (`POST /api/reload`), validate before swap
- Reject invalid configs with clear errors, keep running with last-known-good

**Cross-project requirements:**

- Temporal only: "did project X succeed within the last N hours?"
- Checked against SQLite run history at DAG start
- No data-level coupling — projects own their internal structure completely
- Fails with a clear message if requirement not met

---

### 4. DAG Executor

**Runtime:** Go

- Receives a run_id and resolved DAG graph from the scheduler
- Executes tasks in topological order, parallelising independent branches
- Configurable concurrency limit per DAG (default 4)
- Per-task state machine: `pending → running → success | failed | skipped | upstream_failed`
- On task failure: retry up to N times with configurable delay, mark downstream as `upstream_failed`, continue independent branches
- On DAG timeout: cancel all running tasks via context cancellation
- All state transitions recorded in SQLite with timestamps

---

### 5. Run Snapshots

**When a run begins, Pit copies the project directory to a temporary snapshot directory.** Tasks execute from the snapshot, not the source.

```
runs/
└── {run_id}/
    ├── project/           # snapshot of the project at run start
    │   ├── pit.toml
    │   ├── pyproject.toml
    │   ├── src/
    │   │   └── claims/
    │   └── tasks/
    │       ├── extract.py
    │       └── validate.py
    ├── logs/              # task log files for this run
    │   ├── extract.log
    │   └── validate.log
    └── data/              # inter-task Parquet files
        └── extracted_claims.parquet
```

**Why:**

- **Git pull safety:** The server can pull from main at any time without affecting in-flight runs. No race conditions between deployment and execution.
- **Consistency:** Every task in a run sees the exact same code, even if the run takes 30 minutes and someone merges a PR at minute 5.
- **Isolation:** If a task writes temp files, they don't pollute the source project directory.
- **Cleanup:** Delete the run directory when retention expires. Everything associated with a run is in one place.

The snapshot is cheap — it's just scripts (Python, SQL, shell), shared code, and TOML files, not large data. For Python projects, the venv is not copied; `uv run --project` still points at the shared, pre-materialised venv.

---

### 6. Task Runner

**Runtime:** Go (manages subprocess lifecycle + built-in SQL operator)

Each task runs based on its runner type — either as an isolated subprocess or via the built-in Go SQL operator. The orchestrator manages the full lifecycle.

**Process contract (for subprocess-based runners):**

| Channel | Purpose |
|---------|---------|
| `PIT_SOCKET` env var | Path to SDK Unix socket for secrets and connectors |
| `PIT_RUN_ID` env var | Current run identifier |
| `PIT_TASK_NAME` env var | Current task name |
| `PIT_DAG_NAME` env var | Current DAG name |
| `PIT_DATA_DIR` env var | Path to run's data directory for inter-task Parquet files |
| stdout / stderr | Written to log file in run directory |
| Exit code | 0 = success, non-zero = failure |

**Python tasks (`.py`):**

```bash
uv run --project /projects/{project}/ /runs/{run_id}/project/tasks/{script}.py
```

UV resolves the pre-materialised venv. The script runs from the snapshot directory. Near-zero startup overhead.

**Shell tasks (`.sh`):**

```bash
bash /runs/{run_id}/project/tasks/{script}.sh
```

Standard subprocess with the same environment variables. Exit code determines success/failure.

**SQL tasks (`.sql`) — Go SQL operator:**

SQL tasks run in-process via Go's `database/sql`, not as a subprocess. The runner:

1. Reads the `.sql` file from the snapshot directory
2. Resolves the database connection from the project's `[dag.sql]` config + secrets
3. Sends the entire file contents to the database as a single execution — no statement splitting, no parsing by pit
4. Logs rows affected and execution time
5. Returns success/failure based on query result

The SQL file is sent as-is. Multi-statement scripts, temp tables, transactions, CTEs — whatever the database supports. Pit doesn't parse SQL, it just delivers it. This is pit's "poor man's dbt" — SQL files with task dependencies defined in TOML, executed by Go with no Python dependency. Ideal for warehouse transformations, staging loads, and data quality checks.

```sql
-- tasks/staging_claims.sql
INSERT INTO staging.claims
SELECT * FROM raw.claims
WHERE load_date = CAST(GETDATE() AS DATE);
```

**Custom runner tasks (`$ prefix`):**

```bash
{command} /runs/{run_id}/project/tasks/{script}
```

The command after `$` is used as the prefix. Same subprocess contract as shell tasks.

**Process management:**

- Subprocess tasks run in their own OS process (full isolation)
- SQL tasks run in-process but with their own database connection and timeout
- Context cancellation via SIGTERM (subprocess) or query cancellation (SQL), then SIGKILL after grace period
- All output written to `runs/{run_id}/logs/{task_name}.log` in real-time

---

### 7. SDK & Go Communication Layer

**The bridge between Python tasks and the Go orchestrator. This is foundational — every connector, every secret, every future capability flows through this channel.**

**Protocol:** JSON over Unix domain socket.

**Go side (~100 lines):** Listens on a Unix socket, routes JSON requests to handler functions, returns JSON responses.

```
→ {"method": "get_secret", "params": {"key": "claims_db"}}
← {"result": "Server=...", "error": null}

→ {"method": "load_data", "params": {"file": "output.parquet", "table": "staging.claims", "connection": "claims_db", "schema": "dbo", "mode": "append"}}
← {"result": "50000 rows loaded", "error": null}
```

**Python side:** Thin client that reads `PIT_SOCKET` from env, sends JSON, reads JSON. Database reads use ConnectorX (Rust-native, no ODBC). Database writes go through the Go orchestrator's Parquet-based bulk loader.

```python
from pit_sdk import get_secret, read_sql, write_output, load_data

conn_str = get_secret("claims_db")
table = read_sql(conn_str, "SELECT * FROM claims")
write_output("claims", table)
load_data("claims.parquet", "staging.claims", "claims_db")
```

**Design principles:**

- SDK is a thin RPC client — no business logic, no caching, no state
- All I/O happens in the Go process, Python only handles data transforms
- Same socket, same protocol, same behaviour locally and in production
- Adding a new capability = adding a method handler in Go + a function in the Python SDK
- Tasks can also use any Python library for I/O directly — the Go connectors are the recommended path where Go has a clear advantage, not a requirement

---

### 8. Environment Management (Python Projects)

**Runtime:** UV

Python projects have their own `pyproject.toml` with pinned dependencies. UV manages isolated venvs per project. SQL-only and shell-only projects skip this entirely — they have no Python dependencies to manage.

**Lifecycle:**

1. **Define:** `pyproject.toml` in each project directory (also configures the shared package)
2. **Lock:** `uv lock` pins exact versions
3. **Sync:** `pit sync` or on startup — materialises venvs for any changed lockfiles
4. **Run:** `uv run --project` uses the pre-built venv with near-zero overhead
5. **Update:** Change deps, `uv lock`, commit, deploy

**Startup sync:**

- Hash each `uv.lock`, compare against stored hashes in SQLite
- Only `uv sync` projects with changed lockfiles
- Parallel sync across projects
- All venvs hot before scheduler starts accepting runs

**Shared environments:**

Most projects get their own deps. Projects with identical requirements (e.g., multiple dbt projects) can share a `pyproject.toml` via symlink or by pointing at a common env directory. Start with per-project, consolidate only if disk or sync time becomes a problem.

**UV cache:** Global cache at `~/.cache/uv` shared across all projects. Hardlink deduplication on Linux means multiple projects using pandas don't actually duplicate the files on disk.

---

### 9. Secrets Management

**Runtime:** Go (served via SDK socket)

**v1: Plaintext TOML file, secure delivery.**

The priority is keeping secrets out of task environments, logs, and error messages — not encrypting storage. The secrets file lives outside the git repo on the server.

```
/etc/pit/secrets.toml
```

```toml
[global]
smtp_password = "..."

[claims_pipeline]
claims_db = "Server=...;User Id=...;Password=..."

[dbt_warehouse]
warehouse_db = "Server=...;User Id=...;Password=..."
```

- Go binary loads secrets file on startup
- Tasks request secrets at runtime via SDK: `sdk.get_secret("claims_db")`
- Secrets resolved by project scope first, then global fallback
- Never exposed as environment variables — passed only through the SDK socket on request
- Secrets never touch disk in the task process or appear in crash dumps
- Audit log: which task accessed which secret, when

**Local development:** Developers need a local secrets file for running tasks that require database connections or other secrets. The path defaults to `/etc/pit/secrets.toml` but can be overridden:

```bash
pit run --secrets ~/.pit/secrets.toml warehouse_transforms
```

Same format, same resolution rules. Developers maintain their own file with dev/staging credentials.

**Known limitations (v1):**

- **Plaintext storage.** The secrets file is not encrypted at rest. Security relies on filesystem permissions and keeping the file outside the git repo. Acceptable for a single-server deployment where the operator controls access.
- **Trust boundary is the repo.** Any task in the repo can request any secret via the SDK socket. There is no per-project secret scoping enforcement — a task in `claims_pipeline` could request `warehouse_db` if it wanted to. The scoped resolution (project-first, then global) is a convenience for naming, not an access control boundary.
- **`PIT_SOCKET` is visible to all tasks.** The Unix socket path is passed via environment variable. Any process running as the same user could connect to it. This is fine when all code in the repo is trusted, but there's no authentication on the socket itself.
- **No rotation or expiry.** Changing a secret requires editing the file and restarting the process (or triggering a reload). No automatic rotation, no expiry warnings.

**Later:** Encrypt the file at rest, add key rotation, per-project ACLs on secret access, integrate with external vault. Same SDK interface, no task code changes.

---

### 10. Go Connectors

High-performance I/O operations embedded in the Go binary, exposed to tasks via the SDK and used directly by the Go SQL operator. These are the recommended path where Go has clear advantages — but tasks are free to use any library directly for I/O.

| Connector | Use case | Why Go |
|-----------|----------|--------|
| MSSQL bulk load | High-throughput Parquet → table via `mssql.CopyIn` | TDS bulk copy, zero external deps |
| MSSQL query (SQL runner) | Execute `.sql` files in-process | go-mssqldb: pure TDS, no ODBC |
| SMTP | Email notifications and alerts | Already built |
| HTTP | API calls with retry/backoff | Go stdlib |
| Minio/S3 | File storage for large artifacts | Native Go SDK |

**Architecture:** Database reads use ConnectorX (Rust-native) from Python — no ODBC. Database writes use the Go `load_data` RPC handler which reads Parquet files (Arrow Go) and bulk-loads via the native driver. The SQL runner executes `.sql` files directly against configured connections.

**Adding Go connectors:** Write a handler function, register it on the socket router, add a corresponding Python SDK function.

**Adding Python connectors:** Write a Python module in the project's `src/` package. No framework involvement needed — it's just an import.

---

### 11. Inter-Task Data (Parquet Files)

**Runtime:** Arrow (Go + Python)

Named Parquet files for data passing between tasks within a run. Each run gets a `data/` directory at `runs/{run_id}/data/`. Tasks discover it via the `PIT_DATA_DIR` environment variable.

**Writing data (Python task):**

```python
from pit_sdk import write_output

write_output("raw_claims", arrow_table)  # writes {data_dir}/raw_claims.parquet
```

**Reading data (downstream Python task):**

```python
from pit_sdk import read_input

table = read_input("raw_claims")  # reads {data_dir}/raw_claims.parquet
```

**Loading data into a database (Python task → Go bulk loader):**

```python
from pit_sdk import load_data

load_data("raw_claims.parquet", "staging_claims", "warehouse_db")
```

The `load_data` call sends an RPC to the Go orchestrator, which reads the Parquet file using Arrow Go and bulk-loads it into the target database using the native driver (MSSQL via TDS bulk copy). No ODBC drivers required.

**Database reads:** Python tasks use ConnectorX (Rust-native) via `read_sql()` — also no ODBC drivers required.

**Why Parquet files:**

- Standard, portable format — readable by any language or tool
- Columnar and compressed — efficient for analytical workloads
- No extra infrastructure — just files on disk
- Easy to inspect: `duckdb` CLI, `pyarrow.parquet`, `pandas.read_parquet`
- Cleanup is simple: delete the run directory

**DuckDB:** Users who want DuckDB for in-task analytics can add it as a Python dependency. It's not bundled with the Go orchestrator (too much binary bloat).

---

### 12. Logging

**Logs are files. That's it.**

- Task runner writes stdout/stderr to `runs/{run_id}/logs/{task_name}.log` in real-time
- CLI reads directly from log files: `pit logs claims_pipeline/extract` tails the file
- API serves log content by reading the file
- No SQLite writes during task execution — zero contention on the metadata DB

**Retention:**

- Run directories (including logs) are cleaned up based on age — configurable retention period
- Deleting a run directory removes everything: snapshot, logs, all in one place

**Later:** Archive completed logs for historical queries ("show me all errors from last week"). Not needed until someone asks for it.

---

### 13. Outputs Registry

**A searchable inventory of what each project produces in external systems. Pure metadata — Pit doesn't create, validate, or manage these artifacts. It just knows who's responsible for them.**

Each project declares its external outputs in `pit.toml`:

```toml
[[outputs]]
name = "claims_staging"
type = "table"
location = "warehouse.staging.claims"

[[outputs]]
name = "claim_lines_staging"
type = "table"
location = "warehouse.staging.claim_lines"

[[outputs]]
name = "daily_claims_report"
type = "file"
location = "//sftp/reports/claims_daily.csv"

[[outputs]]
name = "claims_notification"
type = "email"
recipients = ["team@org.com"]
```

**What it gives you:**

- "Who writes to this table?" — `pit outputs --location warehouse.staging.claims`
- "What does this project produce?" — `pit outputs --project claims_pipeline`
- "List all external tables" — `pit outputs --type table`
- New team member onboarding: `pit outputs` shows everything the system produces
- Migration planning: know exactly what's affected before changing anything

**What it doesn't do (intentionally):**

- No runtime validation that outputs were actually created
- No automatic lineage graph or cross-project dependency resolution
- No requirement to declare outputs — it's optional documentation
- No enforcement that outputs match reality

It's a registry, not an engine. Extensible later with additional fields like `owner`, `description`, `schema`, `retention` as needed — adding fields to TOML doesn't break anything.

---

### 14. Metadata Store

**Runtime:** SQLite (WAL mode)

Focused purely on orchestration state. No log content, no large blobs.

| Table | Purpose |
|-------|---------|
| `dags` | Registered DAGs, config hash, last parsed |
| `runs` | Run instances: dag, status, timestamps, trigger type |
| `task_instances` | Per-task state: run_id, task, status, attempt, timestamps |
| `env_hashes` | Lockfile hashes for sync tracking |
| `secret_access_log` | Audit trail for secret access |

**Why SQLite:**

- Embedded, zero config, single file
- WAL mode handles concurrent reads from API while scheduler writes
- More than sufficient for < 100 DAGs
- Easy backup: copy the file
- Swap to Postgres later if needed — same SQL, different driver

---

### 15. API & Observability

**Runtime:** Go (net/http)

**Endpoints:**

```
GET  /api/dags                        — list all DAGs with current status
GET  /api/dags/{name}                 — DAG detail with task graph
POST /api/dags/{name}/trigger         — manual trigger
GET  /api/runs                        — recent runs
GET  /api/runs/{id}                   — run detail with task statuses
GET  /api/runs/{id}/tasks/{name}/logs — task logs (reads from file or DuckLake)
GET  /api/outputs                     — full outputs registry
GET  /api/health                      — health check
```

**Notifications:**

- On DAG failure: email via SMTP connector
- Webhook support for Slack/Teams
- Configurable per-DAG: notify on failure, success, or both

**UI (later):**

- Read-only status dashboard: task boxes coloured by state, click for logs
- Simple HTML + JS polling the JSON API
- Visual DAG editor that writes TOML (much later, if ever needed)

---

### 16. Deployment

**Git-based. No artifact registry, no deploy pipeline, no DAG sync daemon.**

1. Developers work locally in `projects/`, run tasks with `pit run`
2. PR reviewed — diff is task scripts and TOML config
3. Merge to main
4. Server pulls main on a schedule (or webhook trigger)
5. `pit sync` any changed lockfiles, hot-reload changed `pit.toml` files
6. Rollback = revert the commit

**Container layout:**

```
pit:latest
├── pit binary (~15MB)
├── uv + python         (only needed if projects use Python tasks)
├── projects/           (git clone, periodically pulled)
├── runs/               (run snapshots, logs, and inter-task data, persistent volume)
├── metadata.db         (Pit state, persistent volume)

/etc/pit/
└── secrets.toml        (outside repo, not in git)
```

**Resources:** ~50-100MB RAM idle, spikes during task execution. Compared to Airflow's 2GB+ baseline.

---

## What's Intentionally Not in v1

| Feature | Rationale |
|---------|-----------|
| Asset-level lineage | Outputs registry covers "who produces what." Full lineage graph can extend from there later. |
| Encrypted secret storage | Secure delivery via SDK socket is the priority. Encrypt the file later. |
| Distributed execution | Single machine handles < 100 DAGs comfortably |
| Multi-tenancy / RBAC | Small team, everyone's an admin |
| Event-driven triggers | Cron + manual covers it |
| Plugin system | Projects have a `src/` package for shared code. No framework-level plugin abstraction needed. |
| Kubernetes operator | Runs in a container. `docker run` is sufficient |
| Visual DAG editor | Edit TOML in your IDE |
| Remote dev mode | `pit run --remote` for production secrets against local code. Useful but not v1 |
| Output validation | Registry is documentation, not enforcement. Validate when there's a reason to. |
