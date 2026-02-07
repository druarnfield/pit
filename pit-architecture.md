# Pit — Lightweight Data Orchestration

A minimal, opinionated orchestration tool for small-to-medium data teams. Go orchestrator, Python tasks via UV, DuckLake for inter-task data, SQLite for metadata.

---

## Philosophy

- Tasks are just scripts. No decorators, no magic, no framework lock-in.
- Projects are just Python packages. LSP, autocomplete, and type checking work out of the box.
- Go connectors are the recommended path for I/O where Go excels (MSSQL, bulk insert, SMTP), but tasks can use any Python library directly. It's your code.
- The orchestrator is a single binary. The deployment is a git pull.
- Only build what's needed. Add complexity when someone asks for it twice.

---

## Project Structure

Everything is organised by project. Each project is a self-contained pipeline with its own DAG definition, tasks, dependencies, and shared code — structured as a standard Python package.

```
projects/
├── claims_pipeline/
│   ├── pit.toml              # DAG definition
│   ├── pyproject.toml        # Python deps + package config
│   ├── uv.lock
│   ├── src/
│   │   └── claims/           # shared package for this project
│   │       ├── __init__.py
│   │       ├── sftp.py       # team's SFTP helper
│   │       ├── formatting.py # shared data cleaning utils
│   │       └── validation.py # domain-specific validators
│   └── tasks/
│       ├── extract.py        # from claims.sftp import upload
│       ├── validate.py       # from claims.validation import check_claims
│       └── load.py
├── dbt_warehouse/
│   ├── pit.toml
│   ├── pyproject.toml
│   ├── uv.lock
│   └── tasks/
│       └── run_dbt.sh
└── monthly_reports/
    ├── pit.toml
    ├── pyproject.toml
    ├── uv.lock
    ├── src/
    │   └── reports/
    │       ├── __init__.py
    │       └── templates.py
    └── tasks/
        ├── generate.py
        └── email.py
```

**Why this structure:**

- Everything about a pipeline lives together — no cross-referencing between directories
- Dependencies are per-project by default via collocated `pyproject.toml`
- Shared code is a proper Python package — LSP, autocomplete, type checking all work naturally
- Git-friendly: PRs are self-contained, ownership is clear
- Local dev works naturally: `cd projects/my_pipeline && pit run extract`
- Portable: a project directory is a complete unit

**Task isolation:**

Tasks cannot import from each other. Shared logic goes in the project's package, task-specific logic stays in the task file. The dependency direction is always one way:

```
tasks/extract.py      → imports from → claims/ (shared package)
tasks/validate.py     → imports from → claims/ (shared package)
tasks/load.py         → imports from → claims/ (shared package)
```

Never:

```
tasks/extract.py      → imports from → tasks/validate.py
```

Each task file is self-contained and readable top to bottom. The shared package provides utilities — connection helpers, data cleaning functions, domain validators — not orchestration logic.

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

**Scaffolding:** `pit init my_pipeline` generates the full structure including `src/`, `__init__.py`, `pyproject.toml` with build system config, and a sample task. Nobody needs to remember the boilerplate.

---

## Core Components

### 1. CLI & Server

Pit is a single Go binary with two modes.

**CLI mode** — local development and ad-hoc execution:

```bash
pit run claims_pipeline              # run entire DAG locally
pit run claims_pipeline/extract      # run a single task
pit validate                         # validate all pit.toml files
pit sync                             # sync all UV environments
pit status                           # show DAG/task states
pit init my_pipeline                 # scaffold a new project
pit outputs                          # list all registered outputs
pit outputs --type table             # filter by type
pit outputs --location warehouse.*   # search by location
pit logs claims_pipeline/extract     # tail task logs
```

**Server mode** — production scheduler with API:

```bash
pit serve                            # start scheduler, API, git sync
```

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

**Loading and validation:**

- On startup, scan `projects/*/pit.toml`
- Parse and validate: check for cycles, missing dependencies, unknown scripts
- Build in-memory adjacency graph per DAG (topological sort)
- Watch for file changes (fsnotify) and hot-reload with validation before swap
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
    └── logs/              # task log files for this run
        ├── extract.log
        └── validate.log
```

**Why:**

- **Git pull safety:** The server can pull from main at any time without affecting in-flight runs. No race conditions between deployment and execution.
- **Consistency:** Every task in a run sees the exact same code, even if the run takes 30 minutes and someone merges a PR at minute 5.
- **Isolation:** If a task writes temp files, they don't pollute the source project directory.
- **Cleanup:** Delete the run directory when retention expires. Everything associated with a run is in one place.

The snapshot is cheap — it's just Python scripts, shared code, and TOML files, not large data. The venv is not copied; `uv run --project` still points at the shared, pre-materialised venv.

---

### 6. Task Runner

**Runtime:** Go (manages subprocess lifecycle)

Each task runs as an isolated subprocess from the run snapshot directory. The orchestrator manages the full lifecycle.

**Process contract:**

| Channel | Purpose |
|---------|---------|
| `PIT_SOCKET` env var | Path to SDK Unix socket for secrets and connectors |
| `PIT_RUN_ID` env var | Current run identifier |
| `PIT_TASK_NAME` env var | Current task name |
| `PIT_DAG_NAME` env var | Current DAG name |
| stdout / stderr | Written to log file in run directory |
| Exit code | 0 = success, non-zero = failure |

**Python tasks:**

```bash
uv run --project /projects/{project}/ /runs/{run_id}/project/tasks/{script}.py
```

UV resolves the pre-materialised venv. The script runs from the snapshot directory. Near-zero startup overhead.

**Non-Python tasks:**

Any executable that honours the exit code contract. Shell scripts, Go binaries, Rust binaries — the runner doesn't care.

**Process management:**

- Each task runs in its own OS process (full isolation)
- Context cancellation via SIGTERM, then SIGKILL after grace period
- Stdout/stderr written to `runs/{run_id}/logs/{task_name}.log` in real-time

---

### 7. SDK & Go Communication Layer

**The bridge between Python tasks and the Go orchestrator. This is foundational — every connector, every secret, every future capability flows through this channel.**

**Protocol:** JSON over Unix domain socket.

**Go side (~100 lines):** Listens on a Unix socket, routes JSON requests to handler functions, returns JSON responses.

```
→ {"method": "get_secret", "params": {"key": "claims_db"}}
← {"result": "Server=...", "error": null}

→ {"method": "mssql_query", "params": {"conn": "claims_db", "sql": "SELECT ..."}}
← {"result": [{"id": 1, "name": "..."}], "error": null}

→ {"method": "mssql_bulk_insert", "params": {"conn": "warehouse", "table": "staging.claims", ...}}
← {"result": {"rows_inserted": 50000}, "error": null}
```

**Python side (~50 lines):** Thin client that reads `PIT_SOCKET` from env, sends JSON, reads JSON.

```python
from pit import sdk

conn_str = sdk.get_secret("claims_db")
rows = sdk.mssql_query("claims_db", "SELECT * FROM claims")
sdk.mssql_bulk_insert("warehouse", "staging.claims", data)
```

**Design principles:**

- SDK is a thin RPC client — no business logic, no caching, no state
- All I/O happens in the Go process, Python only handles data transforms
- Same socket, same protocol, same behaviour locally and in production
- Adding a new capability = adding a method handler in Go + a function in the Python SDK
- Tasks can also use any Python library for I/O directly — the Go connectors are the recommended path where Go has a clear advantage, not a requirement

---

### 8. Environment Management

**Runtime:** UV

Each project has its own `pyproject.toml` with pinned dependencies. UV manages isolated venvs per project.

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

**Later:** Encrypt the file at rest, add key rotation, integrate with external vault. Same SDK interface, no task code changes.

---

### 10. Go Connectors

High-performance I/O operations embedded in the Go binary, exposed to tasks via the SDK. These are the recommended path where Go has clear advantages — but tasks are free to use Python libraries directly for any I/O.

| Connector | Use case | Why Go |
|-----------|----------|--------|
| MSSQL query | Run queries, return results | go-mssqldb: pure TDS, no ODBC |
| MSSQL bulk insert | High-throughput loads via `mssql.CopyIn` | TDS bulk copy, zero external deps |
| SMTP | Email notifications and alerts | Already built |
| HTTP | API calls with retry/backoff | Go stdlib |
| Minio/S3 | File storage for large artifacts | Native Go SDK |

**Adding Go connectors:** Write a handler function, register it on the socket router, add a corresponding Python SDK function.

**Adding Python connectors:** Write a Python module in the project's `src/` package. No framework involvement needed — it's just an import.

---

### 11. Inter-Task Data (DuckLake)

**Runtime:** DuckDB + DuckLake extension

Typed, versioned, schema-enforced data passing between tasks within a project. This is internal scratch space — not shared across projects.

**Setup:**

- DuckLake catalog in SQLite (separate from Pit metadata)
- Data stored as Parquet files on local disk
- Each task writes to / reads from DuckLake tables using standard SQL via DuckDB

**Writing data (in a task):**

```python
import duckdb

conn = duckdb.connect()
conn.execute("ATTACH 'ducklake:catalog.db' AS lake (DATA_PATH 'data/')")
conn.execute("INSERT INTO lake.pipeline.raw_claims SELECT * FROM df")
```

**Reading data (in a downstream task):**

```python
conn.execute("ATTACH 'ducklake:catalog.db' AS lake (DATA_PATH 'data/')")
df = conn.execute("SELECT * FROM lake.pipeline.raw_claims").fetchdf()
```

**Retention:**

- Scheduled cleanup expires snapshots older than N days
- `ducklake_expire_snapshots` + `ducklake_cleanup_old_files`
- Configurable retention period
- Time travel within retention window for debugging failed runs

**Why DuckLake over raw files or XCom:**

- Real tables with enforced schemas, not serialised blobs
- Handles any data volume — it's just Parquet underneath
- Time travel for inspecting exactly what a task produced at any point
- No coupling to orchestrator metadata
- If DuckLake development stopped tomorrow, you'd still have standard Parquet files and a queryable SQLite catalog

---

### 12. Logging

**Logs are files first, archived to DuckLake later.**

**During execution:**

- Task runner writes stdout/stderr to `runs/{run_id}/logs/{task_name}.log` in real-time
- CLI and API read directly from log files for live tailing
- No SQLite writes during task execution — zero contention on the metadata DB

**After execution:**

- Background job periodically loads completed log files into DuckLake
- Logs become Parquet files — compressed, queryable, subject to retention policy
- Original log files cleaned up after successful archival

**Historical queries:**

- "Show me all errors from last week" — SQL query on DuckLake log table
- "What did this task output 3 days ago?" — time travel to that snapshot
- Retention handled by the same DuckLake cleanup that manages inter-task data

**Log structure in DuckLake:**

| Column | Type |
|--------|------|
| run_id | VARCHAR |
| dag_name | VARCHAR |
| task_name | VARCHAR |
| attempt | INTEGER |
| timestamp | TIMESTAMP |
| stream | VARCHAR (stdout/stderr) |
| content | VARCHAR |

This keeps SQLite focused purely on orchestration state — lightweight, fast, no bloat from log data accumulating over time.

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
2. PR reviewed — diff is Python files and TOML config
3. Merge to main
4. Server pulls main on a schedule (or webhook trigger)
5. `pit sync` any changed lockfiles, hot-reload changed `pit.toml` files
6. Rollback = revert the commit

**Container layout:**

```
pit:latest
├── pit binary (~15MB)
├── uv + python
├── projects/          (git clone, periodically pulled)
├── runs/              (run snapshots + logs, persistent volume)
├── data/              (DuckLake parquet, persistent volume)
├── metadata.db        (Pit state, persistent volume)
└── catalog.db         (DuckLake catalog, persistent volume)

/etc/pit/
└── secrets.toml       (outside repo, not in git)
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
