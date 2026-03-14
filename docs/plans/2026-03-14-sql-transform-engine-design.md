# SQL Transform Engine — Design Document

**Date:** 2026-03-14
**Status:** Approved

## Overview

A native SQL transformation engine for pit that turns SQL SELECT statements into materialized database objects. Models are `.sql` files containing SELECT queries with `{{ ref }}` template references. Pit wraps each SELECT in the correct DDL/DML based on the model's configured materialization, then executes them in dependency order.

### Motivation

- **Eliminate Python dependency** — teams doing SQL transforms don't need dbt, Python, or uvx
- **MSSQL-first** — purpose-built materialization templates for SQL Server, avoiding dbt adapter quirks
- **Cross-database models (future)** — intermediate save to Parquet + bulk load enables cross-database transforms, which dbt cannot do
- **Pit philosophy** — no magic, no framework lock-in, explicit configuration

### Key Properties

- Pure Go, no external dependencies
- DAG inferred automatically from `{{ ref }}` calls, with TOML overrides for hybrid DAGs
- Materializations are Go `text/template` files per dialect — new dialects are just new template directories
- Config cascades through the directory tree via `[defaults]` sections
- Compiled SQL written to `compiled_models/` for inspection and debugging
- All mutating materializations use explicit transactions (no backup table renaming)

## Project Structure

A transform project uses a `models/` directory alongside the existing `tasks/` directory. Models are template-rendered and DDL-wrapped; tasks are raw SQL execution.

```
projects/warehouse_transforms/
├── pit.toml
├── models/
│   ├── defaults.toml              # root-level defaults
│   ├── staging/
│   │   ├── defaults.toml          # schema = "staging", materialization = "view"
│   │   ├── stg_claims.sql
│   │   └── stg_providers.sql
│   ├── marts/
│   │   ├── marts.toml             # schema = "analytics", materialization = "table"
│   │   ├── dim_providers.sql
│   │   ├── fact_claims.sql        # per-model override in marts.toml
│   │   └── incremental/
│   │       ├── defaults.toml      # strategy = "merge"
│   │       └── fact_daily.sql
├── compiled_models/               # generated, git-ignored
│   ├── staging/
│   │   ├── stg_claims.sql
│   │   └── stg_providers.sql
│   └── marts/
│       ├── dim_providers.sql
│       ├── fact_claims.sql
│       └── incremental/
│           └── fact_daily.sql
└── tasks/                         # optional, plain SQL exec or other runners
    └── post_transform_check.sql
```

### pit.toml

```toml
[dag]
name = "warehouse_transforms"
schedule = "0 7 * * *"
timeout = "30m"

[dag.sql]
connection = "warehouse_db"

[dag.transform]
dialect = "mssql"
```

The `[dag.transform]` section signals that this project has models. Pit scans `models/**/*.sql` for model files and `models/**/*.toml` for config. Model names are derived from filenames (sans `.sql`). The `dialect` field selects the materialization template set.

## Model Configuration

### TOML Schema

Any `.toml` file under `models/` is treated as model configuration. Files can be named anything.

```toml
# Directory-wide defaults — apply to all models at this level and below
[defaults]
materialization = "table"
schema = "analytics"

# Per-model overrides — key is the model filename (sans .sql)
[fact_claims]
materialization = "incremental"
strategy = "merge"
unique_key = ["claim_id"]
schema = "analytics"
```

### Config Fields

| Field | Type | Description |
|-------|------|-------------|
| `materialization` | string | `"view"`, `"table"`, `"incremental"`, `"ephemeral"` |
| `strategy` | string | `"merge"` or `"delete_insert"` (incremental only) |
| `unique_key` | []string | Column(s) for incremental match (supports composite keys) |
| `schema` | string | Target schema for the materialized object |
| `connection` | string | Override `[dag.sql].connection` for this model |

### Config Resolution Order

Most specific wins:

1. Per-model `[model_name]` section in any TOML at the same level or above
2. `[defaults]` in the nearest TOML file in the same directory
3. `[defaults]` in parent directories, walking up to `models/`
4. No implicit defaults — if materialization isn't set anywhere, validation fails

## DAG Inference & Template Functions

### DAG Construction

1. Glob `models/**/*.sql` — each file becomes a model node
2. Parse each file for `{{ ref "model_name" }}` calls
3. Build adjacency graph from refs
4. Merge with any explicit `depends_on` from `pit.toml` `[[tasks]]` entries
5. Cycle detection (reuses `internal/dag`)
6. Topological sort → execution order

### Hybrid DAGs

Models can depend on non-model tasks and vice versa. If a `[[tasks]]` entry in `pit.toml` shares a name with a model file, the TOML config augments the model (adds dependencies, overrides timeout) rather than creating a separate task.

```toml
[[tasks]]
name = "extract_raw"
script = "tasks/extract.py"

[[tasks]]
name = "stg_claims"
depends_on = ["extract_raw"]    # model depends on a Python task
```

### Template Functions

| Function | Example | Description |
|----------|---------|-------------|
| `ref` | `{{ ref "stg_claims" }}` | Resolves to the fully qualified table/view name (`[schema].[model_name]`) and adds a DAG dependency edge |
| `this` | `{{ this }}` | Resolves to the current model's fully qualified name |

### Example Models

**`models/staging/stg_claims.sql`:**
```sql
SELECT
    claim_id,
    provider_id,
    claim_date,
    amount
FROM raw.claims
WHERE load_date = CAST(GETDATE() AS DATE)
```

**`models/marts/fact_claims.sql`:**
```sql
SELECT
    c.claim_id,
    c.claim_date,
    c.amount,
    p.provider_name,
    p.provider_type
FROM {{ ref "stg_claims" }} c
JOIN {{ ref "dim_providers" }} p
    ON c.provider_id = p.provider_id
```

## Materializations

Each materialization is a Go `text/template` file. Templates are organized by dialect and embedded in the binary via `embed.FS`.

### Template Input

```go
type MaterializeContext struct {
    ModelName   string     // "fact_claims"
    Schema      string     // "analytics"
    SQL         string     // rendered SELECT (refs already resolved)
    UniqueKey   []string   // ["claim_id"] for incremental
    This        string     // "[analytics].[fact_claims]"
    Columns     []string   // from INFORMATION_SCHEMA introspection
}
```

### Supported Materializations

#### view

```sql
CREATE OR ALTER VIEW {{ .This }} AS
{{ .SQL }}
```

#### table (full refresh)

```sql
BEGIN TRANSACTION;

IF OBJECT_ID('{{ .This }}', 'U') IS NOT NULL
    DROP TABLE {{ .This }};

SELECT *
INTO {{ .This }}
FROM (
{{ .SQL }}
) AS __pit_model;

COMMIT TRANSACTION;
```

#### incremental (merge)

```sql
BEGIN TRANSACTION;

MERGE {{ .This }} AS target
USING (
{{ .SQL }}
) AS source
ON {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}target.[{{ $k }}] = source.[{{ $k }}]{{ end }}
WHEN MATCHED THEN
    UPDATE SET {{ "{{ columns_update }}" }}
WHEN NOT MATCHED THEN
    INSERT ({{ "{{ columns_list }}" }})
    VALUES ({{ "{{ columns_values }}" }});

COMMIT TRANSACTION;
```

Column lists for UPDATE/INSERT are resolved at runtime via `INFORMATION_SCHEMA.COLUMNS` introspection on the target table.

#### incremental (delete+insert)

```sql
BEGIN TRANSACTION;

DELETE FROM {{ .This }}
WHERE {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}[{{ $k }}] IN (SELECT [{{ $k }}] FROM ({{ .SQL }}) AS __pit_source){{ end }};

INSERT INTO {{ .This }}
{{ .SQL }};

COMMIT TRANSACTION;
```

#### ephemeral

No DDL emitted. The model's rendered SELECT is inlined as a CTE into every downstream model that refs it.

### Incremental First-Run Handling

If an incremental model's target table doesn't exist yet (checked via `OBJECT_ID`), pit falls back to the `table` materialization (SELECT INTO) for that run. Subsequent runs use the configured incremental strategy.

## Execution Flow

### 1. Discovery & Config Resolution

- Glob `models/**/*.sql` → list of model files
- Glob `models/**/*.toml` → merge configs with directory-scoped `[defaults]` inheritance
- Validate: every model has a materialization resolved from the config cascade

### 2. DAG Construction

- Parse each model file for `{{ ref "..." }}` calls
- Build adjacency graph from refs
- Merge with any explicit `depends_on` from `pit.toml` `[[tasks]]` entries
- Cycle detection, topological sort
- Identify ephemeral models (will be inlined, not executed as separate nodes)

### 3. Compilation

For each model in topological order:

1. Resolve ephemeral refs → inline as CTEs
2. Render `{{ ref }}` → fully qualified names, `{{ this }}` → self name
3. Load the dialect materialization template
4. For incremental merge: query `INFORMATION_SCHEMA.COLUMNS` on the target table (if it exists — first run falls back to table materialization)
5. Execute the materialization template with the rendered SELECT
6. Write the final SQL to `compiled_models/` (mirroring the `models/` directory structure)

### 4. Execution

- Execute each compiled model against the database in topological order, parallelizing independent branches (existing engine behavior)
- Each model runs inside the transaction defined by its materialization template
- Log rows affected, timing — same as current SQL runner
- Plain `[[tasks]]` entries execute normally (raw SQL, Python, shell) in their correct DAG position

## Package Structure

```
internal/transform/
├── transform.go          # core orchestration: discover, resolve config, build DAG, compile, execute
├── config.go             # model config parsing, directory-scoped defaults resolution
├── ref.go                # ref extraction from SQL files, DAG edge building
├── compile.go            # template rendering pipeline: refs → SELECT → materialization wrapper
├── introspect.go         # INFORMATION_SCHEMA queries for column metadata
├── dialects/
│   └── mssql/
│       ├── view.sql
│       ├── table.sql
│       ├── incremental_merge.sql
│       └── incremental_delete_insert.sql
└── transform_test.go
```

### Integration Points

- `internal/config` — `DAGConfig` gets a new `Transform *TransformConfig` field
- `internal/engine` — executor calls into `transform` for projects with `[dag.transform]`; the transform package returns a resolved DAG of compiled SQL, which the engine executes using the existing SQL runner
- `internal/runner/sql.go` — unchanged; compiled models are just SQL strings by the time they reach the runner
- `internal/dag` — reused for cycle detection and topo sort on the inferred model graph

## CLI

### New Commands

| Command | Description |
|---------|-------------|
| `pit compile <project>` | Compile all models to `compiled_models/` without executing. For review, debugging, and CI validation. |

### Scaffold

`pit init --type transform my_transforms` generates the `models/` directory with a sample `defaults.toml` and example model.

## Out of Scope (Future)

| Feature | Notes |
|---------|-------|
| Cross-database models | Save to Parquet + bulk load behind the scenes, leveraging existing save/load infrastructure |
| Additional dialects | Postgres, ClickHouse, Oracle — add new template directories |
| `source()` function | Hardcode external table names for now |
| Tests/assertions | dbt-style `tests:` on models |
| Documentation generation | Auto-generated docs from model metadata |
