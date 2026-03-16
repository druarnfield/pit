# Multi-Database Loader & SQL Task Types Design

## Problem

The loader and SQL runner are MSSQL-only. There is no way to save query results to Parquet or load Parquet into a database as a first-class task type. Data movement between databases requires custom Python scripts.

## Decision

Refactor the loader into a driver-based architecture supporting MSSQL, Postgres, ClickHouse, and Oracle. Add `load` and `save` SQL task types to pit.toml. Use native bulk operations per database.

## Target Databases

| Database | Go Driver | Bulk Mechanism |
|---|---|---|
| MSSQL | `github.com/microsoft/go-mssqldb` (existing) | `mssql.CopyIn` |
| Postgres | `github.com/jackc/pgx/v5` | `conn.CopyFrom()` (COPY protocol) |
| ClickHouse | `github.com/ClickHouse/clickhouse-go/v2` | `PrepareBatch` + `batch.Append` |
| Oracle | `github.com/sijms/go-ora/v2` | `oracle.BulkInsert()` |

MySQL excluded — no good native bulk path in Go.

## Driver Interface

```go
type Driver interface {
    BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error)
    CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error
    DropTable(ctx context.Context, db *sql.DB, schema, table string) error
    TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error
    ArrowType(dt arrow.DataType) (string, error)
    SQLTypeToArrow(dbTypeName string) (arrow.DataType, error)
    DefaultSchema() string
    QuoteIdentifier(name string) string
}
```

Each driver implements this interface. The shared `Load()` function handles mode logic (append/truncate/create_or_replace) by calling the driver primitives, then delegates to `BulkLoad()`.

## Driver Detection

`DetectDriver()` in `internal/runner/sql.go` expands:

```go
func DetectDriver(connStr string) (string, error) {
    lower := strings.ToLower(connStr)
    switch {
    case strings.HasPrefix(lower, "sqlserver://"), strings.HasPrefix(lower, "mssql://"):
        return "mssql", nil
    case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
        return "postgres", nil
    case strings.HasPrefix(lower, "clickhouse://"):
        return "clickhouse", nil
    case strings.HasPrefix(lower, "oracle://"):
        return "oracle", nil
    default:
        return "", fmt.Errorf("cannot detect SQL driver (supported: sqlserver://, postgres://, clickhouse://, oracle://)")
    }
}
```

## Driver Registry

```go
var drivers = map[string]Driver{
    "mssql":      &MSSQLDriver{},
    "postgres":   &PostgresDriver{},
    "clickhouse": &ClickHouseDriver{},
    "oracle":     &OracleDriver{},
}

func GetDriver(name string) (Driver, error) {
    d, ok := drivers[name]
    if !ok {
        return nil, fmt.Errorf("unsupported database driver %q", name)
    }
    return d, nil
}
```

## Package Layout

```
internal/loader/
  driver.go            # Driver interface + registry
  driver_mssql.go      # MSSQL (refactored from mssql.go)
  driver_postgres.go   # Postgres via pgx
  driver_clickhouse.go # ClickHouse
  driver_oracle.go     # Oracle via go-ora
  loader.go            # Load() entry point (shared mode handling)
  saver.go             # Save() — query to Parquet
  parquet.go           # parquetStream + arrowValue (shared)
  parquet_writer.go    # Parquet file writing (for save)
  types.go             # Arrow type aliases (unchanged)
```

## Task Config Changes

New fields on `TaskConfig` in `internal/config/config.go`:

```go
type TaskConfig struct {
    // ... existing fields ...
    Type       string   `toml:"type"`       // "load", "save", or "" (default exec)
    Source     string   `toml:"source"`     // Parquet file for load (relative to data dir)
    Output     string   `toml:"output"`     // Parquet file for save (relative to data dir)
    Table      string   `toml:"table"`      // target table for load (e.g., "staging.customers")
    Mode       string   `toml:"mode"`       // "append", "truncate_and_load", "create_or_replace"
    Connection string   `toml:"connection"` // overrides [dag.sql].connection
}
```

### Validation Rules

- `type` must be `""`, `"load"`, or `"save"`
- `type = "load"`: `source` and `table` required, `script` must be empty
- `type = "save"`: `script` and `output` required, `source` and `table` must be empty
- `type = ""`: current behaviour, `script` required
- `mode` only valid on `type = "load"`, defaults to `"append"`
- `connection` optional on any sql task — falls back to `[dag.sql].connection`

### Example pit.toml

```toml
[dag]
name = "customer_pipeline"

[dag.sql]
connection = "postgres_warehouse"

[[tasks]]
name = "extract_customers"
type = "save"
script = "tasks/extract.sql"
output = "customers.parquet"
connection = "oracle_source"

[[tasks]]
name = "load_customers"
type = "load"
source = "customers.parquet"
table = "staging.customers"
mode = "truncate_and_load"
depends_on = ["extract_customers"]
```

## Save Path (Query to Parquet)

```go
type SaveParams struct {
    Query    string
    FilePath string
    ConnStr  string
}

func Save(ctx context.Context, params SaveParams) (int64, error)
```

Flow:
1. Detect driver, open `database/sql` connection
2. Execute SELECT via `db.QueryContext(ctx, query)`
3. Call `rows.ColumnTypes()` for column metadata
4. Map `DatabaseTypeName()` to Arrow types via driver's `SQLTypeToArrow()`
5. Build Arrow schema
6. Stream rows in batches (65536), build Arrow arrays via builders
7. Write each batch to Parquet via `pqarrow.WriteTable`

NUMERIC/DECIMAL maps to String — avoids silent precision loss since `DatabaseTypeName()` doesn't reliably provide precision/scale.

### Per-Driver Type Mapping (SQLTypeToArrow)

Each driver maps its database type names to Arrow types. Example for Postgres:

| Database Type | Arrow Type |
|---|---|
| INT4, INTEGER | Int32 |
| INT8, BIGINT | Int64 |
| FLOAT4, REAL | Float32 |
| FLOAT8 | Float64 |
| TEXT, VARCHAR | String |
| BOOL | Boolean |
| TIMESTAMP, TIMESTAMPTZ | Timestamp_us |
| DATE | Date32 |
| BYTEA | Binary |
| NUMERIC, DECIMAL | String |

## Executor Integration

In `executeTask`, before runner resolution:

```go
if tc.Type == "load" || tc.Type == "save" {
    err = executeSQLTask(ctx, ti, run, cfg, opts, logWriter)
} else {
    // existing runner path
}
```

`executeSQLTask` handles both types:

**Load:**
1. Resolve connection (task `Connection` > `[dag.sql].connection`) from secrets
2. Build source path: `filepath.Join(run.DataDir, tc.Source)`
3. Call `loader.Load(ctx, LoadParams{...})`

**Save:**
1. Resolve connection (same override logic)
2. Read SQL file from script path
3. Build output path: `filepath.Join(run.DataDir, tc.Output)`
4. Call `loader.Save(ctx, SaveParams{...})`

Connection resolution helper:

```go
func resolveTaskConnection(tc *config.TaskConfig, cfg *config.ProjectConfig) string {
    if tc.Connection != "" {
        return tc.Connection
    }
    return cfg.DAG.SQL.Connection
}
```

The existing `load_data` SDK handler (Python tasks) benefits from multi-driver support automatically.

## Per-Driver Details

### MSSQL (refactored)
- `BulkLoad`: `mssql.CopyIn` (same as today)
- DDL: `[schema].[table]` quoting, `OBJECT_ID` for existence checks
- Default schema: `dbo`

### Postgres
- `BulkLoad`: `pgx.CopyFrom` with `pgx.CopyFromRows` (COPY protocol)
- DDL: `"schema"."table"` quoting, `DROP TABLE IF EXISTS`
- Default schema: `public`

### ClickHouse
- `BulkLoad`: `PrepareBatch` + `batch.Append` per row, `batch.Send` per row group
- DDL: `DROP TABLE IF EXISTS`, no schema concept
- Default schema: `""` (empty)

### Oracle
- `BulkLoad`: `go-ora` `BulkInsert()` with batch slices
- DDL: `"SCHEMA"."TABLE"` quoting, uppercase identifiers
- Default schema: derived from connection user

## Error Handling

- Connection string not found in secrets: clear error naming the key and project
- Driver not detected: list supported prefixes
- Parquet source missing (load): error before opening DB connection
- Output directory missing (save): create parent dirs (data dir should exist)
- Unsupported column type (save): error with column name and database type
- Bulk load mid-stream failure: transaction rollback, report rows loaded before failure
- No partial retries: failed load/save retries from scratch via existing retry logic

## Implementation Order

1. Refactor loader into driver interface — extract `Driver`, move MSSQL to `driver_mssql.go`, registry, shared mode handling. No new functionality.
2. Expand `DetectDriver()` — add postgres, clickhouse, oracle prefixes.
3. Add task config fields — `Type`, `Source`, `Output`, `Table`, `Mode`, `Connection`. Validation rules.
4. Postgres driver — `driver_postgres.go` with pgx bulk load, type mappings, DDL.
5. ClickHouse driver — `driver_clickhouse.go` with batch insert, type mappings.
6. Oracle driver — `driver_oracle.go` with go-ora bulk insert, type mappings.
7. Save path — `saver.go`, `parquet_writer.go`, `SQLTypeToArrow` per driver.
8. Executor integration — `executeSQLTask` for load/save, connection resolution with task override.
9. Update README — document task types, supported databases, config examples.
10. Integration tests — behind `//go:build integration`, round-trip save+load per database.
