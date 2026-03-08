# Multi-Database Loader & SQL Task Types Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor the MSSQL-only loader into a driver-based architecture supporting MSSQL, Postgres, ClickHouse, and Oracle, and add `load`/`save` SQL task types to pit.toml.

**Architecture:** Extract a `Driver` interface from the existing MSSQL loader. Each database implements its own driver with native bulk operations. A shared `Load()` entry point handles mode logic (append/truncate/create_or_replace) by calling driver primitives. New `load` and `save` task types in pit.toml allow declarative data movement between databases and Parquet files. The save path uses `database/sql` `ColumnTypes()` mapped to Arrow types via per-driver `SQLTypeToArrow()`.

**Tech Stack:** Go, Arrow/Parquet (`apache/arrow-go`), `pgx/v5`, `clickhouse-go/v2`, `go-ora/v2`, `go-mssqldb` (existing)

**Design doc:** `docs/plans/2026-03-08-multi-db-loader-design.md`

---

### Task 1: Extract Driver Interface & Refactor MSSQL

Introduce the `Driver` interface and driver registry. Move MSSQL-specific code behind the interface. No new functionality — this is a pure refactor.

**Files:**
- Create: `internal/loader/driver.go`
- Modify: `internal/loader/mssql.go` → rename to `internal/loader/driver_mssql.go`
- Modify: `internal/loader/loader.go`
- Test: `internal/loader/loader_test.go`

**Step 1: Write failing test for Driver interface and registry**

Add to `internal/loader/loader_test.go`:

```go
func TestGetDriver_MSSQL(t *testing.T) {
	d, err := GetDriver("mssql")
	if err != nil {
		t.Fatalf("GetDriver(\"mssql\") unexpected error: %v", err)
	}
	if d == nil {
		t.Fatal("GetDriver(\"mssql\") returned nil")
	}
	if got := d.DefaultSchema(); got != "dbo" {
		t.Errorf("DefaultSchema() = %q, want %q", got, "dbo")
	}
}

func TestGetDriver_Unknown(t *testing.T) {
	_, err := GetDriver("unknown")
	if err == nil {
		t.Error("GetDriver(\"unknown\") expected error, got nil")
	}
}

func TestMSSQLDriver_QuoteIdentifier(t *testing.T) {
	d, _ := GetDriver("mssql")
	if got := d.QuoteIdentifier("my_table"); got != "[my_table]" {
		t.Errorf("QuoteIdentifier() = %q, want %q", got, "[my_table]")
	}
}

func TestMSSQLDriver_ArrowType(t *testing.T) {
	d, _ := GetDriver("mssql")
	tests := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int32, "INT"},
		{arrow.PrimitiveTypes.Int64, "BIGINT"},
		{arrow.PrimitiveTypes.Float64, "FLOAT"},
		{arrow.BinaryTypes.String, "NVARCHAR(MAX)"},
		{arrow.FixedWidthTypes.Boolean, "BIT"},
	}
	for _, tt := range tests {
		got, err := d.ArrowType(tt.dt)
		if err != nil {
			t.Errorf("ArrowType(%s) error: %v", tt.dt, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ArrowType(%s) = %q, want %q", tt.dt, got, tt.want)
		}
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/loader/ -run 'TestGetDriver|TestMSSQLDriver' -v`
Expected: FAIL — `GetDriver` undefined

**Step 3: Create `driver.go` with interface and registry**

Create `internal/loader/driver.go`:

```go
package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Driver abstracts database-specific bulk load and DDL operations.
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

var drivers = map[string]Driver{
	"mssql": &MSSQLDriver{},
}

// GetDriver returns the Driver for the given name.
func GetDriver(name string) (Driver, error) {
	d, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("unsupported database driver %q", name)
	}
	return d, nil
}
```

**Step 4: Refactor `mssql.go` into `driver_mssql.go` implementing the Driver interface**

Rename `internal/loader/mssql.go` to `internal/loader/driver_mssql.go`. Wrap existing functions as methods on `MSSQLDriver`:

```go
package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	mssql "github.com/microsoft/go-mssqldb"
)

// MSSQLDriver implements Driver for Microsoft SQL Server.
type MSSQLDriver struct{}

func (d *MSSQLDriver) DefaultSchema() string { return "dbo" }

func (d *MSSQLDriver) QuoteIdentifier(name string) string {
	return "[" + name + "]"
}

func (d *MSSQLDriver) ArrowType(dt arrow.DataType) (string, error) {
	// Same as existing arrowTypeToMSSQL
	switch dt.ID() {
	case arrow.INT8:
		return "SMALLINT", nil
	case arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT32:
		return "INT", nil
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.UINT8:
		return "TINYINT", nil
	case arrow.UINT16:
		return "INT", nil
	case arrow.UINT32:
		return "BIGINT", nil
	case arrow.UINT64:
		return "BIGINT", nil
	case arrow.FLOAT32:
		return "REAL", nil
	case arrow.FLOAT64:
		return "FLOAT", nil
	case arrow.STRING:
		return "NVARCHAR(MAX)", nil
	case arrow.BOOL:
		return "BIT", nil
	case arrow.TIMESTAMP:
		return "DATETIME2", nil
	case arrow.DATE32:
		return "DATE", nil
	case arrow.BINARY:
		return "VARBINARY(MAX)", nil
	case arrow.LARGE_STRING:
		return "NVARCHAR(MAX)", nil
	default:
		return "", fmt.Errorf("unsupported Arrow type %s for MSSQL column", dt)
	}
}

func (d *MSSQLDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	// Implemented in Task 7 (save path)
	return nil, fmt.Errorf("SQLTypeToArrow not yet implemented for MSSQL")
}

func (d *MSSQLDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
	var cols []string
	for _, f := range arrowSchema.Fields() {
		sqlType, err := d.ArrowType(f.Type)
		if err != nil {
			return fmt.Errorf("column %q: %w", f.Name, err)
		}
		null := "NOT NULL"
		if f.Nullable {
			null = "NULL"
		}
		cols = append(cols, fmt.Sprintf("    [%s] %s %s", f.Name, sqlType, null))
	}
	ddl := fmt.Sprintf("CREATE TABLE [%s].[%s] (\n%s\n)", schema, table, joinStrings(cols, ",\n"))
	_, err := db.ExecContext(ctx, ddl)
	return err
}

func (d *MSSQLDriver) DropTable(ctx context.Context, db *sql.DB, schema, table string) error {
	dropSQL := fmt.Sprintf("IF OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL DROP TABLE [%s].[%s]",
		schema, table, schema, table)
	_, err := db.ExecContext(ctx, dropSQL)
	return err
}

func (d *MSSQLDriver) TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error {
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE [%s].[%s]", schema, table)
	_, err := db.ExecContext(ctx, truncateSQL)
	return err
}

func (d *MSSQLDriver) BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error) {
	schema := stream.Schema()
	colNames := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		colNames[i] = f.Name
	}

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.PrepareContext(ctx, mssql.CopyIn(
		fmt.Sprintf("[%s].[%s]", params.Schema, params.Table),
		mssql.BulkOptions{},
		colNames...,
	))
	if err != nil {
		return 0, fmt.Errorf("preparing bulk copy: %w", err)
	}
	defer stmt.Close()

	var totalRows int64
	for stream.Next() {
		rec := stream.Record()
		numRows := int(rec.NumRows())
		numCols := int(rec.NumCols())

		for row := 0; row < numRows; row++ {
			vals := make([]interface{}, numCols)
			for col := 0; col < numCols; col++ {
				v, err := arrowValue(rec.Column(col), row)
				if err != nil {
					return totalRows, fmt.Errorf("row %d col %d: %w", row, col, err)
				}
				vals[col] = v
			}
			if _, err := stmt.ExecContext(ctx, vals...); err != nil {
				return totalRows, fmt.Errorf("exec row %d: %w", row, err)
			}
		}
		totalRows += int64(numRows)
	}
	if err := stream.Err(); err != nil {
		return totalRows, fmt.Errorf("reading parquet: %w", err)
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return totalRows, fmt.Errorf("flushing bulk copy: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return totalRows, fmt.Errorf("committing transaction: %w", err)
	}

	return totalRows, nil
}
```

**Step 5: Refactor `loader.go` to use Driver interface for mode handling**

Replace the `Load()` function in `internal/loader/loader.go`:

```go
package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/druarnfield/pit/internal/runner"
)

// LoadMode controls how data is loaded into the target table.
type LoadMode string

const (
	ModeAppend          LoadMode = "append"
	ModeTruncateAndLoad LoadMode = "truncate_and_load"
	ModeCreateOrReplace LoadMode = "create_or_replace"
)

// LoadParams configures a data load operation.
type LoadParams struct {
	FilePath string   // path to the Parquet file
	Table    string   // target table name
	Schema   string   // target schema (default depends on driver)
	Mode     LoadMode // append, truncate_and_load, or create_or_replace
	ConnStr  string   // database connection string
}

// Load reads a Parquet file and bulk-loads it into the target database.
func Load(ctx context.Context, params LoadParams) (int64, error) {
	driverName, err := runner.DetectDriver(params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("detecting driver: %w", err)
	}

	drv, err := GetDriver(driverName)
	if err != nil {
		return 0, fmt.Errorf("getting driver: %w", err)
	}

	if params.Schema == "" {
		params.Schema = drv.DefaultSchema()
	}
	if params.Mode == "" {
		params.Mode = ModeAppend
	}

	stream, err := openParquetStream(params.FilePath)
	if err != nil {
		return 0, fmt.Errorf("reading parquet file: %w", err)
	}
	defer stream.Close()

	db, err := sql.Open(driverName, params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close()

	// Handle mode: create_or_replace drops and recreates
	if params.Mode == ModeCreateOrReplace {
		if err := drv.DropTable(ctx, db, params.Schema, params.Table); err != nil {
			return 0, fmt.Errorf("dropping table: %w", err)
		}
		if err := drv.CreateTable(ctx, db, params.Schema, params.Table, stream.Schema()); err != nil {
			return 0, fmt.Errorf("creating table: %w", err)
		}
	}

	// Handle mode: truncate_and_load truncates first
	if params.Mode == ModeTruncateAndLoad {
		if err := drv.TruncateTable(ctx, db, params.Schema, params.Table); err != nil {
			return 0, fmt.Errorf("truncating table: %w", err)
		}
	}

	return drv.BulkLoad(ctx, db, params, stream)
}
```

**Step 6: Update existing tests — the old `arrowTypeToMSSQL` tests should call through the driver**

In `internal/loader/loader_test.go`, update `TestArrowTypeToMSSQL` to use the driver interface (the function is now a method). Update `TestCreateTableDDL` — `createTableDDL` is now `MSSQLDriver.CreateTable` (but we can keep the old test helper for DDL string verification or just remove it since `CreateTable` actually executes SQL). Keep `createTableDDL` as a package-level helper if tests need it, or refactor. The simplest approach: keep the old `arrowTypeToMSSQL` and `createTableDDL` functions as thin wrappers that delegate to the driver, or remove them and update tests to use the driver methods directly.

**Recommended approach:** Remove the old standalone functions entirely. Update tests to call `d.ArrowType()` and verify the DDL via `d.CreateTable()` behavior. The `TestCreateTableDDL` test needs to change since `CreateTable` now executes against a DB — convert it to test the DDL string generation via `ArrowType` only, or keep a helper.

Actually, to keep tests simple and avoid needing a real DB: extract DDL generation into a helper method `buildCreateTableDDL` on `MSSQLDriver` that returns the string, and have `CreateTable` call it.

```go
// In driver_mssql.go, add:
func (d *MSSQLDriver) buildCreateTableDDL(schema, table string, arrowSchema *arrow.Schema) (string, error) {
	var cols []string
	for _, f := range arrowSchema.Fields() {
		sqlType, err := d.ArrowType(f.Type)
		if err != nil {
			return "", fmt.Errorf("column %q: %w", f.Name, err)
		}
		null := "NOT NULL"
		if f.Nullable {
			null = "NULL"
		}
		cols = append(cols, fmt.Sprintf("    [%s] %s %s", f.Name, sqlType, null))
	}
	return fmt.Sprintf("CREATE TABLE [%s].[%s] (\n%s\n)", schema, table, joinStrings(cols, ",\n")), nil
}

func (d *MSSQLDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
	ddl, err := d.buildCreateTableDDL(schema, table, arrowSchema)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, ddl)
	return err
}
```

Update test:

```go
func TestMSSQLDriver_BuildCreateTableDDL(t *testing.T) {
	d := &MSSQLDriver{}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	ddl, err := d.buildCreateTableDDL("dbo", "test_table", schema)
	if err != nil {
		t.Fatalf("buildCreateTableDDL() error: %v", err)
	}
	expectations := []string{
		"CREATE TABLE [dbo].[test_table]",
		"[id] INT NOT NULL",
		"[name] NVARCHAR(MAX) NULL",
	}
	for _, exp := range expectations {
		if !containsStr(ddl, exp) {
			t.Errorf("DDL missing %q\ngot:\n%s", exp, ddl)
		}
	}
}
```

**Step 7: Run all tests**

Run: `go test -race ./internal/loader/ -v`
Expected: All PASS

**Step 8: Commit**

```bash
git add internal/loader/driver.go internal/loader/driver_mssql.go internal/loader/loader.go internal/loader/loader_test.go
git rm internal/loader/mssql.go  # if renamed
git commit -m "Refactor loader into driver interface, extract MSSQL driver"
```

---

### Task 2: Expand DetectDriver for All Databases

Add postgres, clickhouse, and oracle prefix detection to `DetectDriver()` in `internal/runner/sql.go`.

**Files:**
- Modify: `internal/runner/sql.go:85-93`
- Test: `internal/runner/sql_test.go` (may need to create)

**Step 1: Write failing tests**

Check if `internal/runner/sql_test.go` exists. If not, create it. Add:

```go
func TestDetectDriver(t *testing.T) {
	tests := []struct {
		connStr string
		want    string
		wantErr bool
	}{
		{"sqlserver://host/db", "mssql", false},
		{"mssql://host/db", "mssql", false},
		{"SQLSERVER://HOST/DB", "mssql", false},
		{"postgres://host/db", "postgres", false},
		{"postgresql://host/db", "postgres", false},
		{"clickhouse://host/db", "clickhouse", false},
		{"oracle://host/db", "oracle", false},
		{"mysql://host/db", "", true},
		{"http://example.com", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.connStr, func(t *testing.T) {
			got, err := DetectDriver(tt.connStr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("DetectDriver(%q) expected error, got nil", tt.connStr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DetectDriver(%q) unexpected error: %v", tt.connStr, err)
			}
			if got != tt.want {
				t.Errorf("DetectDriver(%q) = %q, want %q", tt.connStr, got, tt.want)
			}
		})
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/runner/ -run TestDetectDriver -v`
Expected: FAIL — postgres/clickhouse/oracle cases fail

**Step 3: Update DetectDriver**

In `internal/runner/sql.go`, replace `DetectDriver`:

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
		return "", fmt.Errorf("cannot detect SQL driver from connection string (supported: sqlserver://, postgres://, clickhouse://, oracle://)")
	}
}
```

**Step 4: Run tests**

Run: `go test -race ./internal/runner/ -run TestDetectDriver -v`
Expected: All PASS

**Step 5: Update loader test expectation**

In `internal/loader/loader_test.go`, `TestLoadParams_Defaults` uses `postgres://host/db` and expects `"detecting driver"` error. After this change, it will get past driver detection and fail at `GetDriver("postgres")` instead. Update the test:

```go
// The error message will now be about unsupported driver (no postgres driver registered yet)
expected := "getting driver"
```

**Step 6: Run all tests**

Run: `go test -race ./internal/loader/ ./internal/runner/ -v`
Expected: All PASS

**Step 7: Commit**

```bash
git add internal/runner/sql.go internal/runner/sql_test.go internal/loader/loader_test.go
git commit -m "Expand DetectDriver to support postgres, clickhouse, oracle prefixes"
```

---

### Task 3: Add Task Config Fields & Validation

Add `Type`, `Source`, `Output`, `Table`, `Mode`, and `Connection` fields to `TaskConfig`. Add validation rules to `dag/validate.go`.

**Files:**
- Modify: `internal/config/config.go:98-106`
- Modify: `internal/dag/validate.go`
- Test: `internal/dag/validate_test.go`

**Step 1: Write failing validation tests**

Add to `internal/dag/validate_test.go`:

```go
func TestValidate_LoadTask_Valid(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test", SQL: config.SQLConfig{Connection: "pg_conn"}},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data.parquet", Table: "staging.customers", Mode: "truncate_and_load"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
}

func TestValidate_LoadTask_MissingSource(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test", SQL: config.SQLConfig{Connection: "pg_conn"}},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Table: "staging.customers"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) == 0 {
		t.Fatal("expected validation error for missing source")
	}
}

func TestValidate_LoadTask_ScriptNotAllowed(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test", SQL: config.SQLConfig{Connection: "pg_conn"}},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data.parquet", Table: "staging.customers", Script: "tasks/load.sql"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) == 0 {
		t.Fatal("expected validation error for script on load task")
	}
}

func TestValidate_SaveTask_Valid(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "tasks"), 0755)
	os.WriteFile(filepath.Join(dir, "tasks/extract.sql"), []byte("SELECT 1"), 0644)
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test", SQL: config.SQLConfig{Connection: "pg_conn"}},
		Tasks: []config.TaskConfig{
			{Name: "save_data", Type: "save", Script: "tasks/extract.sql", Output: "out.parquet"},
		},
	}
	errs := Validate(cfg, dir)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got %v", errs)
	}
}

func TestValidate_SaveTask_MissingOutput(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "tasks"), 0755)
	os.WriteFile(filepath.Join(dir, "tasks/extract.sql"), []byte("SELECT 1"), 0644)
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test", SQL: config.SQLConfig{Connection: "pg_conn"}},
		Tasks: []config.TaskConfig{
			{Name: "save_data", Type: "save", Script: "tasks/extract.sql"},
		},
	}
	errs := Validate(cfg, dir)
	if len(errs) == 0 {
		t.Fatal("expected validation error for missing output")
	}
}

func TestValidate_InvalidTaskType(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "bad", Type: "unknown", Script: "tasks/run.sh"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) == 0 {
		t.Fatal("expected validation error for invalid task type")
	}
}

func TestValidate_LoadTask_InvalidMode(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test", SQL: config.SQLConfig{Connection: "pg_conn"}},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data.parquet", Table: "staging.t", Mode: "invalid"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) == 0 {
		t.Fatal("expected validation error for invalid mode")
	}
}

func TestValidate_ModeOnNonLoadTask(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "tasks"), 0755)
	os.WriteFile(filepath.Join(dir, "tasks/run.sh"), []byte("echo hi"), 0644)
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "run", Script: "tasks/run.sh", Mode: "append"},
		},
	}
	errs := Validate(cfg, dir)
	if len(errs) == 0 {
		t.Fatal("expected validation error for mode on non-load task")
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/dag/ -run 'TestValidate_LoadTask|TestValidate_SaveTask|TestValidate_InvalidTaskType|TestValidate_ModeOn' -v`
Expected: FAIL — `Type`, `Source`, etc. fields don't exist on `TaskConfig`

**Step 3: Add fields to TaskConfig**

In `internal/config/config.go`, update `TaskConfig`:

```go
type TaskConfig struct {
	Name       string   `toml:"name"`
	Script     string   `toml:"script"`
	Runner     string   `toml:"runner"`
	DependsOn  []string `toml:"depends_on"`
	Timeout    Duration `toml:"timeout"`
	Retries    int      `toml:"retries"`
	RetryDelay Duration `toml:"retry_delay"`
	Type       string   `toml:"type"`       // "load", "save", or "" (default exec)
	Source     string   `toml:"source"`     // Parquet file for load
	Output     string   `toml:"output"`     // Parquet file for save
	Table      string   `toml:"table"`      // target table for load
	Mode       string   `toml:"mode"`       // "append", "truncate_and_load", "create_or_replace"
	Connection string   `toml:"connection"` // overrides [dag.sql].connection
}
```

**Step 4: Add validation rules to validate.go**

In `internal/dag/validate.go`, in the task loop (after the `depends_on` check, before the script check), add:

```go
// Validate task type
validTypes := map[string]bool{"": true, "load": true, "save": true}
if !validTypes[t.Type] {
	errs = append(errs, &ValidationError{
		DAG:     dagName,
		Task:    t.Name,
		Message: fmt.Sprintf("invalid task type %q (must be load or save)", t.Type),
	})
}

// Validate mode only on load tasks
if t.Mode != "" && t.Type != "load" {
	errs = append(errs, &ValidationError{
		DAG:     dagName,
		Task:    t.Name,
		Message: "mode is only valid on type = \"load\" tasks",
	})
}

if t.Type == "load" {
	validModes := map[string]bool{"": true, "append": true, "truncate_and_load": true, "create_or_replace": true}
	if !validModes[t.Mode] {
		errs = append(errs, &ValidationError{
			DAG:     dagName,
			Task:    t.Name,
			Message: fmt.Sprintf("invalid mode %q (must be append, truncate_and_load, or create_or_replace)", t.Mode),
		})
	}
	if t.Source == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "load task requires source"})
	}
	if t.Table == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "load task requires table"})
	}
	if t.Script != "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "load task must not have script"})
	}
}

if t.Type == "save" {
	if t.Script == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "save task requires script"})
	}
	if t.Output == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "save task requires output"})
	}
	if t.Source != "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "save task must not have source"})
	}
	if t.Table != "" {
		errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "save task must not have table"})
	}
}
```

Also, skip the script file existence check for `type = "load"` tasks (they have no script). Update the existing script check to exclude load tasks:

```go
if t.Type != "load" && t.Runner == "dbt" {
    // existing dbt check
} else if t.Type != "load" && t.Script != "" && cfg.DAG.GitURL == "" {
    // existing script existence check
} else if t.Type == "" && t.Script == "" && t.Runner != "dbt" {
    // existing missing script error
}
```

**Step 5: Run tests**

Run: `go test -race ./internal/dag/ -v`
Expected: All PASS

**Step 6: Run full test suite**

Run: `go test -race ./...`
Expected: All PASS (no breakage)

**Step 7: Commit**

```bash
git add internal/config/config.go internal/dag/validate.go internal/dag/validate_test.go
git commit -m "Add load/save task type config fields and validation"
```

---

### Task 4: Postgres Driver

Implement the Postgres driver using pgx's COPY protocol for bulk loading.

**Files:**
- Create: `internal/loader/driver_postgres.go`
- Modify: `internal/loader/driver.go` (register driver)
- Test: `internal/loader/loader_test.go`

**Step 1: Add pgx dependency**

Run: `go get github.com/jackc/pgx/v5`

**Step 2: Write failing test**

```go
func TestGetDriver_Postgres(t *testing.T) {
	d, err := GetDriver("postgres")
	if err != nil {
		t.Fatalf("GetDriver(\"postgres\") unexpected error: %v", err)
	}
	if got := d.DefaultSchema(); got != "public" {
		t.Errorf("DefaultSchema() = %q, want %q", got, "public")
	}
	if got := d.QuoteIdentifier("my_table"); got != `"my_table"` {
		t.Errorf("QuoteIdentifier() = %q, want %q", got, `"my_table"`)
	}
}

func TestPostgresDriver_ArrowType(t *testing.T) {
	d, _ := GetDriver("postgres")
	tests := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int32, "INTEGER"},
		{arrow.PrimitiveTypes.Int64, "BIGINT"},
		{arrow.PrimitiveTypes.Float64, "DOUBLE PRECISION"},
		{arrow.BinaryTypes.String, "TEXT"},
		{arrow.FixedWidthTypes.Boolean, "BOOLEAN"},
		{&arrow.TimestampType{Unit: arrow.Microsecond}, "TIMESTAMP"},
		{arrow.FixedWidthTypes.Date32, "DATE"},
		{arrow.BinaryTypes.Binary, "BYTEA"},
	}
	for _, tt := range tests {
		got, err := d.ArrowType(tt.dt)
		if err != nil {
			t.Errorf("ArrowType(%s) error: %v", tt.dt, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ArrowType(%s) = %q, want %q", tt.dt, got, tt.want)
		}
	}
}
```

**Step 3: Run test to verify it fails**

Run: `go test ./internal/loader/ -run 'TestGetDriver_Postgres|TestPostgresDriver' -v`
Expected: FAIL

**Step 4: Implement Postgres driver**

Create `internal/loader/driver_postgres.go`:

```go
package loader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
)

type PostgresDriver struct{}

func (d *PostgresDriver) DefaultSchema() string        { return "public" }
func (d *PostgresDriver) QuoteIdentifier(name string) string { return `"` + name + `"` }

func (d *PostgresDriver) ArrowType(dt arrow.DataType) (string, error) {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT32:
		return "INTEGER", nil
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.UINT8, arrow.UINT16:
		return "INTEGER", nil
	case arrow.UINT32:
		return "BIGINT", nil
	case arrow.UINT64:
		return "BIGINT", nil
	case arrow.FLOAT32:
		return "REAL", nil
	case arrow.FLOAT64:
		return "DOUBLE PRECISION", nil
	case arrow.STRING, arrow.LARGE_STRING:
		return "TEXT", nil
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.TIMESTAMP:
		return "TIMESTAMP", nil
	case arrow.DATE32:
		return "DATE", nil
	case arrow.BINARY:
		return "BYTEA", nil
	default:
		return "", fmt.Errorf("unsupported Arrow type %s for Postgres column", dt)
	}
}

func (d *PostgresDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	switch strings.ToUpper(dbTypeName) {
	case "INT2", "SMALLINT", "INT4", "INTEGER":
		return arrow.PrimitiveTypes.Int32, nil
	case "INT8", "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT4", "REAL":
		return arrow.PrimitiveTypes.Float32, nil
	case "FLOAT8", "DOUBLE PRECISION":
		return arrow.PrimitiveTypes.Float64, nil
	case "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME":
		return arrow.BinaryTypes.String, nil
	case "BOOL", "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nil
	case "TIMESTAMP", "TIMESTAMPTZ":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "BYTEA":
		return arrow.BinaryTypes.Binary, nil
	case "NUMERIC", "DECIMAL":
		return arrow.BinaryTypes.String, nil
	default:
		return nil, fmt.Errorf("unsupported Postgres type %q for Arrow mapping", dbTypeName)
	}
}

func (d *PostgresDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
	ddl, err := d.buildCreateTableDDL(schema, table, arrowSchema)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, ddl)
	return err
}

func (d *PostgresDriver) buildCreateTableDDL(schema, table string, arrowSchema *arrow.Schema) (string, error) {
	var cols []string
	for _, f := range arrowSchema.Fields() {
		sqlType, err := d.ArrowType(f.Type)
		if err != nil {
			return "", fmt.Errorf("column %q: %w", f.Name, err)
		}
		null := "NOT NULL"
		if f.Nullable {
			null = "NULL"
		}
		cols = append(cols, fmt.Sprintf("    %s %s %s", d.QuoteIdentifier(f.Name), sqlType, null))
	}
	fqTable := fmt.Sprintf("%s.%s", d.QuoteIdentifier(schema), d.QuoteIdentifier(table))
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)", fqTable, joinStrings(cols, ",\n")), nil
}

func (d *PostgresDriver) DropTable(ctx context.Context, db *sql.DB, schema, table string) error {
	fqTable := fmt.Sprintf("%s.%s", d.QuoteIdentifier(schema), d.QuoteIdentifier(table))
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+fqTable)
	return err
}

func (d *PostgresDriver) TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error {
	fqTable := fmt.Sprintf("%s.%s", d.QuoteIdentifier(schema), d.QuoteIdentifier(table))
	_, err := db.ExecContext(ctx, "TRUNCATE TABLE "+fqTable)
	return err
}

func (d *PostgresDriver) BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error) {
	schema := stream.Schema()
	colNames := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		colNames[i] = f.Name
	}

	// pgx requires a raw connection for COPY protocol
	conn, err := pgx.Connect(ctx, params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("connecting via pgx: %w", err)
	}
	defer conn.Close(ctx)

	fqTable := fmt.Sprintf("%s.%s", d.QuoteIdentifier(params.Schema), d.QuoteIdentifier(params.Table))
	quotedCols := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = d.QuoteIdentifier(c)
	}

	var totalRows int64
	for stream.Next() {
		rec := stream.Record()
		numRows := int(rec.NumRows())
		numCols := int(rec.NumCols())

		rows := make([][]interface{}, numRows)
		for row := 0; row < numRows; row++ {
			vals := make([]interface{}, numCols)
			for col := 0; col < numCols; col++ {
				v, err := arrowValue(rec.Column(col), row)
				if err != nil {
					return totalRows, fmt.Errorf("row %d col %d: %w", row, col, err)
				}
				vals[col] = v
			}
			rows[row] = vals
		}

		copyCount, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{params.Schema, params.Table},
			colNames,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return totalRows, fmt.Errorf("copy from: %w", err)
		}
		totalRows += copyCount
	}
	if err := stream.Err(); err != nil {
		return totalRows, fmt.Errorf("reading parquet: %w", err)
	}

	return totalRows, nil
}
```

Register in `driver.go`:

```go
var drivers = map[string]Driver{
	"mssql":    &MSSQLDriver{},
	"postgres": &PostgresDriver{},
}
```

**Step 5: Run tests**

Run: `go test -race ./internal/loader/ -v`
Expected: All PASS

**Step 6: Commit**

```bash
go mod tidy
git add internal/loader/driver_postgres.go internal/loader/driver.go internal/loader/loader_test.go go.mod go.sum
git commit -m "Add Postgres driver with pgx COPY protocol bulk load"
```

---

### Task 5: ClickHouse Driver

Implement the ClickHouse driver using batch insert.

**Files:**
- Create: `internal/loader/driver_clickhouse.go`
- Modify: `internal/loader/driver.go` (register)
- Test: `internal/loader/loader_test.go`

**Step 1: Add dependency**

Run: `go get github.com/ClickHouse/clickhouse-go/v2`

**Step 2: Write failing test**

```go
func TestGetDriver_ClickHouse(t *testing.T) {
	d, err := GetDriver("clickhouse")
	if err != nil {
		t.Fatalf("GetDriver(\"clickhouse\") unexpected error: %v", err)
	}
	if got := d.DefaultSchema(); got != "" {
		t.Errorf("DefaultSchema() = %q, want empty", got)
	}
}

func TestClickHouseDriver_ArrowType(t *testing.T) {
	d, _ := GetDriver("clickhouse")
	tests := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int32, "Int32"},
		{arrow.PrimitiveTypes.Int64, "Int64"},
		{arrow.PrimitiveTypes.Float64, "Float64"},
		{arrow.BinaryTypes.String, "String"},
		{arrow.FixedWidthTypes.Boolean, "Bool"},
	}
	for _, tt := range tests {
		got, err := d.ArrowType(tt.dt)
		if err != nil {
			t.Errorf("ArrowType(%s) error: %v", tt.dt, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ArrowType(%s) = %q, want %q", tt.dt, got, tt.want)
		}
	}
}
```

**Step 3: Run test to verify it fails**

Run: `go test ./internal/loader/ -run 'TestGetDriver_ClickHouse|TestClickHouseDriver' -v`

**Step 4: Implement ClickHouse driver**

Create `internal/loader/driver_clickhouse.go` with:
- `ArrowType`: Int8→`Int8`, Int16→`Int16`, Int32→`Int32`, Int64→`Int64`, Float32→`Float32`, Float64→`Float64`, String→`String`, Bool→`Bool`, Timestamp→`DateTime64(6)`, Date32→`Date`, Binary→`String` (base64)
- `SQLTypeToArrow`: reverse mapping
- `DefaultSchema`: `""` (empty — ClickHouse has databases, not schemas)
- `QuoteIdentifier`: backtick quoting
- `CreateTable`: `CREATE TABLE <table> (\n...\n) ENGINE = MergeTree() ORDER BY tuple()`
- `DropTable`: `DROP TABLE IF EXISTS <table>`
- `TruncateTable`: `TRUNCATE TABLE <table>`
- `BulkLoad`: use `clickhouse-go` native protocol — `conn.PrepareBatch`, `batch.Append`, `batch.Send`

For ClickHouse, the `BulkLoad` needs the native `clickhouse-go` connection, not `database/sql`. Use `clickhouse.Open` from the connection string, or use `database/sql` with the clickhouse driver registered. The `clickhouse-go/v2` package supports both interfaces. Use `database/sql` approach with `Prepare` + batch — but ClickHouse's batch API works via the native connection. Best approach: use the `db *sql.DB` parameter but obtain native connection via `db.Conn()` and type assertion, or simply open a native connection separately from `params.ConnStr`.

Simpler: use `database/sql` interface with `db.Begin()` + `tx.Prepare(INSERT INTO ...)` + `stmt.Exec(values...)` + `tx.Commit()`. The clickhouse-go driver supports batch inserts via this pattern when used with `database/sql`.

**Step 5: Run tests**

Run: `go test -race ./internal/loader/ -v`

**Step 6: Commit**

```bash
go mod tidy
git add internal/loader/driver_clickhouse.go internal/loader/driver.go internal/loader/loader_test.go go.mod go.sum
git commit -m "Add ClickHouse driver with batch insert bulk load"
```

---

### Task 6: Oracle Driver

Implement the Oracle driver using go-ora bulk insert.

**Files:**
- Create: `internal/loader/driver_oracle.go`
- Modify: `internal/loader/driver.go` (register)
- Test: `internal/loader/loader_test.go`

**Step 1: Add dependency**

Run: `go get github.com/sijms/go-ora/v2`

**Step 2: Write failing test**

```go
func TestGetDriver_Oracle(t *testing.T) {
	d, err := GetDriver("oracle")
	if err != nil {
		t.Fatalf("GetDriver(\"oracle\") unexpected error: %v", err)
	}
	// Oracle default schema is empty (derived from connection user at runtime)
	if got := d.DefaultSchema(); got != "" {
		t.Errorf("DefaultSchema() = %q, want empty", got)
	}
}

func TestOracleDriver_ArrowType(t *testing.T) {
	d, _ := GetDriver("oracle")
	tests := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int32, "NUMBER(10)"},
		{arrow.PrimitiveTypes.Int64, "NUMBER(19)"},
		{arrow.PrimitiveTypes.Float64, "BINARY_DOUBLE"},
		{arrow.BinaryTypes.String, "CLOB"},
		{arrow.FixedWidthTypes.Boolean, "NUMBER(1)"},
	}
	for _, tt := range tests {
		got, err := d.ArrowType(tt.dt)
		if err != nil {
			t.Errorf("ArrowType(%s) error: %v", tt.dt, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ArrowType(%s) = %q, want %q", tt.dt, got, tt.want)
		}
	}
}
```

**Step 3: Run test to verify it fails**

Run: `go test ./internal/loader/ -run 'TestGetDriver_Oracle|TestOracleDriver' -v`

**Step 4: Implement Oracle driver**

Create `internal/loader/driver_oracle.go` with:
- `ArrowType`: Int8→`NUMBER(3)`, Int16→`NUMBER(5)`, Int32→`NUMBER(10)`, Int64→`NUMBER(19)`, Float32→`BINARY_FLOAT`, Float64→`BINARY_DOUBLE`, String→`CLOB`, Bool→`NUMBER(1)`, Timestamp→`TIMESTAMP`, Date32→`DATE`, Binary→`BLOB`
- `SQLTypeToArrow`: reverse mapping
- `DefaultSchema`: `""` (derived from connection user)
- `QuoteIdentifier`: double-quote `"NAME"`
- `CreateTable`: `CREATE TABLE "SCHEMA"."TABLE" (...)` with uppercase identifiers
- `DropTable`: PL/SQL block to check existence before dropping
- `TruncateTable`: `TRUNCATE TABLE "SCHEMA"."TABLE"`
- `BulkLoad`: use `go-ora` — open connection, prepare INSERT, batch exec. For go-ora bulk insert, accumulate values into column-oriented slices and use `go_ora.BulkInsert`.

**Step 5: Run tests**

Run: `go test -race ./internal/loader/ -v`

**Step 6: Commit**

```bash
go mod tidy
git add internal/loader/driver_oracle.go internal/loader/driver.go internal/loader/loader_test.go go.mod go.sum
git commit -m "Add Oracle driver with go-ora bulk insert"
```

---

### Task 7: Save Path (Query to Parquet)

Implement `Save()` — executes a SQL query and writes results to a Parquet file. Add `SQLTypeToArrow()` implementation to the MSSQL driver.

**Files:**
- Create: `internal/loader/saver.go`
- Create: `internal/loader/parquet_writer.go`
- Modify: `internal/loader/driver_mssql.go` (implement `SQLTypeToArrow`)
- Test: `internal/loader/saver_test.go`

**Step 1: Write failing test for SQLTypeToArrow on MSSQL**

Add to `internal/loader/loader_test.go`:

```go
func TestMSSQLDriver_SQLTypeToArrow(t *testing.T) {
	d := &MSSQLDriver{}
	tests := []struct {
		dbType string
		want   arrow.DataType
	}{
		{"INT", arrow.PrimitiveTypes.Int32},
		{"BIGINT", arrow.PrimitiveTypes.Int64},
		{"FLOAT", arrow.PrimitiveTypes.Float64},
		{"NVARCHAR", arrow.BinaryTypes.String},
		{"BIT", arrow.FixedWidthTypes.Boolean},
		{"DATETIME2", &arrow.TimestampType{Unit: arrow.Microsecond}},
		{"DATE", arrow.FixedWidthTypes.Date32},
		{"DECIMAL", arrow.BinaryTypes.String},
	}
	for _, tt := range tests {
		got, err := d.SQLTypeToArrow(tt.dbType)
		if err != nil {
			t.Errorf("SQLTypeToArrow(%q) error: %v", tt.dbType, err)
			continue
		}
		if got.ID() != tt.want.ID() {
			t.Errorf("SQLTypeToArrow(%q) = %s, want %s", tt.dbType, got, tt.want)
		}
	}
}
```

**Step 2: Implement MSSQL SQLTypeToArrow**

In `internal/loader/driver_mssql.go`, replace the stub:

```go
func (d *MSSQLDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	switch strings.ToUpper(dbTypeName) {
	case "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "TINYINT":
		return arrow.PrimitiveTypes.Uint8, nil
	case "REAL":
		return arrow.PrimitiveTypes.Float32, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float64, nil
	case "NVARCHAR", "VARCHAR", "CHAR", "NCHAR", "NTEXT", "TEXT", "XML", "UNIQUEIDENTIFIER":
		return arrow.BinaryTypes.String, nil
	case "BIT":
		return arrow.FixedWidthTypes.Boolean, nil
	case "DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "VARBINARY", "BINARY", "IMAGE":
		return arrow.BinaryTypes.Binary, nil
	case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
		return arrow.BinaryTypes.String, nil
	default:
		return nil, fmt.Errorf("unsupported MSSQL type %q for Arrow mapping", dbTypeName)
	}
}
```

**Step 3: Write failing test for Save params types**

Create `internal/loader/saver_test.go`:

```go
package loader

import (
	"testing"
)

func TestSaveParams_Fields(t *testing.T) {
	// Verify SaveParams struct exists with expected fields
	p := SaveParams{
		Query:    "SELECT 1",
		FilePath: "/tmp/out.parquet",
		ConnStr:  "postgres://localhost/db",
	}
	if p.Query != "SELECT 1" {
		t.Errorf("Query = %q, want %q", p.Query, "SELECT 1")
	}
}
```

**Step 4: Implement saver.go**

Create `internal/loader/saver.go`:

```go
package loader

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/druarnfield/pit/internal/runner"
)

// SaveParams configures a query-to-Parquet save operation.
type SaveParams struct {
	Query    string // SQL SELECT query
	FilePath string // output Parquet file path
	ConnStr  string // database connection string
}

// Save executes a SQL query and writes the results to a Parquet file.
// Returns the number of rows written.
func Save(ctx context.Context, params SaveParams) (int64, error) {
	driverName, err := runner.DetectDriver(params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("detecting driver: %w", err)
	}

	drv, err := GetDriver(driverName)
	if err != nil {
		return 0, fmt.Errorf("getting driver: %w", err)
	}

	db, err := sql.Open(driverName, params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, params.Query)
	if err != nil {
		return 0, fmt.Errorf("executing query: %w", err)
	}
	defer rows.Close()

	// Get column metadata
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("getting column types: %w", err)
	}

	// Build Arrow schema from column types
	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		arrowType, err := drv.SQLTypeToArrow(ct.DatabaseTypeName())
		if err != nil {
			return 0, fmt.Errorf("column %q (type %q): %w", ct.Name(), ct.DatabaseTypeName(), err)
		}
		nullable, _ := ct.Nullable()
		fields[i] = arrow.Field{
			Name:     ct.Name(),
			Type:     arrowType,
			Nullable: nullable,
		}
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	// Create output file
	if err := os.MkdirAll(filepath.Dir(params.FilePath), 0755); err != nil {
		return 0, fmt.Errorf("creating output directory: %w", err)
	}

	return writeRowsToParquet(rows, colTypes, arrowSchema, params.FilePath)
}
```

**Step 5: Implement parquet_writer.go**

Create `internal/loader/parquet_writer.go`:

```go
package loader

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const saveBatchSize = 65536

// writeRowsToParquet streams database rows into a Parquet file.
func writeRowsToParquet(rows *sql.Rows, colTypes []*sql.ColumnType, schema *arrow.Schema, filePath string) (int64, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("creating output file: %w", err)
	}
	defer f.Close()

	writerProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	writer, err := pqarrow.NewFileWriter(schema, f, nil, writerProps)
	if err != nil {
		return 0, fmt.Errorf("creating parquet writer: %w", err)
	}

	pool := memory.DefaultAllocator
	var totalRows int64

	for {
		builder := array.NewRecordBuilder(pool, schema)
		var batchRows int64

		for batchRows < saveBatchSize && rows.Next() {
			// Create scan targets
			scanVals := make([]interface{}, len(colTypes))
			for i := range scanVals {
				scanVals[i] = new(interface{})
			}

			if err := rows.Scan(scanVals...); err != nil {
				builder.Release()
				writer.Close()
				return totalRows, fmt.Errorf("scanning row: %w", err)
			}

			for i, sv := range scanVals {
				val := *(sv.(*interface{}))
				appendToBuilder(builder.Field(i), schema.Field(i).Type, val)
			}
			batchRows++
		}

		if batchRows == 0 {
			builder.Release()
			break
		}

		rec := builder.NewRecord()
		if err := writer.Write(rec); err != nil {
			rec.Release()
			builder.Release()
			writer.Close()
			return totalRows, fmt.Errorf("writing batch: %w", err)
		}
		totalRows += batchRows
		rec.Release()
		builder.Release()
	}

	if err := rows.Err(); err != nil {
		writer.Close()
		return totalRows, fmt.Errorf("iterating rows: %w", err)
	}

	if err := writer.Close(); err != nil {
		return totalRows, fmt.Errorf("closing parquet writer: %w", err)
	}

	return totalRows, nil
}

// appendToBuilder appends a scanned database value to the appropriate Arrow builder.
func appendToBuilder(fb array.Builder, dt arrow.DataType, val interface{}) {
	if val == nil {
		fb.AppendNull()
		return
	}

	switch b := fb.(type) {
	case *array.Int8Builder:
		b.Append(toInt8(val))
	case *array.Int16Builder:
		b.Append(toInt16(val))
	case *array.Int32Builder:
		b.Append(toInt32(val))
	case *array.Int64Builder:
		b.Append(toInt64(val))
	case *array.Uint8Builder:
		b.Append(toUint8(val))
	case *array.Float32Builder:
		b.Append(toFloat32(val))
	case *array.Float64Builder:
		b.Append(toFloat64(val))
	case *array.StringBuilder:
		b.Append(fmt.Sprintf("%v", val))
	case *array.BooleanBuilder:
		b.Append(toBool(val))
	case *array.TimestampBuilder:
		ts := toTime(val)
		b.Append(arrow.Timestamp(ts.UnixMicro()))
	case *array.Date32Builder:
		ts := toTime(val)
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(ts.Sub(epoch).Hours() / 24)
		b.Append(arrow.Date32(days))
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		default:
			b.Append([]byte(fmt.Sprintf("%v", val)))
		}
	default:
		fb.AppendNull()
	}
}

// Type conversion helpers for appendToBuilder
func toInt8(v interface{}) int8   { /* switch on int64, float64, etc */ }
func toInt16(v interface{}) int16 { /* ... */ }
func toInt32(v interface{}) int32 {
	switch v := v.(type) {
	case int64:
		return int32(v)
	case float64:
		return int32(v)
	case int32:
		return v
	default:
		return 0
	}
}
func toInt64(v interface{}) int64 {
	switch v := v.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int32:
		return int64(v)
	default:
		return 0
	}
}
func toUint8(v interface{}) uint8 { /* ... */ }
func toFloat32(v interface{}) float32 {
	switch v := v.(type) {
	case float64:
		return float32(v)
	case float32:
		return v
	default:
		return 0
	}
}
func toFloat64(v interface{}) float64 {
	switch v := v.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}
func toBool(v interface{}) bool {
	switch v := v.(type) {
	case bool:
		return v
	case int64:
		return v != 0
	default:
		return false
	}
}
func toTime(v interface{}) time.Time {
	switch v := v.(type) {
	case time.Time:
		return v
	default:
		return time.Time{}
	}
}
```

**Step 6: Run tests**

Run: `go test -race ./internal/loader/ -v`
Expected: All PASS

**Step 7: Commit**

```bash
git add internal/loader/saver.go internal/loader/parquet_writer.go internal/loader/driver_mssql.go internal/loader/saver_test.go internal/loader/loader_test.go
git commit -m "Add save path: SQL query to Parquet with per-driver type mapping"
```

---

### Task 8: Executor Integration

Wire `load` and `save` task types into the executor. Add per-task connection override support.

**Files:**
- Modify: `internal/engine/executor.go:411-638` (executeTask function)
- Modify: `internal/engine/executor.go:667-727` (makeLoadDataHandler — update default schema)
- Test: `internal/engine/executor_test.go` (or add to existing)

**Step 1: Write test for connection resolution**

Add a test (in an appropriate test file) for the `resolveTaskConnection` helper:

```go
func TestResolveTaskConnection(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{SQL: config.SQLConfig{Connection: "default_conn"}},
	}
	// Task override wins
	tc := &config.TaskConfig{Connection: "task_conn"}
	if got := resolveTaskConnection(tc, cfg); got != "task_conn" {
		t.Errorf("got %q, want %q", got, "task_conn")
	}
	// Falls back to DAG default
	tc2 := &config.TaskConfig{}
	if got := resolveTaskConnection(tc2, cfg); got != "default_conn" {
		t.Errorf("got %q, want %q", got, "default_conn")
	}
}
```

**Step 2: Implement executor changes**

In `executeTask`, before the runner resolution block, add handling for load/save task types. Find the task config from `cfg.Tasks` matching `ti.Name`:

```go
// Find the task config
var tc *config.TaskConfig
for i := range cfg.Tasks {
	if cfg.Tasks[i].Name == ti.Name {
		tc = &cfg.Tasks[i]
		break
	}
}

if tc != nil && (tc.Type == "load" || tc.Type == "save") {
	err := executeSQLTask(ctx, ti, run, cfg, tc, opts, logWriter)
	if err != nil {
		run.mu.Lock()
		ti.Status = StatusFailed
		ti.Error = err
		ti.EndedAt = time.Now()
		run.mu.Unlock()
	} else {
		run.mu.Lock()
		ti.Status = StatusSuccess
		ti.EndedAt = time.Now()
		run.mu.Unlock()
	}
	return
}
```

Add `executeSQLTask` function:

```go
func executeSQLTask(ctx context.Context, ti *TaskInstance, run *Run, cfg *config.ProjectConfig, tc *config.TaskConfig, opts ExecuteOpts, logWriter io.Writer) error {
	connKey := resolveTaskConnection(tc, cfg)
	if connKey == "" {
		return fmt.Errorf("no connection configured (set connection on task or [dag.sql])")
	}
	if run.SecretsResolver == nil {
		return fmt.Errorf("secrets store not configured (use --secrets flag)")
	}

	connStr, err := run.SecretsResolver.Resolve(run.DAGName, connKey)
	if err != nil {
		return fmt.Errorf("resolving connection %q: %w", connKey, err)
	}

	start := time.Now()

	switch tc.Type {
	case "load":
		sourcePath := filepath.Join(run.DataDir, tc.Source)
		mode := tc.Mode
		if mode == "" {
			mode = "append"
		}
		// Parse schema.table
		schema, table := parseSchemaTable(tc.Table)
		rows, err := loader.Load(ctx, loader.LoadParams{
			FilePath: sourcePath,
			Table:    table,
			Schema:   schema,
			Mode:     loader.LoadMode(mode),
			ConnStr:  connStr,
		})
		if err != nil {
			return fmt.Errorf("loading data: %w", err)
		}
		elapsed := time.Since(start)
		fmt.Fprintf(logWriter, "[load] %s → %s: %d rows loaded in %s\n",
			tc.Source, tc.Table, rows, elapsed.Round(time.Millisecond))

	case "save":
		scriptPath := filepath.Join(run.SnapshotDir, tc.Script)
		query, err := os.ReadFile(scriptPath)
		if err != nil {
			return fmt.Errorf("reading SQL script %s: %w", tc.Script, err)
		}
		outputPath := filepath.Join(run.DataDir, tc.Output)
		rows, err := loader.Save(ctx, loader.SaveParams{
			Query:    string(query),
			FilePath: outputPath,
			ConnStr:  connStr,
		})
		if err != nil {
			return fmt.Errorf("saving data: %w", err)
		}
		elapsed := time.Since(start)
		fmt.Fprintf(logWriter, "[save] %s → %s: %d rows saved in %s\n",
			tc.Script, tc.Output, rows, elapsed.Round(time.Millisecond))
	}

	return nil
}

func resolveTaskConnection(tc *config.TaskConfig, cfg *config.ProjectConfig) string {
	if tc.Connection != "" {
		return tc.Connection
	}
	return cfg.DAG.SQL.Connection
}

// parseSchemaTable splits "schema.table" into schema and table parts.
// If no dot is present, returns empty schema and the full string as table.
func parseSchemaTable(fqTable string) (string, string) {
	parts := strings.SplitN(fqTable, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
```

Also update `makeLoadDataHandler` to use driver's `DefaultSchema()` instead of hardcoded `"dbo"`:

```go
if schema == "" {
	driverName, _ := runner.DetectDriver(connStr)
	if drv, err := loader.GetDriver(driverName); err == nil {
		schema = drv.DefaultSchema()
	}
	if schema == "" {
		schema = "dbo" // fallback
	}
}
```

**Step 3: Update TaskInstance creation in Execute()**

In the loop that builds `TaskInstance` from `TaskConfig`, pass through the new fields so `executeSQLTask` can find the config. Actually, `executeSQLTask` already takes the `*config.TaskConfig` directly by finding it from `cfg.Tasks`, so no change needed to `TaskInstance`.

**Step 4: Run tests**

Run: `go test -race ./internal/engine/ -v`
Expected: All PASS

**Step 5: Run full test suite**

Run: `go test -race ./...`
Expected: All PASS

**Step 6: Commit**

```bash
git add internal/engine/executor.go
git commit -m "Add executor integration for load/save SQL task types"
```

---

### Task 9: Update README

Document the new multi-database support, task types, and configuration.

**Files:**
- Modify: `README.md`

**Step 1: Read current README**

Read `README.md` to find the right sections to update.

**Step 2: Update README**

Add/update sections:
- **Supported Databases** — MSSQL, Postgres, ClickHouse, Oracle with connection string prefixes
- **SQL Task Types** — document `type = "load"`, `type = "save"`, and default exec behavior
- **Configuration Examples** — show load/save tasks in pit.toml with connection overrides
- **Roadmap** — move multi-database from roadmap to implemented

**Step 3: Commit**

```bash
git add README.md
git commit -m "Document multi-database loader and SQL task types in README"
```

---

### Task 10: Integration Tests

Add integration tests for round-trip save+load operations. These require real database connections and are gated behind `//go:build integration`.

**Files:**
- Create: `internal/loader/integration_test.go`

**Step 1: Create integration test file**

```go
//go:build integration

package loader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Integration tests require database connection strings in environment variables:
// - TEST_MSSQL_CONN: sqlserver://...
// - TEST_POSTGRES_CONN: postgres://...
// - TEST_CLICKHOUSE_CONN: clickhouse://...
// - TEST_ORACLE_CONN: oracle://...

func TestIntegration_MSSQL_RoundTrip(t *testing.T) {
	connStr := os.Getenv("TEST_MSSQL_CONN")
	if connStr == "" {
		t.Skip("TEST_MSSQL_CONN not set")
	}
	testRoundTrip(t, connStr, "dbo", "pit_test_roundtrip")
}

func TestIntegration_Postgres_RoundTrip(t *testing.T) {
	connStr := os.Getenv("TEST_POSTGRES_CONN")
	if connStr == "" {
		t.Skip("TEST_POSTGRES_CONN not set")
	}
	testRoundTrip(t, connStr, "public", "pit_test_roundtrip")
}

func TestIntegration_ClickHouse_RoundTrip(t *testing.T) {
	connStr := os.Getenv("TEST_CLICKHOUSE_CONN")
	if connStr == "" {
		t.Skip("TEST_CLICKHOUSE_CONN not set")
	}
	testRoundTrip(t, connStr, "", "pit_test_roundtrip")
}

func TestIntegration_Oracle_RoundTrip(t *testing.T) {
	connStr := os.Getenv("TEST_ORACLE_CONN")
	if connStr == "" {
		t.Skip("TEST_ORACLE_CONN not set")
	}
	testRoundTrip(t, connStr, "", "pit_test_roundtrip")
}

// testRoundTrip: create_or_replace load → save → verify row count
func testRoundTrip(t *testing.T, connStr, schema, table string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	// Create a test Parquet file
	// ... (use writeTestParquet helper with sample data)

	// Load into database
	rows, err := Load(ctx, LoadParams{
		FilePath: filepath.Join(dir, "input.parquet"),
		Table:    table,
		Schema:   schema,
		Mode:     ModeCreateOrReplace,
		ConnStr:  connStr,
	})
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if rows != 3 {
		t.Errorf("Load() rows = %d, want 3", rows)
	}

	// Save back to Parquet
	outputPath := filepath.Join(dir, "output.parquet")
	savedRows, err := Save(ctx, SaveParams{
		Query:    fmt.Sprintf("SELECT * FROM %s", table), // adjust quoting per driver
		FilePath: outputPath,
		ConnStr:  connStr,
	})
	if err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	if savedRows != 3 {
		t.Errorf("Save() rows = %d, want 3", savedRows)
	}

	// Verify output Parquet
	records, _, err := readParquet(outputPath)
	if err != nil {
		t.Fatalf("readParquet() error: %v", err)
	}
	var totalRows int64
	for _, r := range records {
		totalRows += r.NumRows()
		r.Release()
	}
	if totalRows != 3 {
		t.Errorf("output parquet rows = %d, want 3", totalRows)
	}
}
```

**Step 2: Commit**

```bash
git add internal/loader/integration_test.go
git commit -m "Add integration tests for multi-database round-trip load/save"
```
