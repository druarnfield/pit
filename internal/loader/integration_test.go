//go:build integration

package loader

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/druarnfield/pit/internal/runner"
)

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

func testRoundTrip(t *testing.T, connStr, schema, table string) {
	t.Helper()
	ctx := context.Background()

	// Detect driver and get driver implementation for cleanup
	driverName, err := runner.DetectDriver(connStr)
	if err != nil {
		t.Fatalf("DetectDriver() error: %v", err)
	}
	drv, err := GetDriver(driverName)
	if err != nil {
		t.Fatalf("GetDriver(%q) error: %v", driverName, err)
	}

	// Ensure test table is cleaned up after the test
	t.Cleanup(func() {
		db, err := sql.Open(driverName, connStr)
		if err != nil {
			t.Logf("cleanup: failed to open db: %v", err)
			return
		}
		defer db.Close()
		if err := drv.DropTable(context.Background(), db, schema, table); err != nil {
			t.Logf("cleanup: failed to drop table: %v", err)
		}
	})

	// Step 1: Create a test Parquet file with 3 rows
	pool := memory.DefaultAllocator
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(
		[]string{"alice", "bob", "charlie"},
		[]bool{true, true, true},
	)
	builder.Field(2).(*array.Float64Builder).AppendValues(
		[]float64{95.5, 87.3, 0},
		[]bool{true, true, false}, // third value is null
	)

	rec := builder.NewRecord()
	defer rec.Release()

	dir := t.TempDir()
	inputPath := writeTestParquet(t, dir, "input.parquet", arrowSchema, rec)

	// Step 2: Load into database with ModeCreateOrReplace
	rows, err := Load(ctx, LoadParams{
		FilePath: inputPath,
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

	// Step 3: Save back to Parquet via SELECT
	var selectSQL string
	if schema != "" {
		selectSQL = fmt.Sprintf("SELECT * FROM %s.%s",
			drv.QuoteIdentifier(schema), drv.QuoteIdentifier(table))
	} else {
		selectSQL = fmt.Sprintf("SELECT * FROM %s", drv.QuoteIdentifier(table))
	}

	outputPath := filepath.Join(dir, "output.parquet")
	savedRows, err := Save(ctx, SaveParams{
		Query:    selectSQL,
		FilePath: outputPath,
		ConnStr:  connStr,
	})
	if err != nil {
		t.Fatalf("Save() error: %v", err)
	}
	if savedRows != 3 {
		t.Errorf("Save() rows = %d, want 3", savedRows)
	}

	// Step 4: Read output Parquet and verify row count
	records, _, err := readParquet(outputPath)
	if err != nil {
		t.Fatalf("readParquet() error: %v", err)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	var totalRows int64
	for _, r := range records {
		totalRows += r.NumRows()
	}
	if totalRows != 3 {
		t.Errorf("output parquet total rows = %d, want 3", totalRows)
	}
}
