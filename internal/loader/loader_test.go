package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// writeTestParquet creates a Parquet file with the given schema and record for testing.
func writeTestParquet(t *testing.T, dir string, name string, schema *arrow.Schema, rec arrow.Record) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating parquet file: %v", err)
	}

	writerProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	writer, err := pqarrow.NewFileWriter(schema, f, nil, writerProps)
	if err != nil {
		f.Close()
		t.Fatalf("creating parquet writer: %v", err)
	}

	if err := writer.Write(rec); err != nil {
		writer.Close()
		t.Fatalf("writing record: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("closing parquet writer: %v", err)
	}

	return path
}

func TestReadParquet_BasicTypes(t *testing.T) {
	pool := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
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
	builder.Field(3).(*array.BooleanBuilder).AppendValues(
		[]bool{true, false, true}, nil,
	)

	rec := builder.NewRecord()
	defer rec.Release()

	dir := t.TempDir()
	path := writeTestParquet(t, dir, "test.parquet", schema, rec)

	records, readSchema, err := readParquet(path)
	if err != nil {
		t.Fatalf("readParquet() error: %v", err)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	// Verify schema
	if readSchema.NumFields() != 4 {
		t.Fatalf("schema has %d fields, want 4", readSchema.NumFields())
	}
	if readSchema.Field(0).Name != "id" {
		t.Errorf("field 0 name = %q, want %q", readSchema.Field(0).Name, "id")
	}
	if readSchema.Field(1).Name != "name" {
		t.Errorf("field 1 name = %q, want %q", readSchema.Field(1).Name, "name")
	}

	// Count total rows
	var totalRows int64
	for _, r := range records {
		totalRows += r.NumRows()
	}
	if totalRows != 3 {
		t.Errorf("total rows = %d, want 3", totalRows)
	}

	// Verify first record's values
	r := records[0]
	idCol := r.Column(0).(*array.Int32)
	if idCol.Value(0) != 1 {
		t.Errorf("id[0] = %d, want 1", idCol.Value(0))
	}

	nameCol := r.Column(1).(*array.String)
	if nameCol.Value(0) != "alice" {
		t.Errorf("name[0] = %q, want %q", nameCol.Value(0), "alice")
	}

	scoreCol := r.Column(2).(*array.Float64)
	if !scoreCol.IsNull(2) {
		t.Error("score[2] should be null")
	}
}

func TestReadParquet_EmptyFile(t *testing.T) {
	pool := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	rec := builder.NewRecord()
	defer rec.Release()

	dir := t.TempDir()
	path := writeTestParquet(t, dir, "empty.parquet", schema, rec)

	records, readSchema, err := readParquet(path)
	if err != nil {
		t.Fatalf("readParquet() error: %v", err)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	if readSchema.NumFields() != 1 {
		t.Errorf("schema fields = %d, want 1", readSchema.NumFields())
	}

	var totalRows int64
	for _, r := range records {
		totalRows += r.NumRows()
	}
	if totalRows != 0 {
		t.Errorf("total rows = %d, want 0", totalRows)
	}
}

func TestReadParquet_FileNotFound(t *testing.T) {
	_, _, err := readParquet("/nonexistent/path.parquet")
	if err == nil {
		t.Fatal("readParquet() expected error for nonexistent file, got nil")
	}
}

func TestArrowValue_AllTypes(t *testing.T) {
	pool := memory.DefaultAllocator

	// Test int32
	t.Run("int32", func(t *testing.T) {
		b := array.NewInt32Builder(pool)
		defer b.Release()
		b.Append(42)
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != int32(42) {
			t.Errorf("arrowValue() = %v, want 42", v)
		}
	})

	// Test int64
	t.Run("int64", func(t *testing.T) {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.Append(100)
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != int64(100) {
			t.Errorf("arrowValue() = %v, want 100", v)
		}
	})

	// Test float32
	t.Run("float32", func(t *testing.T) {
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		b.Append(3.14)
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != float32(3.14) {
			t.Errorf("arrowValue() = %v, want 3.14", v)
		}
	})

	// Test float64
	t.Run("float64", func(t *testing.T) {
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		b.Append(2.718)
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != 2.718 {
			t.Errorf("arrowValue() = %v, want 2.718", v)
		}
	})

	// Test string
	t.Run("string", func(t *testing.T) {
		b := array.NewStringBuilder(pool)
		defer b.Release()
		b.Append("hello")
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != "hello" {
			t.Errorf("arrowValue() = %v, want %q", v, "hello")
		}
	})

	// Test boolean
	t.Run("boolean", func(t *testing.T) {
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		b.Append(true)
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != true {
			t.Errorf("arrowValue() = %v, want true", v)
		}
	})

	// Test timestamp
	t.Run("timestamp", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Microsecond}
		b := array.NewTimestampBuilder(pool, dt)
		defer b.Release()
		ts := arrow.Timestamp(time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC).UnixMicro())
		b.Append(ts)
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		got, ok := v.(time.Time)
		if !ok {
			t.Fatalf("arrowValue() type = %T, want time.Time", v)
		}
		want := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Errorf("arrowValue() = %v, want %v", got, want)
		}
	})

	// Test null value
	t.Run("null", func(t *testing.T) {
		b := array.NewInt32Builder(pool)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()

		v, err := arrowValue(arr, 0)
		if err != nil {
			t.Fatalf("arrowValue() error: %v", err)
		}
		if v != nil {
			t.Errorf("arrowValue() = %v, want nil", v)
		}
	})
}

func TestArrowValue_UnsupportedType(t *testing.T) {
	pool := memory.DefaultAllocator
	// Use a list type which is not supported
	lb := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer lb.Release()
	lb.Append(true)
	lb.ValueBuilder().(*array.Int32Builder).Append(1)
	arr := lb.NewArray()
	defer arr.Release()

	_, err := arrowValue(arr, 0)
	if err == nil {
		t.Error("arrowValue() expected error for unsupported type, got nil")
	}
}

func TestLoadParams_Defaults(t *testing.T) {
	// Verify Load handles defaults correctly by testing with an invalid driver
	// (the point is that Schema and Mode get defaulted before driver dispatch)
	pool := memory.DefaultAllocator

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).Append(1)
	rec := builder.NewRecord()
	defer rec.Release()

	dir := t.TempDir()
	path := writeTestParquet(t, dir, "test.parquet", schema, rec)

	_, err := Load(t.Context(), LoadParams{
		FilePath: path,
		Table:    "test_table",
		ConnStr:  "postgres://host/db", // unsupported driver
	})
	if err == nil {
		t.Fatal("Load() expected error for unsupported driver, got nil")
	}
	// Error should mention driver detection, not schema/mode
	expected := "detecting driver"
	if got := fmt.Sprintf("%v", err); !containsStr(got, expected) {
		t.Errorf("error = %q, want it to contain %q", got, expected)
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestArrowTypeToMSSQL(t *testing.T) {
	tests := []struct {
		name    string
		dt      arrow.DataType
		want    string
		wantErr bool
	}{
		{"int8", arrow.PrimitiveTypes.Int8, "SMALLINT", false},
		{"int16", arrow.PrimitiveTypes.Int16, "SMALLINT", false},
		{"int32", arrow.PrimitiveTypes.Int32, "INT", false},
		{"int64", arrow.PrimitiveTypes.Int64, "BIGINT", false},
		{"uint8", arrow.PrimitiveTypes.Uint8, "TINYINT", false},
		{"uint16", arrow.PrimitiveTypes.Uint16, "INT", false},
		{"uint32", arrow.PrimitiveTypes.Uint32, "BIGINT", false},
		{"uint64", arrow.PrimitiveTypes.Uint64, "BIGINT", false},
		{"float32", arrow.PrimitiveTypes.Float32, "REAL", false},
		{"float64", arrow.PrimitiveTypes.Float64, "FLOAT", false},
		{"string", arrow.BinaryTypes.String, "NVARCHAR(MAX)", false},
		{"bool", arrow.FixedWidthTypes.Boolean, "BIT", false},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Microsecond}, "DATETIME2", false},
		{"date32", arrow.FixedWidthTypes.Date32, "DATE", false},
		{"binary", arrow.BinaryTypes.Binary, "VARBINARY(MAX)", false},
		{"unsupported_list", arrow.ListOf(arrow.PrimitiveTypes.Int32), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := arrowTypeToMSSQL(tt.dt)
			if tt.wantErr {
				if err == nil {
					t.Errorf("arrowTypeToMSSQL(%s) expected error, got nil", tt.dt)
				}
				return
			}
			if err != nil {
				t.Fatalf("arrowTypeToMSSQL(%s) unexpected error: %v", tt.dt, err)
			}
			if got != tt.want {
				t.Errorf("arrowTypeToMSSQL(%s) = %q, want %q", tt.dt, got, tt.want)
			}
		})
	}
}

func TestCreateTableDDL(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}, nil)

	ddl, err := createTableDDL("dbo", "test_table", schema)
	if err != nil {
		t.Fatalf("createTableDDL() unexpected error: %v", err)
	}

	// Verify the DDL contains the expected fragments
	expectations := []string{
		"CREATE TABLE [dbo].[test_table]",
		"[id] INT NOT NULL",
		"[name] NVARCHAR(MAX) NULL",
		"[score] FLOAT NULL",
		"[active] BIT NOT NULL",
	}
	for _, exp := range expectations {
		if !containsStr(ddl, exp) {
			t.Errorf("DDL missing %q\ngot:\n%s", exp, ddl)
		}
	}
}

func TestCreateTableDDL_UnsupportedType(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "bad", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: false},
	}, nil)

	_, err := createTableDDL("dbo", "test_table", schema)
	if err == nil {
		t.Error("createTableDDL() expected error for unsupported type, got nil")
	}
	if !containsStr(err.Error(), "column \"bad\"") {
		t.Errorf("error = %q, want it to mention column name", err)
	}
}
