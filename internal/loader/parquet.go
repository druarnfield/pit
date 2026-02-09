package loader

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// readParquet reads a Parquet file and returns Arrow record batches and schema.
// Callers must Release() each returned record when done.
func readParquet(filePath string) ([]arrow.Record, *arrow.Schema, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("opening file: %w", err)
	}
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	if err != nil {
		return nil, nil, fmt.Errorf("opening parquet reader: %w", err)
	}
	defer pf.Close()

	pool := memory.DefaultAllocator
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 65536}, pool)
	if err != nil {
		return nil, nil, fmt.Errorf("creating arrow reader: %w", err)
	}

	schema, err := reader.Schema()
	if err != nil {
		return nil, nil, fmt.Errorf("reading schema: %w", err)
	}

	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("reading table: %w", err)
	}
	defer table.Release()

	// Convert table to record batches
	tr := newTableRecordReader(table, 65536)
	defer tr.Release()

	var records []arrow.Record
	for tr.Next() {
		rec := tr.Record()
		rec.Retain()
		records = append(records, rec)
	}

	if len(records) == 0 {
		return nil, schema, nil
	}

	return records, schema, nil
}

// arrowValue extracts a Go value from an Arrow array at the given index.
// Supports the common types needed for database bulk loading.
func arrowValue(col arrow.Array, idx int) (interface{}, error) {
	if col.IsNull(idx) {
		return nil, nil
	}

	switch c := col.(type) {
	case *int32Array:
		return c.Value(idx), nil
	case *int64Array:
		return c.Value(idx), nil
	case *float32Array:
		return c.Value(idx), nil
	case *float64Array:
		return c.Value(idx), nil
	case *stringArray:
		return c.Value(idx), nil
	case *boolArray:
		return c.Value(idx), nil
	case *timestampArray:
		return c.Value(idx).ToTime(c.DataType().(*arrow.TimestampType).Unit), nil
	case *date32Array:
		return c.Value(idx).ToTime(), nil
	case *binaryArray:
		return c.Value(idx), nil
	case *int8Array:
		return c.Value(idx), nil
	case *int16Array:
		return c.Value(idx), nil
	case *uint8Array:
		return c.Value(idx), nil
	case *uint16Array:
		return c.Value(idx), nil
	case *uint32Array:
		return c.Value(idx), nil
	case *uint64Array:
		return c.Value(idx), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type %T for column at index %d", col, idx)
	}
}
