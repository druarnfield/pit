package loader

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// parquetStream provides streaming access to a Parquet file's record batches.
// Only one row group's worth of data is held in memory at a time.
type parquetStream struct {
	file   *os.File
	pf     *file.Reader
	reader *pqarrow.FileReader
	schema *arrow.Schema

	// iteration state
	rgIdx  int              // next row group index to read
	curTbl arrow.Table       // current row group table (nil until first Next)
	curTR  *array.TableReader // current batch reader within the row group
	curRec arrow.Record      // most recent record from Record()
	err    error
}

// openParquetStream opens a Parquet file for streaming reads.
// Call Close() when done, even if iteration ends early.
func openParquetStream(filePath string) (*parquetStream, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	pf, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("opening parquet reader: %w", err)
	}

	pool := memory.DefaultAllocator
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 65536}, pool)
	if err != nil {
		pf.Close()
		f.Close()
		return nil, fmt.Errorf("creating arrow reader: %w", err)
	}

	schema, err := reader.Schema()
	if err != nil {
		pf.Close()
		f.Close()
		return nil, fmt.Errorf("reading schema: %w", err)
	}

	return &parquetStream{file: f, pf: pf, reader: reader, schema: schema}, nil
}

// Schema returns the Arrow schema of the Parquet file.
func (ps *parquetStream) Schema() *arrow.Schema { return ps.schema }

// Next advances to the next record batch. Returns false when exhausted or on error.
// The previous batch's memory is released when Next is called again.
func (ps *parquetStream) Next() bool {
	for {
		// Try the current row group's batch reader first
		if ps.curTR != nil && ps.curTR.Next() {
			ps.curRec = ps.curTR.Record()
			return true
		}

		// Release current row group resources
		if ps.curTR != nil {
			ps.curTR.Release()
			ps.curTR = nil
		}
		if ps.curTbl != nil {
			ps.curTbl.Release()
			ps.curTbl = nil
		}

		// No more row groups — done
		if ps.rgIdx >= ps.pf.NumRowGroups() {
			return false
		}

		// Read the next row group
		tbl, err := ps.reader.ReadRowGroups(context.Background(), []int{ps.rgIdx}, nil)
		if err != nil {
			ps.err = fmt.Errorf("reading row group %d: %w", ps.rgIdx, err)
			return false
		}
		ps.rgIdx++
		ps.curTbl = tbl
		ps.curTR = newTableRecordReader(tbl, 65536)
	}
}

// Record returns the current record batch. Valid until the next call to Next.
func (ps *parquetStream) Record() arrow.Record { return ps.curRec }

// Err returns any error encountered during iteration.
func (ps *parquetStream) Err() error { return ps.err }

// Close releases all resources held by the stream.
func (ps *parquetStream) Close() {
	if ps.curTR != nil {
		ps.curTR.Release()
	}
	if ps.curTbl != nil {
		ps.curTbl.Release()
	}
	ps.pf.Close()
	ps.file.Close()
}

// readParquet reads all record batches from a Parquet file into memory.
// Used by tests — production code should use openParquetStream for streaming.
func readParquet(filePath string) ([]arrow.Record, *arrow.Schema, error) {
	stream, err := openParquetStream(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer stream.Close()

	var records []arrow.Record
	for stream.Next() {
		rec := stream.Record()
		rec.Retain() // keep alive after stream advances/closes
		records = append(records, rec)
	}
	if err := stream.Err(); err != nil {
		for _, r := range records {
			r.Release()
		}
		return nil, nil, err
	}

	return records, stream.Schema(), nil
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
