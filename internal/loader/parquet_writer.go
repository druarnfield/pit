package loader

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const saveBatchSize = 65536

// writeRowsToParquet streams database rows into a Parquet file.
func writeRowsToParquet(rows *sql.Rows, schema *arrow.Schema, filePath string) (int64, error) {
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
	numCols := schema.NumFields()
	var totalRows int64

	for {
		builder := array.NewRecordBuilder(pool, schema)
		var batchRows int64

		for batchRows < saveBatchSize && rows.Next() {
			scanVals := make([]interface{}, numCols)
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
				if err := appendToBuilder(builder.Field(i), schema.Field(i).Type, val); err != nil {
					builder.Release()
					writer.Close()
					return totalRows, fmt.Errorf("row %d col %d: %w", totalRows+batchRows, i, err)
				}
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
// Returns an error if the builder type is not supported.
func appendToBuilder(fb array.Builder, dt arrow.DataType, val interface{}) error {
	if val == nil {
		fb.AppendNull()
		return nil
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
	case *array.Uint16Builder:
		b.Append(toUint16(val))
	case *array.Uint32Builder:
		b.Append(toUint32(val))
	case *array.Uint64Builder:
		b.Append(toUint64(val))
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
		return fmt.Errorf("unsupported builder type %T for Arrow type %s", fb, dt)
	}
	return nil
}

func toInt8(v interface{}) int8 {
	switch v := v.(type) {
	case int64:
		return int8(v)
	case int32:
		return int8(v)
	case int:
		return int8(v)
	case float64:
		return int8(v)
	case int8:
		return v
	default:
		return 0
	}
}

func toInt16(v interface{}) int16 {
	switch v := v.(type) {
	case int64:
		return int16(v)
	case int32:
		return int16(v)
	case int:
		return int16(v)
	case float64:
		return int16(v)
	case int16:
		return v
	default:
		return 0
	}
}

func toInt32(v interface{}) int32 {
	switch v := v.(type) {
	case int64:
		return int32(v)
	case int32:
		return v
	case int:
		return int32(v)
	case float64:
		return int32(v)
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch v := v.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func toUint8(v interface{}) uint8 {
	switch v := v.(type) {
	case int64:
		return uint8(v)
	case int32:
		return uint8(v)
	case int:
		return uint8(v)
	case float64:
		return uint8(v)
	case uint8:
		return v
	default:
		return 0
	}
}

func toUint16(v interface{}) uint16 {
	switch v := v.(type) {
	case int64:
		return uint16(v)
	case int32:
		return uint16(v)
	case int:
		return uint16(v)
	case float64:
		return uint16(v)
	default:
		return 0
	}
}

func toUint32(v interface{}) uint32 {
	switch v := v.(type) {
	case int64:
		return uint32(v)
	case int32:
		return uint32(v)
	case int:
		return uint32(v)
	case float64:
		return uint32(v)
	default:
		return 0
	}
}

func toUint64(v interface{}) uint64 {
	switch v := v.(type) {
	case int64:
		return uint64(v)
	case int32:
		return uint64(v)
	case int:
		return uint64(v)
	case uint64:
		return v
	case float64:
		return uint64(v)
	default:
		return 0
	}
}

func toFloat32(v interface{}) float32 {
	switch v := v.(type) {
	case float64:
		return float32(v)
	case float32:
		return v
	case int64:
		return float32(v)
	case int:
		return float32(v)
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
	case int32:
		return float64(v)
	case int:
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
	case int:
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
