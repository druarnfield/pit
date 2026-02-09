package loader

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Type aliases for Arrow array types used in arrowValue.
type (
	int8Array      = array.Int8
	int16Array     = array.Int16
	int32Array     = array.Int32
	int64Array     = array.Int64
	uint8Array     = array.Uint8
	uint16Array    = array.Uint16
	uint32Array    = array.Uint32
	uint64Array    = array.Uint64
	float32Array   = array.Float32
	float64Array   = array.Float64
	stringArray    = array.String
	boolArray      = array.Boolean
	timestampArray = array.Timestamp
	date32Array    = array.Date32
	binaryArray    = array.Binary
)

// newTableRecordReader wraps array.NewTableReader for readability.
func newTableRecordReader(tbl arrow.Table, chunkSize int64) *array.TableReader {
	return array.NewTableReader(tbl, chunkSize)
}
