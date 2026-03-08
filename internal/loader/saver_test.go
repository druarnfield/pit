package loader

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSaveParams_Fields(t *testing.T) {
	p := SaveParams{
		Query:    "SELECT 1",
		FilePath: "/tmp/out.parquet",
		ConnStr:  "postgres://localhost/db",
	}
	if p.Query != "SELECT 1" {
		t.Errorf("Query = %q, want %q", p.Query, "SELECT 1")
	}
	if p.FilePath != "/tmp/out.parquet" {
		t.Errorf("FilePath = %q, want %q", p.FilePath, "/tmp/out.parquet")
	}
}

func TestAppendToBuilder_Types(t *testing.T) {
	pool := memory.DefaultAllocator

	t.Run("int32_from_int64", func(t *testing.T) {
		b := array.NewInt32Builder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.PrimitiveTypes.Int32, int64(42))
		arr := b.NewArray()
		defer arr.Release()
		if arr.(*array.Int32).Value(0) != 42 {
			t.Errorf("got %d, want 42", arr.(*array.Int32).Value(0))
		}
	})

	t.Run("string_from_any", func(t *testing.T) {
		b := array.NewStringBuilder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.BinaryTypes.String, "hello")
		arr := b.NewArray()
		defer arr.Release()
		if arr.(*array.String).Value(0) != "hello" {
			t.Errorf("got %q, want %q", arr.(*array.String).Value(0), "hello")
		}
	})

	t.Run("null_value", func(t *testing.T) {
		b := array.NewInt32Builder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.PrimitiveTypes.Int32, nil)
		arr := b.NewArray()
		defer arr.Release()
		if !arr.IsNull(0) {
			t.Error("expected null, got non-null")
		}
	})

	t.Run("bool_from_bool", func(t *testing.T) {
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.FixedWidthTypes.Boolean, true)
		arr := b.NewArray()
		defer arr.Release()
		if !arr.(*array.Boolean).Value(0) {
			t.Error("got false, want true")
		}
	})

	t.Run("float64_from_float64", func(t *testing.T) {
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.PrimitiveTypes.Float64, float64(3.14))
		arr := b.NewArray()
		defer arr.Release()
		if arr.(*array.Float64).Value(0) != 3.14 {
			t.Errorf("got %f, want 3.14", arr.(*array.Float64).Value(0))
		}
	})

	t.Run("int64_from_int64", func(t *testing.T) {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.PrimitiveTypes.Int64, int64(999))
		arr := b.NewArray()
		defer arr.Release()
		if arr.(*array.Int64).Value(0) != 999 {
			t.Errorf("got %d, want 999", arr.(*array.Int64).Value(0))
		}
	})

	t.Run("uint8_from_int64", func(t *testing.T) {
		b := array.NewUint8Builder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.PrimitiveTypes.Uint8, int64(255))
		arr := b.NewArray()
		defer arr.Release()
		if arr.(*array.Uint8).Value(0) != 255 {
			t.Errorf("got %d, want 255", arr.(*array.Uint8).Value(0))
		}
	})

	t.Run("float32_from_float64", func(t *testing.T) {
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		appendToBuilder(b, arrow.PrimitiveTypes.Float32, float64(1.5))
		arr := b.NewArray()
		defer arr.Release()
		if arr.(*array.Float32).Value(0) != 1.5 {
			t.Errorf("got %f, want 1.5", arr.(*array.Float32).Value(0))
		}
	})

	t.Run("binary_from_bytes", func(t *testing.T) {
		b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
		defer b.Release()
		appendToBuilder(b, arrow.BinaryTypes.Binary, []byte{0x01, 0x02})
		arr := b.NewArray()
		defer arr.Release()
		got := arr.(*array.Binary).Value(0)
		if len(got) != 2 || got[0] != 0x01 || got[1] != 0x02 {
			t.Errorf("got %v, want [1 2]", got)
		}
	})
}
