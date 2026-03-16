package loader

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// ClickHouseDriver implements the Driver interface for ClickHouse.
type ClickHouseDriver struct{}

// DefaultSchema returns an empty string; ClickHouse uses databases, not schemas.
func (d *ClickHouseDriver) DefaultSchema() string { return "" }

// QuoteIdentifier wraps a name in backtick quoting for ClickHouse.
func (d *ClickHouseDriver) QuoteIdentifier(name string) string { return "`" + name + "`" }

// ArrowType maps an Arrow data type to a ClickHouse column type string.
func (d *ClickHouseDriver) ArrowType(dt arrow.DataType) (string, error) {
	switch dt.ID() {
	case arrow.INT8:
		return "Int8", nil
	case arrow.INT16:
		return "Int16", nil
	case arrow.INT32:
		return "Int32", nil
	case arrow.INT64:
		return "Int64", nil
	case arrow.UINT8:
		return "UInt8", nil
	case arrow.UINT16:
		return "UInt16", nil
	case arrow.UINT32:
		return "UInt32", nil
	case arrow.UINT64:
		return "UInt64", nil
	case arrow.FLOAT32:
		return "Float32", nil
	case arrow.FLOAT64:
		return "Float64", nil
	case arrow.STRING, arrow.LARGE_STRING:
		return "String", nil
	case arrow.BOOL:
		return "Bool", nil
	case arrow.TIMESTAMP:
		return "DateTime64(6)", nil
	case arrow.DATE32:
		return "Date", nil
	case arrow.BINARY:
		return "String", nil
	default:
		return "", fmt.Errorf("unsupported Arrow type %s for ClickHouse column", dt)
	}
}

// SQLTypeToArrow maps a ClickHouse type name to an Arrow data type.
func (d *ClickHouseDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	// Strip Nullable(...) wrapper if present.
	name := dbTypeName
	if strings.HasPrefix(name, "Nullable(") && strings.HasSuffix(name, ")") {
		name = name[len("Nullable(") : len(name)-1]
	}

	switch name {
	case "Int8":
		return arrow.PrimitiveTypes.Int8, nil
	case "Int16":
		return arrow.PrimitiveTypes.Int16, nil
	case "Int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "Int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "UInt8":
		return arrow.PrimitiveTypes.Uint8, nil
	case "UInt16":
		return arrow.PrimitiveTypes.Uint16, nil
	case "UInt32":
		return arrow.PrimitiveTypes.Uint32, nil
	case "UInt64":
		return arrow.PrimitiveTypes.Uint64, nil
	case "Float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "Float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "String":
		return arrow.BinaryTypes.String, nil
	case "Bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "Date", "Date32":
		return arrow.FixedWidthTypes.Date32, nil
	default:
		// Handle FixedString(N) variants.
		if strings.HasPrefix(name, "FixedString") {
			return arrow.BinaryTypes.String, nil
		}
		// Handle DateTime (no precision) as microseconds.
		if name == "DateTime" {
			return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
		}
		// Handle DateTime64(N) — parse the precision to choose the Arrow unit.
		// ClickHouse precision: 0=seconds, 1-3=milliseconds, 4-6=microseconds, 7-9=nanoseconds.
		if strings.HasPrefix(name, "DateTime64(") && strings.HasSuffix(name, ")") {
			inner := name[len("DateTime64(") : len(name)-1]
			precision, err := strconv.Atoi(strings.TrimSpace(inner))
			if err != nil {
				return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
			}
			switch {
			case precision == 0:
				return &arrow.TimestampType{Unit: arrow.Second}, nil
			case precision <= 3:
				return &arrow.TimestampType{Unit: arrow.Millisecond}, nil
			case precision <= 6:
				return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
			default:
				return &arrow.TimestampType{Unit: arrow.Nanosecond}, nil
			}
		}
		return nil, fmt.Errorf("unsupported ClickHouse type %q for Arrow mapping", dbTypeName)
	}
}

// buildCreateTableDDL builds a CREATE TABLE statement from an Arrow schema.
func (d *ClickHouseDriver) buildCreateTableDDL(schemaName, tableName string, schema *arrow.Schema) (string, error) {
	var cols []string
	for _, f := range schema.Fields() {
		sqlType, err := d.ArrowType(f.Type)
		if err != nil {
			return "", fmt.Errorf("column %q: %w", f.Name, err)
		}
		colDef := sqlType
		if f.Nullable {
			colDef = fmt.Sprintf("Nullable(%s)", sqlType)
		}
		cols = append(cols, fmt.Sprintf("    %s %s", d.QuoteIdentifier(f.Name), colDef))
	}

	var qualifiedName string
	if schemaName == "" {
		qualifiedName = d.QuoteIdentifier(tableName)
	} else {
		qualifiedName = d.QuoteIdentifier(schemaName) + "." + d.QuoteIdentifier(tableName)
	}

	ddl := fmt.Sprintf("CREATE TABLE %s (\n%s\n) ENGINE = MergeTree() ORDER BY tuple()",
		qualifiedName, joinStrings(cols, ",\n"))
	return ddl, nil
}

// CreateTable creates a table in the database from an Arrow schema.
func (d *ClickHouseDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
	ddl, err := d.buildCreateTableDDL(schema, table, arrowSchema)
	if err != nil {
		return fmt.Errorf("building create table DDL: %w", err)
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	return nil
}

// DropTable drops a table if it exists.
func (d *ClickHouseDriver) DropTable(ctx context.Context, db *sql.DB, schema, table string) error {
	var qualifiedName string
	if schema == "" {
		qualifiedName = d.QuoteIdentifier(table)
	} else {
		qualifiedName = d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
	}
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedName)
	if _, err := db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}
	return nil
}

// TruncateTable truncates a table.
func (d *ClickHouseDriver) TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error {
	var qualifiedName string
	if schema == "" {
		qualifiedName = d.QuoteIdentifier(table)
	} else {
		qualifiedName = d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
	}
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE %s", qualifiedName)
	if _, err := db.ExecContext(ctx, truncateSQL); err != nil {
		return fmt.Errorf("truncating table: %w", err)
	}
	return nil
}

// BulkLoad streams Arrow record batches into a ClickHouse table using batch inserts.
// The clickhouse-go driver accumulates rows in the prepared statement and sends them
// as a batch on tx.Commit().
func (d *ClickHouseDriver) BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error) {
	schema := stream.Schema()

	// Build column names and INSERT statement.
	colNames := make([]string, schema.NumFields())
	placeholders := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		colNames[i] = d.QuoteIdentifier(f.Name)
		placeholders[i] = "?"
	}

	var qualifiedName string
	if params.Schema == "" {
		qualifiedName = d.QuoteIdentifier(params.Table)
	} else {
		qualifiedName = d.QuoteIdentifier(params.Schema) + "." + d.QuoteIdentifier(params.Table)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s)",
		qualifiedName, joinStrings(colNames, ", "))

	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return 0, fmt.Errorf("preparing insert: %w", err)
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

	if err := tx.Commit(); err != nil {
		return totalRows, fmt.Errorf("committing transaction: %w", err)
	}

	return totalRows, nil
}
