package loader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresDriver implements the Driver interface for PostgreSQL.
type PostgresDriver struct{}

// DefaultSchema returns the default schema for PostgreSQL.
func (d *PostgresDriver) DefaultSchema() string { return "public" }

// QuoteIdentifier wraps a name in double-quote identifiers for PostgreSQL.
func (d *PostgresDriver) QuoteIdentifier(name string) string { return "\"" + name + "\"" }

// ArrowType maps an Arrow data type to a PostgreSQL column type string.
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
	case arrow.UINT32, arrow.UINT64:
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
		return "", fmt.Errorf("unsupported Arrow type %s for PostgreSQL column", dt)
	}
}

// SQLTypeToArrow maps a PostgreSQL type name to an Arrow data type.
func (d *PostgresDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	switch strings.ToUpper(dbTypeName) {
	case "INT2", "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "INT4", "INTEGER":
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
		return nil, fmt.Errorf("unsupported PostgreSQL type %q for Arrow mapping", dbTypeName)
	}
}

// buildCreateTableDDL builds a CREATE TABLE statement from an Arrow schema.
func (d *PostgresDriver) buildCreateTableDDL(schemaName, tableName string, schema *arrow.Schema) (string, error) {
	var cols []string
	for _, f := range schema.Fields() {
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
	ddl := fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n)",
		d.QuoteIdentifier(schemaName), d.QuoteIdentifier(tableName),
		joinStrings(cols, ",\n"))
	return ddl, nil
}

// CreateTable creates a table in the database from an Arrow schema.
func (d *PostgresDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
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
func (d *PostgresDriver) DropTable(ctx context.Context, db *sql.DB, schema, table string) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		d.QuoteIdentifier(schema), d.QuoteIdentifier(table))
	if _, err := db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}
	return nil
}

// TruncateTable truncates a table.
func (d *PostgresDriver) TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error {
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE %s.%s",
		d.QuoteIdentifier(schema), d.QuoteIdentifier(table))
	if _, err := db.ExecContext(ctx, truncateSQL); err != nil {
		return fmt.Errorf("truncating table: %w", err)
	}
	return nil
}

// BulkLoad streams Arrow record batches into a PostgreSQL table using pgx COPY protocol.
// It opens a separate pgx native connection for the COPY operation (the db *sql.DB param
// is used by the shared Load() caller for DDL but is not needed here).
func (d *PostgresDriver) BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error) {
	schema := stream.Schema()

	colNames := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		colNames[i] = f.Name
	}

	conn, err := pgx.Connect(ctx, params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("connecting via pgx: %w", err)
	}
	defer conn.Close(ctx)

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

		copied, err := conn.CopyFrom(
			ctx,
			pgx.Identifier{params.Schema, params.Table},
			colNames,
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return totalRows, fmt.Errorf("copy from: %w", err)
		}
		totalRows += copied
	}
	if err := stream.Err(); err != nil {
		return totalRows, fmt.Errorf("reading parquet: %w", err)
	}

	return totalRows, nil
}
