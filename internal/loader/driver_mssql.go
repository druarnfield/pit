package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	mssql "github.com/microsoft/go-mssqldb"
)

// MSSQLDriver implements the Driver interface for Microsoft SQL Server.
type MSSQLDriver struct{}

// DefaultSchema returns the default schema for MSSQL.
func (d *MSSQLDriver) DefaultSchema() string { return "dbo" }

// QuoteIdentifier wraps a name in MSSQL bracket-quoting.
func (d *MSSQLDriver) QuoteIdentifier(name string) string { return "[" + name + "]" }

// ArrowType maps an Arrow data type to a MSSQL column type string.
func (d *MSSQLDriver) ArrowType(dt arrow.DataType) (string, error) {
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

// SQLTypeToArrow maps a database type name to an Arrow data type.
func (d *MSSQLDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	return nil, fmt.Errorf("SQLTypeToArrow not yet implemented for MSSQL")
}

// buildCreateTableDDL builds a CREATE TABLE statement from an Arrow schema.
func (d *MSSQLDriver) buildCreateTableDDL(schemaName, tableName string, schema *arrow.Schema) (string, error) {
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
		cols = append(cols, fmt.Sprintf("    [%s] %s %s", f.Name, sqlType, null))
	}
	ddl := fmt.Sprintf("CREATE TABLE [%s].[%s] (\n%s\n)", schemaName, tableName, joinStrings(cols, ",\n"))
	return ddl, nil
}

// CreateTable creates a table in the database from an Arrow schema.
func (d *MSSQLDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
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
func (d *MSSQLDriver) DropTable(ctx context.Context, db *sql.DB, schema, table string) error {
	dropSQL := fmt.Sprintf("IF OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL DROP TABLE [%s].[%s]",
		schema, table, schema, table)
	if _, err := db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}
	return nil
}

// TruncateTable truncates a table.
func (d *MSSQLDriver) TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error {
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE [%s].[%s]", schema, table)
	if _, err := db.ExecContext(ctx, truncateSQL); err != nil {
		return fmt.Errorf("truncating table: %w", err)
	}
	return nil
}

// BulkLoad streams Arrow record batches from the parquetStream into an MSSQL table.
// Only one row group's worth of data is held in memory at a time.
func (d *MSSQLDriver) BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error) {
	schema := stream.Schema()

	// Build column names from Arrow schema
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

	// Flush the bulk copy
	if _, err := stmt.ExecContext(ctx); err != nil {
		return totalRows, fmt.Errorf("flushing bulk copy: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return totalRows, fmt.Errorf("committing transaction: %w", err)
	}

	return totalRows, nil
}

// joinStrings joins a slice of strings with a separator (avoids importing strings).
func joinStrings(elems []string, sep string) string {
	if len(elems) == 0 {
		return ""
	}
	out := elems[0]
	for _, e := range elems[1:] {
		out += sep + e
	}
	return out
}
