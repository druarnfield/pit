package loader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/sijms/go-ora/v2"
)

// OracleDriver implements the Driver interface for Oracle Database.
type OracleDriver struct{}

// DefaultSchema returns an empty string; Oracle derives the schema from the connection user.
func (d *OracleDriver) DefaultSchema() string { return "" }

// QuoteIdentifier wraps a name in double-quote identifiers with upper-casing for Oracle.
func (d *OracleDriver) QuoteIdentifier(name string) string {
	return "\"" + strings.ToUpper(name) + "\""
}

// ArrowType maps an Arrow data type to an Oracle column type string.
func (d *OracleDriver) ArrowType(dt arrow.DataType) (string, error) {
	switch dt.ID() {
	case arrow.INT8:
		return "NUMBER(3)", nil
	case arrow.INT16:
		return "NUMBER(5)", nil
	case arrow.INT32:
		return "NUMBER(10)", nil
	case arrow.INT64:
		return "NUMBER(19)", nil
	case arrow.UINT8:
		return "NUMBER(3)", nil
	case arrow.UINT16:
		return "NUMBER(5)", nil
	case arrow.UINT32:
		return "NUMBER(10)", nil
	case arrow.UINT64:
		return "NUMBER(19)", nil
	case arrow.FLOAT32:
		return "BINARY_FLOAT", nil
	case arrow.FLOAT64:
		return "BINARY_DOUBLE", nil
	case arrow.STRING, arrow.LARGE_STRING:
		return "CLOB", nil
	case arrow.BOOL:
		return "NUMBER(1)", nil
	case arrow.TIMESTAMP:
		return "TIMESTAMP", nil
	case arrow.DATE32:
		return "DATE", nil
	case arrow.BINARY:
		return "BLOB", nil
	default:
		return "", fmt.Errorf("unsupported Arrow type %s for Oracle column", dt)
	}
}

// SQLTypeToArrow maps an Oracle database type name to an Arrow data type.
func (d *OracleDriver) SQLTypeToArrow(dbTypeName string) (arrow.DataType, error) {
	switch strings.ToUpper(dbTypeName) {
	case "NUMBER":
		return arrow.BinaryTypes.String, nil // precision unknown, safe default
	case "BINARY_FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "BINARY_DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG":
		return arrow.BinaryTypes.String, nil
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "DATE":
		// Oracle DATE includes a time component
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "RAW", "BLOB":
		return arrow.BinaryTypes.Binary, nil
	default:
		return nil, fmt.Errorf("unsupported Oracle type %q for Arrow mapping", dbTypeName)
	}
}

// qualifiedTable returns a fully qualified table reference for Oracle.
// If schema is empty, only the quoted table name is returned.
func (d *OracleDriver) qualifiedTable(schema, table string) string {
	qt := d.QuoteIdentifier(table)
	if schema == "" {
		return qt
	}
	return d.QuoteIdentifier(schema) + "." + qt
}

// buildCreateTableDDL builds a CREATE TABLE statement from an Arrow schema.
func (d *OracleDriver) buildCreateTableDDL(schemaName, tableName string, schema *arrow.Schema) (string, error) {
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
	ddl := fmt.Sprintf("CREATE TABLE %s (\n%s\n)", d.qualifiedTable(schemaName, tableName), joinStrings(cols, ",\n"))
	return ddl, nil
}

// CreateTable creates a table in the database from an Arrow schema.
func (d *OracleDriver) CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error {
	ddl, err := d.buildCreateTableDDL(schema, table, arrowSchema)
	if err != nil {
		return fmt.Errorf("building create table DDL: %w", err)
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	return nil
}

// DropTable drops a table if it exists using PL/SQL to suppress ORA-00942.
func (d *OracleDriver) DropTable(ctx context.Context, db *sql.DB, schema, table string) error {
	ref := d.qualifiedTable(schema, table)
	// Escape single quotes in the identifier so it can be safely embedded
	// inside a PL/SQL string literal (single quote is doubled per SQL standard).
	escapedRef := strings.ReplaceAll(ref, "'", "''")
	dropSQL := fmt.Sprintf(
		"BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;",
		escapedRef,
	)
	if _, err := db.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}
	return nil
}

// TruncateTable truncates a table.
func (d *OracleDriver) TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error {
	truncateSQL := fmt.Sprintf("TRUNCATE TABLE %s", d.qualifiedTable(schema, table))
	if _, err := db.ExecContext(ctx, truncateSQL); err != nil {
		return fmt.Errorf("truncating table: %w", err)
	}
	return nil
}

// BulkLoad streams Arrow record batches into an Oracle table using prepared statements
// with Oracle bind variables (:1, :2, ...) within a transaction.
func (d *OracleDriver) BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error) {
	schema := stream.Schema()

	// Build column names and bind placeholders
	colNames := make([]string, schema.NumFields())
	placeholders := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		colNames[i] = d.QuoteIdentifier(f.Name)
		placeholders[i] = fmt.Sprintf(":%d", i+1)
	}

	ref := d.qualifiedTable(params.Schema, params.Table)
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		ref,
		joinStrings(colNames, ", "),
		joinStrings(placeholders, ", "),
	)

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.PrepareContext(ctx, insertSQL)
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

	if err := txn.Commit(); err != nil {
		return totalRows, fmt.Errorf("committing transaction: %w", err)
	}

	return totalRows, nil
}
