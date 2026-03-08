package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// Driver abstracts database-specific bulk load and DDL operations.
type Driver interface {
	BulkLoad(ctx context.Context, db *sql.DB, params LoadParams, stream *parquetStream) (int64, error)
	CreateTable(ctx context.Context, db *sql.DB, schema, table string, arrowSchema *arrow.Schema) error
	DropTable(ctx context.Context, db *sql.DB, schema, table string) error
	TruncateTable(ctx context.Context, db *sql.DB, schema, table string) error
	ArrowType(dt arrow.DataType) (string, error)
	SQLTypeToArrow(dbTypeName string) (arrow.DataType, error)
	DefaultSchema() string
	QuoteIdentifier(name string) string
}

var drivers = map[string]Driver{
	"mssql": &MSSQLDriver{},
}

// GetDriver returns the Driver for the given name.
func GetDriver(name string) (Driver, error) {
	d, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("unsupported database driver %q", name)
	}
	return d, nil
}
