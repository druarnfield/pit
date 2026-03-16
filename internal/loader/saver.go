package loader

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/druarnfield/pit/internal/runner"
)

// SaveParams configures a query-to-Parquet save operation.
type SaveParams struct {
	Query    string // SQL SELECT query
	FilePath string // output Parquet file path
	ConnStr  string // database connection string
}

// Save executes a SQL query and writes the results to a Parquet file.
// Returns the number of rows written.
func Save(ctx context.Context, params SaveParams) (int64, error) {
	driverName, err := runner.DetectDriver(params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("detecting driver: %w", err)
	}

	drv, err := GetDriver(driverName)
	if err != nil {
		return 0, fmt.Errorf("getting driver: %w", err)
	}

	db, err := sql.Open(driverName, params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, params.Query)
	if err != nil {
		return 0, fmt.Errorf("executing query: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("getting column types: %w", err)
	}

	// Build Arrow schema from database column types
	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		arrowType, err := drv.SQLTypeToArrow(ct.DatabaseTypeName())
		if err != nil {
			return 0, fmt.Errorf("column %q (type %q): %w", ct.Name(), ct.DatabaseTypeName(), err)
		}
		nullable, _ := ct.Nullable()
		fields[i] = arrow.Field{
			Name:     ct.Name(),
			Type:     arrowType,
			Nullable: nullable,
		}
	}
	arrowSchema := arrow.NewSchema(fields, nil)

	// Create output directory if needed
	if dir := filepath.Dir(params.FilePath); dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return 0, fmt.Errorf("creating output directory: %w", err)
		}
	}

	return writeRowsToParquet(rows, arrowSchema, params.FilePath)
}
