package loader

import (
	"context"
	"fmt"

	"github.com/druarnfield/pit/internal/runner"
)

// LoadMode controls how data is loaded into the target table.
type LoadMode string

const (
	ModeAppend           LoadMode = "append"
	ModeTruncateAndLoad  LoadMode = "truncate_and_load"
)

// LoadParams configures a data load operation.
type LoadParams struct {
	FilePath string   // path to the Parquet file
	Table    string   // target table name
	Schema   string   // target schema (default "dbo")
	Mode     LoadMode // append or truncate_and_load
	ConnStr  string   // database connection string
}

// Load reads a Parquet file and bulk-loads it into the target database.
// Returns the number of rows loaded.
func Load(ctx context.Context, params LoadParams) (int64, error) {
	if params.Schema == "" {
		params.Schema = "dbo"
	}
	if params.Mode == "" {
		params.Mode = ModeAppend
	}

	records, schema, err := readParquet(params.FilePath)
	if err != nil {
		return 0, fmt.Errorf("reading parquet file: %w", err)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	driver, err := runner.DetectDriver(params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("detecting driver: %w", err)
	}

	switch driver {
	case "mssql":
		return loadMSSQL(ctx, params, records, schema)
	default:
		return 0, fmt.Errorf("unsupported driver %q for bulk load", driver)
	}
}
