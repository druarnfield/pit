package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/druarnfield/pit/internal/runner"
)

// LoadMode controls how data is loaded into the target table.
type LoadMode string

const (
	ModeAppend          LoadMode = "append"
	ModeTruncateAndLoad LoadMode = "truncate_and_load"
	ModeCreateOrReplace LoadMode = "create_or_replace"
)

// LoadParams configures a data load operation.
type LoadParams struct {
	FilePath string   // path to the Parquet file
	Table    string   // target table name
	Schema   string   // target schema (default depends on driver)
	Mode     LoadMode // append, truncate_and_load, or create_or_replace
	ConnStr  string   // database connection string
}

// Load reads a Parquet file and bulk-loads it into the target database.
// Data is streamed one row group at a time to keep memory usage steady.
// Returns the number of rows loaded.
func Load(ctx context.Context, params LoadParams) (int64, error) {
	driverName, err := runner.DetectDriver(params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("detecting driver: %w", err)
	}

	drv, err := GetDriver(driverName)
	if err != nil {
		return 0, fmt.Errorf("getting driver: %w", err)
	}

	if params.Schema == "" {
		params.Schema = drv.DefaultSchema()
	}
	if params.Mode == "" {
		params.Mode = ModeAppend
	}

	stream, err := openParquetStream(params.FilePath)
	if err != nil {
		return 0, fmt.Errorf("reading parquet file: %w", err)
	}
	defer stream.Close()

	db, err := sql.Open(driverName, params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("opening database connection: %w", err)
	}
	defer db.Close()

	if params.Mode == ModeCreateOrReplace {
		if err := drv.DropTable(ctx, db, params.Schema, params.Table); err != nil {
			return 0, err
		}
		if err := drv.CreateTable(ctx, db, params.Schema, params.Table, stream.Schema()); err != nil {
			return 0, err
		}
	}

	if params.Mode == ModeTruncateAndLoad {
		if err := drv.TruncateTable(ctx, db, params.Schema, params.Table); err != nil {
			return 0, err
		}
	}

	return drv.BulkLoad(ctx, db, params, stream)
}
