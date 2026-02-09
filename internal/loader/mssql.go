package loader

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	mssql "github.com/microsoft/go-mssqldb"
)

// loadMSSQL bulk-loads Arrow records into an MSSQL table.
func loadMSSQL(ctx context.Context, params LoadParams, records []arrow.Record, schema *arrow.Schema) (int64, error) {
	db, err := sql.Open("mssql", params.ConnStr)
	if err != nil {
		return 0, fmt.Errorf("opening mssql connection: %w", err)
	}
	defer db.Close()

	if params.Mode == ModeTruncateAndLoad {
		truncateSQL := fmt.Sprintf("TRUNCATE TABLE [%s].[%s]", params.Schema, params.Table)
		if _, err := db.ExecContext(ctx, truncateSQL); err != nil {
			return 0, fmt.Errorf("truncating table: %w", err)
		}
	}

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
	for _, rec := range records {
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

	// Flush the bulk copy
	if _, err := stmt.ExecContext(ctx); err != nil {
		return totalRows, fmt.Errorf("flushing bulk copy: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return totalRows, fmt.Errorf("committing transaction: %w", err)
	}

	return totalRows, nil
}
