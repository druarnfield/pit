package transform

import (
	"context"
	"database/sql"
	"fmt"
)

// TableExists checks whether a table exists in the database using a
// parameterized query to prevent SQL injection.
func TableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2",
		schema, table,
	).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("checking table existence [%s].[%s]: %w", schema, table, err)
	}
	return count > 0, nil
}

// GetColumns retrieves column names for a table from INFORMATION_SCHEMA using
// a parameterized query to prevent SQL injection.
func GetColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2 ORDER BY ORDINAL_POSITION",
		schema, table,
	)
	if err != nil {
		return nil, fmt.Errorf("querying columns for [%s].[%s]: %w", schema, table, err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning column name: %w", err)
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// BuildUpdateColumns returns all columns minus the unique key columns.
func BuildUpdateColumns(allColumns, uniqueKey []string) []string {
	keySet := make(map[string]bool, len(uniqueKey))
	for _, k := range uniqueKey {
		keySet[k] = true
	}
	var update []string
	for _, col := range allColumns {
		if !keySet[col] {
			update = append(update, col)
		}
	}
	return update
}
