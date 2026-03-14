package transform

import (
	"context"
	"database/sql"
	"fmt"
)

// TableExistsQuery returns a SQL query that checks if a table exists (MSSQL).
func TableExistsQuery(schema, table string) string {
	return fmt.Sprintf(
		"SELECT CASE WHEN OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL THEN 1 ELSE 0 END",
		schema, table,
	)
}

// ColumnsQuery returns a SQL query to get column names from INFORMATION_SCHEMA (MSSQL).
func ColumnsQuery(schema, table string) string {
	return fmt.Sprintf(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
		schema, table,
	)
}

// TableExists checks whether a table exists in the database.
func TableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, TableExistsQuery(schema, table)).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking table existence [%s].[%s]: %w", schema, table, err)
	}
	return exists == 1, nil
}

// GetColumns retrieves column names for a table from INFORMATION_SCHEMA.
func GetColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, ColumnsQuery(schema, table))
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
