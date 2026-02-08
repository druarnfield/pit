package runner

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // register "duckdb" driver
	_ "github.com/microsoft/go-mssqldb" // register "mssql" driver
)

// SQLRunner executes .sql files against a database connection resolved from the secrets store.
type SQLRunner struct{}

func (r *SQLRunner) Run(ctx context.Context, rc RunContext, logFile io.Writer) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sql runner %s: %w", rc.ScriptPath, err)
	}

	// If no secrets resolver is configured, fall back to stub behaviour
	if rc.SecretsResolver == nil || rc.SQLConnection == "" {
		return r.runStub(ctx, rc, logFile)
	}

	// Resolve the connection string from the secrets store
	connStr, err := rc.SecretsResolver.Resolve(rc.DAGName, rc.SQLConnection)
	if err != nil {
		return fmt.Errorf("sql runner resolving connection %q: %w", rc.SQLConnection, err)
	}

	driver, err := DetectDriver(connStr)
	if err != nil {
		return fmt.Errorf("sql runner: %w", err)
	}

	content, err := os.ReadFile(rc.ScriptPath)
	if err != nil {
		return fmt.Errorf("sql runner reading %s: %w", rc.ScriptPath, err)
	}

	db, err := sql.Open(driver, connStr)
	if err != nil {
		return fmt.Errorf("sql runner opening %s connection: %w", driver, err)
	}
	defer db.Close()

	start := time.Now()
	result, err := db.ExecContext(ctx, string(content))
	elapsed := time.Since(start)

	if err != nil {
		return fmt.Errorf("sql runner executing %s: %w", rc.ScriptPath, err)
	}

	rows, _ := result.RowsAffected()
	fmt.Fprintf(logFile, "[sql] %s executed in %s (%d rows affected)\n",
		rc.ScriptPath, elapsed.Round(time.Millisecond), rows)

	return nil
}

// runStub provides backwards-compatible stub behaviour when no secrets are configured.
func (r *SQLRunner) runStub(ctx context.Context, rc RunContext, logFile io.Writer) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sql runner %s: %w", rc.ScriptPath, err)
	}

	content, err := os.ReadFile(rc.ScriptPath)
	if err != nil {
		return fmt.Errorf("sql runner reading %s: %w", rc.ScriptPath, err)
	}

	fmt.Fprintf(logFile, "[sql-stub] would execute against configured connection:\n")
	fmt.Fprintf(logFile, "--- %s ---\n", rc.ScriptPath)
	fmt.Fprintf(logFile, "%s\n", string(content))
	fmt.Fprintf(logFile, "--- end ---\n")
	return nil
}

// DetectDriver determines the database/sql driver name from a connection string.
// Returns "mssql" for sqlserver:// or mssql:// URIs, "duckdb" for duckdb:// URIs
// or file paths ending in .db or .duckdb.
func DetectDriver(connStr string) (string, error) {
	lower := strings.ToLower(connStr)
	switch {
	case strings.HasPrefix(lower, "sqlserver://"), strings.HasPrefix(lower, "mssql://"):
		return "mssql", nil
	case strings.HasPrefix(lower, "duckdb://"):
		return "duckdb", nil
	case strings.HasSuffix(lower, ".db"), strings.HasSuffix(lower, ".duckdb"):
		return "duckdb", nil
	default:
		return "", fmt.Errorf("cannot detect SQL driver from connection string (expected sqlserver://, mssql://, duckdb://, or a .db/.duckdb file path)")
	}
}
