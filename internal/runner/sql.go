package runner

import (
	"context"
	"fmt"
	"io"
	"os"
)

// SQLRunner is a stub that reads the SQL file, logs its content, and succeeds.
// A real implementation will execute the SQL against a configured connection.
type SQLRunner struct{}

func (r *SQLRunner) Run(ctx context.Context, rc RunContext, logFile io.Writer) error {
	// Check context before doing I/O
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
