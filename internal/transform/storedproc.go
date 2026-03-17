package transform

import (
	"fmt"
	"strings"
)

// GenerateStoredProcedure wraps compiled models into a single MSSQL stored
// procedure that executes each model's DDL/DML in topological order. The
// procedure uses SET XACT_ABORT ON so any failure aborts the batch, and wraps
// the body in a TRY/CATCH with THROW to surface errors to the caller.
func GenerateStoredProcedure(procName, schema string, result *CompileResult) (string, error) {
	if len(result.Models) == 0 {
		return "", fmt.Errorf("no compiled models to include in stored procedure")
	}

	var b strings.Builder

	b.WriteString(fmt.Sprintf("CREATE OR ALTER PROCEDURE [%s].[%s]\nAS\nBEGIN\n", schema, procName))
	b.WriteString("    SET NOCOUNT ON;\n")
	b.WriteString("    SET XACT_ABORT ON;\n\n")
	b.WriteString("    BEGIN TRY\n")

	for i, name := range result.Order {
		m, ok := result.Models[name]
		if !ok {
			// Ephemeral models are not in Models — skip.
			continue
		}

		if i > 0 {
			b.WriteString("\n")
		}

		b.WriteString(fmt.Sprintf("        -- [%d] %s (%s)\n", i+1, name, m.Config.Materialization))

		// Indent each line of the compiled SQL.
		for _, line := range strings.Split(strings.TrimRight(m.CompiledSQL, "\n"), "\n") {
			if line == "" {
				b.WriteString("\n")
			} else {
				b.WriteString("        " + line + "\n")
			}
		}
	}

	b.WriteString("\n    END TRY\n")
	b.WriteString("    BEGIN CATCH\n")
	b.WriteString("        THROW;\n")
	b.WriteString("    END CATCH\n")
	b.WriteString("END\n")

	return b.String(), nil
}
