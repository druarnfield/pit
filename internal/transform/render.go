package transform

import (
	"fmt"
	"strings"
)

// QualifiedName returns the MSSQL-quoted fully qualified name for a model.
func QualifiedName(schema, name string) string {
	return fmt.Sprintf("[%s].[%s]", schema, name)
}

// SplitCTEPrefix splits SQL of the form "WITH cte1 AS (...), cte2 AS (...) SELECT ..."
// into the CTE block ("WITH cte1 AS (...), cte2 AS (...)") and the trailing SELECT body.
// If the SQL does not begin with a WITH clause, cteBlock is empty and selectSQL is the
// full input.
//
// The split uses a parenthesis-depth counter so nested parens inside CTE bodies are
// handled correctly without requiring a full SQL parser.
func SplitCTEPrefix(sql string) (cteBlock, selectSQL string) {
	s := strings.TrimSpace(sql)

	// Check for a case-insensitive WITH prefix followed by a word boundary.
	if len(s) < 5 {
		return "", s
	}
	upper := strings.ToUpper(s[:5])
	if upper != "WITH " && upper != "WITH\t" && upper != "WITH\n" && upper != "WITH\r" {
		return "", s
	}

	depth := 0
	i := 0
	for i < len(s) {
		ch := s[i]
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				// At the close of the last CTE body. Skip whitespace to find
				// what follows — a comma means another CTE definition, anything
				// else (SELECT/INSERT/…) is the final statement.
				j := i + 1
				for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
					j++
				}
				if j >= len(s) || s[j] != ',' {
					return strings.TrimSpace(s[:i+1]), strings.TrimSpace(s[j:])
				}
			}
		case '\'':
			// Skip string literal (SQL standard: '' is an escaped single quote).
			i++
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						i++ // escaped quote
					} else {
						break
					}
				}
				i++
			}
		case '-':
			// Skip line comment.
			if i+1 < len(s) && s[i+1] == '-' {
				for i < len(s) && s[i] != '\n' {
					i++
				}
				continue
			}
		case '/':
			// Skip block comment.
			if i+1 < len(s) && s[i+1] == '*' {
				i += 2
				for i+1 < len(s) {
					if s[i] == '*' && s[i+1] == '/' {
						i += 2
						break
					}
					i++
				}
				continue
			}
		}
		i++
	}

	// No split point found — return unchanged.
	return "", s
}

// renderSQLTemplate replaces {{ ref "name" }}, {{ ref 'name' }}, and {{ this }}
// patterns in SQL using regex substitution. This avoids conflicts between the
// Go text/template engine and SQL literals that contain {{ or }} (e.g. JSON).
func renderSQLTemplate(sql string, resolveRef func(string) (string, error), thisVal string) (string, error) {
	var firstErr error
	result := refPattern.ReplaceAllStringFunc(sql, func(match string) string {
		if firstErr != nil {
			return match
		}
		sub := refPattern.FindStringSubmatch(match)
		if sub == nil {
			return match
		}
		// Group 1 = double-quoted name; group 2 = single-quoted name.
		name := sub[1]
		if name == "" {
			name = sub[2]
		}
		val, err := resolveRef(name)
		if err != nil {
			firstErr = err
			return match
		}
		return val
	})
	if firstErr != nil {
		return "", firstErr
	}
	result = thisPattern.ReplaceAllString(result, thisVal)
	return result, nil
}

// collectEphemeralCTEs recursively collects all transitive ephemeral
// dependencies of the SQL string, appending each CTE name to order in
// dependency order (deepest deps first). visited prevents duplicate processing.
func collectEphemeralCTEs(sql string, models map[string]*ModelConfig, ephemeralSQL map[string]string, visited map[string]bool, order *[]string) error {
	refs := ExtractRefs(sql)
	for _, ref := range refs {
		m, ok := models[ref]
		if !ok || m.Materialization != "ephemeral" {
			continue
		}
		if visited[ref] {
			continue
		}
		visited[ref] = true
		epSQL, ok := ephemeralSQL[ref]
		if !ok {
			return fmt.Errorf("ephemeral model %q has no SQL", ref)
		}
		// Recurse to collect transitive deps before appending this one.
		if err := collectEphemeralCTEs(epSQL, models, ephemeralSQL, visited, order); err != nil {
			return err
		}
		*order = append(*order, ref)
	}
	return nil
}

// RenderModel renders a SQL template, resolving {{ ref "name" }} and {{ this }} calls.
func RenderModel(modelName, sql string, models map[string]*ModelConfig) (string, error) {
	currentModel, ok := models[modelName]
	if !ok {
		return "", fmt.Errorf("model %q not found in config", modelName)
	}

	thisVal := QualifiedName(currentModel.Schema, modelName)
	resolveRef := func(name string) (string, error) {
		m, ok := models[name]
		if !ok {
			return "", fmt.Errorf("ref(%q): model not found", name)
		}
		return QualifiedName(m.Schema, name), nil
	}

	return renderSQLTemplate(sql, resolveRef, thisVal)
}

// RenderModelWithEphemerals renders a model, inlining any ephemeral refs as CTEs.
// Transitive ephemeral dependencies are collected recursively and emitted in
// dependency order. ephemeralSQL maps ephemeral model names to their raw SELECT SQL.
func RenderModelWithEphemerals(modelName, sql string, models map[string]*ModelConfig, ephemeralSQL map[string]string) (string, error) {
	currentModel, ok := models[modelName]
	if !ok {
		return "", fmt.Errorf("model %q not found in config", modelName)
	}

	// Collect ephemeral CTEs recursively (transitive deps, dependency order).
	visited := make(map[string]bool)
	var cteOrder []string
	if err := collectEphemeralCTEs(sql, models, ephemeralSQL, visited, &cteOrder); err != nil {
		return "", err
	}

	// resolveRef maps ephemeral names to their CTE alias; others to qualified names.
	resolveRef := func(name string) (string, error) {
		m, ok := models[name]
		if !ok {
			return "", fmt.Errorf("ref(%q): model not found", name)
		}
		if m.Materialization == "ephemeral" {
			return "__pit_ephemeral_" + name, nil
		}
		return QualifiedName(m.Schema, name), nil
	}

	// Build CTE definitions, rendering each ephemeral's SQL to resolve its refs.
	var ctes []string
	for _, cteName := range cteOrder {
		epSQL := ephemeralSQL[cteName]
		m := models[cteName]
		ephThis := QualifiedName(m.Schema, cteName)
		renderedEp, err := renderSQLTemplate(epSQL, resolveRef, ephThis)
		if err != nil {
			return "", fmt.Errorf("rendering ephemeral %q: %w", cteName, err)
		}
		ctes = append(ctes, fmt.Sprintf("__pit_ephemeral_%s AS (\n%s\n)", cteName, renderedEp))
	}

	// Render the main model SQL.
	thisVal := QualifiedName(currentModel.Schema, modelName)
	rendered, err := renderSQLTemplate(sql, resolveRef, thisVal)
	if err != nil {
		return "", fmt.Errorf("rendering model %q: %w", modelName, err)
	}

	// Prepend CTEs if any.
	if len(ctes) > 0 {
		rendered = "WITH " + strings.Join(ctes, ",\n") + "\n" + rendered
	}

	return rendered, nil
}
