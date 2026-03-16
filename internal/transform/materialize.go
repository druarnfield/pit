package transform

import (
	"bytes"
	"embed"
	"fmt"
	"text/template"
)

//go:embed dialects/*/*.sql
var dialectFS embed.FS

// MaterializeContext is the data passed to materialization templates.
type MaterializeContext struct {
	ModelName     string
	Schema        string
	SQL           string // full rendered SQL (includes CTE prefix if any)
	CTEBlock      string // leading WITH ... block extracted from SQL (empty if none)
	SelectSQL     string // the final SELECT body without any CTE prefix
	UniqueKey     []string
	This          string
	Columns       []string
	UpdateColumns []string
}

// Materialize renders a materialization template for the given dialect and type.
func Materialize(dialect, materialization string, ctx *MaterializeContext) (string, error) {
	path := fmt.Sprintf("dialects/%s/%s.sql", dialect, materialization)
	data, err := dialectFS.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("loading materialization template %s/%s: %w", dialect, materialization, err)
	}

	tmpl, err := template.New(materialization).Parse(string(data))
	if err != nil {
		return "", fmt.Errorf("parsing materialization template %s/%s: %w", dialect, materialization, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, ctx); err != nil {
		return "", fmt.Errorf("executing materialization template %s/%s: %w", dialect, materialization, err)
	}

	return buf.String(), nil
}
