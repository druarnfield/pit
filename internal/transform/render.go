package transform

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// QualifiedName returns the MSSQL-quoted fully qualified name for a model.
func QualifiedName(schema, name string) string {
	return fmt.Sprintf("[%s].[%s]", schema, name)
}

// RenderModel renders a SQL template, resolving {{ ref "name" }} and {{ this }} calls.
func RenderModel(modelName, sql string, models map[string]*ModelConfig) (string, error) {
	currentModel, ok := models[modelName]
	if !ok {
		return "", fmt.Errorf("model %q not found in config", modelName)
	}

	funcMap := template.FuncMap{
		"ref": func(name string) (string, error) {
			m, ok := models[name]
			if !ok {
				return "", fmt.Errorf("ref(%q): model not found", name)
			}
			return QualifiedName(m.Schema, name), nil
		},
		"this": func() string {
			return QualifiedName(currentModel.Schema, modelName)
		},
	}

	tmpl, err := template.New(modelName).Funcs(funcMap).Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parsing template for model %q: %w", modelName, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return "", fmt.Errorf("rendering model %q: %w", modelName, err)
	}

	return buf.String(), nil
}

// RenderModelWithEphemerals renders a model, inlining any ephemeral refs as CTEs.
// ephemeralSQL maps ephemeral model names to their raw SELECT SQL.
func RenderModelWithEphemerals(modelName, sql string, models map[string]*ModelConfig, ephemeralSQL map[string]string) (string, error) {
	currentModel, ok := models[modelName]
	if !ok {
		return "", fmt.Errorf("model %q not found in config", modelName)
	}

	// Collect ephemeral refs from this model's SQL
	refs := ExtractRefs(sql)
	var ctes []string
	for _, ref := range refs {
		m, ok := models[ref]
		if !ok {
			continue
		}
		if m.Materialization != "ephemeral" {
			continue
		}
		epSQL, ok := ephemeralSQL[ref]
		if !ok {
			return "", fmt.Errorf("ephemeral model %q has no SQL", ref)
		}
		cteName := "__pit_ephemeral_" + ref
		ctes = append(ctes, fmt.Sprintf("%s AS (\n%s\n)", cteName, epSQL))
	}

	// Build template func map — ephemeral refs resolve to CTE name
	funcMap := template.FuncMap{
		"ref": func(name string) (string, error) {
			m, ok := models[name]
			if !ok {
				return "", fmt.Errorf("ref(%q): model not found", name)
			}
			if m.Materialization == "ephemeral" {
				return "__pit_ephemeral_" + name, nil
			}
			return QualifiedName(m.Schema, name), nil
		},
		"this": func() string {
			return QualifiedName(currentModel.Schema, modelName)
		},
	}

	tmpl, err := template.New(modelName).Funcs(funcMap).Parse(sql)
	if err != nil {
		return "", fmt.Errorf("parsing template for model %q: %w", modelName, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return "", fmt.Errorf("rendering model %q: %w", modelName, err)
	}

	rendered := buf.String()

	// Prepend CTEs if any
	if len(ctes) > 0 {
		rendered = "WITH " + strings.Join(ctes, ",\n") + "\n" + rendered
	}

	return rendered, nil
}
