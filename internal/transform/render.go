package transform

import (
	"bytes"
	"fmt"
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
