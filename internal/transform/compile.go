package transform

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/druarnfield/pit/internal/config"
)

// CompiledModel holds the result of compiling a single model.
type CompiledModel struct {
	Name        string
	Config      *ModelConfig
	RenderedSQL string // SELECT with refs resolved
	CompiledSQL string // full DDL/DML ready to execute
}

// CompileResult holds the output of compiling all models in a project.
type CompileResult struct {
	Models map[string]*CompiledModel
	Order  []string  // topological execution order
	DAG    *ModelDAG // the resolved dependency graph
}

// Compile discovers models, resolves config, builds the DAG, renders templates,
// applies materializations, and writes compiled SQL to outDir.
func Compile(modelsDir, dialect, outDir string, tasks []config.TaskConfig) (*CompileResult, error) {
	// 1. Discover and resolve model configs
	configs, err := ParseModelConfigs(modelsDir)
	if err != nil {
		return nil, fmt.Errorf("parsing model configs: %w", err)
	}

	// 2. Validate: every model must have a materialization and schema
	for name, cfg := range configs {
		if cfg.Materialization == "" {
			return nil, fmt.Errorf("model %q has no materialization configured", name)
		}
		if cfg.Schema == "" {
			return nil, fmt.Errorf("model %q has no schema configured", name)
		}
	}

	// 3. Read SQL contents
	sqlContents := make(map[string]string, len(configs))
	for name, cfg := range configs {
		data, err := os.ReadFile(cfg.SQLPath)
		if err != nil {
			return nil, fmt.Errorf("reading model %q: %w", name, err)
		}
		sqlContents[name] = string(data)
	}

	// 4. Build DAG
	dag, err := BuildDAG(configs, sqlContents, tasks)
	if err != nil {
		return nil, fmt.Errorf("building model DAG: %w", err)
	}

	// 5. Render and materialize in topological order
	result := &CompileResult{
		Models: make(map[string]*CompiledModel, len(configs)),
		Order:  dag.Order(),
		DAG:    dag,
	}

	for _, name := range dag.Order() {
		cfg := configs[name]

		// Skip ephemeral models
		if cfg.Materialization == "ephemeral" {
			continue
		}

		// Render {{ ref }} and {{ this }}
		rendered, err := RenderModel(name, sqlContents[name], configs)
		if err != nil {
			return nil, fmt.Errorf("rendering model %q: %w", name, err)
		}

		// Determine materialization template name
		matTemplate := cfg.Materialization
		if cfg.Materialization == "incremental" {
			strategy := cfg.Strategy
			if strategy == "" {
				strategy = "merge"
			}
			matTemplate = "incremental_" + strategy
		}

		// Build materialization context
		this := QualifiedName(cfg.Schema, name)
		matCtx := &MaterializeContext{
			ModelName: name,
			Schema:    cfg.Schema,
			SQL:       rendered,
			UniqueKey: cfg.UniqueKey,
			This:      this,
		}

		// Apply materialization template
		compiled, err := Materialize(dialect, matTemplate, matCtx)
		if err != nil {
			return nil, fmt.Errorf("materializing model %q: %w", name, err)
		}

		result.Models[name] = &CompiledModel{
			Name:        name,
			Config:      cfg,
			RenderedSQL: rendered,
			CompiledSQL: compiled,
		}

		// Write compiled SQL to output directory
		outPath := filepath.Join(outDir, name+".sql")
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return nil, fmt.Errorf("creating output dir for %q: %w", name, err)
		}
		if err := os.WriteFile(outPath, []byte(compiled), 0o644); err != nil {
			return nil, fmt.Errorf("writing compiled model %q: %w", name, err)
		}
	}

	return result, nil
}
