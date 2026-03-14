package transform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompile_FullPipeline(t *testing.T) {
	outDir := t.TempDir()
	modelsDir := "testdata/compilable/models"

	result, err := Compile(modelsDir, "mssql", outDir, nil)
	if err != nil {
		t.Fatalf("Compile() error: %v", err)
	}

	if len(result.Models) != 2 {
		t.Fatalf("got %d compiled models, want 2", len(result.Models))
	}

	// Check stg_orders compiled as view
	stg, ok := result.Models["stg_orders"]
	if !ok {
		t.Fatal("missing compiled model stg_orders")
	}
	if !strings.Contains(stg.CompiledSQL, "CREATE OR ALTER VIEW [staging].[stg_orders]") {
		t.Errorf("stg_orders expected CREATE OR ALTER VIEW, got:\n%s", stg.CompiledSQL)
	}

	// Check fact_orders compiled as table with ref resolved
	fact, ok := result.Models["fact_orders"]
	if !ok {
		t.Fatal("missing compiled model fact_orders")
	}
	if !strings.Contains(fact.CompiledSQL, "BEGIN TRANSACTION") {
		t.Errorf("fact_orders expected BEGIN TRANSACTION, got:\n%s", fact.CompiledSQL)
	}
	if !strings.Contains(fact.CompiledSQL, "[staging].[stg_orders]") {
		t.Errorf("fact_orders expected resolved ref, got:\n%s", fact.CompiledSQL)
	}

	// Check execution order: stg_orders before fact_orders
	stgIdx, factIdx := -1, -1
	for i, name := range result.Order {
		if name == "stg_orders" {
			stgIdx = i
		}
		if name == "fact_orders" {
			factIdx = i
		}
	}
	if stgIdx >= factIdx {
		t.Errorf("stg_orders (idx %d) should come before fact_orders (idx %d)", stgIdx, factIdx)
	}

	// Check compiled files written to disk
	for _, name := range []string{"stg_orders", "fact_orders"} {
		path := filepath.Join(outDir, name+".sql")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("compiled file not written: %s", path)
		}
	}

	// Check DAG is populated
	if result.DAG == nil {
		t.Error("CompileResult.DAG is nil")
	}
}

func TestCompile_MissingMaterialization(t *testing.T) {
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models")
	if err := os.MkdirAll(modelsDir, 0o755); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "orphan.sql"), []byte("SELECT 1"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}

	_, err := Compile(modelsDir, "mssql", t.TempDir(), nil)
	if err == nil {
		t.Fatal("expected error for missing materialization, got nil")
	}
	if !strings.Contains(err.Error(), "materialization") {
		t.Errorf("error = %q, want it to contain %q", err, "materialization")
	}
}
