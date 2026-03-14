package transform

import (
	"strings"
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func TestBuildDAG_FromRefs(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders":  {Materialization: "view"},
		"fact_orders": {Materialization: "table"},
	}
	sqlContents := map[string]string{
		"stg_orders":  "SELECT * FROM raw.orders",
		"fact_orders": `SELECT * FROM {{ ref "stg_orders" }}`,
	}

	dag, err := BuildDAG(models, sqlContents, nil)
	if err != nil {
		t.Fatalf("BuildDAG() unexpected error: %v", err)
	}

	deps := dag.DependsOn("fact_orders")
	if len(deps) != 1 || deps[0] != "stg_orders" {
		t.Errorf("fact_orders depends_on = %v, want [stg_orders]", deps)
	}

	deps = dag.DependsOn("stg_orders")
	if len(deps) != 0 {
		t.Errorf("stg_orders depends_on = %v, want []", deps)
	}

	order := dag.Order()
	if len(order) != 2 {
		t.Fatalf("Order() returned %d items, want 2", len(order))
	}
	if order[0] != "stg_orders" || order[1] != "fact_orders" {
		t.Errorf("Order() = %v, want [stg_orders fact_orders]", order)
	}
}

func TestBuildDAG_MergeExplicitDeps(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders":  {Materialization: "view"},
		"stg_payments": {Materialization: "view"},
		"fact_orders": {Materialization: "table"},
	}
	sqlContents := map[string]string{
		"stg_orders":   "SELECT * FROM raw.orders",
		"stg_payments": "SELECT * FROM raw.payments",
		"fact_orders":  `SELECT * FROM {{ ref "stg_orders" }}`,
	}
	// Add explicit dependency on stg_payments via task config
	tasks := []config.TaskConfig{
		{
			Name:      "fact_orders",
			DependsOn: []string{"stg_payments"},
		},
	}

	dag, err := BuildDAG(models, sqlContents, tasks)
	if err != nil {
		t.Fatalf("BuildDAG() unexpected error: %v", err)
	}

	deps := dag.DependsOn("fact_orders")
	if len(deps) != 2 {
		t.Fatalf("fact_orders depends_on has %d items, want 2: %v", len(deps), deps)
	}

	// Should depend on both stg_orders (from ref) and stg_payments (from explicit)
	hasSources := map[string]bool{}
	for _, d := range deps {
		hasSources[d] = true
	}
	if !hasSources["stg_orders"] {
		t.Errorf("fact_orders missing ref dependency stg_orders, got %v", deps)
	}
	if !hasSources["stg_payments"] {
		t.Errorf("fact_orders missing explicit dependency stg_payments, got %v", deps)
	}
}

func TestBuildDAG_RefToUnknownModel(t *testing.T) {
	models := map[string]*ModelConfig{
		"fact_orders": {Materialization: "table"},
	}
	sqlContents := map[string]string{
		"fact_orders": `SELECT * FROM {{ ref "stg_orders" }}`,
	}

	_, err := BuildDAG(models, sqlContents, nil)
	if err == nil {
		t.Fatalf("BuildDAG() expected error for unknown ref, got nil")
	}
	if !strings.Contains(err.Error(), "unknown model") {
		t.Errorf("error = %q, want it to contain %q", err, "unknown model")
	}
	if !strings.Contains(err.Error(), "stg_orders") {
		t.Errorf("error = %q, want it to contain %q", err, "stg_orders")
	}
}

func TestBuildDAG_CycleDetection(t *testing.T) {
	models := map[string]*ModelConfig{
		"model_a": {Materialization: "table"},
		"model_b": {Materialization: "table"},
	}
	sqlContents := map[string]string{
		"model_a": `SELECT * FROM {{ ref "model_b" }}`,
		"model_b": `SELECT * FROM {{ ref "model_a" }}`,
	}

	_, err := BuildDAG(models, sqlContents, nil)
	if err == nil {
		t.Fatalf("BuildDAG() expected error for cycle, got nil")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error = %q, want it to contain %q", err, "cycle")
	}
}
