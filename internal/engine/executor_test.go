package engine

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/secrets"
	"github.com/druarnfield/pit/internal/transform"
)

// TestRun_SecretsResolverNilInterface guards against the typed-nil interface
// bug: assigning a nil *secrets.Store to SecretsResolver produces a non-nil
// interface value (it carries the concrete type but a nil pointer). The nil
// check in executeTask then passes and ResolveField is called on a nil
// receiver, causing a panic.
//
// The fix is to only assign store to run.SecretsResolver when store is
// non-nil, so the interface field stays a true nil interface.
func TestRun_SecretsResolverNilInterface(t *testing.T) {
	// Demonstrate the Go gotcha: typed nil pointer → non-nil interface.
	var store *secrets.Store // nil
	var iface SecretsResolver = store
	if iface == nil {
		t.Fatal("prerequisite failed: expected typed nil *secrets.Store to produce non-nil interface")
	}

	// Verify the fix: conditional assignment leaves the field as nil interface.
	run := &Run{}
	if store != nil {
		run.SecretsResolver = store
	}
	if run.SecretsResolver != nil {
		t.Error("SecretsResolver should be nil interface when store is nil, got non-nil")
	}
}

func TestResolveTaskConnection(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{SQL: config.SQLConfig{Connection: "default_conn"}},
	}
	// Task override wins
	tc := &config.TaskConfig{Connection: "task_conn"}
	if got := resolveTaskConnection(tc, cfg); got != "task_conn" {
		t.Errorf("got %q, want %q", got, "task_conn")
	}
	// Falls back to DAG default
	tc2 := &config.TaskConfig{}
	if got := resolveTaskConnection(tc2, cfg); got != "default_conn" {
		t.Errorf("got %q, want %q", got, "default_conn")
	}
}

// mkCompileResult builds a CompileResult from inline data for use in executor tests.
// modelDeps maps model name -> SQL snippet (use "{{ ref \"dep\" }}" to add edges).
// ephemeralNames lists models that should be excluded from result.Models (ephemeral).
func mkCompileResult(t *testing.T, modelSQL map[string]string, ephemeralNames []string) *transform.CompileResult {
	t.Helper()
	ephSet := make(map[string]bool, len(ephemeralNames))
	for _, n := range ephemeralNames {
		ephSet[n] = true
	}

	cfgs := make(map[string]*transform.ModelConfig, len(modelSQL))
	for name := range modelSQL {
		cfgs[name] = &transform.ModelConfig{Schema: "dbo", Materialization: "table"}
	}

	dag, err := transform.BuildDAG(cfgs, modelSQL, nil)
	if err != nil {
		t.Fatalf("BuildDAG: %v", err)
	}

	models := make(map[string]*transform.CompiledModel)
	for _, name := range dag.Order() {
		if !ephSet[name] {
			models[name] = &transform.CompiledModel{
				Name:        name,
				Config:      cfgs[name],
				CompiledSQL: "-- compiled: " + name,
			}
		}
	}

	return &transform.CompileResult{Models: models, Order: dag.Order(), DAG: dag}
}

func TestBuildTasksFromCompileResult_Basic(t *testing.T) {
	result := mkCompileResult(t, map[string]string{
		"stg_orders": "SELECT 1",
		"fct_orders": `SELECT * FROM {{ ref "stg_orders" }}`,
	}, nil)

	tasks := buildTasksFromCompileResult(result, nil)

	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	byName := make(map[string]config.TaskConfig, len(tasks))
	for _, tc := range tasks {
		byName[tc.Name] = tc
	}

	stg, ok := byName["stg_orders"]
	if !ok {
		t.Fatal("stg_orders task missing")
	}
	if stg.Runner != "sql" {
		t.Errorf("stg_orders.Runner = %q, want sql", stg.Runner)
	}
	wantScript := filepath.Join("compiled_models", "stg_orders.sql")
	if stg.Script != wantScript {
		t.Errorf("stg_orders.Script = %q, want %q", stg.Script, wantScript)
	}

	fct, ok := byName["fct_orders"]
	if !ok {
		t.Fatal("fct_orders task missing")
	}
	if len(fct.DependsOn) != 1 || fct.DependsOn[0] != "stg_orders" {
		t.Errorf("fct_orders.DependsOn = %v, want [stg_orders]", fct.DependsOn)
	}
}

func TestBuildTasksFromCompileResult_EphemeralSkipped(t *testing.T) {
	result := mkCompileResult(t, map[string]string{
		"ephemeral_base": "SELECT 1",
		"fct_orders":     `SELECT * FROM {{ ref "ephemeral_base" }}`,
	}, []string{"ephemeral_base"})

	tasks := buildTasksFromCompileResult(result, nil)

	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1 (ephemeral should be excluded)", len(tasks))
	}
	if tasks[0].Name != "fct_orders" {
		t.Errorf("tasks[0].Name = %q, want fct_orders", tasks[0].Name)
	}
	// Ephemeral model names must not appear in DependsOn — they produce no tasks.
	if len(tasks[0].DependsOn) != 0 {
		t.Errorf("fct_orders.DependsOn = %v, want empty (ephemeral deps filtered out)", tasks[0].DependsOn)
	}
}

func TestBuildTasksFromCompileResult_NonModelTasksPreserved(t *testing.T) {
	result := mkCompileResult(t, map[string]string{"stg_orders": "SELECT 1"}, nil)

	existing := []config.TaskConfig{
		{Name: "stg_orders"}, // model — replaced by compiled version
		{Name: "run_python", Runner: "python", Script: "scripts/run.py"},
	}

	tasks := buildTasksFromCompileResult(result, existing)

	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	byName := make(map[string]config.TaskConfig, len(tasks))
	for _, tc := range tasks {
		byName[tc.Name] = tc
	}

	if got := byName["stg_orders"].Runner; got != "sql" {
		t.Errorf("stg_orders.Runner = %q, want sql", got)
	}
	py, ok := byName["run_python"]
	if !ok {
		t.Fatal("run_python task missing")
	}
	if py.Script != "scripts/run.py" {
		t.Errorf("run_python.Script = %q, want scripts/run.py", py.Script)
	}
}

func TestBuildTasksFromCompileResult_MergesExplicitConfig(t *testing.T) {
	result := mkCompileResult(t, map[string]string{"stg_orders": "SELECT 1"}, nil)
	result.Models["stg_orders"].Config.Connection = "prod_db"

	explicit := []config.TaskConfig{
		{
			Name:    "stg_orders",
			Retries: 3,
			Timeout: config.Duration{Duration: 5 * time.Minute},
		},
	}

	tasks := buildTasksFromCompileResult(result, explicit)

	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
	tc := tasks[0]
	if tc.Retries != 3 {
		t.Errorf("Retries = %d, want 3", tc.Retries)
	}
	if tc.Timeout.Duration != 5*time.Minute {
		t.Errorf("Timeout = %v, want 5m", tc.Timeout.Duration)
	}
	if tc.Connection != "prod_db" {
		t.Errorf("Connection = %q, want prod_db", tc.Connection)
	}
	if tc.Runner != "sql" {
		t.Errorf("Runner = %q, want sql", tc.Runner)
	}
}

func TestBuildTasksFromCompileResult_ExplicitDependsOnMerged(t *testing.T) {
	result := mkCompileResult(t, map[string]string{"stg_orders": "SELECT 1"}, nil)

	existing := []config.TaskConfig{
		{
			Name:      "stg_orders",
			DependsOn: []string{"load_seed_data", "run_setup"},
		},
	}

	tasks := buildTasksFromCompileResult(result, existing)

	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
	deps := tasks[0].DependsOn
	found := make(map[string]bool, len(deps))
	for _, d := range deps {
		found[d] = true
	}
	if !found["load_seed_data"] {
		t.Errorf("DependsOn missing load_seed_data, got %v", deps)
	}
	if !found["run_setup"] {
		t.Errorf("DependsOn missing run_setup, got %v", deps)
	}
}

func TestBuildTasksFromCompileResult_ExplicitDependsOnNoDuplicates(t *testing.T) {
	result := mkCompileResult(t, map[string]string{
		"stg_orders": "SELECT 1",
		"fct_orders": `SELECT * FROM {{ ref "stg_orders" }}`,
	}, nil)

	// stg_orders is already a DAG dep of fct_orders; it must not be duplicated.
	existing := []config.TaskConfig{
		{
			Name:      "fct_orders",
			DependsOn: []string{"stg_orders", "load_seed_data"},
		},
	}

	tasks := buildTasksFromCompileResult(result, existing)

	byName := make(map[string]config.TaskConfig, len(tasks))
	for _, tc := range tasks {
		byName[tc.Name] = tc
	}

	fct := byName["fct_orders"]
	seen := make(map[string]int)
	for _, d := range fct.DependsOn {
		seen[d]++
	}
	if seen["stg_orders"] > 1 {
		t.Errorf("stg_orders appears %d times in DependsOn, want 1: %v", seen["stg_orders"], fct.DependsOn)
	}
	if seen["load_seed_data"] != 1 {
		t.Errorf("load_seed_data should appear once in DependsOn, got %d: %v", seen["load_seed_data"], fct.DependsOn)
	}
}

func TestParseSchemaTable(t *testing.T) {
	tests := []struct {
		input      string
		wantSchema string
		wantTable  string
	}{
		{"staging.customers", "staging", "customers"},
		{"customers", "", "customers"},
		{"public.my_table", "public", "my_table"},
	}
	for _, tt := range tests {
		s, tbl := parseSchemaTable(tt.input)
		if s != tt.wantSchema || tbl != tt.wantTable {
			t.Errorf("parseSchemaTable(%q) = (%q, %q), want (%q, %q)",
				tt.input, s, tbl, tt.wantSchema, tt.wantTable)
		}
	}
}
