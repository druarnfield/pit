# SQL Transform Engine Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a native SQL transformation engine to pit that wraps SELECT statements in dialect-specific DDL/DML materializations, with DAG inference from `{{ ref }}` calls.

**Architecture:** New `internal/transform/` package handles model discovery, config resolution, ref parsing, DAG building, template compilation, and column introspection. Integrates with the existing engine executor — compiled models are just SQL strings dispatched to the existing SQL runner. Materialization templates are embedded via `embed.FS` and organized by dialect.

**Tech Stack:** Go stdlib (`text/template`, `embed`, `path/filepath`, `regexp`), existing `internal/config`, `internal/dag`, `internal/runner`, `internal/engine` packages.

**Design doc:** `docs/plans/2026-03-14-sql-transform-engine-design.md`

---

### Task 1: Model Config Types & Parsing

Define the config types for transform models and parse TOML files from the `models/` directory.

**Files:**
- Create: `internal/transform/config.go`
- Create: `internal/transform/config_test.go`
- Create: `internal/transform/testdata/simple/defaults.toml`
- Create: `internal/transform/testdata/simple/stg_orders.sql`

**Step 1: Write test fixtures**

Create `internal/transform/testdata/simple/defaults.toml`:
```toml
[defaults]
materialization = "view"
schema = "staging"
```

Create `internal/transform/testdata/simple/stg_orders.sql`:
```sql
SELECT order_id, customer_id, order_date
FROM raw.orders
```

**Step 2: Write the failing tests**

Create `internal/transform/config_test.go`:
```go
package transform

import (
	"testing"
)

func TestParseModelConfigs_Simple(t *testing.T) {
	configs, err := ParseModelConfigs("testdata/simple")
	if err != nil {
		t.Fatalf("ParseModelConfigs() error: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("got %d configs, want 1", len(configs))
	}
	cfg, ok := configs["stg_orders"]
	if !ok {
		t.Fatalf("missing config for stg_orders, got keys: %v", configKeys(configs))
	}
	if cfg.Materialization != "view" {
		t.Errorf("materialization = %q, want %q", cfg.Materialization, "view")
	}
	if cfg.Schema != "staging" {
		t.Errorf("schema = %q, want %q", cfg.Schema, "staging")
	}
}

func TestParseModelConfigs_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	configs, err := ParseModelConfigs(dir)
	if err != nil {
		t.Fatalf("ParseModelConfigs() error: %v", err)
	}
	if len(configs) != 0 {
		t.Errorf("got %d configs, want 0", len(configs))
	}
}

func configKeys(m map[string]*ModelConfig) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
```

**Step 3: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestParseModelConfigs -v`
Expected: FAIL — package doesn't exist yet

**Step 4: Write minimal implementation**

Create `internal/transform/config.go`:
```go
package transform

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// ModelConfig holds the resolved configuration for a single model.
type ModelConfig struct {
	Materialization string   `toml:"materialization"`
	Strategy        string   `toml:"strategy"`     // "merge" or "delete_insert" (incremental only)
	UniqueKey       []string `toml:"unique_key"`   // columns for incremental match
	Schema          string   `toml:"schema"`       // target schema
	Connection      string   `toml:"connection"`   // override [dag.sql].connection
	SQLPath         string   // absolute path to the .sql file (set during discovery)
	RelPath         string   // path relative to models/ dir (e.g. "staging/stg_orders.sql")
}

// modelFileEntry represents a raw TOML file under models/.
// It can contain a [defaults] section and per-model sections.
type modelFileEntry struct {
	Defaults *ModelConfig            `toml:"defaults"`
	Models   map[string]*ModelConfig // remaining keys parsed as model overrides
}

// parseModelTOML parses a single TOML file, separating [defaults] from per-model configs.
func parseModelTOML(path string) (*ModelConfig, map[string]*ModelConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading model config %q: %w", path, err)
	}

	// First pass: extract [defaults]
	var withDefaults struct {
		Defaults *ModelConfig `toml:"defaults"`
	}
	if err := toml.Unmarshal(data, &withDefaults); err != nil {
		return nil, nil, fmt.Errorf("parsing model config %q: %w", path, err)
	}

	// Second pass: extract all top-level keys except "defaults" as model overrides
	var raw map[string]toml.Primitive
	md, err := toml.Decode(string(data), &raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing model config %q: %w", path, err)
	}

	models := make(map[string]*ModelConfig)
	for key := range raw {
		if key == "defaults" {
			continue
		}
		var mc ModelConfig
		if err := md.PrimitiveDecode(raw[key], &mc); err != nil {
			return nil, nil, fmt.Errorf("parsing model %q in %q: %w", key, path, err)
		}
		models[key] = &mc
	}

	return withDefaults.Defaults, models, nil
}

// discoverModels finds all .sql files under modelsDir and returns their names and paths.
func discoverModels(modelsDir string) (map[string]string, error) {
	models := make(map[string]string)
	err := filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".sql") {
			return nil
		}
		name := strings.TrimSuffix(d.Name(), ".sql")
		if _, exists := models[name]; exists {
			return fmt.Errorf("duplicate model name %q", name)
		}
		models[name] = path
		return nil
	})
	return models, err
}

// ParseModelConfigs discovers all models under modelsDir and resolves their configuration.
// Returns a map of model name → resolved ModelConfig.
func ParseModelConfigs(modelsDir string) (map[string]*ModelConfig, error) {
	// Discover .sql files
	sqlFiles, err := discoverModels(modelsDir)
	if err != nil {
		return nil, err
	}

	if len(sqlFiles) == 0 {
		return make(map[string]*ModelConfig), nil
	}

	// Discover and parse all .toml files
	type tomlEntry struct {
		dir      string
		defaults *ModelConfig
		models   map[string]*ModelConfig
	}
	var tomlEntries []tomlEntry

	err = filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".toml") {
			return nil
		}
		defaults, models, parseErr := parseModelTOML(path)
		if parseErr != nil {
			return parseErr
		}
		tomlEntries = append(tomlEntries, tomlEntry{
			dir:      filepath.Dir(path),
			defaults: defaults,
			models:   models,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Resolve config for each model
	result := make(map[string]*ModelConfig, len(sqlFiles))
	for name, sqlPath := range sqlFiles {
		mc := &ModelConfig{
			SQLPath: sqlPath,
		}
		relPath, _ := filepath.Rel(modelsDir, sqlPath)
		mc.RelPath = relPath
		modelDir := filepath.Dir(sqlPath)

		// Apply defaults from TOML files, innermost first
		// Walk from modelsDir down to modelDir, applying defaults at each level
		mc = applyDefaults(mc, modelDir, modelsDir, tomlEntries)

		// Apply per-model overrides
		for _, entry := range tomlEntries {
			if override, ok := entry.models[name]; ok {
				mc = mergeConfig(mc, override)
			}
		}

		result[name] = mc
	}

	return result, nil
}

// applyDefaults resolves defaults by walking from modelsDir down to modelDir.
func applyDefaults(mc *ModelConfig, modelDir, modelsDir string, entries []tomlEntry) *ModelConfig {
	// Build path from modelsDir to modelDir
	rel, err := filepath.Rel(modelsDir, modelDir)
	if err != nil {
		return mc
	}

	// Collect directories from root to leaf
	var dirs []string
	dirs = append(dirs, modelsDir)
	if rel != "." {
		parts := strings.Split(rel, string(filepath.Separator))
		current := modelsDir
		for _, p := range parts {
			current = filepath.Join(current, p)
			dirs = append(dirs, current)
		}
	}

	// Apply defaults from outermost to innermost (innermost wins)
	for _, dir := range dirs {
		for _, entry := range entries {
			if entry.dir == dir && entry.defaults != nil {
				mc = mergeConfig(mc, entry.defaults)
			}
		}
	}

	return mc
}

// mergeConfig overlays non-zero values from overlay onto base.
func mergeConfig(base, overlay *ModelConfig) *ModelConfig {
	result := *base
	if overlay.Materialization != "" {
		result.Materialization = overlay.Materialization
	}
	if overlay.Strategy != "" {
		result.Strategy = overlay.Strategy
	}
	if len(overlay.UniqueKey) > 0 {
		result.UniqueKey = overlay.UniqueKey
	}
	if overlay.Schema != "" {
		result.Schema = overlay.Schema
	}
	if overlay.Connection != "" {
		result.Connection = overlay.Connection
	}
	return &result
}
```

**Step 5: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestParseModelConfigs -v`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/transform/config.go internal/transform/config_test.go internal/transform/testdata/
git commit -m "Add transform model config parsing with defaults resolution"
```

---

### Task 2: Directory-Scoped Config Inheritance

Test the cascade behavior where nested directories inherit and override parent defaults.

**Files:**
- Modify: `internal/transform/config_test.go`
- Create: `internal/transform/testdata/nested/defaults.toml`
- Create: `internal/transform/testdata/nested/staging/defaults.toml`
- Create: `internal/transform/testdata/nested/staging/stg_orders.sql`
- Create: `internal/transform/testdata/nested/marts/marts.toml`
- Create: `internal/transform/testdata/nested/marts/dim_customers.sql`
- Create: `internal/transform/testdata/nested/marts/incremental/defaults.toml`
- Create: `internal/transform/testdata/nested/marts/incremental/fact_daily.sql`

**Step 1: Write test fixtures**

`testdata/nested/defaults.toml`:
```toml
[defaults]
materialization = "view"
schema = "dbo"
```

`testdata/nested/staging/defaults.toml`:
```toml
[defaults]
schema = "staging"
```

`testdata/nested/staging/stg_orders.sql`:
```sql
SELECT order_id FROM raw.orders
```

`testdata/nested/marts/marts.toml`:
```toml
[defaults]
materialization = "table"
schema = "analytics"

[dim_customers]
materialization = "view"
```

`testdata/nested/marts/dim_customers.sql`:
```sql
SELECT customer_id FROM raw.customers
```

`testdata/nested/marts/incremental/defaults.toml`:
```toml
[defaults]
materialization = "incremental"
strategy = "merge"
```

`testdata/nested/marts/incremental/fact_daily.sql`:
```sql
SELECT order_date, SUM(amount) as total FROM raw.orders GROUP BY order_date
```

**Step 2: Write the failing tests**

Add to `config_test.go`:
```go
func TestParseModelConfigs_NestedDefaults(t *testing.T) {
	configs, err := ParseModelConfigs("testdata/nested")
	if err != nil {
		t.Fatalf("ParseModelConfigs() error: %v", err)
	}

	tests := []struct {
		name            string
		wantMat         string
		wantSchema      string
		wantStrategy    string
	}{
		// staging/stg_orders: root defaults (view) + staging override (schema=staging)
		{"stg_orders", "view", "staging", ""},
		// marts/dim_customers: per-model override (view) + marts defaults (schema=analytics)
		{"dim_customers", "view", "analytics", ""},
		// marts/incremental/fact_daily: incremental defaults override marts table default
		{"fact_daily", "incremental", "analytics", "merge"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, ok := configs[tt.name]
			if !ok {
				t.Fatalf("missing config for %s", tt.name)
			}
			if cfg.Materialization != tt.wantMat {
				t.Errorf("materialization = %q, want %q", cfg.Materialization, tt.wantMat)
			}
			if cfg.Schema != tt.wantSchema {
				t.Errorf("schema = %q, want %q", cfg.Schema, tt.wantSchema)
			}
			if cfg.Strategy != tt.wantStrategy {
				t.Errorf("strategy = %q, want %q", cfg.Strategy, tt.wantStrategy)
			}
		})
	}
}

func TestParseModelConfigs_DuplicateModelName(t *testing.T) {
	dir := t.TempDir()
	// Create two .sql files with the same name in different subdirs
	os.MkdirAll(filepath.Join(dir, "a"), 0o755)
	os.MkdirAll(filepath.Join(dir, "b"), 0o755)
	os.WriteFile(filepath.Join(dir, "a", "dup.sql"), []byte("SELECT 1"), 0o644)
	os.WriteFile(filepath.Join(dir, "b", "dup.sql"), []byte("SELECT 2"), 0o644)

	_, err := ParseModelConfigs(dir)
	if err == nil {
		t.Errorf("expected error for duplicate model name, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate model name") {
		t.Errorf("error = %q, want it to contain %q", err, "duplicate model name")
	}
}
```

Add `"os"`, `"path/filepath"`, and `"strings"` to the test imports.

**Step 3: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestParseModelConfigs -v`
Expected: Tests for nested defaults may fail depending on iteration order. Fix any issues in resolution logic.

**Step 4: Fix any ordering issues and verify all pass**

Run: `go test ./internal/transform/ -run TestParseModelConfigs -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/transform/config_test.go internal/transform/testdata/nested/
git commit -m "Add tests for directory-scoped config inheritance"
```

---

### Task 3: Ref Extraction from SQL Files

Parse `{{ ref "model_name" }}` calls from SQL template files to build dependency edges.

**Files:**
- Create: `internal/transform/ref.go`
- Create: `internal/transform/ref_test.go`

**Step 1: Write the failing tests**

Create `internal/transform/ref_test.go`:
```go
package transform

import (
	"testing"
)

func TestExtractRefs(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    []string
	}{
		{
			name: "no refs",
			sql:  "SELECT 1",
			want: nil,
		},
		{
			name: "single ref double quotes",
			sql:  `SELECT * FROM {{ ref "stg_orders" }}`,
			want: []string{"stg_orders"},
		},
		{
			name: "multiple refs",
			sql: `SELECT a.id, b.name
FROM {{ ref "stg_orders" }} a
JOIN {{ ref "dim_customers" }} b ON a.cust_id = b.id`,
			want: []string{"stg_orders", "dim_customers"},
		},
		{
			name: "ref with extra whitespace",
			sql:  `SELECT * FROM {{  ref  "stg_orders"  }}`,
			want: []string{"stg_orders"},
		},
		{
			name: "ref with trimming markers",
			sql:  `SELECT * FROM {{- ref "stg_orders" -}}`,
			want: []string{"stg_orders"},
		},
		{
			name: "deduplicated refs",
			sql: `SELECT * FROM {{ ref "stg_orders" }} a
JOIN {{ ref "stg_orders" }} b ON a.id = b.id`,
			want: []string{"stg_orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractRefs(tt.sql)
			if len(got) != len(tt.want) {
				t.Fatalf("ExtractRefs() = %v, want %v", got, tt.want)
			}
			for i, ref := range got {
				if ref != tt.want[i] {
					t.Errorf("ref[%d] = %q, want %q", i, ref, tt.want[i])
				}
			}
		})
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestExtractRefs -v`
Expected: FAIL — `ExtractRefs` not defined

**Step 3: Write minimal implementation**

Create `internal/transform/ref.go`:
```go
package transform

import (
	"regexp"
)

// refPattern matches {{ ref "model_name" }} with optional whitespace and trim markers.
var refPattern = regexp.MustCompile(`\{\{-?\s*ref\s+"([^"]+)"\s*-?\}\}`)

// ExtractRefs parses a SQL template string and returns all unique model names
// referenced via {{ ref "name" }} calls, in order of first appearance.
func ExtractRefs(sql string) []string {
	matches := refPattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	var refs []string
	for _, m := range matches {
		name := m[1]
		if !seen[name] {
			seen[name] = true
			refs = append(refs, name)
		}
	}
	return refs
}
```

**Step 4: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestExtractRefs -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/transform/ref.go internal/transform/ref_test.go
git commit -m "Add ref extraction from SQL template files"
```

---

### Task 4: DAG Building from Refs

Build a dependency graph from extracted refs, merging with explicit `depends_on` from TOML.

**Files:**
- Create: `internal/transform/dag.go`
- Create: `internal/transform/dag_test.go`

**Step 1: Write the failing tests**

Create `internal/transform/dag_test.go`:
```go
package transform

import (
	"strings"
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func TestBuildDAG_FromRefs(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders": {
			Materialization: "view",
			Schema:          "staging",
			SQLPath:         "testdata/nested/staging/stg_orders.sql",
		},
		"fact_orders": {
			Materialization: "table",
			Schema:          "analytics",
			SQLPath:         "fake", // we'll supply SQL content directly
		},
	}

	// Simulate fact_orders referencing stg_orders
	sqlContents := map[string]string{
		"stg_orders":  "SELECT * FROM raw.orders",
		"fact_orders": `SELECT * FROM {{ ref "stg_orders" }}`,
	}

	dag, err := BuildDAG(models, sqlContents, nil)
	if err != nil {
		t.Fatalf("BuildDAG() error: %v", err)
	}

	deps := dag.DependsOn("fact_orders")
	if len(deps) != 1 || deps[0] != "stg_orders" {
		t.Errorf("fact_orders depends_on = %v, want [stg_orders]", deps)
	}

	deps = dag.DependsOn("stg_orders")
	if len(deps) != 0 {
		t.Errorf("stg_orders depends_on = %v, want []", deps)
	}
}

func TestBuildDAG_MergeExplicitDeps(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders": {
			Materialization: "view",
			Schema:          "staging",
		},
	}

	sqlContents := map[string]string{
		"stg_orders": "SELECT * FROM raw.orders",
	}

	// Explicit dependency from pit.toml on a non-model task
	tasks := []config.TaskConfig{
		{Name: "stg_orders", DependsOn: []string{"extract_raw"}},
	}

	dag, err := BuildDAG(models, sqlContents, tasks)
	if err != nil {
		t.Fatalf("BuildDAG() error: %v", err)
	}

	deps := dag.DependsOn("stg_orders")
	if len(deps) != 1 || deps[0] != "extract_raw" {
		t.Errorf("stg_orders depends_on = %v, want [extract_raw]", deps)
	}
}

func TestBuildDAG_RefToUnknownModel(t *testing.T) {
	models := map[string]*ModelConfig{
		"fact_orders": {Materialization: "table", Schema: "analytics"},
	}
	sqlContents := map[string]string{
		"fact_orders": `SELECT * FROM {{ ref "nonexistent" }}`,
	}

	_, err := BuildDAG(models, sqlContents, nil)
	if err == nil {
		t.Fatal("expected error for ref to unknown model, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error = %q, want it to contain %q", err, "nonexistent")
	}
}

func TestBuildDAG_CycleDetection(t *testing.T) {
	models := map[string]*ModelConfig{
		"a": {Materialization: "view", Schema: "dbo"},
		"b": {Materialization: "view", Schema: "dbo"},
	}
	sqlContents := map[string]string{
		"a": `SELECT * FROM {{ ref "b" }}`,
		"b": `SELECT * FROM {{ ref "a" }}`,
	}

	_, err := BuildDAG(models, sqlContents, nil)
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error = %q, want it to contain %q", err, "cycle")
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestBuildDAG -v`
Expected: FAIL — `BuildDAG` not defined

**Step 3: Write minimal implementation**

Create `internal/transform/dag.go`:
```go
package transform

import (
	"fmt"

	"github.com/druarnfield/pit/internal/config"
)

// ModelDAG represents the dependency graph of transform models.
type ModelDAG struct {
	// edges maps model name → list of models it depends on
	edges map[string][]string
	// order is the topologically sorted list of model names
	order []string
}

// DependsOn returns the dependencies for a given model.
func (d *ModelDAG) DependsOn(model string) []string {
	return d.edges[model]
}

// Order returns models in topological execution order.
func (d *ModelDAG) Order() []string {
	return d.order
}

// BuildDAG constructs a dependency graph from model ref() calls and explicit task dependencies.
// sqlContents maps model name → SQL template content.
// tasks are optional explicit [[tasks]] entries from pit.toml that augment model dependencies.
func BuildDAG(models map[string]*ModelConfig, sqlContents map[string]string, tasks []config.TaskConfig) (*ModelDAG, error) {
	edges := make(map[string][]string)

	// Initialize all models
	for name := range models {
		edges[name] = nil
	}

	// Extract ref-based dependencies
	for name, sql := range sqlContents {
		refs := ExtractRefs(sql)
		for _, ref := range refs {
			if _, ok := models[ref]; !ok {
				return nil, fmt.Errorf("model %q references unknown model %q via ref()", name, ref)
			}
			edges[name] = appendUnique(edges[name], ref)
		}
	}

	// Merge explicit depends_on from pit.toml [[tasks]]
	for _, tc := range tasks {
		if _, isModel := models[tc.Name]; !isModel {
			continue // task doesn't correspond to a model
		}
		for _, dep := range tc.DependsOn {
			edges[tc.Name] = appendUnique(edges[tc.Name], dep)
		}
	}

	// Topological sort with cycle detection (Kahn's algorithm)
	order, err := topoSort(edges)
	if err != nil {
		return nil, err
	}

	return &ModelDAG{edges: edges, order: order}, nil
}

// topoSort performs Kahn's algorithm on the edges map.
// Returns models in execution order or an error if a cycle is detected.
func topoSort(edges map[string][]string) ([]string, error) {
	inDegree := make(map[string]int)
	for node := range edges {
		if _, ok := inDegree[node]; !ok {
			inDegree[node] = 0
		}
		for _, dep := range edges[node] {
			inDegree[dep] += 0 // ensure dep exists in map
		}
	}

	// Count incoming edges
	for _, deps := range edges {
		for _, dep := range deps {
			_ = dep // inDegree is based on who depends on whom
		}
	}

	// Build reverse: for each dependency, track who depends on it
	dependents := make(map[string][]string)
	for node, deps := range edges {
		for _, dep := range deps {
			dependents[dep] = append(dependents[dep], node)
			inDegree[node]++ // node depends on dep, so node has an incoming edge
		}
		if _, ok := inDegree[node]; !ok {
			inDegree[node] = 0
		}
	}

	// Seed queue with zero in-degree nodes
	var queue []string
	for node, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, node)
		}
	}

	var order []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)
		for _, dependent := range dependents[node] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(order) != len(inDegree) {
		return nil, fmt.Errorf("cycle detected in model dependencies")
	}

	return order, nil
}

func appendUnique(slice []string, val string) []string {
	for _, s := range slice {
		if s == val {
			return slice
		}
	}
	return append(slice, val)
}
```

**Step 4: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestBuildDAG -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/transform/dag.go internal/transform/dag_test.go
git commit -m "Add DAG building from ref extraction with cycle detection"
```

---

### Task 5: Template Rendering — Ref & This Resolution

Render `{{ ref "name" }}` into fully qualified table names and `{{ this }}` into the current model's qualified name.

**Files:**
- Create: `internal/transform/render.go`
- Create: `internal/transform/render_test.go`

**Step 1: Write the failing tests**

Create `internal/transform/render_test.go`:
```go
package transform

import (
	"strings"
	"testing"
)

func TestRenderModel_RefResolution(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders":    {Schema: "staging", Materialization: "view"},
		"fact_orders":   {Schema: "analytics", Materialization: "table"},
	}

	sql := `SELECT * FROM {{ ref "stg_orders" }} WHERE 1=1`
	got, err := RenderModel("fact_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() error: %v", err)
	}
	want := `SELECT * FROM [staging].[stg_orders] WHERE 1=1`
	if got != want {
		t.Errorf("RenderModel() =\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderModel_ThisResolution(t *testing.T) {
	models := map[string]*ModelConfig{
		"fact_orders": {Schema: "analytics", Materialization: "incremental"},
	}

	sql := `SELECT * FROM raw.orders WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})`
	got, err := RenderModel("fact_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() error: %v", err)
	}
	want := `SELECT * FROM raw.orders WHERE order_date > (SELECT MAX(order_date) FROM [analytics].[fact_orders])`
	if got != want {
		t.Errorf("RenderModel() =\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderModel_UnknownRef(t *testing.T) {
	models := map[string]*ModelConfig{
		"fact_orders": {Schema: "analytics"},
	}

	sql := `SELECT * FROM {{ ref "nonexistent" }}`
	_, err := RenderModel("fact_orders", sql, models)
	if err == nil {
		t.Fatal("expected error for unknown ref, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error = %q, want it to contain %q", err, "nonexistent")
	}
}

func TestRenderModel_MultipleRefs(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders":    {Schema: "staging"},
		"dim_customers": {Schema: "staging"},
		"fact_orders":   {Schema: "analytics"},
	}

	sql := `SELECT o.*, c.name
FROM {{ ref "stg_orders" }} o
JOIN {{ ref "dim_customers" }} c ON o.cust_id = c.id`

	got, err := RenderModel("fact_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() error: %v", err)
	}
	if !strings.Contains(got, "[staging].[stg_orders]") {
		t.Errorf("expected [staging].[stg_orders] in output, got:\n%s", got)
	}
	if !strings.Contains(got, "[staging].[dim_customers]") {
		t.Errorf("expected [staging].[dim_customers] in output, got:\n%s", got)
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestRenderModel -v`
Expected: FAIL — `RenderModel` not defined

**Step 3: Write minimal implementation**

Create `internal/transform/render.go`:
```go
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
```

**Step 4: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestRenderModel -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/transform/render.go internal/transform/render_test.go
git commit -m "Add template rendering with ref and this resolution"
```

---

### Task 6: Materialization Templates — Embedded Dialect Files

Create the MSSQL materialization templates and embed them via `embed.FS`.

**Files:**
- Create: `internal/transform/dialects/mssql/view.sql`
- Create: `internal/transform/dialects/mssql/table.sql`
- Create: `internal/transform/dialects/mssql/incremental_merge.sql`
- Create: `internal/transform/dialects/mssql/incremental_delete_insert.sql`
- Create: `internal/transform/materialize.go`
- Create: `internal/transform/materialize_test.go`

**Step 1: Write the MSSQL dialect templates**

`internal/transform/dialects/mssql/view.sql`:
```sql
CREATE OR ALTER VIEW {{ .This }} AS
{{ .SQL }}
```

`internal/transform/dialects/mssql/table.sql`:
```sql
BEGIN TRANSACTION;

IF OBJECT_ID('{{ .This }}', 'U') IS NOT NULL
    DROP TABLE {{ .This }};

SELECT *
INTO {{ .This }}
FROM (
{{ .SQL }}
) AS __pit_model;

COMMIT TRANSACTION;
```

`internal/transform/dialects/mssql/incremental_merge.sql`:
```sql
BEGIN TRANSACTION;

MERGE {{ .This }} AS target
USING (
{{ .SQL }}
) AS source
ON {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}target.[{{ $k }}] = source.[{{ $k }}]{{ end }}
WHEN MATCHED THEN
    UPDATE SET {{ range $i, $c := .UpdateColumns }}{{ if $i }}, {{ end }}target.[{{ $c }}] = source.[{{ $c }}]{{ end }}
WHEN NOT MATCHED THEN
    INSERT ({{ range $i, $c := .Columns }}{{ if $i }}, {{ end }}[{{ $c }}]{{ end }})
    VALUES ({{ range $i, $c := .Columns }}{{ if $i }}, {{ end }}source.[{{ $c }}]{{ end }});

COMMIT TRANSACTION;
```

`internal/transform/dialects/mssql/incremental_delete_insert.sql`:
```sql
BEGIN TRANSACTION;

DELETE FROM {{ .This }}
WHERE EXISTS (
    SELECT 1 FROM (
{{ .SQL }}
    ) AS __pit_source
    WHERE {{ range $i, $k := .UniqueKey }}{{ if $i }} AND {{ end }}{{ $.This }}.[{{ $k }}] = __pit_source.[{{ $k }}]{{ end }}
);

INSERT INTO {{ .This }}
{{ .SQL }};

COMMIT TRANSACTION;
```

**Step 2: Write the failing tests**

Create `internal/transform/materialize_test.go`:
```go
package transform

import (
	"strings"
	"testing"
)

func TestMaterialize_View(t *testing.T) {
	ctx := &MaterializeContext{
		ModelName: "stg_orders",
		Schema:    "staging",
		SQL:       "SELECT order_id, amount FROM raw.orders",
		This:      "[staging].[stg_orders]",
	}
	got, err := Materialize("mssql", "view", ctx)
	if err != nil {
		t.Fatalf("Materialize() error: %v", err)
	}
	if !strings.Contains(got, "CREATE OR ALTER VIEW [staging].[stg_orders]") {
		t.Errorf("expected CREATE OR ALTER VIEW, got:\n%s", got)
	}
	if !strings.Contains(got, "SELECT order_id, amount FROM raw.orders") {
		t.Errorf("expected SELECT in output, got:\n%s", got)
	}
}

func TestMaterialize_Table(t *testing.T) {
	ctx := &MaterializeContext{
		ModelName: "dim_customers",
		Schema:    "analytics",
		SQL:       "SELECT customer_id, name FROM raw.customers",
		This:      "[analytics].[dim_customers]",
	}
	got, err := Materialize("mssql", "table", ctx)
	if err != nil {
		t.Fatalf("Materialize() error: %v", err)
	}
	if !strings.Contains(got, "BEGIN TRANSACTION") {
		t.Errorf("expected BEGIN TRANSACTION, got:\n%s", got)
	}
	if !strings.Contains(got, "DROP TABLE [analytics].[dim_customers]") {
		t.Errorf("expected DROP TABLE, got:\n%s", got)
	}
	if !strings.Contains(got, "SELECT *\nINTO [analytics].[dim_customers]") {
		t.Errorf("expected SELECT INTO, got:\n%s", got)
	}
	if !strings.Contains(got, "COMMIT TRANSACTION") {
		t.Errorf("expected COMMIT TRANSACTION, got:\n%s", got)
	}
}

func TestMaterialize_IncrementalMerge(t *testing.T) {
	ctx := &MaterializeContext{
		ModelName:     "fact_orders",
		Schema:        "analytics",
		SQL:           "SELECT order_id, amount FROM raw.orders",
		This:          "[analytics].[fact_orders]",
		UniqueKey:     []string{"order_id"},
		Columns:       []string{"order_id", "amount"},
		UpdateColumns: []string{"amount"},
	}
	got, err := Materialize("mssql", "incremental_merge", ctx)
	if err != nil {
		t.Fatalf("Materialize() error: %v", err)
	}
	if !strings.Contains(got, "MERGE [analytics].[fact_orders] AS target") {
		t.Errorf("expected MERGE, got:\n%s", got)
	}
	if !strings.Contains(got, "target.[order_id] = source.[order_id]") {
		t.Errorf("expected ON clause, got:\n%s", got)
	}
	if !strings.Contains(got, "target.[amount] = source.[amount]") {
		t.Errorf("expected UPDATE SET, got:\n%s", got)
	}
}

func TestMaterialize_IncrementalDeleteInsert(t *testing.T) {
	ctx := &MaterializeContext{
		ModelName: "fact_daily",
		Schema:    "analytics",
		SQL:       "SELECT order_date, total FROM staging.daily",
		This:      "[analytics].[fact_daily]",
		UniqueKey: []string{"order_date"},
	}
	got, err := Materialize("mssql", "incremental_delete_insert", ctx)
	if err != nil {
		t.Fatalf("Materialize() error: %v", err)
	}
	if !strings.Contains(got, "DELETE FROM [analytics].[fact_daily]") {
		t.Errorf("expected DELETE, got:\n%s", got)
	}
	if !strings.Contains(got, "INSERT INTO [analytics].[fact_daily]") {
		t.Errorf("expected INSERT INTO, got:\n%s", got)
	}
}

func TestMaterialize_UnknownDialect(t *testing.T) {
	ctx := &MaterializeContext{This: "[dbo].[x]", SQL: "SELECT 1"}
	_, err := Materialize("mysql", "view", ctx)
	if err == nil {
		t.Fatal("expected error for unknown dialect, got nil")
	}
}

func TestMaterialize_UnknownMaterialization(t *testing.T) {
	ctx := &MaterializeContext{This: "[dbo].[x]", SQL: "SELECT 1"}
	_, err := Materialize("mssql", "snapshot", ctx)
	if err == nil {
		t.Fatal("expected error for unknown materialization, got nil")
	}
}
```

**Step 3: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestMaterialize -v`
Expected: FAIL — `Materialize` not defined

**Step 4: Write minimal implementation**

Create `internal/transform/materialize.go`:
```go
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
	ModelName     string   // "fact_claims"
	Schema        string   // "analytics"
	SQL           string   // rendered SELECT (refs already resolved)
	UniqueKey     []string // ["claim_id"] for incremental
	This          string   // "[analytics].[fact_claims]"
	Columns       []string // all columns from INFORMATION_SCHEMA
	UpdateColumns []string // columns minus unique key (for MERGE UPDATE SET)
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
```

**Step 5: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestMaterialize -v`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/transform/dialects/ internal/transform/materialize.go internal/transform/materialize_test.go
git commit -m "Add materialization templates with embedded MSSQL dialect"
```

---

### Task 7: Compilation Pipeline

Wire everything together: discover models, resolve config, build DAG, render templates, apply materializations, write compiled output.

**Files:**
- Create: `internal/transform/compile.go`
- Create: `internal/transform/compile_test.go`
- Create: `internal/transform/testdata/compilable/pit.toml`
- Create: `internal/transform/testdata/compilable/models/defaults.toml`
- Create: `internal/transform/testdata/compilable/models/stg_orders.sql`
- Create: `internal/transform/testdata/compilable/models/fact_orders.sql`

**Step 1: Write test fixtures**

`testdata/compilable/pit.toml`:
```toml
[dag]
name = "test_transforms"

[dag.sql]
connection = "test_db"

[dag.transform]
dialect = "mssql"
```

`testdata/compilable/models/defaults.toml`:
```toml
[defaults]
materialization = "view"
schema = "staging"

[fact_orders]
materialization = "table"
schema = "analytics"
```

`testdata/compilable/models/stg_orders.sql`:
```sql
SELECT order_id, customer_id, amount
FROM raw.orders
WHERE is_active = 1
```

`testdata/compilable/models/fact_orders.sql`:
```sql
SELECT
    o.order_id,
    o.customer_id,
    o.amount
FROM {{ ref "stg_orders" }} o
```

**Step 2: Write the failing tests**

Create `internal/transform/compile_test.go`:
```go
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

	// Check we got both models
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
	if !strings.Contains(fact.CompiledSQL, "SELECT *\nINTO [analytics].[fact_orders]") {
		t.Errorf("fact_orders expected SELECT INTO, got:\n%s", fact.CompiledSQL)
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
	stgFile := filepath.Join(outDir, "stg_orders.sql")
	if _, err := os.Stat(stgFile); os.IsNotExist(err) {
		t.Errorf("compiled file not written: %s", stgFile)
	}
	factFile := filepath.Join(outDir, "fact_orders.sql")
	if _, err := os.Stat(factFile); os.IsNotExist(err) {
		t.Errorf("compiled file not written: %s", factFile)
	}
}

func TestCompile_MissingMaterialization(t *testing.T) {
	dir := t.TempDir()
	modelsDir := filepath.Join(dir, "models")
	os.MkdirAll(modelsDir, 0o755)
	// Model with no config at all — should fail validation
	os.WriteFile(filepath.Join(modelsDir, "orphan.sql"), []byte("SELECT 1"), 0o644)

	_, err := Compile(modelsDir, "mssql", t.TempDir(), nil)
	if err == nil {
		t.Fatal("expected error for missing materialization, got nil")
	}
	if !strings.Contains(err.Error(), "materialization") {
		t.Errorf("error = %q, want it to contain %q", err, "materialization")
	}
}
```

**Step 3: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run TestCompile -v`
Expected: FAIL — `Compile` not defined

**Step 4: Write minimal implementation**

Create `internal/transform/compile.go`:
```go
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
	Order  []string // topological execution order
}

// Compile discovers models, resolves config, builds the DAG, renders templates,
// applies materializations, and writes compiled SQL to outDir.
func Compile(modelsDir, dialect, outDir string, tasks []config.TaskConfig) (*CompileResult, error) {
	// 1. Discover and resolve model configs
	configs, err := ParseModelConfigs(modelsDir)
	if err != nil {
		return nil, fmt.Errorf("parsing model configs: %w", err)
	}

	// 2. Validate: every model must have a materialization
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
	}

	for _, name := range dag.Order() {
		cfg := configs[name]

		// Skip ephemeral models — they get inlined by RenderModel
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
				strategy = "merge" // default
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
```

**Step 5: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestCompile -v`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/transform/compile.go internal/transform/compile_test.go internal/transform/testdata/compilable/
git commit -m "Add compilation pipeline wiring config, DAG, rendering, and materialization"
```

---

### Task 8: Column Introspection for MERGE

Query `INFORMATION_SCHEMA.COLUMNS` to get column lists for incremental MERGE materializations.

**Files:**
- Create: `internal/transform/introspect.go`
- Create: `internal/transform/introspect_test.go`

**Step 1: Write the failing tests**

Create `internal/transform/introspect_test.go`:
```go
package transform

import (
	"testing"
)

func TestBuildUpdateColumns(t *testing.T) {
	allColumns := []string{"order_id", "customer_id", "amount", "updated_at"}
	uniqueKey := []string{"order_id"}

	got := BuildUpdateColumns(allColumns, uniqueKey)
	want := []string{"customer_id", "amount", "updated_at"}

	if len(got) != len(want) {
		t.Fatalf("BuildUpdateColumns() = %v, want %v", got, want)
	}
	for i, col := range got {
		if col != want[i] {
			t.Errorf("col[%d] = %q, want %q", i, col, want[i])
		}
	}
}

func TestBuildUpdateColumns_CompositeKey(t *testing.T) {
	allColumns := []string{"order_id", "line_id", "amount", "qty"}
	uniqueKey := []string{"order_id", "line_id"}

	got := BuildUpdateColumns(allColumns, uniqueKey)
	want := []string{"amount", "qty"}

	if len(got) != len(want) {
		t.Fatalf("BuildUpdateColumns() = %v, want %v", got, want)
	}
	for i, col := range got {
		if col != want[i] {
			t.Errorf("col[%d] = %q, want %q", i, col, want[i])
		}
	}
}

func TestTableExists_QueryFormat(t *testing.T) {
	// Test that the query builder produces valid SQL
	query := TableExistsQuery("analytics", "fact_orders")
	if query == "" {
		t.Error("TableExistsQuery() returned empty string")
	}
}

func TestColumnsQuery_Format(t *testing.T) {
	query := ColumnsQuery("analytics", "fact_orders")
	if query == "" {
		t.Error("ColumnsQuery() returned empty string")
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/transform/ -run "TestBuildUpdateColumns|TestTableExists|TestColumnsQuery" -v`
Expected: FAIL — functions not defined

**Step 3: Write minimal implementation**

Create `internal/transform/introspect.go`:
```go
package transform

import (
	"context"
	"database/sql"
	"fmt"
)

// TableExistsQuery returns a SQL query that checks if a table exists (MSSQL).
func TableExistsQuery(schema, table string) string {
	return fmt.Sprintf(
		"SELECT CASE WHEN OBJECT_ID('[%s].[%s]', 'U') IS NOT NULL THEN 1 ELSE 0 END",
		schema, table,
	)
}

// ColumnsQuery returns a SQL query to get column names from INFORMATION_SCHEMA (MSSQL).
func ColumnsQuery(schema, table string) string {
	return fmt.Sprintf(
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' ORDER BY ORDINAL_POSITION",
		schema, table,
	)
}

// TableExists checks whether a table exists in the database.
func TableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	var exists int
	err := db.QueryRowContext(ctx, TableExistsQuery(schema, table)).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking table existence [%s].[%s]: %w", schema, table, err)
	}
	return exists == 1, nil
}

// GetColumns retrieves column names for a table from INFORMATION_SCHEMA.
func GetColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, ColumnsQuery(schema, table))
	if err != nil {
		return nil, fmt.Errorf("querying columns for [%s].[%s]: %w", schema, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scanning column name: %w", err)
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// BuildUpdateColumns returns all columns minus the unique key columns.
// These are the columns used in the MERGE UPDATE SET clause.
func BuildUpdateColumns(allColumns, uniqueKey []string) []string {
	keySet := make(map[string]bool, len(uniqueKey))
	for _, k := range uniqueKey {
		keySet[k] = true
	}
	var update []string
	for _, col := range allColumns {
		if !keySet[col] {
			update = append(update, col)
		}
	}
	return update
}
```

**Step 4: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run "TestBuildUpdateColumns|TestTableExists|TestColumnsQuery" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/transform/introspect.go internal/transform/introspect_test.go
git commit -m "Add column introspection for MERGE materialization"
```

---

### Task 9: TransformConfig in ProjectConfig

Add the `[dag.transform]` config field to the existing config package.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go` (or the relevant test file)
- Create: `internal/config/testdata/transform_project/pit.toml`

**Step 1: Write test fixture**

Create `internal/config/testdata/transform_project/pit.toml`:
```toml
[dag]
name = "test_transforms"
schedule = "0 7 * * *"
timeout = "30m"

[dag.sql]
connection = "warehouse_db"

[dag.transform]
dialect = "mssql"
```

**Step 2: Write the failing test**

Add to `internal/config/config_test.go`:
```go
func TestLoad_TransformProject(t *testing.T) {
	cfg, err := Load("testdata/transform_project/pit.toml")
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.DAG.Transform == nil {
		t.Fatal("DAG.Transform is nil, want non-nil")
	}
	if cfg.DAG.Transform.Dialect != "mssql" {
		t.Errorf("Transform.Dialect = %q, want %q", cfg.DAG.Transform.Dialect, "mssql")
	}
	if cfg.DAG.SQL.Connection != "warehouse_db" {
		t.Errorf("SQL.Connection = %q, want %q", cfg.DAG.SQL.Connection, "warehouse_db")
	}
}
```

**Step 3: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestLoad_TransformProject -v`
Expected: FAIL — `Transform` field doesn't exist on `DAGConfig`

**Step 4: Add TransformConfig to config.go**

Add the struct and field to `internal/config/config.go`:

```go
// TransformConfig holds the SQL transform engine configuration.
type TransformConfig struct {
	Dialect string `toml:"dialect"` // e.g. "mssql"
}
```

Add to `DAGConfig`:
```go
Transform *TransformConfig `toml:"transform"`
```

**Step 5: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestLoad_TransformProject -v`
Expected: PASS

**Step 6: Run all existing config tests to check for regressions**

Run: `go test ./internal/config/ -v`
Expected: all PASS

**Step 7: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go internal/config/testdata/transform_project/
git commit -m "Add TransformConfig to DAGConfig for SQL transform projects"
```

---

### Task 10: Validation for Transform Projects

Add validation rules for transform projects in the DAG validation package.

**Files:**
- Modify: `internal/dag/validate.go`
- Modify: `internal/dag/validate_test.go`
- Create: `internal/dag/testdata/transform_valid/pit.toml`
- Create: `internal/dag/testdata/transform_valid/models/defaults.toml`
- Create: `internal/dag/testdata/transform_valid/models/stg_orders.sql`

**Step 1: Write test fixtures**

`testdata/transform_valid/pit.toml`:
```toml
[dag]
name = "test_transforms"

[dag.sql]
connection = "warehouse_db"

[dag.transform]
dialect = "mssql"
```

`testdata/transform_valid/models/defaults.toml`:
```toml
[defaults]
materialization = "view"
schema = "staging"
```

`testdata/transform_valid/models/stg_orders.sql`:
```sql
SELECT order_id FROM raw.orders
```

**Step 2: Write the failing tests**

Add to `internal/dag/validate_test.go`:
```go
func TestValidate_TransformValid(t *testing.T) {
	cfg := loadTestdata(t, "transform_valid")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) != 0 {
		t.Errorf("Validate() returned %d errors, want 0:", len(errs))
		for _, e := range errs {
			t.Errorf("  %s", e)
		}
	}
}

func TestValidate_TransformNoSQL(t *testing.T) {
	cfg := &config.ProjectConfig{}
	cfg.DAG.Name = "test"
	cfg.DAG.Transform = &config.TransformConfig{Dialect: "mssql"}
	// No [dag.sql] connection

	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "connection") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected validation error about missing SQL connection, got: %v", errs)
	}
}

func TestValidate_TransformNoDialect(t *testing.T) {
	cfg := &config.ProjectConfig{}
	cfg.DAG.Name = "test"
	cfg.DAG.SQL.Connection = "test_db"
	cfg.DAG.Transform = &config.TransformConfig{} // empty dialect

	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "dialect") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected validation error about missing dialect, got: %v", errs)
	}
}

func TestValidate_TransformNoModelsDir(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.ProjectConfig{}
	cfg.DAG.Name = "test"
	cfg.DAG.SQL.Connection = "test_db"
	cfg.DAG.Transform = &config.TransformConfig{Dialect: "mssql"}
	// No models/ directory exists

	errs := Validate(cfg, dir)
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "models") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected validation error about missing models directory, got: %v", errs)
	}
}
```

**Step 3: Run tests to verify they fail**

Run: `go test ./internal/dag/ -run TestValidate_Transform -v`
Expected: FAIL — no transform validation logic yet

**Step 4: Add transform validation rules to validate.go**

Add to the `Validate` function in `internal/dag/validate.go` (after existing checks):

```go
// Transform project validation
if cfg.DAG.Transform != nil {
	if cfg.DAG.SQL.Connection == "" {
		errs = append(errs, &ValidationError{
			Field:   "dag.sql.connection",
			Message: "transform projects require a [dag.sql] connection",
		})
	}
	if cfg.DAG.Transform.Dialect == "" {
		errs = append(errs, &ValidationError{
			Field:   "dag.transform.dialect",
			Message: "transform projects require a dialect (e.g. \"mssql\")",
		})
	}
	if !cfg.DAG.IsGitBacked() {
		modelsDir := filepath.Join(projectDir, "models")
		if info, err := os.Stat(modelsDir); err != nil || !info.IsDir() {
			errs = append(errs, &ValidationError{
				Field:   "models/",
				Message: "transform projects require a models/ directory",
			})
		}
	}
}
```

Note: You may need to add `IsGitBacked()` helper if it doesn't exist, or inline the check as `cfg.DAG.GitURL == ""`.

**Step 5: Run tests to verify they pass**

Run: `go test ./internal/dag/ -run TestValidate_Transform -v`
Expected: PASS

**Step 6: Run all DAG tests for regressions**

Run: `go test ./internal/dag/ -v`
Expected: all PASS

**Step 7: Commit**

```bash
git add internal/dag/validate.go internal/dag/validate_test.go internal/dag/testdata/transform_valid/
git commit -m "Add validation rules for transform projects"
```

---

### Task 11: Scaffold — pit init --type transform

Add the `transform` project type to the scaffold package.

**Files:**
- Modify: `internal/scaffold/scaffold.go`
- Modify: `internal/scaffold/scaffold_test.go`

**Step 1: Write the failing test**

Add to `internal/scaffold/scaffold_test.go`:
```go
func TestCreate_Transform(t *testing.T) {
	dir := t.TempDir()
	if err := Create(dir, "my_transforms", TypeTransform); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	// Check pit.toml exists and has [dag.transform]
	pitToml := filepath.Join(dir, "projects", "my_transforms", "pit.toml")
	data, err := os.ReadFile(pitToml)
	if err != nil {
		t.Fatalf("reading pit.toml: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "[dag.transform]") {
		t.Errorf("pit.toml missing [dag.transform] section")
	}
	if !strings.Contains(content, `dialect = "mssql"`) {
		t.Errorf("pit.toml missing dialect setting")
	}

	// Check models/ directory exists
	modelsDir := filepath.Join(dir, "projects", "my_transforms", "models")
	info, err := os.Stat(modelsDir)
	if err != nil || !info.IsDir() {
		t.Errorf("models/ directory not created")
	}

	// Check defaults.toml exists
	defaultsToml := filepath.Join(modelsDir, "defaults.toml")
	if _, err := os.Stat(defaultsToml); err != nil {
		t.Errorf("models/defaults.toml not created")
	}

	// Check sample model exists
	sampleModel := filepath.Join(modelsDir, "example_model.sql")
	if _, err := os.Stat(sampleModel); err != nil {
		t.Errorf("models/example_model.sql not created")
	}
}

func TestValidType_Transform(t *testing.T) {
	if !ValidType("transform") {
		t.Errorf("ValidType(%q) = false, want true", "transform")
	}
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/scaffold/ -run "TestCreate_Transform|TestValidType_Transform" -v`
Expected: FAIL — `TypeTransform` not defined

**Step 3: Add transform scaffold to scaffold.go**

Add `TypeTransform` constant:
```go
TypeTransform ProjectType = "transform"
```

Add to `ValidType` and the scaffold's type switch. Add template functions:

```go
func pitTomlTransform(name string) string {
	return fmt.Sprintf(`[dag]
name = %q
# schedule = "0 7 * * *"
# overlap = "skip"
# timeout = "30m"

[dag.sql]
connection = "warehouse_db"

[dag.transform]
dialect = "mssql"
`, name)
}

func defaultsTomlTransform() string {
	return `[defaults]
materialization = "view"
schema = "dbo"
`
}

func exampleModelSQL() string {
	return `SELECT
    id,
    name,
    created_at
FROM raw.example_table
WHERE is_active = 1
`
}
```

Wire into the `Create` function's type switch to create:
- `projects/{name}/pit.toml` (using `pitTomlTransform`)
- `projects/{name}/models/defaults.toml` (using `defaultsTomlTransform`)
- `projects/{name}/models/example_model.sql` (using `exampleModelSQL`)

**Step 4: Run tests to verify they pass**

Run: `go test ./internal/scaffold/ -run "TestCreate_Transform|TestValidType_Transform" -v`
Expected: PASS

**Step 5: Run all scaffold tests for regressions**

Run: `go test ./internal/scaffold/ -v`
Expected: all PASS

**Step 6: Commit**

```bash
git add internal/scaffold/scaffold.go internal/scaffold/scaffold_test.go
git commit -m "Add transform project type to scaffold"
```

---

### Task 12: CLI — pit compile Command

Add the `pit compile <project>` command that compiles models without executing.

**Files:**
- Create: `internal/cli/compile.go`
- Modify: `internal/cli/root.go` (add compile command)

**Step 1: Write the compile command**

Create `internal/cli/compile.go`:
```go
package cli

import (
	"fmt"
	"path/filepath"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/dag"
	"github.com/druarnfield/pit/internal/transform"
	"github.com/spf13/cobra"
)

func newCompileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "compile [dag]",
		Short: "Compile transform models to SQL without executing",
		Long:  "Renders all models in a transform project, applying materialization templates, and writes the compiled SQL to the compiled_models/ directory.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dagName := args[0]
			projectDir, _ := cmd.Flags().GetString("project-dir")

			configs, err := config.Discover(projectDir)
			if err != nil {
				return fmt.Errorf("discovering projects: %w", err)
			}

			cfg, ok := configs[dagName]
			if !ok {
				return fmt.Errorf("DAG %q not found", dagName)
			}

			if cfg.DAG.Transform == nil {
				return fmt.Errorf("DAG %q is not a transform project (missing [dag.transform])", dagName)
			}

			// Validate
			if errs := dag.Validate(cfg, cfg.Dir()); len(errs) > 0 {
				for _, e := range errs {
					fmt.Fprintf(cmd.ErrOrStderr(), "  %s\n", e)
				}
				return fmt.Errorf("validation failed with %d errors", len(errs))
			}

			modelsDir := filepath.Join(cfg.Dir(), "models")
			outDir := filepath.Join(cfg.Dir(), "compiled_models")

			result, err := transform.Compile(modelsDir, cfg.DAG.Transform.Dialect, outDir, cfg.Tasks)
			if err != nil {
				return fmt.Errorf("compilation failed: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Compiled %d models to %s\n", len(result.Models), outDir)
			for _, name := range result.Order {
				if m, ok := result.Models[name]; ok {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s (%s)\n", name, m.Config.Materialization)
				}
			}

			return nil
		},
	}
}
```

**Step 2: Register in root.go**

Add `rootCmd.AddCommand(newCompileCmd())` alongside the other command registrations.

**Step 3: Run vet and build to verify compilation**

Run: `go vet ./internal/cli/ && go build ./cmd/pit`
Expected: clean build

**Step 4: Commit**

```bash
git add internal/cli/compile.go internal/cli/root.go
git commit -m "Add pit compile command for transform projects"
```

---

### Task 13: Engine Integration

Wire the transform compilation into the engine executor so `pit run` works for transform projects.

**Files:**
- Modify: `internal/engine/executor.go`

**Step 1: Understand the integration point**

In `executor.go`, the `Execute` function builds `TaskInstance` entries from `cfg.Tasks`. For transform projects, we need to:

1. Check if `cfg.DAG.Transform` is non-nil
2. Run `transform.Compile()` to get compiled models and execution order
3. Convert compiled models into `TaskInstance` entries with the compiled SQL as the script content
4. Merge with any explicit `[[tasks]]` from pit.toml
5. Let the existing executor run them via the SQL runner

**Step 2: Add transform integration**

In the `Execute` function, after snapshot creation but before task instance building, add:

```go
// If this is a transform project, compile models and merge into task list
if cfg.DAG.Transform != nil {
	modelsDir := filepath.Join(snapshotDir, "models")
	compiledDir := filepath.Join(snapshotDir, "compiled_models")

	compileResult, err := transform.Compile(modelsDir, cfg.DAG.Transform.Dialect, compiledDir, cfg.Tasks)
	if err != nil {
		run.Status = StatusFailed
		return run, fmt.Errorf("compiling transform models: %w", err)
	}

	// Convert compiled models to task configs
	var modelTasks []config.TaskConfig
	for _, name := range compileResult.Order {
		cm, ok := compileResult.Models[name]
		if !ok {
			continue // ephemeral model
		}
		tc := config.TaskConfig{
			Name:   name,
			Script: filepath.Join("compiled_models", name+".sql"),
			Runner: "sql",
		}
		// Inherit dependencies from the model DAG
		tc.DependsOn = compileResult.DAG.DependsOn(name)

		// Merge with any explicit task config (timeout, retries, etc.)
		for _, explicit := range cfg.Tasks {
			if explicit.Name == name {
				if explicit.Timeout.Duration > 0 {
					tc.Timeout = explicit.Timeout
				}
				if explicit.Retries > 0 {
					tc.Retries = explicit.Retries
				}
				if explicit.RetryDelay.Duration > 0 {
					tc.RetryDelay = explicit.RetryDelay
				}
				// Merge explicit depends_on (non-model deps like Python tasks)
				for _, dep := range explicit.DependsOn {
					tc.DependsOn = appendUnique(tc.DependsOn, dep)
				}
				break
			}
		}

		if cm.Config.Connection != "" {
			tc.Connection = cm.Config.Connection
		}

		modelTasks = append(modelTasks, tc)
	}

	// Add non-model tasks from pit.toml
	for _, tc := range cfg.Tasks {
		isModel := false
		for _, name := range compileResult.Order {
			if tc.Name == name {
				isModel = true
				break
			}
		}
		if !isModel {
			modelTasks = append(modelTasks, tc)
		}
	}

	// Replace cfg.Tasks with merged list
	cfg.Tasks = modelTasks
}
```

Note: The `CompileResult` struct needs to expose the `DAG` field. Update `internal/transform/compile.go` to add `DAG *ModelDAG` to `CompileResult` and set it during compilation.

**Step 3: Run vet and build**

Run: `go vet ./internal/engine/ && go build ./cmd/pit`
Expected: clean build

**Step 4: Run all tests**

Run: `go test -race ./...`
Expected: all PASS

**Step 5: Commit**

```bash
git add internal/engine/executor.go internal/transform/compile.go
git commit -m "Integrate transform compilation into engine executor"
```

---

### Task 14: Ephemeral Model Support

Implement ephemeral models that get inlined as CTEs into downstream models.

**Files:**
- Modify: `internal/transform/render.go`
- Modify: `internal/transform/render_test.go`
- Modify: `internal/transform/compile.go`

**Step 1: Write the failing test**

Add to `internal/transform/render_test.go`:
```go
func TestRenderModel_EphemeralInlining(t *testing.T) {
	models := map[string]*ModelConfig{
		"helper":      {Schema: "staging", Materialization: "ephemeral"},
		"fact_orders": {Schema: "analytics", Materialization: "table"},
	}

	ephemeralSQL := map[string]string{
		"helper": "SELECT order_id, amount * 1.1 AS adjusted FROM raw.orders",
	}

	sql := `SELECT * FROM {{ ref "helper" }}`
	got, err := RenderModelWithEphemerals("fact_orders", sql, models, ephemeralSQL)
	if err != nil {
		t.Fatalf("RenderModelWithEphemerals() error: %v", err)
	}

	// Should contain CTE
	if !strings.Contains(got, "WITH") {
		t.Errorf("expected WITH clause, got:\n%s", got)
	}
	if !strings.Contains(got, "__pit_ephemeral_helper") {
		t.Errorf("expected CTE name __pit_ephemeral_helper, got:\n%s", got)
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/transform/ -run TestRenderModel_Ephemeral -v`
Expected: FAIL

**Step 3: Implement ephemeral inlining**

Add to `render.go`:
```go
// RenderModelWithEphemerals renders a model, inlining any ephemeral refs as CTEs.
// ephemeralSQL maps ephemeral model names to their raw SELECT SQL.
func RenderModelWithEphemerals(modelName, sql string, models map[string]*ModelConfig, ephemeralSQL map[string]string) (string, error) {
	// Collect ephemeral refs
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

	// For ephemeral refs, resolve to CTE name instead of schema.table
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
			currentModel := models[modelName]
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
```

Add `"strings"` to render.go imports.

**Step 4: Update compile.go to use ephemeral-aware rendering**

In the compilation loop, collect ephemeral SQL and call `RenderModelWithEphemerals` instead of `RenderModel`.

**Step 5: Run tests to verify they pass**

Run: `go test ./internal/transform/ -run TestRenderModel -v`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/transform/render.go internal/transform/render_test.go internal/transform/compile.go
git commit -m "Add ephemeral model inlining as CTEs"
```

---

### Task 15: README & Cleanup

Update README with the new transform engine documentation and add `compiled_models/` to `.gitignore`.

**Files:**
- Modify: `README.md`
- Modify: `.gitignore` (if it exists, otherwise the scaffold .gitignore template)

**Step 1: Update README**

Add a new section after "SQL Execution" covering:
- What transform projects are
- Project structure with `models/` and `compiled_models/`
- Model configuration (TOML defaults, per-model overrides)
- Template functions (`ref`, `this`)
- Materializations (view, table, incremental merge, incremental delete+insert, ephemeral)
- `pit compile` command
- `pit init --type transform`
- Example pit.toml for a transform project

**Step 2: Add to .gitignore**

Add `compiled_models/` to any `.gitignore` template in the scaffold and to the project root `.gitignore` if it exists.

**Step 3: Update the roadmap**

Move "SQL transform engine" to implemented (if it was listed) or add it to the implemented features section.

**Step 4: Commit**

```bash
git add README.md .gitignore
git commit -m "Document SQL transform engine in README"
```

---

### Task 16: Final Verification

**Step 1: Run all tests with race detector**

Run: `go test -race ./...`
Expected: all PASS

**Step 2: Run vet**

Run: `go vet ./...`
Expected: clean

**Step 3: Build**

Run: `go build ./cmd/pit`
Expected: clean build

**Step 4: Manual smoke test**

```bash
# Create a transform project
./pit init --type transform smoke_test

# Inspect the generated structure
ls -la projects/smoke_test/
ls -la projects/smoke_test/models/

# Validate
./pit validate

# Compile (will work for the sample model)
./pit compile smoke_test

# Inspect compiled output
cat projects/smoke_test/compiled_models/example_model.sql
```

**Step 5: Clean up smoke test**

```bash
rm -rf projects/smoke_test
```
