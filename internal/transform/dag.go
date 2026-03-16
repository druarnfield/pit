package transform

import (
	"fmt"
	"sort"

	"github.com/druarnfield/pit/internal/config"
)

// ModelDAG represents the dependency graph of transform models.
type ModelDAG struct {
	edges map[string][]string // model name -> list of models it depends on
	order []string            // topologically sorted model names
}

// DependsOn returns the list of models that the given model depends on.
func (d *ModelDAG) DependsOn(model string) []string { return d.edges[model] }

// Order returns the topologically sorted list of model names.
func (d *ModelDAG) Order() []string { return d.order }

// BuildDAG constructs a dependency graph from model ref() calls and explicit task dependencies.
func BuildDAG(models map[string]*ModelConfig, sqlContents map[string]string, tasks []config.TaskConfig) (*ModelDAG, error) {
	edges := make(map[string][]string)
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

	// Merge explicit depends_on from pit.toml [[tasks]].
	// Only add deps that are themselves models; non-model deps (e.g. seed
	// loaders) control task ordering in the engine but must not enter the
	// model DAG or they will appear in Order() without a ModelConfig.
	for _, tc := range tasks {
		if _, isModel := models[tc.Name]; !isModel {
			continue
		}
		for _, dep := range tc.DependsOn {
			if _, isModel := models[dep]; isModel {
				edges[tc.Name] = appendUnique(edges[tc.Name], dep)
			}
		}
	}

	// Topological sort with cycle detection (Kahn's algorithm)
	order, err := topoSort(edges)
	if err != nil {
		return nil, err
	}

	return &ModelDAG{edges: edges, order: order}, nil
}

// topoSort performs a topological sort using Kahn's algorithm.
// Returns an error if a cycle is detected.
func topoSort(edges map[string][]string) ([]string, error) {
	inDegree := make(map[string]int)
	dependents := make(map[string][]string)

	for node := range edges {
		if _, ok := inDegree[node]; !ok {
			inDegree[node] = 0
		}
		for _, dep := range edges[node] {
			dependents[dep] = append(dependents[dep], node)
			inDegree[node]++
			if _, ok := inDegree[dep]; !ok {
				inDegree[dep] = 0
			}
		}
	}

	// Seed queue with zero in-degree nodes, sorted for deterministic output
	var queue []string
	for node, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, node)
		}
	}
	sort.Strings(queue)

	var order []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		// Sort dependents for deterministic ordering
		deps := dependents[node]
		sort.Strings(deps)
		for _, dep := range deps {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
				sort.Strings(queue)
			}
		}
	}

	if len(order) != len(inDegree) {
		return nil, fmt.Errorf("cycle detected in model dependencies")
	}

	return order, nil
}

// appendUnique appends val to slice if not already present.
func appendUnique(slice []string, val string) []string {
	for _, s := range slice {
		if s == val {
			return slice
		}
	}
	return append(slice, val)
}
