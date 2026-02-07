package dag

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/druarnfield/pit/internal/config"
)

// ValidationError represents a single validation problem.
type ValidationError struct {
	DAG     string
	Task    string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Task != "" {
		return fmt.Sprintf("[%s] task %q: %s", e.DAG, e.Task, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.DAG, e.Message)
}

var validOverlap = map[string]bool{
	"":      true,
	"skip":  true,
	"wait":  true,
	"allow": true,
}

// Validate checks a single ProjectConfig for errors.
// projectDir is the directory containing the pit.toml (used to resolve script paths).
func Validate(cfg *config.ProjectConfig, projectDir string) []*ValidationError {
	var errs []*ValidationError
	dagName := cfg.DAG.Name

	// DAG name required
	if dagName == "" {
		errs = append(errs, &ValidationError{DAG: "(unnamed)", Message: "dag.name is required"})
		dagName = "(unnamed)"
	}

	// Valid overlap value
	if !validOverlap[cfg.DAG.Overlap] {
		errs = append(errs, &ValidationError{
			DAG:     dagName,
			Message: fmt.Sprintf("invalid dag.overlap value %q (must be skip, wait, or allow)", cfg.DAG.Overlap),
		})
	}

	// Build task name set and check for duplicates
	taskNames := make(map[string]bool, len(cfg.Tasks))
	for _, t := range cfg.Tasks {
		if t.Name == "" {
			errs = append(errs, &ValidationError{DAG: dagName, Message: "task with empty name"})
			continue
		}
		if taskNames[t.Name] {
			errs = append(errs, &ValidationError{DAG: dagName, Task: t.Name, Message: "duplicate task name"})
		}
		taskNames[t.Name] = true
	}

	// Check depends_on references and script files
	for _, t := range cfg.Tasks {
		if t.Name == "" {
			continue
		}
		for _, dep := range t.DependsOn {
			if !taskNames[dep] {
				errs = append(errs, &ValidationError{
					DAG:     dagName,
					Task:    t.Name,
					Message: fmt.Sprintf("depends_on references unknown task %q", dep),
				})
			}
		}
		if t.Script != "" {
			scriptPath := filepath.Join(projectDir, t.Script)
			if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
				errs = append(errs, &ValidationError{
					DAG:     dagName,
					Task:    t.Name,
					Message: fmt.Sprintf("script %q not found", t.Script),
				})
			}
		}
	}

	// Cycle detection via Kahn's algorithm
	if cycleErrs := detectCycles(cfg, dagName); len(cycleErrs) > 0 {
		errs = append(errs, cycleErrs...)
	}

	return errs
}

// detectCycles uses Kahn's algorithm for topological sort.
// Returns errors if a cycle is found.
func detectCycles(cfg *config.ProjectConfig, dagName string) []*ValidationError {
	// Build adjacency list and in-degree map
	inDegree := make(map[string]int, len(cfg.Tasks))
	dependents := make(map[string][]string, len(cfg.Tasks))

	for _, t := range cfg.Tasks {
		if t.Name == "" {
			continue
		}
		if _, ok := inDegree[t.Name]; !ok {
			inDegree[t.Name] = 0
		}
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], t.Name)
			inDegree[t.Name]++
		}
	}

	// Seed the queue with tasks that have no dependencies
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var sorted int
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		sorted++

		for _, dep := range dependents[node] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if sorted < len(inDegree) {
		// Find the tasks involved in cycles
		var cycleNodes []string
		for name, deg := range inDegree {
			if deg > 0 {
				cycleNodes = append(cycleNodes, name)
			}
		}
		return []*ValidationError{{
			DAG:     dagName,
			Message: fmt.Sprintf("dependency cycle detected involving tasks: %v", cycleNodes),
		}}
	}

	return nil
}

// ValidateAll discovers all projects under rootDir and validates each one.
func ValidateAll(rootDir string) ([]*ValidationError, error) {
	configs, err := config.Discover(rootDir)
	if err != nil {
		return nil, err
	}

	if len(configs) == 0 {
		return nil, fmt.Errorf("no projects found in %s/projects/", rootDir)
	}

	var allErrs []*ValidationError
	for _, cfg := range configs {
		errs := Validate(cfg, cfg.Dir())
		allErrs = append(allErrs, errs...)
	}

	return allErrs, nil
}
