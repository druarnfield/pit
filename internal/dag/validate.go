package dag

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/druarnfield/pit/internal/config"
	"github.com/robfig/cron/v3"
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
		if t.Runner == "dbt" {
			// dbt tasks: script is a dbt command, not a file path
			if t.Script == "" {
				errs = append(errs, &ValidationError{
					DAG:     dagName,
					Task:    t.Name,
					Message: "dbt task requires a non-empty script (dbt command, e.g. \"run --select staging\")",
				})
			}
		} else if t.Script != "" {
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

	// Validate schedule as cron expression
	if cfg.DAG.Schedule != "" {
		if _, err := cron.ParseStandard(cfg.DAG.Schedule); err != nil {
			errs = append(errs, &ValidationError{
				DAG:     dagName,
				Message: fmt.Sprintf("invalid schedule %q: %s", cfg.DAG.Schedule, err),
			})
		}
	}

	// Validate FTP watch config
	if cfg.DAG.FTPWatch != nil {
		errs = append(errs, validateFTPWatch(cfg.DAG.FTPWatch, dagName)...)
	}

	// Validate keep_artifacts
	for _, a := range cfg.DAG.KeepArtifacts {
		if !config.ValidArtifacts[a] {
			errs = append(errs, &ValidationError{
				DAG:     dagName,
				Message: fmt.Sprintf("invalid keep_artifacts value %q (must be logs, project, or data)", a),
			})
		}
	}

	// Validate dbt config
	if cfg.DAG.DBT != nil {
		errs = append(errs, validateDBT(cfg.DAG.DBT, dagName, projectDir)...)
	}

	// Cycle detection via Kahn's algorithm
	if cycleErrs := detectCycles(cfg, dagName); len(cycleErrs) > 0 {
		errs = append(errs, cycleErrs...)
	}

	return errs
}

// validateFTPWatch checks required fields and applies defaults for FTP watch config.
func validateFTPWatch(fw *config.FTPWatchConfig, dagName string) []*ValidationError {
	var errs []*ValidationError

	if fw.Host == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "ftp_watch.host is required"})
	}
	if fw.User == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "ftp_watch.user is required"})
	}
	if fw.PasswordSecret == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "ftp_watch.password_secret is required"})
	}
	if fw.Directory == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "ftp_watch.directory is required"})
	}
	if fw.Pattern == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "ftp_watch.pattern is required"})
	}

	// Apply defaults
	if fw.Port == 0 {
		fw.Port = 21
	}
	if fw.PollInterval.Duration == 0 {
		fw.PollInterval.Duration = 30 * 1e9 // 30s in nanoseconds
	}
	if fw.StableSeconds == 0 {
		fw.StableSeconds = 30
	}

	return errs
}

// validateDBT checks required fields for dbt config.
func validateDBT(dbt *config.DBTConfig, dagName string, projectDir string) []*ValidationError {
	var errs []*ValidationError

	if dbt.Version == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "dbt.version is required"})
	}
	if dbt.Adapter == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "dbt.adapter is required"})
	}
	if dbt.ProjectDir == "" {
		errs = append(errs, &ValidationError{DAG: dagName, Message: "dbt.project_dir is required"})
	} else {
		dbtDir := filepath.Join(projectDir, dbt.ProjectDir)
		info, err := os.Stat(dbtDir)
		if err != nil {
			errs = append(errs, &ValidationError{
				DAG:     dagName,
				Message: fmt.Sprintf("dbt.project_dir %q not found", dbt.ProjectDir),
			})
		} else if !info.IsDir() {
			errs = append(errs, &ValidationError{
				DAG:     dagName,
				Message: fmt.Sprintf("dbt.project_dir %q is not a directory", dbt.ProjectDir),
			})
		}
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
