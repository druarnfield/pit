package runner

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// RunContext holds the information a runner needs to execute a task.
type RunContext struct {
	ScriptPath     string   // absolute path to script in snapshot
	SnapshotDir    string   // runs/{run_id}/project/
	OrigProjectDir string   // original projects/{name}/ (for uv --project)
	Env            []string // full process environment (os.Environ() + PIT_* vars)
}

// ValidateScript checks that ScriptPath is contained within SnapshotDir,
// preventing path traversal attacks (e.g. script = "../../etc/passwd").
func (rc RunContext) ValidateScript() error {
	rel, err := filepath.Rel(rc.SnapshotDir, rc.ScriptPath)
	if err != nil {
		return fmt.Errorf("resolving script path: %w", err)
	}
	if strings.HasPrefix(rel, "..") {
		return fmt.Errorf("script path %q escapes snapshot directory", rc.ScriptPath)
	}
	return nil
}

// Runner executes a task script.
//
// Contract:
//   - Run must respect ctx cancellation and return promptly when ctx is done.
//   - logFile receives combined stdout and stderr from the task process.
//   - Errors returned should wrap the underlying cause for debuggability.
type Runner interface {
	Run(ctx context.Context, rc RunContext, logFile io.Writer) error
}

// Package-level singletons for stateless runners.
var (
	shellRunner  = &ShellRunner{}
	pythonRunner = &PythonRunner{}
	sqlRunner    = &SQLRunner{}
)

// Resolve returns the appropriate Runner for a task based on the runner field
// and script file extension.
//
// Dispatch rules:
//   - If runner is set and starts with "$ ", use CustomRunner with the command after "$ "
//   - If runner is set to "python", "bash", or "sql", use the corresponding runner
//   - If runner is set to anything else, return an error
//   - If runner is unset, dispatch by file extension: .py→Python, .sh→Shell, .sql→SQL
//   - If no extension matches, return an error (no silent fallback)
func Resolve(taskRunner string, scriptPath string) (Runner, error) {
	if taskRunner != "" {
		if strings.HasPrefix(taskRunner, "$ ") {
			cmd := strings.TrimPrefix(taskRunner, "$ ")
			if cmd == "" {
				return nil, fmt.Errorf("custom runner command is empty")
			}
			return &CustomRunner{Command: cmd}, nil
		}
		switch taskRunner {
		case "python":
			return pythonRunner, nil
		case "bash":
			return shellRunner, nil
		case "sql":
			return sqlRunner, nil
		default:
			return nil, fmt.Errorf("unknown runner %q (use python, bash, sql, or $ <command>)", taskRunner)
		}
	}

	ext := filepath.Ext(scriptPath)
	switch ext {
	case ".py":
		return pythonRunner, nil
	case ".sh":
		return shellRunner, nil
	case ".sql":
		return sqlRunner, nil
	default:
		return nil, fmt.Errorf("unsupported script extension %q — set runner explicitly in pit.toml (python, bash, sql, or $ <command>)", ext)
	}
}
