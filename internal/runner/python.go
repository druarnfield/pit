package runner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

// PythonRunner executes Python scripts using uv run.
// It points --project at the original project directory so uv resolves
// the pyproject.toml and virtualenv from there, not from the snapshot.
type PythonRunner struct{}

func (r *PythonRunner) Run(ctx context.Context, rc RunContext, logFile io.Writer) error {
	cmd := exec.CommandContext(ctx, "uv", "run", "--project", rc.OrigProjectDir, rc.ScriptPath)
	cmd.Dir = rc.SnapshotDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = rc.Env
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python runner %s: %w", rc.ScriptPath, err)
	}
	return nil
}
