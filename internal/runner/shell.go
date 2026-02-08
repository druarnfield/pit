package runner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

// ShellRunner executes scripts using bash.
type ShellRunner struct{}

func (r *ShellRunner) Run(ctx context.Context, rc RunContext, logFile io.Writer) error {
	cmd := exec.CommandContext(ctx, "bash", rc.ScriptPath)
	cmd.Dir = rc.SnapshotDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = rc.Env
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("shell runner %s: %w", rc.ScriptPath, err)
	}
	return nil
}
