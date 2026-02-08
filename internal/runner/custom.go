package runner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// CustomRunner executes scripts using a user-specified command.
// The command string (from "$ <command>") is split on whitespace and the
// script path is appended as the final argument.
//
// This is a trust boundary: the user controls the command via pit.toml.
// The command is executed as-is without sandboxing.
type CustomRunner struct {
	Command string
}

func (r *CustomRunner) Run(ctx context.Context, rc RunContext, logFile io.Writer) error {
	parts := strings.Fields(r.Command)
	// Three-index slice prevents append from mutating the backing array of parts.
	args := append(parts[1:len(parts):len(parts)], rc.ScriptPath)

	// Validate binary exists on PATH for a clearer error message.
	if _, err := exec.LookPath(parts[0]); err != nil {
		return fmt.Errorf("custom runner: command %q not found: %w", parts[0], err)
	}

	cmd := exec.CommandContext(ctx, parts[0], args...)
	cmd.Dir = rc.SnapshotDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Env = rc.Env
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("custom runner %q %s: %w", r.Command, rc.ScriptPath, err)
	}
	return nil
}
