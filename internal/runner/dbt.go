package runner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/druarnfield/pit/internal/config"
)

// DBTRunner executes dbt commands via uvx.
type DBTRunner struct {
	Config      *config.DBTConfig
	ProfilesDir string
}

// NewDBTRunner creates a DBTRunner from a dbt config and a profiles directory.
func NewDBTRunner(cfg *config.DBTConfig, profilesDir string) *DBTRunner {
	return &DBTRunner{Config: cfg, ProfilesDir: profilesDir}
}

// BuildArgs constructs the uvx command arguments for a dbt invocation.
// The dbtCommand is the raw dbt subcommand string (e.g. "run --select staging").
func (r *DBTRunner) BuildArgs(dbtCommand string) []string {
	args := []string{"--from", fmt.Sprintf("dbt-core==%s", r.Config.Version)}

	// Add adapter as --with
	args = append(args, "--with", r.Config.Adapter)

	// Add extra dependencies
	for _, dep := range r.Config.ExtraDeps {
		args = append(args, "--with", dep)
	}

	// dbt executable + subcommand + args + log format
	args = append(args, "dbt")
	args = append(args, strings.Fields(dbtCommand)...)
	args = append(args, "--log-format", "json")

	return args
}

func (r *DBTRunner) Run(ctx context.Context, rc RunContext, logFile io.Writer) error {
	if r.Config == nil {
		return fmt.Errorf("dbt runner: config is nil")
	}
	if r.Config.Version == "" {
		return fmt.Errorf("dbt runner: version is required")
	}
	if r.Config.Adapter == "" {
		return fmt.Errorf("dbt runner: adapter is required")
	}

	dbtCommand := rc.ScriptPath // for dbt tasks, ScriptPath holds the dbt command string
	args := r.BuildArgs(dbtCommand)

	cmd := exec.CommandContext(ctx, "uvx", args...)
	cmd.Dir = rc.SnapshotDir

	// Set environment with dbt-specific vars
	env := rc.Env
	if r.ProfilesDir != "" {
		env = append(env, "DBT_PROFILES_DIR="+r.ProfilesDir)
	}
	if r.Config.ProjectDir != "" {
		env = append(env, "DBT_PROJECT_DIR="+r.Config.ProjectDir)
	}
	cmd.Env = env

	// Pipe stdout through the JSON log parser, stderr goes direct
	parser := newDBTLogParser(logFile)
	cmd.Stdout = parser
	cmd.Stderr = logFile

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("dbt runner: %w", err)
	}
	return nil
}
