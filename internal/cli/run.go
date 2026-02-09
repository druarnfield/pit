package cli

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/dag"
	"github.com/druarnfield/pit/internal/engine"
	"github.com/spf13/cobra"
)

// errRunFailed is returned when a DAG run completes with failed tasks.
// Cobra's error handling in root.go calls os.Exit(1) on any returned error.
var errRunFailed = errors.New("run failed")

func newRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run <dag>[/<task>]",
		Short: "Execute a DAG run",
		Long:  "Run a full DAG or a single task within a DAG. Use dag/task syntax to run a single task.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse dag/task argument
			dagName, taskName, err := parseRunArg(args[0])
			if err != nil {
				return err
			}

			// Discover projects
			configs, err := config.Discover(projectDir)
			if err != nil {
				return err
			}

			cfg, ok := configs[dagName]
			if !ok {
				return fmt.Errorf("DAG %q not found (available: %s)", dagName, availableDAGs(configs))
			}

			// Validate before running
			if errs := dag.Validate(cfg, cfg.Dir()); len(errs) > 0 {
				for _, e := range errs {
					cmd.PrintErrf("ERROR: %s\n", e)
				}
				return fmt.Errorf("validation failed with %d error(s)", len(errs))
			}

			// Set up signal handling for graceful cancellation
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			opts := engine.ExecuteOpts{
				RunsDir:       resolveRunsDir(),
				TaskName:      taskName,
				Verbose:       verbose,
				SecretsPath:   secretsPath,
				DBTDriver:     resolveDBTDriver(),
				KeepArtifacts: resolveKeepArtifacts(cfg.DAG.KeepArtifacts),
			}

			run, err := engine.Execute(ctx, cfg, opts)
			if err != nil {
				return err
			}

			if run.Status == engine.StatusFailed {
				return errRunFailed
			}

			return nil
		},
	}
}

// parseRunArg splits "dag/task" into dag name and optional task name.
// Returns an error for empty dag names or trailing slashes with no task.
func parseRunArg(arg string) (dagName, taskName string, err error) {
	parts := strings.SplitN(arg, "/", 2)
	dagName = parts[0]
	if dagName == "" {
		return "", "", fmt.Errorf("DAG name cannot be empty")
	}
	if len(parts) == 2 {
		taskName = parts[1]
		if taskName == "" {
			return "", "", fmt.Errorf("task name cannot be empty in %q (use just %q to run the full DAG)", arg, dagName)
		}
	}
	return dagName, taskName, nil
}

// availableDAGs returns a sorted comma-separated list of DAG names.
func availableDAGs(configs map[string]*config.ProjectConfig) string {
	names := make([]string, 0, len(configs))
	for name := range configs {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
