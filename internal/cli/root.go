package cli

import (
	"fmt"
	"os"

	"github.com/druarnfield/pit/internal/config"
	"github.com/spf13/cobra"
)

var (
	projectDir  string
	verbose     bool
	secretsPath string

	// Workspace config — populated in PersistentPreRunE, nil if no pit_config.toml
	workspaceCfg *config.PitConfig
)

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "pit",
		Short: "Lightweight data pipeline orchestrator",
		Long:  "Pit is a lightweight data orchestration tool that manages DAGs of Python tasks via UV.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Load workspace-level config if it exists
			pitCfg, err := config.LoadPitConfig(projectDir)
			if err != nil {
				return fmt.Errorf("loading pit_config.toml: %w", err)
			}
			workspaceCfg = pitCfg

			if pitCfg != nil {
				// Apply secrets_dir from config if CLI flag wasn't explicitly set
				if secretsPath == "" && pitCfg.SecretsDir != "" {
					secretsPath = pitCfg.SecretsDir
				}
			}

			return nil
		},
	}

	root.PersistentFlags().StringVar(&projectDir, "project-dir", ".", "root project directory")
	root.PersistentFlags().BoolVar(&verbose, "verbose", false, "enable verbose output")
	root.PersistentFlags().StringVar(&secretsPath, "secrets", "", "path to secrets file")

	root.AddCommand(
		newValidateCmd(),
		newInitCmd(),
		newRunCmd(),
		newSyncCmd(),
		newStatusCmd(),
		newOutputsCmd(),
		newLogsCmd(),
		newServeCmd(),
	)

	return root
}

// resolveRunsDir returns the runs directory from workspace config or the default.
func resolveRunsDir() string {
	if workspaceCfg != nil && workspaceCfg.RunsDir != "" {
		return workspaceCfg.RunsDir
	}
	return "runs"
}

// resolveDBTDriver returns the dbt ODBC driver from workspace config or the default.
func resolveDBTDriver() string {
	if workspaceCfg != nil && workspaceCfg.DBTDriver != "" {
		return workspaceCfg.DBTDriver
	}
	return config.DefaultDBTDriver
}

// resolveKeepArtifacts returns the keep_artifacts list, resolving per-project > workspace > default.
func resolveKeepArtifacts(perProject []string) []string {
	if len(perProject) > 0 {
		return perProject
	}
	if workspaceCfg != nil && workspaceCfg.KeepArtifacts != nil {
		return workspaceCfg.KeepArtifacts
	}
	return config.DefaultKeepArtifacts
}

// Execute runs the root command.
func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
