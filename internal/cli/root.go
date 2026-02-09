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

			// Apply secrets_dir from config if CLI flag wasn't explicitly set
			if pitCfg != nil && secretsPath == "" && pitCfg.SecretsDir != "" {
				secretsPath = pitCfg.SecretsDir
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

// Execute runs the root command.
func Execute() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
