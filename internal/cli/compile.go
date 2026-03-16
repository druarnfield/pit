package cli

import (
	"fmt"
	"path/filepath"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/dag"
	"github.com/druarnfield/pit/internal/transform"
	"github.com/spf13/cobra"
)

func newCompileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "compile [dag]",
		Short: "Compile transform models to SQL without executing",
		Long:  "Renders all models in a transform project, applying materialization templates, and writes the compiled SQL to the compiled_models/ directory.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dagName := args[0]

			configs, err := config.Discover(projectDir)
			if err != nil {
				return fmt.Errorf("discovering projects: %w", err)
			}

			cfg, ok := configs[dagName]
			if !ok {
				return fmt.Errorf("DAG %q not found", dagName)
			}

			if cfg.DAG.Transform == nil {
				return fmt.Errorf("DAG %q is not a transform project (missing [dag.transform])", dagName)
			}

			if errs := dag.Validate(cfg, cfg.Dir()); len(errs) > 0 {
				for _, e := range errs {
					fmt.Fprintf(cmd.ErrOrStderr(), "  %s\n", e)
				}
				return fmt.Errorf("validation failed with %d errors", len(errs))
			}

			modelsDir := filepath.Join(cfg.Dir(), "models")
			outDir := filepath.Join(cfg.Dir(), "compiled_models")

			result, err := transform.Compile(modelsDir, cfg.DAG.Transform.Dialect, outDir, cfg.Tasks)
			if err != nil {
				return fmt.Errorf("compilation failed: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Compiled %d models to %s\n", len(result.Models), outDir)
			for _, name := range result.Order {
				if m, ok := result.Models[name]; ok {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s (%s)\n", name, m.Config.Materialization)
				}
			}

			return nil
		},
	}
}
