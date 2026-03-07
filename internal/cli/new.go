package cli

import (
	"fmt"

	"github.com/druarnfield/pit/internal/scaffold"
	"github.com/spf13/cobra"
)

func newNewCmd() *cobra.Command {
	var projectType string

	cmd := &cobra.Command{
		Use:   "new <name>",
		Short: "Create a new Pit workspace",
		Long:  "Create a new workspace directory with configuration, a sample project, and git repository.\nUse --type to choose the sample project type: python (default), sql, shell, or dbt.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			pt := scaffold.ProjectType(projectType)

			if !scaffold.ValidType(projectType) {
				return fmt.Errorf("unknown project type %q (must be python, sql, shell, or dbt)", projectType)
			}

			if err := scaffold.CreateWorkspace(".", name, pt); err != nil {
				return err
			}

			fmt.Printf("Created workspace %q with a %s sample project.\n\n", name, projectType)
			fmt.Println("Next steps:")
			fmt.Printf("  cd %s\n", name)
			fmt.Println("  pit validate")
			fmt.Println("  pit run sample_pipeline")
			return nil
		},
	}

	cmd.Flags().StringVar(&projectType, "type", "python", "sample project type: python, sql, shell, or dbt")

	return cmd
}
