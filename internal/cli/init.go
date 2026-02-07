package cli

import (
	"fmt"

	"github.com/druarnfield/pit/internal/scaffold"
	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	var projectType string

	cmd := &cobra.Command{
		Use:   "init <name>",
		Short: "Scaffold a new pipeline project",
		Long:  "Create a new project directory with pit.toml and sample tasks.\nUse --type to choose the project type: python (default), sql, or shell.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			pt := scaffold.ProjectType(projectType)

			if !scaffold.ValidType(projectType) {
				return fmt.Errorf("unknown project type %q (must be python, sql, or shell)", projectType)
			}

			if err := scaffold.Create(projectDir, name, pt); err != nil {
				return err
			}

			fmt.Printf("Created %s project %q in projects/%s/\n", projectType, name, name)
			fmt.Println("\nNext steps:")
			fmt.Printf("  1. Edit projects/%s/pit.toml to configure your DAG\n", name)
			fmt.Printf("  2. Add task scripts to projects/%s/tasks/\n", name)
			fmt.Println("  3. Run `pit validate` to check your configuration")
			return nil
		},
	}

	cmd.Flags().StringVar(&projectType, "type", "python", "project type: python, sql, or shell")

	return cmd
}
