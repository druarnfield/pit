package cli

import (
	"fmt"

	"github.com/druarnfield/pit/internal/scaffold"
	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init <name>",
		Short: "Scaffold a new pipeline project",
		Long:  "Create a new project directory with pit.toml, pyproject.toml, and sample task.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := scaffold.Create(projectDir, name); err != nil {
				return err
			}

			fmt.Printf("Created project %q in projects/%s/\n", name, name)
			fmt.Println("\nNext steps:")
			fmt.Printf("  1. Edit projects/%s/pit.toml to configure your DAG\n", name)
			fmt.Printf("  2. Add task scripts to projects/%s/tasks/\n", name)
			fmt.Println("  3. Run `pit validate` to check your configuration")
			return nil
		},
	}
}
