package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run [dag]",
		Short: "Execute a DAG run",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("run: not yet implemented")
			return nil
		},
	}
}
