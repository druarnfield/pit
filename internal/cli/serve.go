package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the pit API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("serve: not yet implemented")
			return nil
		},
	}
}
