package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newLogsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "logs",
		Short: "View pipeline logs",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("logs: not yet implemented")
			return nil
		},
	}
}
