package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newSyncCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sync",
		Short: "Sync project environments",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("sync: not yet implemented")
			return nil
		},
	}
}
