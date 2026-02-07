package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newOutputsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "outputs",
		Short: "List pipeline outputs",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("outputs: not yet implemented")
			return nil
		},
	}

	cmd.Flags().String("type", "", "filter by output type")
	cmd.Flags().String("location", "", "filter by output location")

	return cmd
}
