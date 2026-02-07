package cli

import (
	"fmt"
	"os"

	"github.com/druarnfield/pit/internal/dag"
	"github.com/spf13/cobra"
)

func newValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate all project configurations",
		Long:  "Parse all pit.toml files under projects/, check for errors, and detect dependency cycles.",
		RunE: func(cmd *cobra.Command, args []string) error {
			errs, err := dag.ValidateAll(projectDir)
			if err != nil {
				return err
			}

			if len(errs) == 0 {
				fmt.Println("All projects validated successfully.")
				return nil
			}

			for _, e := range errs {
				fmt.Fprintf(os.Stderr, "ERROR: %s\n", e)
			}
			return fmt.Errorf("validation found %d error(s)", len(errs))
		},
	}
}
