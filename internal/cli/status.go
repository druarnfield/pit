package cli

import (
	"fmt"
	"time"

	"github.com/druarnfield/pit/internal/meta"
	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show pipeline status",
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := meta.Open(resolveMetadataDB())
			if err != nil {
				return fmt.Errorf("opening metadata store: %w", err)
			}
			defer store.Close()

			runs, err := store.LatestRunPerDAG()
			if err != nil {
				return fmt.Errorf("querying status: %w", err)
			}

			if len(runs) == 0 {
				fmt.Println("No runs recorded yet.")
				return nil
			}

			fmt.Printf("%-20s %-21s %-8s %s\n", "DAG", "Last Run", "Status", "Duration")
			fmt.Printf("%-20s %-21s %-8s %s\n", "───", "────────", "──────", "────────")

			for _, r := range runs {
				var duration string
				if r.EndedAt != nil {
					duration = r.EndedAt.Sub(r.StartedAt).Round(time.Second).String()
				} else {
					duration = "running"
				}
				fmt.Printf("%-20s %-21s %-8s %s\n",
					r.DAGName,
					r.StartedAt.Local().Format("2006-01-02 15:04:05"),
					r.Status,
					duration,
				)
			}
			return nil
		},
	}
}
