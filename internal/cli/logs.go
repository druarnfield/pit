package cli

import (
	"fmt"
	"path/filepath"

	"github.com/druarnfield/pit/internal/engine"
	"github.com/spf13/cobra"
)

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <dag>[/<task>]",
		Short: "View pipeline logs",
		Long:  "View task logs from DAG runs. Use dag/task syntax to view a single task's log.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			listMode, _ := cmd.Flags().GetBool("list")
			runID, _ := cmd.Flags().GetString("run-id")

			dagName, taskName, err := parseRunArg(args[0])
			if err != nil {
				return err
			}

			runsDir := filepath.Join(projectDir, "runs")
			w := cmd.OutOrStdout()

			// --list mode: show available runs
			if listMode {
				runs, err := engine.DiscoverRuns(runsDir, dagName)
				if err != nil {
					return err
				}
				if len(runs) == 0 {
					fmt.Fprintf(w, "no runs found for DAG %q\n", dagName)
					return nil
				}

				fmt.Fprintf(w, "  %-40s  %s\n", "RUN ID", "TIMESTAMP")
				fmt.Fprintf(w, "  %-40s  %s\n", "------", "---------")
				for _, r := range runs {
					fmt.Fprintf(w, "  %-40s  %s\n", r.ID, r.Timestamp.Format("2006-01-02 15:04:05"))
				}
				return nil
			}

			// Find the target run
			var logDir string
			if runID != "" {
				// Validate run ID belongs to requested DAG
				runDAG, err := engine.DAGNameFromRunID(runID)
				if err != nil {
					return err
				}
				if runDAG != dagName {
					return fmt.Errorf("run %q belongs to DAG %q, not %q", runID, runDAG, dagName)
				}

				runDir := filepath.Join(runsDir, runID)
				logDir = filepath.Join(runDir, "logs")
			} else {
				// Use latest run
				runs, err := engine.DiscoverRuns(runsDir, dagName)
				if err != nil {
					return err
				}
				if len(runs) == 0 {
					fmt.Fprintf(w, "no runs found for DAG %q\n", dagName)
					return nil
				}
				logDir = runs[0].LogDir
			}

			// Read and display logs
			if taskName != "" {
				data, err := engine.ReadTaskLog(logDir, taskName)
				if err != nil {
					return err
				}
				w.Write(data)
			} else {
				if err := engine.ReadAllTaskLogs(logDir, w); err != nil {
					return err
				}
			}

			return nil
		},
	}

	cmd.Flags().Bool("list", false, "list available runs")
	cmd.Flags().String("run-id", "", "show logs from a specific run")

	return cmd
}
