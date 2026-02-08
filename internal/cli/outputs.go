package cli

import (
	"fmt"
	"io"
	"path/filepath"
	"sort"

	"github.com/druarnfield/pit/internal/config"
	"github.com/spf13/cobra"
)

// outputRow holds a single row for the outputs table display.
type outputRow struct {
	Project  string
	Name     string
	Type     string
	Location string
}

func newOutputsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "outputs",
		Short: "List pipeline outputs",
		RunE: func(cmd *cobra.Command, args []string) error {
			projectFilter, _ := cmd.Flags().GetString("project")
			typeFilter, _ := cmd.Flags().GetString("type")
			locationFilter, _ := cmd.Flags().GetString("location")

			configs, err := config.Discover(projectDir)
			if err != nil {
				return err
			}

			rows := collectOutputs(configs, projectFilter, typeFilter, locationFilter)
			if len(rows) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no outputs found")
				return nil
			}

			printOutputTable(cmd.OutOrStdout(), rows)
			return nil
		},
	}

	cmd.Flags().String("project", "", "filter by project name")
	cmd.Flags().String("type", "", "filter by output type")
	cmd.Flags().String("location", "", "filter by output location (glob pattern)")

	return cmd
}

// collectOutputs gathers output rows from configs, applying optional filters.
// Results are sorted by project then name.
func collectOutputs(configs map[string]*config.ProjectConfig, projectFilter, typeFilter, locationFilter string) []outputRow {
	var rows []outputRow

	for dagName, cfg := range configs {
		if projectFilter != "" && dagName != projectFilter {
			continue
		}
		for _, out := range cfg.Outputs {
			if typeFilter != "" && out.Type != typeFilter {
				continue
			}
			if locationFilter != "" {
				matched, err := filepath.Match(locationFilter, out.Location)
				if err != nil || !matched {
					continue
				}
			}
			rows = append(rows, outputRow{
				Project:  dagName,
				Name:     out.Name,
				Type:     out.Type,
				Location: out.Location,
			})
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Project != rows[j].Project {
			return rows[i].Project < rows[j].Project
		}
		return rows[i].Name < rows[j].Name
	})

	return rows
}

// printOutputTable writes a formatted table of output rows to w with dynamic column widths.
func printOutputTable(w io.Writer, rows []outputRow) {
	// Calculate column widths
	pW, nW, tW, lW := len("PROJECT"), len("NAME"), len("TYPE"), len("LOCATION")
	for _, r := range rows {
		if len(r.Project) > pW {
			pW = len(r.Project)
		}
		if len(r.Name) > nW {
			nW = len(r.Name)
		}
		if len(r.Type) > tW {
			tW = len(r.Type)
		}
		if len(r.Location) > lW {
			lW = len(r.Location)
		}
	}

	fmtStr := fmt.Sprintf("  %%-%ds  %%-%ds  %%-%ds  %%s\n", pW, nW, tW)

	// Header
	fmt.Fprintf(w, fmtStr, "PROJECT", "NAME", "TYPE", "LOCATION")

	// Separator
	fmt.Fprintf(w, fmtStr, dashes(pW), dashes(nW), dashes(tW), dashes(lW))

	// Rows
	for _, r := range rows {
		fmt.Fprintf(w, fmtStr, r.Project, r.Name, r.Type, r.Location)
	}
}

// dashes returns a string of n dashes.
func dashes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '-'
	}
	return string(b)
}
