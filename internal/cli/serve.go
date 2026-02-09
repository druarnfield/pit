package cli

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/druarnfield/pit/internal/serve"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the scheduler (cron and FTP watch triggers)",
		Long:  "Start pit in serve mode. Monitors all projects for scheduled triggers and FTP file watches, executing DAGs automatically.",
		RunE: func(cmd *cobra.Command, args []string) error {
			var wsArtifacts []string
			if workspaceCfg != nil {
				wsArtifacts = workspaceCfg.KeepArtifacts
			}
			srv, err := serve.NewServer(projectDir, secretsPath, verbose, serve.Options{
				RunsDir:            resolveRunsDir(),
				DBTDriver:          resolveDBTDriver(),
				WorkspaceArtifacts: wsArtifacts,
			})
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return srv.Start(ctx)
		},
	}
}
