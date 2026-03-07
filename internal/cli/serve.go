package cli

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/druarnfield/pit/internal/meta"
	"github.com/druarnfield/pit/internal/serve"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	var port int

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the scheduler (cron, FTP watch, and webhook triggers)",
		Long:  "Start pit in serve mode. Monitors all projects for scheduled triggers, FTP file watches, and inbound webhooks, executing DAGs automatically.",
		RunE: func(cmd *cobra.Command, args []string) error {
			metaStore, err := meta.Open(resolveMetadataDB())
			if err != nil {
				return fmt.Errorf("opening metadata store: %w", err)
			}
			defer metaStore.Close()

			var wsArtifacts []string
			if workspaceCfg != nil {
				wsArtifacts = workspaceCfg.KeepArtifacts
			}
			srv, err := serve.NewServer(projectDir, secretsPath, verbose, serve.Options{
				RunsDir:            resolveRunsDir(),
				RepoCacheDir:       resolveRepoCacheDir(),
				DBTDriver:          resolveDBTDriver(),
				WorkspaceArtifacts: wsArtifacts,
				WebhookPort:        port,
				MetaStore:          metaStore,
				APIToken:           resolveAPIToken(),
			})
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return srv.Start(ctx)
		},
	}

	cmd.Flags().IntVar(&port, "port", 9090, "port for inbound webhook HTTP listener")
	return cmd
}
