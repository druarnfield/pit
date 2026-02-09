package serve

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/dag"
	"github.com/druarnfield/pit/internal/engine"
	pitftp "github.com/druarnfield/pit/internal/ftp"
	"github.com/druarnfield/pit/internal/secrets"
	"github.com/druarnfield/pit/internal/trigger"
)

// Server manages triggers and executes DAGs in response to events.
type Server struct {
	rootDir    string
	configs    map[string]*config.ProjectConfig
	store      *secrets.Store
	triggers   []trigger.Trigger
	ftpConfigs map[string]*config.FTPWatchConfig
	eventCh            chan trigger.Event
	opts               engine.ExecuteOpts
	workspaceArtifacts []string // workspace-level keep_artifacts (nil = use default)

	mu         sync.Mutex
	activeRuns map[string]bool
}

// Options holds workspace-level settings passed from the CLI layer.
type Options struct {
	RunsDir            string
	DBTDriver          string
	WorkspaceArtifacts []string // workspace-level keep_artifacts (nil = use default)
}

// NewServer discovers projects, validates them, and registers triggers.
func NewServer(rootDir, secretsPath string, verbose bool, srvOpts Options) (*Server, error) {
	configs, err := config.Discover(rootDir)
	if err != nil {
		return nil, fmt.Errorf("discovering projects: %w", err)
	}
	if len(configs) == 0 {
		return nil, fmt.Errorf("no projects found in %s/projects/", rootDir)
	}

	// Load secrets if configured
	var store *secrets.Store
	if secretsPath != "" {
		store, err = secrets.Load(secretsPath)
		if err != nil {
			return nil, fmt.Errorf("loading secrets: %w", err)
		}
	}

	s := &Server{
		rootDir:    rootDir,
		configs:    configs,
		store:      store,
		ftpConfigs: make(map[string]*config.FTPWatchConfig),
		eventCh:    make(chan trigger.Event, 64),
		opts: engine.ExecuteOpts{
			RunsDir:     srvOpts.RunsDir,
			Verbose:     verbose,
			SecretsPath: secretsPath,
			DBTDriver:   srvOpts.DBTDriver,
		},
		workspaceArtifacts: srvOpts.WorkspaceArtifacts,
		activeRuns:         make(map[string]bool),
	}

	// Register triggers for each DAG
	for dagName, cfg := range configs {
		// Validate before registering
		if errs := dag.Validate(cfg, cfg.Dir()); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("WARNING: %s", e)
			}
		}

		if cfg.DAG.Schedule != "" {
			ct, err := trigger.NewCronTrigger(dagName, cfg.DAG.Schedule)
			if err != nil {
				return nil, fmt.Errorf("DAG %q: %w", dagName, err)
			}
			s.triggers = append(s.triggers, ct)
		}

		if cfg.DAG.FTPWatch != nil {
			var resolver trigger.SecretsResolver
			if store != nil {
				resolver = store
			}
			ft, err := trigger.NewFTPWatchTrigger(dagName, cfg.DAG.FTPWatch, resolver)
			if err != nil {
				return nil, fmt.Errorf("DAG %q: %w", dagName, err)
			}
			s.triggers = append(s.triggers, ft)
			s.ftpConfigs[dagName] = cfg.DAG.FTPWatch
		}
	}

	if len(s.triggers) == 0 {
		return nil, fmt.Errorf("no triggers registered (set schedule or ftp_watch in at least one DAG)")
	}

	return s, nil
}

// Start launches all triggers and processes events until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	log.Printf("pit serve: %d trigger(s) registered", len(s.triggers))
	for _, t := range s.triggers {
		log.Printf("  %s", t.Name())
	}

	// Launch triggers
	triggerCtx, triggerCancel := context.WithCancel(ctx)
	defer triggerCancel()

	var triggerWg sync.WaitGroup
	for _, t := range s.triggers {
		triggerWg.Add(1)
		go func(trig trigger.Trigger) {
			defer triggerWg.Done()
			if err := trig.Start(triggerCtx, s.eventCh); err != nil {
				log.Printf("trigger %s error: %v", trig.Name(), err)
			}
		}(t)
	}

	// Process events
	var runWg sync.WaitGroup
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-s.eventCh:
				s.handleEvent(ctx, ev, &runWg)
			}
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("pit serve: shutting down...")

	// Cancel triggers and wait
	triggerCancel()
	triggerWg.Wait()

	// Wait for active runs to finish
	runWg.Wait()
	log.Println("pit serve: stopped")
	return nil
}

func (s *Server) handleEvent(ctx context.Context, ev trigger.Event, wg *sync.WaitGroup) {
	cfg, ok := s.configs[ev.DAGName]
	if !ok {
		log.Printf("event for unknown DAG %q, skipping", ev.DAGName)
		return
	}

	// Check overlap policy
	overlap := cfg.DAG.Overlap
	if overlap == "" {
		overlap = "allow"
	}

	s.mu.Lock()
	isActive := s.activeRuns[ev.DAGName]
	if isActive && overlap == "skip" {
		s.mu.Unlock()
		log.Printf("[%s] skipping: DAG already running (overlap=skip)", ev.DAGName)
		return
	}
	s.activeRuns[ev.DAGName] = true
	s.mu.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			s.mu.Lock()
			s.activeRuns[ev.DAGName] = false
			s.mu.Unlock()
		}()

		log.Printf("[%s] triggered by %s", ev.DAGName, ev.Source)

		opts := s.opts

		// Resolve keep_artifacts: per-project > workspace > default
		opts.KeepArtifacts = resolveArtifacts(cfg.DAG.KeepArtifacts, s.workspaceArtifacts)

		// For FTP events, download files to temp dir
		var seedDir string
		if ev.Source == "ftp_watch" && len(ev.Files) > 0 {
			var err error
			seedDir, err = s.downloadFTPFiles(ev)
			if err != nil {
				log.Printf("[%s] FTP download failed: %v", ev.DAGName, err)
				return
			}
			defer os.RemoveAll(seedDir)
			opts.DataSeedDir = seedDir
		}

		run, err := engine.Execute(ctx, cfg, opts)
		if err != nil {
			log.Printf("[%s] execution error: %v", ev.DAGName, err)
			return
		}

		log.Printf("[%s] completed: %s", ev.DAGName, run.Status)

		// Archive FTP files on success
		if ev.Source == "ftp_watch" && run.Status == engine.StatusSuccess {
			if err := s.archiveFTPFiles(ev); err != nil {
				log.Printf("[%s] FTP archive failed: %v", ev.DAGName, err)
			}
		}
	}()
}

func (s *Server) downloadFTPFiles(ev trigger.Event) (string, error) {
	ftpCfg, ok := s.ftpConfigs[ev.DAGName]
	if !ok {
		return "", fmt.Errorf("no FTP config for DAG %q", ev.DAGName)
	}

	password, err := s.store.Resolve(ev.DAGName, ftpCfg.PasswordSecret)
	if err != nil {
		return "", fmt.Errorf("resolving password: %w", err)
	}

	client, err := pitftp.Connect(ftpCfg.Host, ftpCfg.Port, ftpCfg.User, password, ftpCfg.TLS)
	if err != nil {
		return "", err
	}
	defer client.Close()

	tmpDir, err := os.MkdirTemp("", "pit-ftp-*")
	if err != nil {
		return "", fmt.Errorf("creating temp dir: %w", err)
	}

	for _, name := range ev.Files {
		remotePath := filepath.Join(ftpCfg.Directory, name)
		localPath := filepath.Join(tmpDir, name)
		if err := client.Download(remotePath, localPath); err != nil {
			os.RemoveAll(tmpDir)
			return "", fmt.Errorf("downloading %q: %w", name, err)
		}
		log.Printf("[%s] downloaded %s", ev.DAGName, name)
	}

	return tmpDir, nil
}

// resolveArtifacts returns the keep_artifacts list: per-project > workspace > default.
func resolveArtifacts(perProject, workspace []string) []string {
	if len(perProject) > 0 {
		return perProject
	}
	if workspace != nil {
		return workspace
	}
	return config.DefaultKeepArtifacts
}

func (s *Server) archiveFTPFiles(ev trigger.Event) error {
	ftpCfg, ok := s.ftpConfigs[ev.DAGName]
	if !ok || ftpCfg.ArchiveDir == "" {
		return nil
	}

	password, err := s.store.Resolve(ev.DAGName, ftpCfg.PasswordSecret)
	if err != nil {
		return fmt.Errorf("resolving password: %w", err)
	}

	client, err := pitftp.Connect(ftpCfg.Host, ftpCfg.Port, ftpCfg.User, password, ftpCfg.TLS)
	if err != nil {
		return err
	}
	defer client.Close()

	client.MkdirAll(ftpCfg.ArchiveDir)

	for _, name := range ev.Files {
		src := filepath.Join(ftpCfg.Directory, name)
		dst := filepath.Join(ftpCfg.ArchiveDir, name)
		if err := client.Move(src, dst); err != nil {
			return fmt.Errorf("archiving %q: %w", name, err)
		}
		log.Printf("[%s] archived %s → %s", ev.DAGName, name, ftpCfg.ArchiveDir)
	}

	return nil
}
