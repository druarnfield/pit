package trigger

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/druarnfield/pit/internal/config"
	pitftp "github.com/druarnfield/pit/internal/ftp"
)

// SecretsResolver resolves secrets by project scope.
type SecretsResolver interface {
	Resolve(project, key string) (string, error)
	ResolveField(project, secret, field string) (string, error)
}

// fileState tracks a file's stability during polling.
type fileState struct {
	Size      int64
	FirstSeen time.Time
}

// FTPWatchTrigger polls an FTP server for stable files matching a pattern.
type FTPWatchTrigger struct {
	dagName string
	cfg     *config.FTPWatchConfig
	secrets SecretsResolver
}

// NewFTPWatchTrigger creates an FTP watch trigger.
func NewFTPWatchTrigger(dagName string, cfg *config.FTPWatchConfig, secrets SecretsResolver) (*FTPWatchTrigger, error) {
	if secrets == nil {
		return nil, fmt.Errorf("secrets store required for FTP watch")
	}
	return &FTPWatchTrigger{dagName: dagName, cfg: cfg, secrets: secrets}, nil
}

// Name returns a human-readable identifier for this trigger.
func (ft *FTPWatchTrigger) Name() string {
	return fmt.Sprintf("ftp_watch(%s:%d%s %s) → %s",
		ft.cfg.Host, ft.cfg.Port, ft.cfg.Directory, ft.cfg.Pattern, ft.dagName)
}

// Start begins the poll loop and sends events when stable files are found.
// Blocks until the context is cancelled.
func (ft *FTPWatchTrigger) Start(ctx context.Context, events chan<- Event) error {
	ticker := time.NewTicker(ft.cfg.PollInterval.Duration)
	defer ticker.Stop()

	tracking := make(map[string]fileState)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			ft.poll(ctx, events, tracking)
		}
	}
}

// resolveFTPCredentials resolves host, user, and password for the FTP connection.
// When cfg.Secret is set, all three are pulled from a structured secret.
// Otherwise falls back to legacy cfg.Host / cfg.User / cfg.PasswordSecret fields.
func (ft *FTPWatchTrigger) resolveFTPCredentials() (host, user, password string, err error) {
	if ft.cfg.Secret != "" {
		host, err = ft.secrets.ResolveField(ft.dagName, ft.cfg.Secret, "host")
		if err != nil {
			return "", "", "", fmt.Errorf("resolving %s.host: %w", ft.cfg.Secret, err)
		}
		user, err = ft.secrets.ResolveField(ft.dagName, ft.cfg.Secret, "user")
		if err != nil {
			return "", "", "", fmt.Errorf("resolving %s.user: %w", ft.cfg.Secret, err)
		}
		password, err = ft.secrets.ResolveField(ft.dagName, ft.cfg.Secret, "password")
		if err != nil {
			return "", "", "", fmt.Errorf("resolving %s.password: %w", ft.cfg.Secret, err)
		}
		return host, user, password, nil
	}

	// Legacy: host and user from config, password from plain secret
	password, err = ft.secrets.Resolve(ft.dagName, ft.cfg.PasswordSecret)
	if err != nil {
		return "", "", "", fmt.Errorf("resolving password secret %q: %w", ft.cfg.PasswordSecret, err)
	}
	return ft.cfg.Host, ft.cfg.User, password, nil
}

func (ft *FTPWatchTrigger) poll(ctx context.Context, events chan<- Event, tracking map[string]fileState) {
	host, user, password, err := ft.resolveFTPCredentials()
	if err != nil {
		log.Printf("[ftp_watch] %s: %v", ft.dagName, err)
		return
	}

	client, err := pitftp.Connect(host, ft.cfg.Port, user, password, ft.cfg.TLS)
	if err != nil {
		log.Printf("[ftp_watch] %s: connect: %v", ft.dagName, err)
		return
	}
	defer client.Close()

	files, err := client.List(ft.cfg.Directory, ft.cfg.Pattern)
	if err != nil {
		log.Printf("[ftp_watch] %s: list: %v", ft.dagName, err)
		return
	}

	now := time.Now()
	stableThreshold := time.Duration(ft.cfg.StableSeconds) * time.Second

	// Update tracking map with current files
	seen := make(map[string]bool, len(files))
	for _, f := range files {
		seen[f.Name] = true
		prev, exists := tracking[f.Name]
		if !exists || prev.Size != f.Size {
			// New file or size changed — (re)start stability timer
			tracking[f.Name] = fileState{Size: f.Size, FirstSeen: now}
		}
	}

	// Remove files that disappeared
	for name := range tracking {
		if !seen[name] {
			delete(tracking, name)
		}
	}

	// Find stable files
	stable := FindStableFiles(tracking, stableThreshold, now)
	if len(stable) == 0 {
		return
	}

	// Remove stable files from tracking before sending event
	for _, name := range stable {
		delete(tracking, name)
	}

	select {
	case events <- Event{
		DAGName: ft.dagName,
		Source:  "ftp_watch",
		Files:   stable,
	}:
	case <-ctx.Done():
	}
}

// FindStableFiles returns filenames that have been stable for at least the threshold duration.
// Exported for testability.
func FindStableFiles(tracking map[string]fileState, threshold time.Duration, now time.Time) []string {
	var stable []string
	for name, state := range tracking {
		if now.Sub(state.FirstSeen) >= threshold {
			stable = append(stable, name)
		}
	}
	return stable
}
