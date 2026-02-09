package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
)

// Duration wraps time.Duration for TOML unmarshalling.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(text), err)
	}
	return nil
}

// ProjectConfig is the top-level structure parsed from a pit.toml file.
type ProjectConfig struct {
	DAG     DAGConfig    `toml:"dag"`
	Tasks   []TaskConfig `toml:"tasks"`
	Outputs []Output     `toml:"outputs"`
	path    string       // unexported: filesystem path of the pit.toml
}

// Path returns the filesystem path this config was loaded from.
func (p *ProjectConfig) Path() string {
	return p.path
}

// Dir returns the directory containing this config file.
func (p *ProjectConfig) Dir() string {
	return filepath.Dir(p.path)
}

// DAGConfig holds the DAG-level settings.
type DAGConfig struct {
	Name     string          `toml:"name"`
	Schedule string          `toml:"schedule"`
	Overlap  string          `toml:"overlap"`
	Timeout  Duration        `toml:"timeout"`
	Requires []string        `toml:"requires"`
	SQL      SQLConfig       `toml:"sql"`
	FTPWatch *FTPWatchConfig `toml:"ftp_watch"`
	DBT      *DBTConfig      `toml:"dbt"`
}

// DBTConfig holds the dbt project configuration for a DAG.
type DBTConfig struct {
	Version    string   `toml:"version"`     // dbt-core version, e.g. "1.9.1"
	Adapter    string   `toml:"adapter"`     // pip name, e.g. "dbt-sqlserver"
	ExtraDeps  []string `toml:"extra_deps"`  // additional pip packages
	ProjectDir string   `toml:"project_dir"` // relative path to dbt project root
	Profile    string   `toml:"profile"`     // profile name (default: dag name)
	Target     string   `toml:"target"`      // target name (default: "prod")
}

// FTPWatchConfig defines an FTP file watch trigger for a DAG.
type FTPWatchConfig struct {
	Host           string   `toml:"host"`
	Port           int      `toml:"port"`
	User           string   `toml:"user"`
	PasswordSecret string   `toml:"password_secret"`
	TLS            bool     `toml:"tls"`
	Directory      string   `toml:"directory"`
	Pattern        string   `toml:"pattern"`
	ArchiveDir     string   `toml:"archive_dir"`
	PollInterval   Duration `toml:"poll_interval"`
	StableSeconds  int      `toml:"stable_seconds"`
}

// SQLConfig holds the default SQL connection for a project's .sql tasks.
type SQLConfig struct {
	Connection string `toml:"connection"`
}

// TaskConfig holds a single task definition.
type TaskConfig struct {
	Name       string   `toml:"name"`
	Script     string   `toml:"script"`
	Runner     string   `toml:"runner"`
	DependsOn  []string `toml:"depends_on"`
	Timeout    Duration `toml:"timeout"`
	Retries    int      `toml:"retries"`
	RetryDelay Duration `toml:"retry_delay"`
}

// Output defines a DAG output artifact.
type Output struct {
	Name       string `toml:"name"`
	Type       string `toml:"type"`
	Location   string `toml:"location"`
	Recipients string `toml:"recipients"`
}

// Load parses a single pit.toml file and returns a ProjectConfig.
func Load(path string) (*ProjectConfig, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolving path %q: %w", path, err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", absPath, err)
	}

	var cfg ProjectConfig
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing %q: %w", absPath, err)
	}

	cfg.path = absPath
	return &cfg, nil
}

// Discover finds all pit.toml files under rootDir/projects/*/pit.toml
// and returns them keyed by DAG name.
func Discover(rootDir string) (map[string]*ProjectConfig, error) {
	pattern := filepath.Join(rootDir, "projects", "*", "pit.toml")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("globbing %q: %w", pattern, err)
	}

	configs := make(map[string]*ProjectConfig, len(matches))
	for _, match := range matches {
		cfg, err := Load(match)
		if err != nil {
			return nil, err
		}
		if cfg.DAG.Name == "" {
			cfg.DAG.Name = filepath.Base(filepath.Dir(match))
		}
		if _, exists := configs[cfg.DAG.Name]; exists {
			return nil, fmt.Errorf("duplicate DAG name %q", cfg.DAG.Name)
		}
		configs[cfg.DAG.Name] = cfg
	}

	return configs, nil
}
