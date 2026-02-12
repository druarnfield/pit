package runner

import (
	"fmt"
	"os"
	"strconv"
	"text/template"

	"github.com/druarnfield/pit/internal/config"
)

var profilesTmpl = template.Must(template.New("profiles").Parse(`{{ .ProfileName }}:
  target: {{ .Target }}
  outputs:
    {{ .Target }}:
      type: sqlserver
      driver: "{{ .Driver }}"
      server: "{{ .Host }}"
      threads: {{ .Threads }}
      port: {{ .Port }}
      database: "{{ .Database }}"
      schema: "{{ .Schema }}"
      user: "{{ .User }}"
      password: "{{ .Password }}"
      encrypt: true
      trust_cert: true
`))

type profileData struct {
	ProfileName string
	Target      string
	Driver      string
	Host        string
	Port        int
	Database    string
	Schema      string
	User        string
	Password    string
	Threads     string
}

// GenerateProfiles creates a temporary directory containing a profiles.yml
// for dbt, populated from a structured secret. The connection parameter names
// the structured secret whose fields (host, port, database, schema, user,
// password) are used to generate the profile.
//
// Returns the directory path and a cleanup function that removes the temp directory.
func GenerateProfiles(cfg *DBTProfilesInput, resolver SecretsResolver) (string, func(), error) {
	noop := func() {}

	if resolver == nil {
		return "", noop, fmt.Errorf("secrets resolver is required for dbt profiles generation")
	}
	if cfg.Connection == "" {
		return "", noop, fmt.Errorf("dbt connection secret name is required (set connection in [dag.dbt])")
	}

	// Resolve required fields from the structured secret
	host, err := resolver.ResolveField(cfg.DAGName, cfg.Connection, "host")
	if err != nil {
		return "", noop, fmt.Errorf("resolving %s.host: %w", cfg.Connection, err)
	}
	portStr, err := resolver.ResolveField(cfg.DAGName, cfg.Connection, "port")
	if err != nil {
		return "", noop, fmt.Errorf("resolving %s.port: %w", cfg.Connection, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", noop, fmt.Errorf("%s.port %q is not a valid integer: %w", cfg.Connection, portStr, err)
	}
	database, err := resolver.ResolveField(cfg.DAGName, cfg.Connection, "database")
	if err != nil {
		return "", noop, fmt.Errorf("resolving %s.database: %w", cfg.Connection, err)
	}
	schema, err := resolver.ResolveField(cfg.DAGName, cfg.Connection, "schema")
	if err != nil {
		return "", noop, fmt.Errorf("resolving %s.schema: %w", cfg.Connection, err)
	}
	user, err := resolver.ResolveField(cfg.DAGName, cfg.Connection, "user")
	if err != nil {
		return "", noop, fmt.Errorf("resolving %s.user: %w", cfg.Connection, err)
	}
	password, err := resolver.ResolveField(cfg.DAGName, cfg.Connection, "password")
	if err != nil {
		return "", noop, fmt.Errorf("resolving %s.password: %w", cfg.Connection, err)
	}

	// Create temp directory for profiles.yml
	tmpDir, err := os.MkdirTemp("", "pit-dbt-profiles-*")
	if err != nil {
		return "", noop, fmt.Errorf("creating temp dir for profiles: %w", err)
	}
	cleanup := func() { os.RemoveAll(tmpDir) }

	profileName := cfg.Profile
	if profileName == "" {
		profileName = cfg.DAGName
	}
	target := cfg.Target
	if target == "" {
		target = "prod"
	}
	driver := cfg.Driver
	if driver == "" {
		driver = config.DefaultDBTDriver
	}

	threads := cfg.Threads
	if threads == "" {
		threads = "4"
	}

	f, err := os.Create(tmpDir + "/profiles.yml")
	if err != nil {
		cleanup()
		return "", noop, fmt.Errorf("creating profiles.yml: %w", err)
	}
	defer f.Close()

	data := profileData{
		ProfileName: profileName,
		Target:      target,
		Driver:      driver,
		Host:        host,
		Port:        port,
		Database:    database,
		Schema:      schema,
		User:        user,
		Password:    password,
		Threads:     threads,
	}
	if err := profilesTmpl.Execute(f, data); err != nil {
		cleanup()
		return "", noop, fmt.Errorf("writing profiles.yml: %w", err)
	}

	return tmpDir, cleanup, nil
}

// DBTProfilesInput holds the inputs needed for profiles generation.
type DBTProfilesInput struct {
	DAGName    string
	Profile    string
	Target     string
	Driver     string // ODBC driver string; defaults to config.DefaultDBTDriver if empty
	Threads    string
	Connection string // structured secret name for db credentials
}
