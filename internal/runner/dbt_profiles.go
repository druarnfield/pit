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
// for dbt, populated from secrets. Returns the directory path and a cleanup
// function that removes the temp directory.
func GenerateProfiles(cfg *DBTProfilesInput, resolver SecretsResolver) (string, func(), error) {
	noop := func() {}

	if resolver == nil {
		return "", noop, fmt.Errorf("secrets resolver is required for dbt profiles generation")
	}

	// Resolve required secrets
	host, err := resolver.Resolve(cfg.DAGName, "dbt_host")
	if err != nil {
		return "", noop, fmt.Errorf("resolving dbt_host: %w", err)
	}
	portStr, err := resolver.Resolve(cfg.DAGName, "dbt_port")
	if err != nil {
		return "", noop, fmt.Errorf("resolving dbt_port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", noop, fmt.Errorf("dbt_port %q is not a valid integer: %w", portStr, err)
	}
	database, err := resolver.Resolve(cfg.DAGName, "dbt_database")
	if err != nil {
		return "", noop, fmt.Errorf("resolving dbt_database: %w", err)
	}
	schema, err := resolver.Resolve(cfg.DAGName, "dbt_schema")
	if err != nil {
		return "", noop, fmt.Errorf("resolving dbt_schema: %w", err)
	}
	user, err := resolver.Resolve(cfg.DAGName, "dbt_user")
	if err != nil {
		return "", noop, fmt.Errorf("resolving dbt_user: %w", err)
	}
	password, err := resolver.Resolve(cfg.DAGName, "dbt_password")
	if err != nil {
		return "", noop, fmt.Errorf("resolving dbt_password: %w", err)
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
	DAGName string
	Profile string
	Target  string
	Driver  string // ODBC driver string; defaults to config.DefaultDBTDriver if empty
	Threads string
}
