package secrets

import (
	"fmt"
	"os"
	"path/filepath"
)

// DefaultIdentityPath is the default location for the age identity file.
var DefaultIdentityPath = filepath.Join(userConfigDir(), "pit", "age-key.txt")

// ResolveIdentityRaw returns the raw age identity from PIT_AGE_KEY env var, or empty string.
func ResolveIdentityRaw() string {
	return os.Getenv("PIT_AGE_KEY")
}

// ResolveIdentityPath determines the age identity file path.
// Priority: PIT_AGE_KEY_FILE env > configPath argument > DefaultIdentityPath.
// Returns an error if the resolved path does not exist.
func ResolveIdentityPath(configPath string) (string, error) {
	if envFile := os.Getenv("PIT_AGE_KEY_FILE"); envFile != "" {
		if _, err := os.Stat(envFile); err != nil {
			return "", fmt.Errorf("PIT_AGE_KEY_FILE %q: %w", envFile, err)
		}
		return envFile, nil
	}

	path := configPath
	if path == "" {
		path = DefaultIdentityPath
	}

	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("age identity file %q: %w", path, err)
	}
	return path, nil
}

func userConfigDir() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return filepath.Join(os.Getenv("HOME"), ".config")
	}
	return dir
}
