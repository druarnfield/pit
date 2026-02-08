package secrets

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Store holds secrets parsed from a TOML file, organised by section.
// Resolution checks the project-scoped section first, then falls back to [global].
type Store struct {
	data map[string]map[string]string
}

// Load parses a TOML secrets file and returns a Store.
// If path is empty, returns nil (secrets are optional).
func Load(path string) (*Store, error) {
	if path == "" {
		return nil, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading secrets file %q: %w", path, err)
	}

	var data map[string]map[string]string
	if err := toml.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("parsing secrets file %q: %w", path, err)
	}

	return &Store{data: data}, nil
}

// Resolve looks up a secret by key, checking the project-scoped section first
// then falling back to the [global] section.
func (s *Store) Resolve(project, key string) (string, error) {
	if section, ok := s.data[project]; ok {
		if val, ok := section[key]; ok {
			return val, nil
		}
	}
	if section, ok := s.data["global"]; ok {
		if val, ok := section[key]; ok {
			return val, nil
		}
	}
	return "", fmt.Errorf("secret %q not found for project %q", key, project)
}
