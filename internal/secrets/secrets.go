package secrets

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Secret holds either a plain string value or a set of named fields.
// Exactly one of Value or Fields is populated.
type Secret struct {
	Value  string            // non-empty for plain "key = value" secrets
	Fields map[string]string // non-nil for structured [scope.name] secrets
}

// Store holds secrets parsed from a TOML file, organised by section.
// Resolution checks the project-scoped section first, then falls back to [global].
type Store struct {
	data map[string]map[string]Secret
}

// Load parses a TOML secrets file and returns a Store.
// If path is empty, returns nil (secrets are optional).
//
// The TOML format supports both plain and structured secrets:
//
//	[global]
//	smtp_password = "plain_value"
//
//	[global.warehouse_db]
//	host = "server.example.com"
//	port = "1433"
//	user = "admin"
//	password = "secret"
//
//	[my_project]
//	api_key = "abc123"
//
//	[my_project.ftp_creds]
//	host = "ftp.example.com"
//	user = "ftpuser"
//	password = "secret"
func Load(path string) (*Store, error) {
	if path == "" {
		return nil, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading secrets file %q: %w", path, err)
	}

	return LoadFromBytes(raw)
}

// LoadFromBytes parses TOML secrets from raw bytes and returns a Store.
func LoadFromBytes(data []byte) (*Store, error) {
	var parsed map[string]interface{}
	if err := toml.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parsing secrets: %w", err)
	}

	store := make(map[string]map[string]Secret)
	for scope, section := range parsed {
		sectionMap, ok := section.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("secrets: section %q is not a table", scope)
		}

		secrets := make(map[string]Secret)
		for key, val := range sectionMap {
			switch v := val.(type) {
			case string:
				secrets[key] = Secret{Value: v}
			case map[string]interface{}:
				fields := make(map[string]string, len(v))
				for fk, fv := range v {
					s, ok := fv.(string)
					if !ok {
						return nil, fmt.Errorf("secrets: field %q.%q.%q must be a string", scope, key, fk)
					}
					fields[fk] = s
				}
				secrets[key] = Secret{Fields: fields}
			default:
				return nil, fmt.Errorf("secrets: key %q.%q must be a string or table", scope, key)
			}
		}
		store[scope] = secrets
	}

	return &Store{data: store}, nil
}

// LoadEncrypted reads an age-encrypted secrets file, decrypts it, and returns a Store.
// If path is empty, returns nil, nil. Identity resolution tries PIT_AGE_KEY env var
// first (raw key), then falls back to file-based identity using identityPath or configIdentity.
func LoadEncrypted(path, identityPath, configIdentity string) (*Store, error) {
	if path == "" {
		return nil, nil
	}

	ciphertext, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading encrypted secrets %q: %w", path, err)
	}

	// Try raw key from environment first.
	if rawKey := ResolveIdentityRaw(); rawKey != "" {
		plaintext, err := DecryptWithRawKey(ciphertext, rawKey)
		if err != nil {
			return nil, fmt.Errorf("decrypting secrets with PIT_AGE_KEY: %w", err)
		}
		return LoadFromBytes(plaintext)
	}

	// Fall back to file-based identity.
	idPath, err := ResolveIdentityPath(identityPath)
	if err != nil {
		// Try configIdentity as fallback.
		if configIdentity != "" {
			idPath, err = ResolveIdentityPath(configIdentity)
		}
		if err != nil {
			return nil, fmt.Errorf("resolving identity for encrypted secrets: %w", err)
		}
	}

	plaintext, err := DecryptWithFile(ciphertext, idPath)
	if err != nil {
		return nil, fmt.Errorf("decrypting secrets %q: %w", path, err)
	}

	return LoadFromBytes(plaintext)
}

// Resolve looks up a plain secret by key, checking the project-scoped section first
// then falling back to the [global] section.
//
// For structured secrets, Resolve returns a JSON object of the fields.
func (s *Store) Resolve(project, key string) (string, error) {
	if sec, ok := s.lookup(project, key); ok {
		if sec.Fields != nil {
			b, err := json.Marshal(sec.Fields)
			if err != nil {
				return "", fmt.Errorf("marshalling structured secret %q: %w", key, err)
			}
			return string(b), nil
		}
		return sec.Value, nil
	}
	return "", fmt.Errorf("secret %q not found for project %q", key, project)
}

// ResolveField looks up a single field within a structured secret.
// Checks the project-scoped section first, then falls back to [global].
func (s *Store) ResolveField(project, secret, field string) (string, error) {
	if sec, ok := s.lookup(project, secret); ok {
		if sec.Fields == nil {
			return "", fmt.Errorf("secret %q is a plain value, not a structured secret (use Resolve instead)", secret)
		}
		if val, ok := sec.Fields[field]; ok {
			return val, nil
		}
		return "", fmt.Errorf("field %q not found in secret %q for project %q", field, secret, project)
	}
	return "", fmt.Errorf("secret %q not found for project %q", secret, project)
}

// lookup finds a Secret by key, checking project scope first then global.
func (s *Store) lookup(project, key string) (Secret, bool) {
	if section, ok := s.data[project]; ok {
		if sec, ok := section[key]; ok {
			return sec, true
		}
	}
	if section, ok := s.data["global"]; ok {
		if sec, ok := section[key]; ok {
			return sec, true
		}
	}
	return Secret{}, false
}
