package secrets

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/BurntSushi/toml"
)

// SetSecret sets a secret in the TOML data and returns the updated TOML bytes.
// If fields is non-nil, creates a structured secret; otherwise creates a plain value secret.
func SetSecret(data []byte, project, key, value string, fields map[string]string) ([]byte, error) {
	var parsed map[string]interface{}
	if len(data) > 0 {
		if err := toml.Unmarshal(data, &parsed); err != nil {
			return nil, fmt.Errorf("parsing TOML: %w", err)
		}
	}
	if parsed == nil {
		parsed = make(map[string]interface{})
	}

	section, ok := parsed[project]
	if !ok {
		section = map[string]interface{}{}
		parsed[project] = section
	}
	sectionMap, ok := section.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("section %q is not a table", project)
	}

	if fields != nil {
		fieldMap := make(map[string]interface{}, len(fields))
		for k, v := range fields {
			fieldMap[k] = v
		}
		sectionMap[key] = fieldMap
	} else {
		sectionMap[key] = value
	}

	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(parsed); err != nil {
		return nil, fmt.Errorf("encoding TOML: %w", err)
	}

	return buf.Bytes(), nil
}

// RemoveSecret removes a secret from the TOML data and returns the updated TOML bytes.
func RemoveSecret(data []byte, project, key string) ([]byte, error) {
	var parsed map[string]interface{}
	if err := toml.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parsing TOML: %w", err)
	}

	section, ok := parsed[project]
	if !ok {
		return nil, fmt.Errorf("secret %q not found in project %q", key, project)
	}
	sectionMap, ok := section.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("section %q is not a table", project)
	}

	if _, ok := sectionMap[key]; !ok {
		return nil, fmt.Errorf("secret %q not found in project %q", key, project)
	}

	delete(sectionMap, key)

	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(parsed); err != nil {
		return nil, fmt.Errorf("encoding TOML: %w", err)
	}

	return buf.Bytes(), nil
}

// ListKeys returns a map of project -> sorted list of secret key names.
// If project is non-empty, only that project's keys are returned.
func (s *Store) ListKeys(project string) map[string][]string {
	result := make(map[string][]string)

	for scope, secrets := range s.data {
		if project != "" && scope != project {
			continue
		}
		var keys []string
		for k := range secrets {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		result[scope] = keys
	}

	return result
}
