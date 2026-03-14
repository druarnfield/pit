package transform

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// ModelConfig holds the resolved configuration for a single model.
type ModelConfig struct {
	Materialization string   `toml:"materialization"`
	Strategy        string   `toml:"strategy"`   // "merge" or "delete_insert" (incremental only)
	UniqueKey       []string `toml:"unique_key"` // columns for incremental match
	Schema          string   `toml:"schema"`     // target schema
	Connection      string   `toml:"connection"` // override [dag.sql].connection
	SQLPath         string   // absolute path to the .sql file (set during discovery)
	RelPath         string   // path relative to models/ dir (e.g. "staging/stg_orders.sql")
}

// tomlEntry holds parsed data from a single TOML file.
type tomlEntry struct {
	dir      string
	defaults *ModelConfig
	models   map[string]*ModelConfig
}

// parseModelTOML parses a single TOML file, separating [defaults] from per-model configs.
func parseModelTOML(path string) (*ModelConfig, map[string]*ModelConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("reading model config %q: %w", path, err)
	}

	// First pass: extract [defaults]
	var withDefaults struct {
		Defaults *ModelConfig `toml:"defaults"`
	}
	if err := toml.Unmarshal(data, &withDefaults); err != nil {
		return nil, nil, fmt.Errorf("parsing model config %q: %w", path, err)
	}

	// Second pass: extract all top-level keys except "defaults" as model overrides
	var raw map[string]toml.Primitive
	md, err := toml.Decode(string(data), &raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing model config %q: %w", path, err)
	}

	models := make(map[string]*ModelConfig)
	for key := range raw {
		if key == "defaults" {
			continue
		}
		var mc ModelConfig
		if err := md.PrimitiveDecode(raw[key], &mc); err != nil {
			return nil, nil, fmt.Errorf("parsing model %q in %q: %w", key, path, err)
		}
		models[key] = &mc
	}

	return withDefaults.Defaults, models, nil
}

// discoverModels finds all .sql files under modelsDir and returns their names and paths.
func discoverModels(modelsDir string) (map[string]string, error) {
	models := make(map[string]string)
	err := filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".sql") {
			return nil
		}
		name := strings.TrimSuffix(d.Name(), ".sql")
		if existing, exists := models[name]; exists {
			return fmt.Errorf("duplicate model name %q: %s and %s", name, existing, path)
		}
		models[name] = path
		return nil
	})
	return models, err
}

// ParseModelConfigs discovers all models under modelsDir and resolves their configuration.
// Returns a map of model name to resolved ModelConfig.
func ParseModelConfigs(modelsDir string) (map[string]*ModelConfig, error) {
	// Discover .sql files
	sqlFiles, err := discoverModels(modelsDir)
	if err != nil {
		return nil, err
	}

	if len(sqlFiles) == 0 {
		return make(map[string]*ModelConfig), nil
	}

	// Discover and parse all .toml files
	var tomlEntries []tomlEntry

	err = filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".toml") {
			return nil
		}
		defaults, models, parseErr := parseModelTOML(path)
		if parseErr != nil {
			return parseErr
		}
		tomlEntries = append(tomlEntries, tomlEntry{
			dir:      filepath.Dir(path),
			defaults: defaults,
			models:   models,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Resolve config for each model
	result := make(map[string]*ModelConfig, len(sqlFiles))
	for name, sqlPath := range sqlFiles {
		mc := &ModelConfig{
			SQLPath: sqlPath,
		}
		relPath, _ := filepath.Rel(modelsDir, sqlPath)
		mc.RelPath = relPath
		modelDir := filepath.Dir(sqlPath)

		// Apply defaults from TOML files, outermost to innermost
		mc, err = applyDefaults(mc, modelDir, modelsDir, tomlEntries)
		if err != nil {
			return nil, err
		}

		// Apply per-model overrides (only from ancestor or same-level directories)
		for _, entry := range tomlEntries {
			if override, ok := entry.models[name]; ok {
				if isAncestorOrEqual(entry.dir, modelDir) {
					mc = mergeConfig(mc, override)
				}
			}
		}

		result[name] = mc
	}

	return result, nil
}

// isAncestorOrEqual reports whether dir is an ancestor of (or equal to) target.
func isAncestorOrEqual(dir, target string) bool {
	rel, err := filepath.Rel(dir, target)
	if err != nil {
		return false
	}
	// rel must not start with ".." — that would mean dir is not an ancestor
	return !strings.HasPrefix(rel, "..")
}

// applyDefaults resolves defaults by walking from modelsDir down to modelDir.
func applyDefaults(mc *ModelConfig, modelDir, modelsDir string, entries []tomlEntry) (*ModelConfig, error) {
	// Build path from modelsDir to modelDir
	rel, err := filepath.Rel(modelsDir, modelDir)
	if err != nil {
		return nil, fmt.Errorf("computing relative path from %q to %q: %w", modelsDir, modelDir, err)
	}

	// Collect directories from root to leaf
	var dirs []string
	dirs = append(dirs, modelsDir)
	if rel != "." {
		parts := strings.Split(rel, string(filepath.Separator))
		current := modelsDir
		for _, p := range parts {
			current = filepath.Join(current, p)
			dirs = append(dirs, current)
		}
	}

	// Apply defaults from outermost to innermost (innermost wins)
	for _, dir := range dirs {
		for _, entry := range entries {
			if entry.dir == dir && entry.defaults != nil {
				mc = mergeConfig(mc, entry.defaults)
			}
		}
	}

	return mc, nil
}

// mergeConfig overlays non-zero values from overlay onto base.
func mergeConfig(base, overlay *ModelConfig) *ModelConfig {
	result := *base
	if overlay.Materialization != "" {
		result.Materialization = overlay.Materialization
	}
	if overlay.Strategy != "" {
		result.Strategy = overlay.Strategy
	}
	if len(overlay.UniqueKey) > 0 {
		result.UniqueKey = overlay.UniqueKey
	}
	if overlay.Schema != "" {
		result.Schema = overlay.Schema
	}
	if overlay.Connection != "" {
		result.Connection = overlay.Connection
	}
	return &result
}
