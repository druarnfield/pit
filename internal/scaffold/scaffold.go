package scaffold

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

var validName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// ProjectType determines what files are scaffolded.
type ProjectType string

const (
	TypePython ProjectType = "python"
	TypeSQL    ProjectType = "sql"
	TypeShell  ProjectType = "shell"
)

// ValidType returns true if the given type string is supported.
func ValidType(t string) bool {
	switch ProjectType(t) {
	case TypePython, TypeSQL, TypeShell:
		return true
	}
	return false
}

// Create scaffolds a new pit project under rootDir/projects/name/.
func Create(rootDir, name string, projectType ProjectType) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("invalid project name %q: must match [a-z][a-z0-9_]*", name)
	}

	projectDir := filepath.Join(rootDir, "projects", name)
	if _, err := os.Stat(projectDir); err == nil {
		return fmt.Errorf("project directory already exists: %s", projectDir)
	}

	switch projectType {
	case TypePython:
		return createPython(projectDir, name)
	case TypeSQL:
		return createSQL(projectDir, name)
	case TypeShell:
		return createShell(projectDir, name)
	default:
		return fmt.Errorf("unknown project type %q", projectType)
	}
}

func createPython(projectDir, name string) error {
	dirs := []string{
		projectDir,
		filepath.Join(projectDir, "src", name),
		filepath.Join(projectDir, "tasks"),
	}
	if err := mkdirs(dirs); err != nil {
		return err
	}

	files := map[string]string{
		filepath.Join(projectDir, "pit.toml"):                 pitTomlPython(name),
		filepath.Join(projectDir, "pyproject.toml"):           pyprojectToml(name),
		filepath.Join(projectDir, "src", name, "__init__.py"): "",
		filepath.Join(projectDir, "tasks", "hello.py"):        helloPy(name),
	}
	return writeFiles(files)
}

func createSQL(projectDir, name string) error {
	dirs := []string{
		projectDir,
		filepath.Join(projectDir, "tasks"),
	}
	if err := mkdirs(dirs); err != nil {
		return err
	}

	files := map[string]string{
		filepath.Join(projectDir, "pit.toml"):            pitTomlSQL(name),
		filepath.Join(projectDir, "tasks", "example.sql"): exampleSQL(name),
	}
	return writeFiles(files)
}

func createShell(projectDir, name string) error {
	dirs := []string{
		projectDir,
		filepath.Join(projectDir, "tasks"),
	}
	if err := mkdirs(dirs); err != nil {
		return err
	}

	files := map[string]string{
		filepath.Join(projectDir, "pit.toml"):           pitTomlShell(name),
		filepath.Join(projectDir, "tasks", "hello.sh"):  helloSh(name),
	}
	return writeFiles(files)
}

func mkdirs(dirs []string) error {
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("creating directory %s: %w", d, err)
		}
	}
	return nil
}

func writeFiles(files map[string]string) error {
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", path, err)
		}
	}
	return nil
}

func pitTomlPython(name string) string {
	return fmt.Sprintf(`[dag]
name = "%s"
schedule = "0 6 * * *"
overlap = "skip"
timeout = "1h"

[[tasks]]
name = "hello"
script = "tasks/hello.py"
timeout = "5m"
retries = 1
retry_delay = "30s"

[[outputs]]
name = "results"
type = "table"
location = "warehouse.%s_results"
`, name, name)
}

func pitTomlSQL(name string) string {
	return fmt.Sprintf(`[dag]
name = "%s"
schedule = "0 6 * * *"
overlap = "skip"
timeout = "1h"

[dag.sql]
connection = "my_database"

[[tasks]]
name = "example"
script = "tasks/example.sql"
timeout = "10m"
`, name)
}

func pitTomlShell(name string) string {
	return fmt.Sprintf(`[dag]
name = "%s"
schedule = "0 6 * * *"
overlap = "skip"
timeout = "1h"

[[tasks]]
name = "hello"
script = "tasks/hello.sh"
timeout = "5m"
`, name)
}

func pyprojectToml(name string) string {
	return fmt.Sprintf(`[project]
name = "%s"
version = "0.1.0"
requires-python = ">=3.11"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
`, name)
}

func helloPy(name string) string {
	return fmt.Sprintf(`"""Sample task for %s."""


def main():
    print("Hello from %s!")


if __name__ == "__main__":
    main()
`, name, name)
}

func exampleSQL(name string) string {
	return fmt.Sprintf(`-- Sample SQL task for %s
SELECT 1 AS health_check;
`, name)
}

func helloSh(name string) string {
	return fmt.Sprintf(`#!/usr/bin/env bash
# Sample task for %s
set -euo pipefail

echo "Hello from %s!"
`, name, name)
}
