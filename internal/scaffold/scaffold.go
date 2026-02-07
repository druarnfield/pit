package scaffold

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

var validName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Create scaffolds a new pit project under rootDir/projects/name/.
func Create(rootDir, name string) error {
	if !validName.MatchString(name) {
		return fmt.Errorf("invalid project name %q: must match [a-z][a-z0-9_]*", name)
	}

	projectDir := filepath.Join(rootDir, "projects", name)
	if _, err := os.Stat(projectDir); err == nil {
		return fmt.Errorf("project directory already exists: %s", projectDir)
	}

	dirs := []string{
		projectDir,
		filepath.Join(projectDir, "src", name),
		filepath.Join(projectDir, "tasks"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("creating directory %s: %w", d, err)
		}
	}

	files := map[string]string{
		filepath.Join(projectDir, "pit.toml"):                    pitToml(name),
		filepath.Join(projectDir, "pyproject.toml"):              pyprojectToml(name),
		filepath.Join(projectDir, "src", name, "__init__.py"):    initPy(),
		filepath.Join(projectDir, "tasks", "hello.py"):           helloPy(name),
	}

	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", path, err)
		}
	}

	return nil
}

func pitToml(name string) string {
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

func pyprojectToml(name string) string {
	return fmt.Sprintf(`[project]
name = "%s"
version = "0.1.0"
requires-python = ">=3.11"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.backends"
`, name)
}

func initPy() string {
	return ""
}

func helloPy(name string) string {
	return fmt.Sprintf(`"""Sample task for %s."""


def main():
    print("Hello from %s!")


if __name__ == "__main__":
    main()
`, name, name)
}
