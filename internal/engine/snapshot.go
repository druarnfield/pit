package engine

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// skipDirs are directories that should not be copied into a snapshot.
var skipDirs = map[string]bool{
	".git":         true,
	"__pycache__":  true,
	".venv":        true,
	"node_modules": true,
}

// Snapshot copies the project directory into the run snapshot directory
// and creates the logs directory. Returns the snapshot and log directory paths.
func Snapshot(projectDir, runsDir, runID string) (snapshotDir, logDir string, err error) {
	absRunsDir, err := filepath.Abs(runsDir)
	if err != nil {
		return "", "", fmt.Errorf("resolving runs dir: %w", err)
	}
	snapshotDir = filepath.Join(absRunsDir, runID, "project")
	logDir = filepath.Join(absRunsDir, runID, "logs")

	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return "", "", fmt.Errorf("creating log dir: %w", err)
	}

	if err := copyDir(projectDir, snapshotDir); err != nil {
		return "", "", fmt.Errorf("copying project to snapshot: %w", err)
	}

	return snapshotDir, logDir, nil
}

// copyDir recursively copies src to dst, skipping directories in skipDirs
// and symlinks.
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip symlinks — they may point outside the project tree.
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		// Get relative path from source root
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		// Check if any path component is in skipDirs
		for _, part := range strings.Split(rel, string(filepath.Separator)) {
			if skipDirs[part] {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		target := filepath.Join(dst, rel)

		if d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(target, info.Mode().Perm())
		}

		return copyFile(path, target)
	})
}

// copyFile copies a single file from src to dst, preserving permissions.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}
