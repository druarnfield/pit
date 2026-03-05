// Package gitrepo manages cloning and updating git repositories used as
// project sources. It shells out to the system git binary so that existing
// SSH agent / credential helper configuration is inherited automatically.
package gitrepo

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Prepare ensures that the repository at url with ref checked out is present
// at cacheDir. If cacheDir does not contain a git repo it is cloned; otherwise
// the remote is fetched and the working tree is reset to ref.
//
// ref may be a branch name, tag, or commit SHA. For branch refs the working
// tree is advanced to the latest remote commit; for tags and SHAs the reset
// is a no-op and the checkout is sufficient.
func Prepare(url, ref, cacheDir string) error {
	if _, err := os.Stat(filepath.Join(cacheDir, ".git")); os.IsNotExist(err) {
		if err := gitRun("", "clone", url, cacheDir); err != nil {
			return fmt.Errorf("git clone %s: %w", url, err)
		}
	} else {
		if err := gitRun(cacheDir, "fetch", "origin"); err != nil {
			return fmt.Errorf("git fetch: %w", err)
		}
	}

	if err := gitRun(cacheDir, "checkout", ref); err != nil {
		return fmt.Errorf("git checkout %s: %w", ref, err)
	}

	// Advance to the latest remote commit for branch refs.
	// For commit SHAs and tags origin/{ref} won't resolve — that's fine,
	// the checkout above already placed the working tree at the right state.
	_ = gitRun(cacheDir, "reset", "--hard", "origin/"+ref)

	return nil
}

// gitRun executes git with the given arguments. If dir is non-empty it is
// used as the working directory (equivalent to git -C dir). Stderr is
// captured and included in the error on failure.
func gitRun(dir string, args ...string) error {
	if dir != "" {
		args = append([]string{"-C", dir}, args...)
	}
	cmd := exec.Command("git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := stderr.String()
		if msg == "" {
			return err
		}
		return fmt.Errorf("%w\n%s", err, msg)
	}
	return nil
}
