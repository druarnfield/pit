package ftp

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/jlaffaye/ftp"
)

// FileInfo represents a remote file's metadata.
type FileInfo struct {
	Name string
	Size int64
}

// Client wraps an FTP connection with higher-level operations.
type Client struct {
	conn *ftp.ServerConn
}

// Connect establishes an FTP connection and logs in.
func Connect(host string, port int, user, password string, useTLS bool) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)

	var opts []ftp.DialOption
	if useTLS {
		opts = append(opts, ftp.DialWithExplicitTLS(nil))
	}

	conn, err := ftp.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", addr, err)
	}

	if err := conn.Login(user, password); err != nil {
		conn.Quit()
		return nil, fmt.Errorf("login as %q: %w", user, err)
	}

	return &Client{conn: conn}, nil
}

// Close gracefully terminates the FTP connection.
func (c *Client) Close() error {
	return c.conn.Quit()
}

// List returns files in dir that match the glob pattern.
func (c *Client) List(dir, pattern string) ([]FileInfo, error) {
	entries, err := c.conn.List(dir)
	if err != nil {
		return nil, fmt.Errorf("listing %q: %w", dir, err)
	}

	var files []FileInfo
	for _, entry := range entries {
		if entry.Type != ftp.EntryTypeFile {
			continue
		}
		if matched, _ := MatchGlob(pattern, entry.Name); matched {
			files = append(files, FileInfo{
				Name: entry.Name,
				Size: int64(entry.Size),
			})
		}
	}
	return files, nil
}

// Download retrieves a remote file and saves it to localPath.
func (c *Client) Download(remotePath, localPath string) error {
	resp, err := c.conn.Retr(remotePath)
	if err != nil {
		return fmt.Errorf("retrieving %q: %w", remotePath, err)
	}
	defer resp.Close()

	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("creating local dir: %w", err)
	}

	out, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("creating %q: %w", localPath, err)
	}

	_, copyErr := io.Copy(out, resp)
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("downloading %q: %w", remotePath, copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

// Upload stores a local file on the FTP server.
func (c *Client) Upload(localPath, remotePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening %q: %w", localPath, err)
	}
	defer f.Close()

	if err := c.conn.Stor(remotePath, f); err != nil {
		return fmt.Errorf("uploading to %q: %w", remotePath, err)
	}
	return nil
}

// Move renames a file on the server (RNFR/RNTO).
func (c *Client) Move(oldPath, newPath string) error {
	if err := c.conn.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("moving %q to %q: %w", oldPath, newPath, err)
	}
	return nil
}

// MkdirAll creates the directory and all parents on the FTP server.
func (c *Client) MkdirAll(dir string) error {
	parts := strings.Split(path.Clean(dir), "/")
	current := ""
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if current == "" && strings.HasPrefix(dir, "/") {
			current = "/" + part
		} else if current == "" {
			current = part
		} else {
			current = current + "/" + part
		}
		// Attempt mkdir; ignore error if dir already exists
		c.conn.MakeDir(current)
	}
	return nil
}

// MatchGlob matches a filename against a glob pattern.
// Exported for testability.
func MatchGlob(pattern, name string) (bool, error) {
	return path.Match(pattern, name)
}
