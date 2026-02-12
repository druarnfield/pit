package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	pitftp "github.com/druarnfield/pit/internal/ftp"
	"github.com/druarnfield/pit/internal/sdk"
	"github.com/druarnfield/pit/internal/secrets"
)

// connectFTP resolves FTP credentials from a structured secret and returns a connected client.
// The structured secret must have host, user, password fields. Optional: port (default 21), tls (default false).
func connectFTP(store *secrets.Store, dagName, secretName string) (*pitftp.Client, error) {
	if store == nil {
		return nil, fmt.Errorf("secrets store not configured (use --secrets flag)")
	}

	host, err := store.ResolveField(dagName, secretName, "host")
	if err != nil {
		return nil, fmt.Errorf("resolving %s.host: %w", secretName, err)
	}
	user, err := store.ResolveField(dagName, secretName, "user")
	if err != nil {
		return nil, fmt.Errorf("resolving %s.user: %w", secretName, err)
	}
	password, err := store.ResolveField(dagName, secretName, "password")
	if err != nil {
		return nil, fmt.Errorf("resolving %s.password: %w", secretName, err)
	}

	port := 21
	if portStr, err := store.ResolveField(dagName, secretName, "port"); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	useTLS := false
	if tlsStr, err := store.ResolveField(dagName, secretName, "tls"); err == nil {
		useTLS = tlsStr == "true"
	}

	return pitftp.Connect(host, port, user, password, useTLS)
}

// makeFTPListHandler returns a handler that lists files on an FTP server.
//
// Params: secret, directory, pattern
// Returns: JSON array of filenames
func makeFTPListHandler(store *secrets.Store, dagName string) sdk.HandlerFunc {
	return func(ctx context.Context, params map[string]string) (string, error) {
		secretName := params["secret"]
		if secretName == "" {
			return "", fmt.Errorf("missing required parameter: secret")
		}
		directory := params["directory"]
		if directory == "" {
			return "", fmt.Errorf("missing required parameter: directory")
		}
		pattern := params["pattern"]
		if pattern == "" {
			pattern = "*"
		}

		client, err := connectFTP(store, dagName, secretName)
		if err != nil {
			return "", err
		}
		defer client.Close()

		files, err := client.List(directory, pattern)
		if err != nil {
			return "", err
		}

		names := make([]string, len(files))
		for i, f := range files {
			names[i] = f.Name
		}

		b, err := json.Marshal(names)
		if err != nil {
			return "", fmt.Errorf("encoding file list: %w", err)
		}
		return string(b), nil
	}
}

// makeFTPDownloadHandler returns a handler that downloads files from an FTP server
// into the run's data directory.
//
// Single file mode:   params: secret, remote_path
// Pattern match mode: params: secret, directory, pattern
// Returns: JSON array of downloaded filenames
func makeFTPDownloadHandler(store *secrets.Store, dagName string, dataDir string) sdk.HandlerFunc {
	return func(ctx context.Context, params map[string]string) (string, error) {
		secretName := params["secret"]
		if secretName == "" {
			return "", fmt.Errorf("missing required parameter: secret")
		}

		client, err := connectFTP(store, dagName, secretName)
		if err != nil {
			return "", err
		}
		defer client.Close()

		var downloaded []string

		if pattern := params["pattern"]; pattern != "" {
			// Batch mode: download all matching files from a directory
			directory := params["directory"]
			if directory == "" {
				return "", fmt.Errorf("missing required parameter: directory (required with pattern)")
			}

			files, err := client.List(directory, pattern)
			if err != nil {
				return "", err
			}

			for _, f := range files {
				remotePath := directory + "/" + f.Name
				localPath := filepath.Join(dataDir, f.Name)
				if err := client.Download(remotePath, localPath); err != nil {
					return "", fmt.Errorf("downloading %q: %w", f.Name, err)
				}
				downloaded = append(downloaded, f.Name)
			}
		} else {
			// Single file mode
			remotePath := params["remote_path"]
			if remotePath == "" {
				return "", fmt.Errorf("missing required parameter: remote_path (or use directory+pattern for batch)")
			}

			fileName := filepath.Base(remotePath)
			localPath := filepath.Join(dataDir, fileName)

			// Prevent directory traversal
			absLocal, _ := filepath.Abs(localPath)
			absData, _ := filepath.Abs(dataDir)
			if !strings.HasPrefix(absLocal, absData+string(filepath.Separator)) {
				return "", fmt.Errorf("filename %q escapes data directory", fileName)
			}

			if err := client.Download(remotePath, localPath); err != nil {
				return "", err
			}
			downloaded = append(downloaded, fileName)
		}

		b, err := json.Marshal(downloaded)
		if err != nil {
			return "", fmt.Errorf("encoding result: %w", err)
		}
		return string(b), nil
	}
}

// makeFTPUploadHandler returns a handler that uploads a file from the data directory
// to an FTP server.
//
// Params: secret, local_name, remote_path
// Returns: empty string on success
func makeFTPUploadHandler(store *secrets.Store, dagName string, dataDir string) sdk.HandlerFunc {
	return func(ctx context.Context, params map[string]string) (string, error) {
		secretName := params["secret"]
		if secretName == "" {
			return "", fmt.Errorf("missing required parameter: secret")
		}
		localName := params["local_name"]
		if localName == "" {
			return "", fmt.Errorf("missing required parameter: local_name")
		}
		remotePath := params["remote_path"]
		if remotePath == "" {
			return "", fmt.Errorf("missing required parameter: remote_path")
		}

		localPath := filepath.Join(dataDir, localName)

		// Prevent directory traversal
		absLocal, _ := filepath.Abs(localPath)
		absData, _ := filepath.Abs(dataDir)
		if !strings.HasPrefix(absLocal, absData+string(filepath.Separator)) {
			return "", fmt.Errorf("filename %q escapes data directory", localName)
		}

		client, err := connectFTP(store, dagName, secretName)
		if err != nil {
			return "", err
		}
		defer client.Close()

		if err := client.Upload(localPath, remotePath); err != nil {
			return "", err
		}
		return "", nil
	}
}

// makeFTPMoveHandler returns a handler that moves/renames a file on an FTP server.
//
// Params: secret, src, dst
// Returns: empty string on success
func makeFTPMoveHandler(store *secrets.Store, dagName string) sdk.HandlerFunc {
	return func(ctx context.Context, params map[string]string) (string, error) {
		secretName := params["secret"]
		if secretName == "" {
			return "", fmt.Errorf("missing required parameter: secret")
		}
		src := params["src"]
		if src == "" {
			return "", fmt.Errorf("missing required parameter: src")
		}
		dst := params["dst"]
		if dst == "" {
			return "", fmt.Errorf("missing required parameter: dst")
		}

		client, err := connectFTP(store, dagName, secretName)
		if err != nil {
			return "", err
		}
		defer client.Close()

		if err := client.Move(src, dst); err != nil {
			return "", err
		}
		return "", nil
	}
}
