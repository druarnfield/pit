package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/druarnfield/pit/internal/secrets"
	"github.com/spf13/cobra"
)

var identityFlag string

func newSecretsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "secrets",
		Short: "Manage encrypted secrets",
		Long:  "Commands for generating age keys, encrypting secrets files, and editing encrypted secrets.",
	}

	cmd.PersistentFlags().StringVar(&identityFlag, "identity", "", "path to age identity (private key) file")

	cmd.AddCommand(
		newSecretsKeygenCmd(),
		newSecretsEncryptCmd(),
		newSecretsEditCmd(),
	)

	return cmd
}

func newSecretsKeygenCmd() *cobra.Command {
	var outputPath string

	cmd := &cobra.Command{
		Use:   "keygen",
		Short: "Generate a new age identity (key pair)",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Check if file already exists
			if _, err := os.Stat(outputPath); err == nil {
				return fmt.Errorf("identity file already exists: %s", outputPath)
			}

			identity, err := secrets.GenerateIdentity()
			if err != nil {
				return fmt.Errorf("generating identity: %w", err)
			}

			// Create parent directories with 0700
			dir := filepath.Dir(outputPath)
			if err := os.MkdirAll(dir, 0700); err != nil {
				return fmt.Errorf("creating directory %q: %w", dir, err)
			}

			// Write identity file with 0600 permissions
			if err := os.WriteFile(outputPath, []byte(identity.String()+"\n"), 0600); err != nil {
				return fmt.Errorf("writing identity file: %w", err)
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Identity written to %s\n", outputPath)
			fmt.Fprintf(out, "Public key: %s\n", identity.Recipient().String())

			return nil
		},
	}

	cmd.Flags().StringVarP(&outputPath, "output", "o", secrets.DefaultIdentityPath, "output path for the identity file")

	return cmd
}

func newSecretsEncryptCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "encrypt",
		Short: "Encrypt a plaintext secrets file",
		Long:  "One-time migration from a plaintext secrets.toml to an encrypted .age file.",
		RunE: func(cmd *cobra.Command, args []string) error {
			src := secretsPath
			if src == "" {
				src = "secrets.toml"
			}

			// Read plaintext secrets
			plaintext, err := os.ReadFile(src)
			if err != nil {
				return fmt.Errorf("reading secrets file %q: %w", src, err)
			}

			// Validate TOML
			if _, err := secrets.LoadFromBytes(plaintext); err != nil {
				return fmt.Errorf("invalid secrets TOML: %w", err)
			}

			// Resolve recipients path
			recipientsPath := resolveSecretsRecipients()
			if recipientsPath == "" {
				// Default: look for recipients file next to the secrets file
				recipientsPath = filepath.Join(filepath.Dir(src), "age-recipients.txt")
			}

			// Encrypt
			ciphertext, err := secrets.Encrypt(plaintext, recipientsPath)
			if err != nil {
				return fmt.Errorf("encrypting: %w", err)
			}

			// Write encrypted file
			dest := src + ".age"
			if err := os.WriteFile(dest, ciphertext, 0644); err != nil {
				return fmt.Errorf("writing encrypted file %q: %w", dest, err)
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Encrypted secrets written to %s\n", dest)
			fmt.Fprintf(out, "\nNext steps:\n")
			fmt.Fprintf(out, "  1. Update pit_config.toml: secrets_dir = %q\n", dest)
			fmt.Fprintf(out, "  2. Delete the plaintext file: rm %s\n", src)

			return nil
		},
	}

	return cmd
}

func newSecretsEditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edit",
		Short: "Decrypt, edit, and re-encrypt secrets",
		Long:  "Opens the decrypted secrets in $EDITOR, validates on save, and re-encrypts.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if secretsPath == "" {
				return fmt.Errorf("--secrets flag is required for edit (path to .age file)")
			}

			// Decrypt
			plaintext, err := decryptSecretsFile(secretsPath)
			if err != nil {
				return err
			}

			// Write decrypted content to temp file
			tmpFile, err := os.CreateTemp("", "pit-secrets-*.toml")
			if err != nil {
				return fmt.Errorf("creating temp file: %w", err)
			}
			tmpPath := tmpFile.Name()

			// Ensure secure cleanup
			defer func() {
				// Overwrite with zeros before removing
				if info, err := os.Stat(tmpPath); err == nil {
					zeros := make([]byte, info.Size())
					_ = os.WriteFile(tmpPath, zeros, 0600)
				}
				os.Remove(tmpPath)
			}()

			if err := os.WriteFile(tmpPath, plaintext, 0600); err != nil {
				return fmt.Errorf("writing temp file: %w", err)
			}
			tmpFile.Close()

			// Open editor
			editor := os.Getenv("EDITOR")
			if editor == "" {
				editor = "vi"
			}

			editorCmd := exec.Command(editor, tmpPath)
			editorCmd.Stdin = os.Stdin
			editorCmd.Stdout = os.Stdout
			editorCmd.Stderr = os.Stderr

			if err := editorCmd.Run(); err != nil {
				return fmt.Errorf("editor exited with error: %w", err)
			}

			// Read edited content
			edited, err := os.ReadFile(tmpPath)
			if err != nil {
				return fmt.Errorf("reading edited file: %w", err)
			}

			// Validate TOML
			if _, err := secrets.LoadFromBytes(edited); err != nil {
				out := cmd.OutOrStdout()
				fmt.Fprintf(out, "Invalid TOML: %v\n", err)
				fmt.Fprintf(out, "Changes NOT saved.\n")
				return nil
			}

			// Resolve recipients for re-encryption
			recipientsPath := resolveSecretsRecipients()
			if recipientsPath == "" {
				recipientsPath = filepath.Join(filepath.Dir(secretsPath), "age-recipients.txt")
			}

			// Re-encrypt
			ciphertext, err := secrets.Encrypt(edited, recipientsPath)
			if err != nil {
				return fmt.Errorf("re-encrypting: %w", err)
			}

			// Write back
			if err := os.WriteFile(secretsPath, ciphertext, 0644); err != nil {
				return fmt.Errorf("writing encrypted file: %w", err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Secrets re-encrypted and saved to %s\n", secretsPath)
			return nil
		},
	}

	return cmd
}

// decryptSecretsFile decrypts an age-encrypted secrets file, trying PIT_AGE_KEY
// env var first, then falling back to file-based identity resolution.
func decryptSecretsFile(path string) ([]byte, error) {
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading encrypted secrets %q: %w", path, err)
	}

	// Try raw key from environment first
	if rawKey := secrets.ResolveIdentityRaw(); rawKey != "" {
		plaintext, err := secrets.DecryptWithRawKey(ciphertext, rawKey)
		if err != nil {
			return nil, fmt.Errorf("decrypting with PIT_AGE_KEY: %w", err)
		}
		return plaintext, nil
	}

	// Resolve identity file path: --identity flag > config > default
	idPath := identityFlag
	if idPath == "" {
		idPath = resolveAgeIdentityPath()
	}

	resolvedPath, err := secrets.ResolveIdentityPath(idPath)
	if err != nil {
		// If the flag path didn't work and we haven't tried config, try it
		if idPath != "" {
			configPath := resolveAgeIdentityPath()
			if configPath != "" && configPath != idPath {
				resolvedPath, err = secrets.ResolveIdentityPath(configPath)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("resolving identity: %w", err)
		}
	}

	plaintext, err := secrets.DecryptWithFile(ciphertext, resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("decrypting %q: %w", path, err)
	}

	return plaintext, nil
}

