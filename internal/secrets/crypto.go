package secrets

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"filippo.io/age"
)

// GenerateIdentity creates a new age X25519 identity (private key).
func GenerateIdentity() (*age.X25519Identity, error) {
	return age.GenerateX25519Identity()
}

// ParseRecipient validates and parses an age public key string.
func ParseRecipient(pubKey string) (*age.X25519Recipient, error) {
	return age.ParseX25519Recipient(pubKey)
}

// Encrypt encrypts plaintext for all recipients listed in recipientsPath.
// The recipients file should contain one age public key per line.
func Encrypt(plaintext []byte, recipientsPath string) ([]byte, error) {
	data, err := os.ReadFile(recipientsPath)
	if err != nil {
		return nil, fmt.Errorf("reading recipients file %q: %w", recipientsPath, err)
	}

	var recipients []age.Recipient
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r, err := age.ParseX25519Recipient(line)
		if err != nil {
			return nil, fmt.Errorf("parsing recipient %q: %w", line, err)
		}
		recipients = append(recipients, r)
	}

	if len(recipients) == 0 {
		return nil, fmt.Errorf("no recipients found in %q", recipientsPath)
	}

	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipients...)
	if err != nil {
		return nil, fmt.Errorf("creating age writer: %w", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		return nil, fmt.Errorf("writing encrypted data: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("finalizing encryption: %w", err)
	}

	return buf.Bytes(), nil
}

// DecryptWithFile decrypts ciphertext using the age identity file at identityPath.
func DecryptWithFile(ciphertext []byte, identityPath string) ([]byte, error) {
	data, err := os.ReadFile(identityPath)
	if err != nil {
		return nil, fmt.Errorf("reading identity file %q: %w", identityPath, err)
	}

	identities, err := age.ParseIdentities(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parsing identity file %q: %w", identityPath, err)
	}

	return decrypt(ciphertext, identities)
}

// DecryptWithRawKey decrypts ciphertext using a raw age secret key string.
func DecryptWithRawKey(ciphertext []byte, rawKey string) ([]byte, error) {
	identity, err := age.ParseX25519Identity(rawKey)
	if err != nil {
		return nil, fmt.Errorf("parsing raw age key: %w", err)
	}

	return decrypt(ciphertext, []age.Identity{identity})
}

func decrypt(ciphertext []byte, identities []age.Identity) ([]byte, error) {
	r, err := age.Decrypt(bytes.NewReader(ciphertext), identities...)
	if err != nil {
		return nil, fmt.Errorf("decrypting: %w", err)
	}

	plaintext, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading decrypted data: %w", err)
	}

	return plaintext, nil
}
