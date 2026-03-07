package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func generateTestIdentity(t *testing.T) (identityPath, recipientsPath, publicKey string) {
	t.Helper()
	dir := t.TempDir()
	identityPath = filepath.Join(dir, "age-key.txt")
	recipientsPath = filepath.Join(dir, "age-recipients.txt")

	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error: %v", err)
	}
	if err := os.WriteFile(identityPath, []byte(identity.String()), 0600); err != nil {
		t.Fatal(err)
	}
	publicKey = identity.Recipient().String()
	if err := os.WriteFile(recipientsPath, []byte(publicKey+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	return
}

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	identityPath, recipientsPath, _ := generateTestIdentity(t)

	plaintext := []byte("[global]\nsmtp_password = \"test_value\"\n")

	ciphertext, err := Encrypt(plaintext, recipientsPath)
	if err != nil {
		t.Fatalf("Encrypt() error: %v", err)
	}

	if string(ciphertext) == string(plaintext) {
		t.Error("Encrypt() output is identical to plaintext")
	}

	decrypted, err := DecryptWithFile(ciphertext, identityPath)
	if err != nil {
		t.Fatalf("DecryptWithFile() error: %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("DecryptWithFile() = %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptDecrypt_RawKey(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error: %v", err)
	}

	dir := t.TempDir()
	recipientsPath := filepath.Join(dir, "age-recipients.txt")
	if err := os.WriteFile(recipientsPath, []byte(identity.Recipient().String()+"\n"), 0644); err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("secret data")
	ciphertext, err := Encrypt(plaintext, recipientsPath)
	if err != nil {
		t.Fatalf("Encrypt() error: %v", err)
	}

	decrypted, err := DecryptWithRawKey(ciphertext, identity.String())
	if err != nil {
		t.Fatalf("DecryptWithRawKey() error: %v", err)
	}

	if string(decrypted) != string(plaintext) {
		t.Errorf("DecryptWithRawKey() = %q, want %q", decrypted, plaintext)
	}
}

func TestEncrypt_MissingRecipients(t *testing.T) {
	_, err := Encrypt([]byte("test"), "/nonexistent/recipients.txt")
	if err == nil {
		t.Error("Encrypt() expected error for missing recipients file, got nil")
	}
}

func TestEncrypt_EmptyRecipients(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(path, []byte(""), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Encrypt([]byte("test"), path)
	if err == nil {
		t.Error("Encrypt() expected error for empty recipients file, got nil")
	}
}

func TestDecryptWithFile_WrongKey(t *testing.T) {
	_, recipientsPath, _ := generateTestIdentity(t)

	plaintext := []byte("secret")
	ciphertext, err := Encrypt(plaintext, recipientsPath)
	if err != nil {
		t.Fatalf("Encrypt() error: %v", err)
	}

	wrongIdentity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error: %v", err)
	}
	wrongPath := filepath.Join(t.TempDir(), "wrong-key.txt")
	if err := os.WriteFile(wrongPath, []byte(wrongIdentity.String()), 0600); err != nil {
		t.Fatal(err)
	}

	_, err = DecryptWithFile(ciphertext, wrongPath)
	if err == nil {
		t.Error("DecryptWithFile() expected error for wrong key, got nil")
	}
}

func TestGenerateIdentity(t *testing.T) {
	identity, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error: %v", err)
	}

	privKey := identity.String()
	if !strings.HasPrefix(privKey, "AGE-SECRET-KEY-") {
		t.Errorf("private key = %q, want AGE-SECRET-KEY- prefix", privKey)
	}

	pubKey := identity.Recipient().String()
	if !strings.HasPrefix(pubKey, "age1") {
		t.Errorf("public key = %q, want age1 prefix", pubKey)
	}
}

func TestEncrypt_MultipleRecipients(t *testing.T) {
	id1, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error: %v", err)
	}
	id2, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity() error: %v", err)
	}

	dir := t.TempDir()
	recipientsPath := filepath.Join(dir, "recipients.txt")
	content := id1.Recipient().String() + "\n" + id2.Recipient().String() + "\n"
	if err := os.WriteFile(recipientsPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	id1Path := filepath.Join(dir, "id1.txt")
	if err := os.WriteFile(id1Path, []byte(id1.String()), 0600); err != nil {
		t.Fatal(err)
	}
	id2Path := filepath.Join(dir, "id2.txt")
	if err := os.WriteFile(id2Path, []byte(id2.String()), 0600); err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("shared secret")
	ciphertext, err := Encrypt(plaintext, recipientsPath)
	if err != nil {
		t.Fatalf("Encrypt() error: %v", err)
	}

	for _, idPath := range []string{id1Path, id2Path} {
		dec, err := DecryptWithFile(ciphertext, idPath)
		if err != nil {
			t.Fatalf("DecryptWithFile(%s) error: %v", idPath, err)
		}
		if string(dec) != string(plaintext) {
			t.Errorf("DecryptWithFile(%s) = %q, want %q", idPath, dec, plaintext)
		}
	}
}
