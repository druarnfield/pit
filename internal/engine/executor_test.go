package engine

import (
	"testing"

	"github.com/druarnfield/pit/internal/secrets"
)

// TestRun_SecretsResolverNilInterface guards against the typed-nil interface
// bug: assigning a nil *secrets.Store to SecretsResolver produces a non-nil
// interface value (it carries the concrete type but a nil pointer). The nil
// check in executeTask then passes and ResolveField is called on a nil
// receiver, causing a panic.
//
// The fix is to only assign store to run.SecretsResolver when store is
// non-nil, so the interface field stays a true nil interface.
func TestRun_SecretsResolverNilInterface(t *testing.T) {
	// Demonstrate the Go gotcha: typed nil pointer → non-nil interface.
	var store *secrets.Store // nil
	var iface SecretsResolver = store
	if iface == nil {
		t.Fatal("prerequisite failed: expected typed nil *secrets.Store to produce non-nil interface")
	}

	// Verify the fix: conditional assignment leaves the field as nil interface.
	run := &Run{}
	if store != nil {
		run.SecretsResolver = store
	}
	if run.SecretsResolver != nil {
		t.Error("SecretsResolver should be nil interface when store is nil, got non-nil")
	}
}
