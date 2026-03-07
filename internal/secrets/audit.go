package secrets

// AuditEvent is emitted when a secret is successfully resolved.
type AuditEvent struct {
	Project string
	Key     string
}
