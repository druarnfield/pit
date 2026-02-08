package engine

import (
	"fmt"
	"sync"
	"time"
)

// TaskStatus represents the state of a task or run.
type TaskStatus string

const (
	StatusPending        TaskStatus = "pending"
	StatusRunning        TaskStatus = "running"
	StatusSuccess        TaskStatus = "success"
	StatusFailed         TaskStatus = "failed"
	StatusSkipped        TaskStatus = "skipped"
	StatusUpstreamFailed TaskStatus = "upstream_failed"
)

// SecretsResolver resolves secrets by project scope.
type SecretsResolver interface {
	Resolve(project, key string) (string, error)
}

// Run holds the state of a single DAG execution.
type Run struct {
	ID          string
	DAGName     string
	SnapshotDir string
	LogDir      string
	Status      TaskStatus
	StartedAt   time.Time
	EndedAt     time.Time
	Tasks       []*TaskInstance

	// SDK fields — zero-value when SDK is not configured.
	SocketPath      string           // Unix socket for task-to-orchestrator communication
	SecretsResolver SecretsResolver  // resolves secrets by project scope

	// mu protects TaskInstance Status and Error fields during concurrent execution.
	mu sync.Mutex
}

// TaskInstance holds the state of a single task within a run.
type TaskInstance struct {
	Name       string
	Script     string
	Runner     string
	Status     TaskStatus
	DependsOn  []string
	Attempt    int
	MaxRetries int
	RetryDelay time.Duration
	Timeout    time.Duration
	StartedAt  time.Time
	EndedAt    time.Time
	Error      error
}

// GenerateRunID creates a run ID in the format: 20240115_143022.123_dag_name
// Millisecond precision reduces collision risk for rapid successive runs.
func GenerateRunID(dagName string) string {
	now := time.Now()
	return fmt.Sprintf("%s_%s", now.Format("20060102_150405.000"), dagName)
}
