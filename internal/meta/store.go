package meta

import "time"

// Store is the metadata storage interface.
type Store interface {
	Close() error
	InsertRun(r RunRecord) error
	UpdateRun(id string, status string, endedAt time.Time, errMsg string) error
	InsertTaskInstance(ti TaskInstanceRecord) error
	UpdateTaskInstance(runID, taskName, status string, endedAt time.Time, attempts int, errMsg string) error
	RecordEnvSnapshot(dagName, hashType, hashValue, runID string) error
	RecordOutputs(runID, dagName string, outputs []OutputRecord) error
	LatestRuns(dagName string, limit int) ([]RunRecord, error)
	RunsByStatus(status string, limit int) ([]RunRecord, error)
	RunDetail(runID string) (*RunRecord, []TaskInstanceRecord, error)
	EnvHistory(dagName, hashType string, limit int) ([]EnvSnapshotRecord, error)
	OutputsByRun(runID string) ([]OutputRecord, error)
	LatestRunPerDAG() ([]RunRecord, error)
}

// RunRecord represents a single DAG run.
type RunRecord struct {
	ID        string
	DAGName   string
	Status    string
	StartedAt time.Time
	EndedAt   *time.Time
	RunDir    string
	Trigger   string
	Error     string
}

// TaskInstanceRecord represents a single task within a run.
type TaskInstanceRecord struct {
	RunID     string
	TaskName  string
	Status    string
	StartedAt *time.Time
	EndedAt   *time.Time
	Attempts  int
	Error     string
	LogPath   string
}

// EnvSnapshotRecord represents a captured environment hash.
type EnvSnapshotRecord struct {
	ID        int
	DAGName   string
	HashType  string
	HashValue string
	FirstSeen time.Time
	RunID     string
}

// OutputRecord represents a named output produced by a run.
type OutputRecord struct {
	RunID    string
	DAGName  string
	Name     string
	Type     string
	Location string
}
