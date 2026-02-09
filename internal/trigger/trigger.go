package trigger

import "context"

// Event represents a trigger firing for a DAG.
type Event struct {
	DAGName string
	Source  string   // "cron" or "ftp_watch"
	Files   []string // filenames for FTP events (empty for cron)
}

// Trigger watches for conditions and emits events.
type Trigger interface {
	Start(ctx context.Context, events chan<- Event) error
	Name() string
}
