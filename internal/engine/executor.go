package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/loader"
	"github.com/druarnfield/pit/internal/runner"
	"github.com/druarnfield/pit/internal/sdk"
	"github.com/druarnfield/pit/internal/secrets"
)

// ExecuteOpts configures a DAG execution.
type ExecuteOpts struct {
	RunsDir       string   // directory for run snapshots (default: "runs")
	TaskName      string   // if set, only run this single task
	Verbose       bool     // stream task output to stdout
	Concurrency   int      // max parallel tasks (0 = unlimited)
	SecretsPath   string   // path to secrets.toml (optional, empty = no secrets)
	DataSeedDir   string   // if set, copy contents into data dir before execution
	DBTDriver     string   // ODBC driver for dbt profiles (default: config.DefaultDBTDriver)
	KeepArtifacts []string // which run subdirs to keep after completion (default: all)
}

// Execute runs a DAG to completion.
func Execute(ctx context.Context, cfg *config.ProjectConfig, opts ExecuteOpts) (*Run, error) {
	if opts.RunsDir == "" {
		opts.RunsDir = "runs"
	}

	runID := GenerateRunID(cfg.DAG.Name)

	// Snapshot the project
	snapshotDir, logDir, dataDir, err := Snapshot(cfg.Dir(), opts.RunsDir, runID)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}

	// Seed data directory with files if configured
	if opts.DataSeedDir != "" {
		if err := copyDirContents(opts.DataSeedDir, dataDir); err != nil {
			return nil, fmt.Errorf("seeding data dir: %w", err)
		}
	}

	// Load secrets and start SDK server if configured
	var store *secrets.Store
	if opts.SecretsPath != "" {
		var err error
		store, err = secrets.Load(opts.SecretsPath)
		if err != nil {
			return nil, fmt.Errorf("loading secrets: %w", err)
		}
	}

	socketHint := filepath.Join(os.TempDir(), fmt.Sprintf("pit-%d.sock", os.Getpid()))
	sdkServer, err := sdk.NewServer(socketHint, store, cfg.DAG.Name)
	if err != nil {
		return nil, fmt.Errorf("starting SDK server: %w", err)
	}

	// Register the load_data handler for Python SDK → Go bulk load
	sdkServer.RegisterHandler("load_data", makeLoadDataHandler(store, cfg.DAG.Name, dataDir))

	socketPath := sdkServer.Addr()
	sdkCtx, sdkCancel := context.WithCancel(context.Background())
	go sdkServer.Serve(sdkCtx)
	defer func() {
		sdkCancel()
		sdkServer.Shutdown()
	}()

	// Build Run from config
	run := &Run{
		ID:              runID,
		DAGName:         cfg.DAG.Name,
		SnapshotDir:     snapshotDir,
		LogDir:          logDir,
		DataDir:         dataDir,
		Status:          StatusRunning,
		StartedAt:       time.Now(),
		SocketPath:      socketPath,
		SecretsResolver: store,
	}

	for _, tc := range cfg.Tasks {
		ti := &TaskInstance{
			Name:       tc.Name,
			Script:     tc.Script,
			Runner:     tc.Runner,
			Status:     StatusPending,
			DependsOn:  tc.DependsOn,
			MaxRetries: tc.Retries,
			RetryDelay: tc.RetryDelay.Duration,
			Timeout:    tc.Timeout.Duration,
		}
		run.Tasks = append(run.Tasks, ti)
	}

	// Apply DAG-level timeout
	if cfg.DAG.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.DAG.Timeout.Duration)
		defer cancel()
	}

	// Single task mode
	if opts.TaskName != "" {
		found := false
		for _, ti := range run.Tasks {
			if ti.Name == opts.TaskName {
				found = true
				ti.Status = StatusPending
			} else {
				ti.Status = StatusSkipped
			}
		}
		if !found {
			return nil, fmt.Errorf("task %q not found in DAG %q", opts.TaskName, cfg.DAG.Name)
		}

		// Warn about skipped dependencies
		for _, ti := range run.Tasks {
			if ti.Name == opts.TaskName && len(ti.DependsOn) > 0 {
				fmt.Fprintf(os.Stderr, "warning: task %q depends on %v — dependencies skipped in single-task mode\n",
					opts.TaskName, ti.DependsOn)
			}
		}

		for _, ti := range run.Tasks {
			if ti.Name == opts.TaskName {
				executeTask(ctx, ti, run, cfg, opts)
				break
			}
		}
	} else {
		// Full DAG execution
		levels, err := topoSort(run.Tasks)
		if err != nil {
			return nil, err
		}
		executeDAG(ctx, levels, run, cfg, opts)
	}

	run.EndedAt = time.Now()

	// Determine overall run status
	run.Status = StatusSuccess
	for _, ti := range run.Tasks {
		if ti.Status == StatusFailed || ti.Status == StatusUpstreamFailed {
			run.Status = StatusFailed
			break
		}
	}

	printSummary(os.Stdout, run)

	// Cleanup artifacts based on keep_artifacts config
	if len(opts.KeepArtifacts) > 0 {
		runDir := filepath.Dir(run.SnapshotDir) // parent of project/
		if err := cleanupArtifacts(runDir, opts.KeepArtifacts); err != nil {
			fmt.Fprintf(os.Stderr, "warning: artifact cleanup failed: %v\n", err)
		}
	}

	return run, nil
}

// topoSort groups tasks into execution levels using Kahn's algorithm.
// Level 0 = no dependencies, level 1 = depends only on level 0, etc.
//
// This is intentionally separate from dag/validate.go's cycle detection:
// that operates on []config.TaskConfig for pre-run validation, while this
// operates on []*TaskInstance for execution-time level grouping.
func topoSort(tasks []*TaskInstance) ([][]*TaskInstance, error) {
	taskMap := make(map[string]*TaskInstance, len(tasks))
	inDegree := make(map[string]int, len(tasks))
	dependents := make(map[string][]string, len(tasks))

	for _, t := range tasks {
		taskMap[t.Name] = t
		inDegree[t.Name] = len(t.DependsOn)
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], t.Name)
		}
	}

	var levels [][]*TaskInstance
	resolved := make(map[string]bool)

	for len(resolved) < len(tasks) {
		var level []*TaskInstance
		for _, t := range tasks {
			if resolved[t.Name] {
				continue
			}
			if inDegree[t.Name] == 0 {
				level = append(level, t)
			}
		}
		if len(level) == 0 {
			return nil, fmt.Errorf("cycle detected in task dependencies")
		}
		for _, t := range level {
			resolved[t.Name] = true
			for _, dep := range dependents[t.Name] {
				inDegree[dep]--
			}
		}
		levels = append(levels, level)
	}

	return levels, nil
}

// executeDAG runs tasks level by level with concurrency control.
func executeDAG(ctx context.Context, levels [][]*TaskInstance, run *Run, cfg *config.ProjectConfig, opts ExecuteOpts) {
	// Set up concurrency semaphore
	var sem chan struct{}
	if opts.Concurrency > 0 {
		sem = make(chan struct{}, opts.Concurrency)
	}

	for _, level := range levels {
		// Check if context is already cancelled
		if ctx.Err() != nil {
			for _, ti := range level {
				run.mu.Lock()
				if ti.Status == StatusPending {
					ti.Status = StatusFailed
					ti.Error = ctx.Err()
				}
				run.mu.Unlock()
			}
			continue
		}

		// Build status snapshot once per level for hasUpstreamFailure checks.
		run.mu.Lock()
		statusMap := make(map[string]TaskStatus, len(run.Tasks))
		for _, t := range run.Tasks {
			statusMap[t.Name] = t.Status
		}
		run.mu.Unlock()

		concurrent := len(level) > 1

		var wg sync.WaitGroup
		for _, ti := range level {
			// Check for upstream failures using the pre-built status map
			if hasUpstreamFailure(ti, statusMap) {
				run.mu.Lock()
				ti.Status = StatusUpstreamFailed
				run.mu.Unlock()
				continue
			}

			wg.Add(1)
			go func(t *TaskInstance) {
				defer wg.Done()

				// Acquire semaphore if configured
				if sem != nil {
					sem <- struct{}{}
					defer func() { <-sem }()
				}

				executeTask(ctx, t, run, cfg, opts, concurrent)
			}(ti)
		}
		wg.Wait()
	}
}

// hasUpstreamFailure checks if any dependency of the task has failed,
// using a pre-built status map to avoid O(n²) lookups.
func hasUpstreamFailure(ti *TaskInstance, statusMap map[string]TaskStatus) bool {
	for _, dep := range ti.DependsOn {
		s := statusMap[dep]
		if s == StatusFailed || s == StatusUpstreamFailed {
			return true
		}
	}
	return false
}

// executeTask runs a single task with retries and timeout.
// The concurrent parameter controls whether verbose output uses line prefixing.
func executeTask(ctx context.Context, ti *TaskInstance, run *Run, cfg *config.ProjectConfig, opts ExecuteOpts, concurrent ...bool) {
	run.mu.Lock()
	ti.Status = StatusRunning
	ti.StartedAt = time.Now()
	run.mu.Unlock()

	scriptPath := filepath.Join(run.SnapshotDir, ti.Script)

	// Resolve the runner — dbt is special-cased since it needs config + profiles
	var r runner.Runner
	var dbtCleanup func()
	isDBT := ti.Runner == "dbt"

	if isDBT {
		if cfg.DAG.DBT == nil {
			run.mu.Lock()
			ti.Status = StatusFailed
			ti.Error = fmt.Errorf("dbt runner requires [dag.dbt] configuration section")
			ti.EndedAt = time.Now()
			run.mu.Unlock()
			return
		}

		profilesInput := &runner.DBTProfilesInput{
			DAGName: run.DAGName,
			Profile: cfg.DAG.DBT.Profile,
			Target:  cfg.DAG.DBT.Target,
			Driver:  opts.DBTDriver,
		}

		var profilesDir string
		var err error
		if run.SecretsResolver != nil {
			profilesDir, dbtCleanup, err = runner.GenerateProfiles(profilesInput, run.SecretsResolver)
			if err != nil {
				run.mu.Lock()
				ti.Status = StatusFailed
				ti.Error = fmt.Errorf("generating dbt profiles: %w", err)
				ti.EndedAt = time.Now()
				run.mu.Unlock()
				return
			}
		} else {
			dbtCleanup = func() {}
		}

		r = runner.NewDBTRunner(cfg.DAG.DBT, profilesDir)
	} else {
		var err error
		r, err = runner.Resolve(ti.Runner, scriptPath)
		if err != nil {
			run.mu.Lock()
			ti.Status = StatusFailed
			ti.Error = err
			ti.EndedAt = time.Now()
			run.mu.Unlock()
			return
		}
	}

	if dbtCleanup != nil {
		defer dbtCleanup()
	}

	logPath := filepath.Join(run.LogDir, ti.Name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		run.mu.Lock()
		ti.Status = StatusFailed
		ti.Error = fmt.Errorf("creating log file: %w", err)
		ti.EndedAt = time.Now()
		run.mu.Unlock()
		return
	}
	defer logFile.Close()

	// Set up log writer — optionally tee to stdout
	var logWriter io.Writer = logFile
	if opts.Verbose {
		isConcurrent := len(concurrent) > 0 && concurrent[0]
		if isConcurrent {
			logWriter = io.MultiWriter(logFile, &prefixWriter{
				prefix: []byte("[" + ti.Name + "] "),
				dest:   os.Stdout,
			})
		} else {
			logWriter = io.MultiWriter(logFile, os.Stdout)
		}
	}

	// Build environment
	env := append(os.Environ(),
		"PIT_RUN_ID="+run.ID,
		"PIT_TASK_NAME="+ti.Name,
		"PIT_DAG_NAME="+run.DAGName,
		"PIT_SOCKET="+run.SocketPath,
		"PIT_DATA_DIR="+run.DataDir,
	)

	rc := runner.RunContext{
		ScriptPath:      scriptPath,
		SnapshotDir:     run.SnapshotDir,
		OrigProjectDir:  cfg.Dir(),
		Env:             env,
		SecretsResolver: run.SecretsResolver,
		DAGName:         run.DAGName,
		SQLConnection:   cfg.DAG.SQL.Connection,
	}

	// For dbt tasks, ScriptPath holds the dbt command (not a file path),
	// and SnapshotDir points to the dbt project within the snapshot.
	if isDBT {
		rc.ScriptPath = ti.Script // raw dbt command string, e.g. "run --select staging"
		if cfg.DAG.DBT.ProjectDir != "" {
			rc.SnapshotDir = filepath.Join(run.SnapshotDir, cfg.DAG.DBT.ProjectDir)
		}
	} else {
		// Validate script path is within snapshot (not applicable for dbt)
		if err := rc.ValidateScript(); err != nil {
			run.mu.Lock()
			ti.Status = StatusFailed
			ti.Error = err
			ti.EndedAt = time.Now()
			run.mu.Unlock()
			return
		}
	}

	maxAttempts := ti.MaxRetries + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		run.mu.Lock()
		ti.Attempt = attempt
		run.mu.Unlock()

		// Check if parent context is cancelled before each attempt
		if ctx.Err() != nil {
			run.mu.Lock()
			ti.Status = StatusFailed
			ti.Error = ctx.Err()
			ti.EndedAt = time.Now()
			run.mu.Unlock()
			return
		}

		// Per-attempt timeout
		var attemptCtx context.Context
		var attemptCancel context.CancelFunc
		if ti.Timeout > 0 {
			attemptCtx, attemptCancel = context.WithTimeout(ctx, ti.Timeout)
		} else {
			attemptCtx, attemptCancel = context.WithCancel(ctx)
		}

		if attempt > 1 {
			fmt.Fprintf(logWriter, "\n--- retry attempt %d/%d ---\n", attempt, maxAttempts)
		}

		err = r.Run(attemptCtx, rc, logWriter)
		attemptCancel()

		if err == nil {
			run.mu.Lock()
			ti.Status = StatusSuccess
			ti.EndedAt = time.Now()
			run.mu.Unlock()
			return
		}

		run.mu.Lock()
		ti.Error = err
		run.mu.Unlock()

		// If this was the last attempt, don't sleep
		if attempt < maxAttempts {
			// Sleep with context-awareness
			if ti.RetryDelay > 0 {
				select {
				case <-ctx.Done():
					run.mu.Lock()
					ti.Status = StatusFailed
					ti.Error = ctx.Err()
					ti.EndedAt = time.Now()
					run.mu.Unlock()
					return
				case <-time.After(ti.RetryDelay):
				}
			}
		}
	}

	run.mu.Lock()
	ti.Status = StatusFailed
	ti.EndedAt = time.Now()
	run.mu.Unlock()
}

// printSummary outputs a table of task results to w.
func printSummary(w io.Writer, run *Run) {
	fmt.Fprintf(w, "\n── Run %s ──\n", run.ID)
	fmt.Fprintf(w, "DAG: %s  Status: %s  Duration: %s\n\n",
		run.DAGName, run.Status, run.EndedAt.Sub(run.StartedAt).Round(time.Millisecond))

	for _, ti := range run.Tasks {
		status := string(ti.Status)
		line := fmt.Sprintf("  %-20s %s", ti.Name, status)

		if ti.Status == StatusFailed && ti.Error != nil {
			line += fmt.Sprintf("  (%s)", ti.Error)
		}
		if ti.Attempt > 1 {
			line += fmt.Sprintf("  [attempt %d/%d]", ti.Attempt, ti.MaxRetries+1)
		}
		if !ti.StartedAt.IsZero() && !ti.EndedAt.IsZero() {
			dur := ti.EndedAt.Sub(ti.StartedAt).Round(time.Millisecond)
			line += fmt.Sprintf("  %s", dur)
		}

		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w)
}

// makeLoadDataHandler returns a HandlerFunc that loads Parquet files into databases.
func makeLoadDataHandler(store *secrets.Store, dagName string, dataDir string) sdk.HandlerFunc {
	return func(ctx context.Context, params map[string]string) (string, error) {
		fileName := params["file"]
		table := params["table"]
		connKey := params["connection"]

		if fileName == "" {
			return "", fmt.Errorf("missing required parameter: file")
		}
		if table == "" {
			return "", fmt.Errorf("missing required parameter: table")
		}
		if connKey == "" {
			return "", fmt.Errorf("missing required parameter: connection")
		}
		if store == nil {
			return "", fmt.Errorf("secrets store not configured (use --secrets flag)")
		}

		schema := params["schema"]
		if schema == "" {
			schema = "dbo"
		}
		mode := params["mode"]
		if mode == "" {
			mode = "append"
		}

		// Resolve file path within data directory (prevent traversal)
		filePath := filepath.Join(dataDir, fileName)
		absFile, err := filepath.Abs(filePath)
		if err != nil {
			return "", fmt.Errorf("resolving file path: %w", err)
		}
		absData, err := filepath.Abs(dataDir)
		if err != nil {
			return "", fmt.Errorf("resolving data dir: %w", err)
		}
		if !strings.HasPrefix(absFile, absData+string(filepath.Separator)) && absFile != absData {
			return "", fmt.Errorf("file path %q escapes data directory", fileName)
		}

		connStr, err := store.Resolve(dagName, connKey)
		if err != nil {
			return "", fmt.Errorf("resolving connection %q: %w", connKey, err)
		}

		rows, err := loader.Load(ctx, loader.LoadParams{
			FilePath: absFile,
			Table:    table,
			Schema:   schema,
			Mode:     loader.LoadMode(mode),
			ConnStr:  connStr,
		})
		if err != nil {
			return "", fmt.Errorf("loading data: %w", err)
		}

		return fmt.Sprintf("%d rows loaded", rows), nil
	}
}

// prefixWriter is an io.Writer that prepends a prefix to each line of output.
// Used in verbose mode when tasks run concurrently to distinguish output.
type prefixWriter struct {
	prefix []byte
	dest   io.Writer
	buf    []byte
}

func (pw *prefixWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	pw.buf = append(pw.buf, p...)
	for {
		idx := -1
		for i, b := range pw.buf {
			if b == '\n' {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}
		line := pw.buf[:idx+1]
		if _, err := pw.dest.Write(pw.prefix); err != nil {
			return n, err
		}
		if _, err := pw.dest.Write(line); err != nil {
			return n, err
		}
		pw.buf = pw.buf[idx+1:]
	}
	return n, nil
}
