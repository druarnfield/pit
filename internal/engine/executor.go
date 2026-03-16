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

	"crypto/sha256"

	"github.com/druarnfield/pit/internal/config"
	"github.com/druarnfield/pit/internal/gitrepo"
	"github.com/druarnfield/pit/internal/loader"
	"github.com/druarnfield/pit/internal/loghub"
	"github.com/druarnfield/pit/internal/runner"
	"github.com/druarnfield/pit/internal/sdk"
	"github.com/druarnfield/pit/internal/secrets"
	"github.com/druarnfield/pit/internal/transform"
)

// ExecuteOpts configures a DAG execution.
type ExecuteOpts struct {
	RunsDir       string           // directory for run snapshots (default: "runs")
	RepoCacheDir  string           // directory for persistent git clones (default: "repo_cache")
	TaskName      string           // if set, only run this single task
	Verbose       bool             // stream task output to stdout
	Concurrency   int              // max parallel tasks (0 = unlimited)
	SecretsPath   string           // path to secrets.toml (optional, empty = no secrets)
	AgeIdentity   string           // path to age identity file (optional, for encrypted secrets)
	DataSeedDir   string           // if set, copy contents into data dir before execution
	DBTDriver     string           // ODBC driver for dbt profiles (default: config.DefaultDBTDriver)
	KeepArtifacts []string         // which run subdirs to keep after completion (default: all)
	MetaStore     MetadataRecorder // nil = no metadata tracking
	Trigger       string           // trigger source: "manual", "cron", "ftp_watch", "webhook"
	LogHub        *loghub.Hub      // nil = no live log streaming
	RunID         string           // if set, use this instead of generating (for webhook streaming)
}

// Execute runs a DAG to completion.
func Execute(ctx context.Context, cfg *config.ProjectConfig, opts ExecuteOpts) (*Run, error) {
	if opts.RunsDir == "" {
		opts.RunsDir = "runs"
	}

	runID := opts.RunID
	if runID == "" {
		runID = GenerateRunID(cfg.DAG.Name)
	}

	// Resolve the project source directory. For git-backed projects the repo
	// is cloned / updated in a persistent cache and that cache becomes the
	// source for the run snapshot. For local projects cfg.Dir() is used as
	// today, with no behaviour change.
	projectDir := cfg.Dir()
	if cfg.DAG.GitURL != "" {
		if opts.RepoCacheDir == "" {
			opts.RepoCacheDir = "repo_cache"
		}
		cacheDir := filepath.Join(opts.RepoCacheDir, cfg.DAG.Name)
		if err := gitrepo.Prepare(cfg.DAG.GitURL, cfg.DAG.GitRef, cacheDir); err != nil {
			return nil, fmt.Errorf("preparing git repo: %w", err)
		}
		projectDir = cacheDir
	}

	// Snapshot the project
	snapshotDir, logDir, dataDir, err := Snapshot(projectDir, opts.RunsDir, runID)
	if err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}

	// Seed data directory with files if configured
	if opts.DataSeedDir != "" {
		if err := copyDirContents(opts.DataSeedDir, dataDir); err != nil {
			return nil, fmt.Errorf("seeding data dir: %w", err)
		}
	}

	// Load secrets — detect encrypted (.age) vs plaintext
	var store *secrets.Store
	if opts.SecretsPath != "" {
		var err error
		if strings.HasSuffix(opts.SecretsPath, ".age") {
			store, err = secrets.LoadEncrypted(opts.SecretsPath, opts.AgeIdentity, "")
		} else {
			store, err = secrets.Load(opts.SecretsPath)
		}
		if err != nil {
			return nil, fmt.Errorf("loading secrets: %w", err)
		}
	}

	// Wire audit callback if metadata store is available
	if store != nil && opts.MetaStore != nil {
		dagName := cfg.DAG.Name
		currentRunID := runID
		store.OnAccess = func(e secrets.AuditEvent) {
			opts.MetaStore.RecordSecretAccess(e.Project, e.Key, dagName, "", currentRunID, time.Now())
		}
	}

	socketHint := filepath.Join(os.TempDir(), fmt.Sprintf("pit-%d.sock", os.Getpid()))
	sdkServer, err := sdk.NewServer(socketHint, store, cfg.DAG.Name)
	if err != nil {
		return nil, fmt.Errorf("starting SDK server: %w", err)
	}

	// Register the load_data handler for Python SDK → Go bulk load
	sdkServer.RegisterHandler("load_data", makeLoadDataHandler(store, cfg.DAG.Name, dataDir))

	// Register FTP handlers for Python SDK → Go FTP operations
	sdkServer.RegisterHandler("ftp_list", makeFTPListHandler(store, cfg.DAG.Name))
	sdkServer.RegisterHandler("ftp_download", makeFTPDownloadHandler(store, cfg.DAG.Name, dataDir))
	sdkServer.RegisterHandler("ftp_upload", makeFTPUploadHandler(store, cfg.DAG.Name, dataDir))
	sdkServer.RegisterHandler("ftp_move", makeFTPMoveHandler(store, cfg.DAG.Name))

	socketPath := sdkServer.Addr()
	sdkCtx, sdkCancel := context.WithCancel(context.Background())
	go sdkServer.Serve(sdkCtx)
	defer func() {
		sdkCancel()
		sdkServer.Shutdown()
	}()

	// Record environment file hashes
	if opts.MetaStore != nil {
		envFiles := map[string]string{
			"pit_toml":  filepath.Join(projectDir, "pit.toml"),
			"uv_lock":   filepath.Join(projectDir, "uv.lock"),
			"pyproject": filepath.Join(projectDir, "pyproject.toml"),
		}
		for hashType, path := range envFiles {
			hash := hashFile(path)
			if hash != "" {
				opts.MetaStore.RecordEnvSnapshot(cfg.DAG.Name, hashType, hash, runID)
			}
		}
	}

	// If this is a transform project, compile models and merge into task list
	if cfg.DAG.Transform != nil {
		modelsDir := filepath.Join(snapshotDir, "models")
		compiledDir := filepath.Join(snapshotDir, "compiled_models")

		compileResult, err := transform.Compile(modelsDir, cfg.DAG.Transform.Dialect, compiledDir, cfg.Tasks)
		if err != nil {
			fmt.Fprintf(os.Stderr, "transform compilation failed: %v\n", err)
			return nil, fmt.Errorf("compiling transform models: %w", err)
		}

		cfg.Tasks = buildTasksFromCompileResult(compileResult, cfg.Tasks)
	}

	// Build Run from config
	run := &Run{
		ID:          runID,
		DAGName:     cfg.DAG.Name,
		ProjectDir:  projectDir,
		SnapshotDir: snapshotDir,
		LogDir:      logDir,
		DataDir:     dataDir,
		Status:      StatusRunning,
		StartedAt:   time.Now(),
		SocketPath:  socketPath,
	}
	// Only assign when store is non-nil. Assigning a typed nil *secrets.Store
	// directly to the SecretsResolver interface produces a non-nil interface
	// value (it carries the type but a nil pointer), which defeats the nil
	// guard in executeTask and causes a nil-pointer panic in ResolveField.
	if store != nil {
		run.SecretsResolver = store
	}

	// Activate run in log hub so SSE clients can discover it
	if opts.LogHub != nil {
		opts.LogHub.Activate(runID)
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

	// Record run start in metadata store
	if opts.MetaStore != nil {
		trigger := opts.Trigger
		if trigger == "" {
			trigger = "manual"
		}
		runDir := filepath.Dir(snapshotDir)
		if err := opts.MetaStore.RecordRunStart(run.ID, run.DAGName, string(run.Status), runDir, trigger, run.StartedAt); err != nil {
			fmt.Fprintf(os.Stderr, "warning: metadata recording failed: %v\n", err)
		}
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

	// Record run end in metadata store
	if opts.MetaStore != nil {
		var errMsg string
		if run.Status == StatusFailed {
			for _, ti := range run.Tasks {
				if ti.Status == StatusFailed && ti.Error != nil {
					errMsg = ti.Error.Error()
					break
				}
			}
		}
		if err := opts.MetaStore.RecordRunEnd(run.ID, string(run.Status), run.EndedAt, errMsg); err != nil {
			fmt.Fprintf(os.Stderr, "warning: metadata recording failed: %v\n", err)
		}
	}

	// Record declared outputs on success
	if opts.MetaStore != nil && run.Status == StatusSuccess {
		for _, o := range cfg.Outputs {
			if err := opts.MetaStore.RecordOutput(run.ID, run.DAGName, o.Name, o.Type, o.Location); err != nil {
				fmt.Fprintf(os.Stderr, "warning: output metadata recording failed: %v\n", err)
			}
		}
	}

	printSummary(os.Stdout, run)

	// Signal hub that run is complete
	if opts.LogHub != nil {
		opts.LogHub.Complete(run.ID, string(run.Status))
	}

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

	// Record task start in metadata store
	if opts.MetaStore != nil {
		logPath := filepath.Join(run.LogDir, ti.Name+".log")
		opts.MetaStore.RecordTaskStart(run.ID, ti.Name, string(StatusRunning), logPath, ti.StartedAt)
		defer func() {
			run.mu.Lock()
			status := string(ti.Status)
			endedAt := ti.EndedAt
			attempts := ti.Attempt
			var errMsg string
			if ti.Error != nil {
				errMsg = ti.Error.Error()
			}
			run.mu.Unlock()
			opts.MetaStore.RecordTaskEnd(run.ID, ti.Name, status, endedAt, attempts, errMsg)
		}()
	}

	// Find the task config for load/save handling
	var tc *config.TaskConfig
	for i := range cfg.Tasks {
		if cfg.Tasks[i].Name == ti.Name {
			tc = &cfg.Tasks[i]
			break
		}
	}

	// Handle load/save SQL task types
	if tc != nil && (tc.Type == "load" || tc.Type == "save") {
		// Set up log file for load/save tasks
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

		writers := []io.Writer{logFile}
		if opts.Verbose {
			isConcurrent := len(concurrent) > 0 && concurrent[0]
			if isConcurrent {
				writers = append(writers, &prefixWriter{
					prefix: []byte("[" + ti.Name + "] "),
					dest:   os.Stdout,
				})
			} else {
				writers = append(writers, os.Stdout)
			}
		}
		if opts.LogHub != nil {
			hubWriter := loghub.NewWriter(opts.LogHub, run.ID, run.DAGName, ti.Name, 1)
			writers = append(writers, hubWriter)
		}
		var logWriter io.Writer = logFile
		if len(writers) > 1 {
			logWriter = io.MultiWriter(writers...)
		}

		err = executeSQLTask(ctx, ti, run, cfg, tc, opts, logWriter)
		run.mu.Lock()
		if err != nil {
			ti.Status = StatusFailed
			ti.Error = err
		} else {
			ti.Status = StatusSuccess
		}
		ti.Attempt = 1
		ti.EndedAt = time.Now()
		run.mu.Unlock()
		return
	}

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
			DAGName:    run.DAGName,
			Profile:    cfg.DAG.DBT.Profile,
			Target:     cfg.DAG.DBT.Target,
			Driver:     opts.DBTDriver,
			Connection: cfg.DAG.DBT.Connection,
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

	// Set up log writer — optionally tee to stdout and/or hub
	writers := []io.Writer{logFile}
	var hubWriter *loghub.Writer
	if opts.Verbose {
		isConcurrent := len(concurrent) > 0 && concurrent[0]
		if isConcurrent {
			writers = append(writers, &prefixWriter{
				prefix: []byte("[" + ti.Name + "] "),
				dest:   os.Stdout,
			})
		} else {
			writers = append(writers, os.Stdout)
		}
	}
	if opts.LogHub != nil {
		hubWriter = loghub.NewWriter(opts.LogHub, run.ID, run.DAGName, ti.Name, 1)
		writers = append(writers, hubWriter)
	}
	var logWriter io.Writer = logFile
	if len(writers) > 1 {
		logWriter = io.MultiWriter(writers...)
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
		OrigProjectDir:  run.ProjectDir,
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
		if hubWriter != nil {
			hubWriter.SetAttempt(attempt)
		}

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

// buildTasksFromCompileResult converts a transform CompileResult into a merged task list.
// Ephemeral models are excluded. Model tasks are built from the DAG order, with settings
// merged from any matching explicit task in existingTasks. Non-model tasks from
// existingTasks are appended after all model tasks.
func buildTasksFromCompileResult(result *transform.CompileResult, existingTasks []config.TaskConfig) []config.TaskConfig {
	var modelTasks []config.TaskConfig
	for _, name := range result.Order {
		cm, ok := result.Models[name]
		if !ok {
			continue // ephemeral model, no compiled output
		}
		tc := config.TaskConfig{
			Name:   name,
			Script: filepath.Join("compiled_models", name+".sql"),
			Runner: "sql",
		}
		// Inherit dependencies from the model DAG, excluding ephemeral models.
		// Ephemerals are inlined as CTEs and produce no executable tasks, so
		// leaving their names in DependsOn would create unresolvable references.
		deps := result.DAG.DependsOn(name)
		var filteredDeps []string
		for _, dep := range deps {
			if _, isCompiled := result.Models[dep]; isCompiled {
				filteredDeps = append(filteredDeps, dep)
			}
		}
		tc.DependsOn = filteredDeps

		// Merge with any explicit task config from pit.toml (timeout, retries,
		// extra depends_on, etc.)
		for _, explicit := range existingTasks {
			if explicit.Name == name {
				if explicit.Timeout.Duration > 0 {
					tc.Timeout = explicit.Timeout
				}
				if explicit.Retries > 0 {
					tc.Retries = explicit.Retries
				}
				if explicit.RetryDelay.Duration > 0 {
					tc.RetryDelay = explicit.RetryDelay
				}
				// Append any extra depends_on entries declared in pit.toml that
				// are not already covered by the model's DAG-derived dependencies.
				if len(explicit.DependsOn) > 0 {
					existing := make(map[string]bool, len(tc.DependsOn))
					for _, d := range tc.DependsOn {
						existing[d] = true
					}
					for _, d := range explicit.DependsOn {
						if !existing[d] {
							tc.DependsOn = append(tc.DependsOn, d)
						}
					}
				}
				break
			}
		}

		if cm.Config.Connection != "" {
			tc.Connection = cm.Config.Connection
		}

		modelTasks = append(modelTasks, tc)
	}

	// Append non-model tasks from pit.toml (Python tasks, shell tasks, etc.)
	modelNames := make(map[string]bool, len(result.Order))
	for _, name := range result.Order {
		modelNames[name] = true
	}
	for _, tc := range existingTasks {
		if !modelNames[tc.Name] {
			modelTasks = append(modelTasks, tc)
		}
	}

	return modelTasks
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

		schema := params["schema"]
		if schema == "" {
			driverName, _ := runner.DetectDriver(connStr)
			if drv, drvErr := loader.GetDriver(driverName); drvErr == nil {
				schema = drv.DefaultSchema()
			}
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

// resolveTaskConnection returns the connection key for a task, falling back to DAG default.
func resolveTaskConnection(tc *config.TaskConfig, cfg *config.ProjectConfig) string {
	if tc.Connection != "" {
		return tc.Connection
	}
	return cfg.DAG.SQL.Connection
}

// parseSchemaTable splits "schema.table" into schema and table parts.
// If no dot, returns empty schema and the full string as table.
func parseSchemaTable(fqTable string) (string, string) {
	parts := strings.SplitN(fqTable, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

// executeSQLTask handles load and save task types.
func executeSQLTask(ctx context.Context, ti *TaskInstance, run *Run, cfg *config.ProjectConfig, tc *config.TaskConfig, opts ExecuteOpts, logWriter io.Writer) error {
	connKey := resolveTaskConnection(tc, cfg)
	if connKey == "" {
		return fmt.Errorf("no connection configured (set connection on task or [dag.sql])")
	}
	if run.SecretsResolver == nil {
		return fmt.Errorf("secrets store not configured (use --secrets flag)")
	}

	connStr, err := run.SecretsResolver.Resolve(run.DAGName, connKey)
	if err != nil {
		return fmt.Errorf("resolving connection %q: %w", connKey, err)
	}

	start := time.Now()

	switch tc.Type {
	case "load":
		sourcePath := filepath.Join(run.DataDir, tc.Source)
		schema, table := parseSchemaTable(tc.Table)
		mode := tc.Mode
		if mode == "" {
			mode = "append"
		}
		rows, err := loader.Load(ctx, loader.LoadParams{
			FilePath: sourcePath,
			Table:    table,
			Schema:   schema,
			Mode:     loader.LoadMode(mode),
			ConnStr:  connStr,
		})
		if err != nil {
			return fmt.Errorf("loading data: %w", err)
		}
		elapsed := time.Since(start)
		fmt.Fprintf(logWriter, "[load] %s -> %s: %d rows loaded in %s\n",
			tc.Source, tc.Table, rows, elapsed.Round(time.Millisecond))

	case "save":
		scriptPath := filepath.Join(run.SnapshotDir, tc.Script)
		query, err := os.ReadFile(scriptPath)
		if err != nil {
			return fmt.Errorf("reading SQL script %s: %w", tc.Script, err)
		}
		outputPath := filepath.Join(run.DataDir, tc.Output)
		rows, err := loader.Save(ctx, loader.SaveParams{
			Query:    string(query),
			FilePath: outputPath,
			ConnStr:  connStr,
		})
		if err != nil {
			return fmt.Errorf("saving data: %w", err)
		}
		elapsed := time.Since(start)
		fmt.Fprintf(logWriter, "[save] %s -> %s: %d rows saved in %s\n",
			tc.Script, tc.Output, rows, elapsed.Round(time.Millisecond))
	}

	return nil
}

// hashFile returns the SHA-256 hex digest of the file at path, or "" on error.
func hashFile(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
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
