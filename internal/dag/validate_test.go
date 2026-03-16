package dag

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func TestValidate_ValidChain(t *testing.T) {
	cfg := loadTestdata(t, "valid_chain")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) != 0 {
		t.Errorf("Validate() returned %d errors, want 0:", len(errs))
		for _, e := range errs {
			t.Errorf("  %s", e)
		}
	}
}

func TestValidate_CycleDetection(t *testing.T) {
	cfg := loadTestdata(t, "cycle")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) == 0 {
		t.Fatal("Validate() returned no errors, want cycle error")
	}

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "cycle") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() errors do not mention cycle: %v", errs)
	}
}

func TestValidate_MissingDependency(t *testing.T) {
	cfg := loadTestdata(t, "missing_dep")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) == 0 {
		t.Fatal("Validate() returned no errors, want missing dependency error")
	}

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "nonexistent") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() errors do not mention 'nonexistent': %v", errs)
	}
}

func TestValidate_DuplicateTask(t *testing.T) {
	cfg := loadTestdata(t, "duplicate_task")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) == 0 {
		t.Fatal("Validate() returned no errors, want duplicate task error")
	}

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "duplicate") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() errors do not mention 'duplicate': %v", errs)
	}
}

func TestValidate_MissingName(t *testing.T) {
	cfg := loadTestdata(t, "no_name")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) == 0 {
		t.Fatal("Validate() returned no errors, want missing name error")
	}

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "dag.name is required") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() errors do not mention 'dag.name is required': %v", errs)
	}
}

func TestValidate_InvalidOverlap(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:    "test",
			Overlap: "invalid_value",
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) == 0 {
		t.Fatal("Validate() returned no errors, want invalid overlap error")
	}

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "invalid dag.overlap") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() errors do not mention invalid overlap: %v", errs)
	}
}

func TestValidate_MissingScript(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "a", Script: "tasks/nonexistent.sh"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) == 0 {
		t.Fatal("Validate() returned no errors, want missing script error")
	}

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "not found") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() errors do not mention script not found: %v", errs)
	}
}

func TestValidationError_Error(t *testing.T) {
	t.Run("with task", func(t *testing.T) {
		e := &ValidationError{DAG: "mydag", Task: "mytask", Message: "something broke"}
		got := e.Error()
		if !strings.Contains(got, "mydag") || !strings.Contains(got, "mytask") || !strings.Contains(got, "something broke") {
			t.Errorf("Error() = %q, want it to contain DAG, task, and message", got)
		}
	})

	t.Run("without task", func(t *testing.T) {
		e := &ValidationError{DAG: "mydag", Message: "dag-level issue"}
		got := e.Error()
		if !strings.Contains(got, "mydag") || !strings.Contains(got, "dag-level issue") {
			t.Errorf("Error() = %q, want it to contain DAG and message", got)
		}
		if strings.Contains(got, "task") {
			t.Errorf("Error() = %q, should not contain 'task' when Task is empty", got)
		}
	})
}

func TestValidate_ValidCronSchedule(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:     "test",
			Schedule: "0 6 * * *",
		},
		Tasks: []config.TaskConfig{
			{Name: "a", Script: ""},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "schedule") {
			t.Errorf("Validate() unexpected schedule error: %s", e)
		}
	}
}

func TestValidate_InvalidCronSchedule(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:     "test",
			Schedule: "not a cron expression",
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "invalid schedule") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Validate() expected 'invalid schedule' error, got none")
	}
}

func TestValidate_FTPWatch_MissingFields(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			FTPWatch: &config.FTPWatchConfig{
				// All required fields empty
			},
		},
	}
	errs := Validate(cfg, t.TempDir())

	requiredFields := []string{
		"ftp_watch.host",
		"ftp_watch.user",
		"ftp_watch.password_secret",
		"ftp_watch.directory",
		"ftp_watch.pattern",
	}
	for _, field := range requiredFields {
		found := false
		for _, e := range errs {
			if strings.Contains(e.Error(), field) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Validate() missing error for %s", field)
		}
	}
}

func TestValidate_FTPWatch_Defaults(t *testing.T) {
	fw := &config.FTPWatchConfig{
		Host:           "ftp.example.com",
		User:           "user",
		PasswordSecret: "pass",
		Directory:      "/data",
		Pattern:        "*.csv",
	}
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:     "test",
			FTPWatch: fw,
		},
	}
	Validate(cfg, t.TempDir())

	if fw.Port != 21 {
		t.Errorf("FTPWatch.Port = %d, want 21 (default)", fw.Port)
	}
	if fw.StableSeconds != 30 {
		t.Errorf("FTPWatch.StableSeconds = %d, want 30 (default)", fw.StableSeconds)
	}
	if fw.PollInterval.Duration == 0 {
		t.Error("FTPWatch.PollInterval should be defaulted, got 0")
	}
}

func TestValidate_FTPWatch_ValidComplete(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			FTPWatch: &config.FTPWatchConfig{
				Host:           "ftp.example.com",
				Port:           2121,
				User:           "user",
				PasswordSecret: "ftp_pass",
				Directory:      "/incoming",
				Pattern:        "data_*.csv",
				StableSeconds:  60,
			},
		},
		Tasks: []config.TaskConfig{
			{Name: "process"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "ftp_watch") {
			t.Errorf("Validate() unexpected ftp_watch error: %s", e)
		}
	}
}

func TestValidate_KeepArtifacts_Valid(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:          "test",
			KeepArtifacts: []string{"logs", "data"},
		},
		Tasks: []config.TaskConfig{
			{Name: "a"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "keep_artifacts") {
			t.Errorf("Validate() unexpected keep_artifacts error: %s", e)
		}
	}
}

func TestValidate_KeepArtifacts_Invalid(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:          "test",
			KeepArtifacts: []string{"logs", "snapshots"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "snapshots") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Validate() expected error for invalid keep_artifacts value 'snapshots'")
	}
}

func TestValidate_ValidDBT(t *testing.T) {
	cfg := loadTestdata(t, "valid_dbt")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) != 0 {
		t.Errorf("Validate() returned %d errors, want 0:", len(errs))
		for _, e := range errs {
			t.Errorf("  %s", e)
		}
	}
}

func TestValidate_DBT_MissingFields(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			DBT: &config.DBTConfig{
				// All required fields empty
			},
		},
	}
	errs := Validate(cfg, t.TempDir())

	requiredFields := []string{
		"dbt.version",
		"dbt.adapter",
	}
	for _, field := range requiredFields {
		found := false
		for _, e := range errs {
			if strings.Contains(e.Error(), field) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Validate() missing error for %s", field)
		}
	}
}

func TestValidate_DBT_ProjectDirNotExists(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			DBT: &config.DBTConfig{
				Version:    "1.9.1",
				Adapter:    "dbt-sqlserver",
				ProjectDir: "nonexistent_dir",
			},
		},
	}
	errs := Validate(cfg, t.TempDir())

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "not found") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected error for missing project_dir, got: %v", errs)
	}
}

func TestValidate_DBT_TaskEmptyScript(t *testing.T) {
	tmpDir := t.TempDir()
	// Create the dbt project dir
	os.MkdirAll(tmpDir+"/dbt_repo", 0o755)

	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			DBT: &config.DBTConfig{
				Version:    "1.9.1",
				Adapter:    "dbt-sqlserver",
				ProjectDir: "dbt_repo",
			},
		},
		Tasks: []config.TaskConfig{
			{Name: "empty_dbt_task", Script: "", Runner: "dbt"},
		},
	}
	errs := Validate(cfg, tmpDir)

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "non-empty script") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected error for empty dbt script, got: %v", errs)
	}
}

func TestValidate_DBT_TaskWithScript(t *testing.T) {
	tmpDir := t.TempDir()
	os.MkdirAll(tmpDir+"/dbt_repo", 0o755)

	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			DBT: &config.DBTConfig{
				Version:    "1.9.1",
				Adapter:    "dbt-sqlserver",
				ProjectDir: "dbt_repo",
			},
		},
		Tasks: []config.TaskConfig{
			{Name: "run_staging", Script: "run --select staging", Runner: "dbt"},
		},
	}
	errs := Validate(cfg, tmpDir)

	for _, e := range errs {
		if strings.Contains(e.Error(), "dbt") && strings.Contains(e.Error(), "script") {
			t.Errorf("Validate() unexpected dbt script error: %s", e)
		}
	}
}

func TestValidate_GitURL_SkipsScriptCheck(t *testing.T) {
	// Script path does not exist on disk — but git_url is set, so the check
	// should be skipped and no error reported.
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:   "test",
			GitURL: "git@github.com:example/repo.git",
			GitRef: "main",
		},
		Tasks: []config.TaskConfig{
			{Name: "extract", Script: "tasks/nonexistent.py"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "not found") {
			t.Errorf("Validate() should skip script existence check for git-backed project, got: %s", e)
		}
	}
}

func TestValidate_GitURL_PairRequired(t *testing.T) {
	tests := []struct {
		name   string
		gitURL string
		gitRef string
	}{
		{name: "url without ref", gitURL: "git@github.com:example/repo.git", gitRef: ""},
		{name: "ref without url", gitURL: "", gitRef: "main"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.ProjectConfig{
				DAG: config.DAGConfig{
					Name:   "test",
					GitURL: tt.gitURL,
					GitRef: tt.gitRef,
				},
			}
			errs := Validate(cfg, t.TempDir())
			found := false
			for _, e := range errs {
				if strings.Contains(e.Error(), "git_url") && strings.Contains(e.Error(), "git_ref") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Validate() expected error about git_url/git_ref pair, got: %v", errs)
			}
		})
	}
}

func TestValidate_GitURL_DBTSkipsDirCheck(t *testing.T) {
	// dbt.project_dir does not exist on disk — git_url is set so the
	// filesystem check should be skipped.
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:   "test",
			GitURL: "git@github.com:example/repo.git",
			GitRef: "main",
			DBT: &config.DBTConfig{
				Version:    "1.9.1",
				Adapter:    "dbt-sqlserver",
				ProjectDir: "dbt_repo_not_on_disk",
			},
		},
		Tasks: []config.TaskConfig{
			{Name: "run_models", Script: "run", Runner: "dbt"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "not found") || strings.Contains(e.Error(), "not a directory") {
			t.Errorf("Validate() should skip dbt.project_dir check for git-backed project, got: %s", e)
		}
	}
}

func TestValidate_Webhook_MissingTokenSecret(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			Webhook: &config.WebhookConfig{
				// TokenSecret intentionally empty
			},
		},
	}
	errs := Validate(cfg, t.TempDir())

	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "webhook.token_secret") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() missing error for webhook.token_secret, got: %v", errs)
	}
}

func TestValidate_Webhook_ValidConfig(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name: "test",
			Webhook: &config.WebhookConfig{
				TokenSecret: "my_token",
			},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "webhook") {
			t.Errorf("Validate() unexpected webhook error: %s", e)
		}
	}
}

func TestValidate_GitURL_DBTEmptyProjectDir(t *testing.T) {
	// project_dir is optional for git-backed DAGs; empty means use repo root.
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{
			Name:   "test",
			GitURL: "git@github.com:example/repo.git",
			GitRef: "main",
			DBT: &config.DBTConfig{
				Version: "1.9.1",
				Adapter: "dbt-sqlserver",
				// ProjectDir intentionally empty
			},
		},
		Tasks: []config.TaskConfig{
			{Name: "run_models", Script: "run", Runner: "dbt"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	for _, e := range errs {
		if strings.Contains(e.Error(), "project_dir") {
			t.Errorf("Validate() unexpected project_dir error for git-backed DAG: %s", e)
		}
	}
}

func TestValidate_LoadTask_Valid(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data/output.parquet", Table: "staging.raw_data", Mode: "append"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	if len(errs) != 0 {
		t.Errorf("Validate() returned %d errors, want 0:", len(errs))
		for _, e := range errs {
			t.Errorf("  %s", e)
		}
	}
}

func TestValidate_LoadTask_MissingSource(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Table: "staging.raw_data"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "load task requires source") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'load task requires source' error, got: %v", errs)
	}
}

func TestValidate_LoadTask_MissingTable(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data/output.parquet"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "load task requires table") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'load task requires table' error, got: %v", errs)
	}
}

func TestValidate_LoadTask_ScriptNotAllowed(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data/output.parquet", Table: "staging.raw_data", Script: "tasks/load.sh"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "load task must not have script") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'load task must not have script' error, got: %v", errs)
	}
}

func TestValidate_SaveTask_Valid(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "tasks")
	os.MkdirAll(scriptPath, 0o755)
	os.WriteFile(filepath.Join(scriptPath, "extract.sql"), []byte("SELECT 1"), 0o644)

	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "save_data", Type: "save", Script: "tasks/extract.sql", Output: "data/output.parquet"},
		},
	}
	errs := Validate(cfg, tmpDir)
	if len(errs) != 0 {
		t.Errorf("Validate() returned %d errors, want 0:", len(errs))
		for _, e := range errs {
			t.Errorf("  %s", e)
		}
	}
}

func TestValidate_SaveTask_MissingOutput(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "tasks")
	os.MkdirAll(scriptPath, 0o755)
	os.WriteFile(filepath.Join(scriptPath, "extract.sql"), []byte("SELECT 1"), 0o644)

	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "save_data", Type: "save", Script: "tasks/extract.sql"},
		},
	}
	errs := Validate(cfg, tmpDir)
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "save task requires output") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'save task requires output' error, got: %v", errs)
	}
}

func TestValidate_SaveTask_MissingScript(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "save_data", Type: "save", Output: "data/output.parquet"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "save task requires script") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'save task requires script' error, got: %v", errs)
	}
}

func TestValidate_InvalidTaskType(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "bad_task", Type: "unknown"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "invalid task type") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'invalid task type' error, got: %v", errs)
	}
}

func TestValidate_LoadTask_InvalidMode(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "load_data", Type: "load", Source: "data/output.parquet", Table: "staging.raw_data", Mode: "invalid"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "invalid mode") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'invalid mode' error, got: %v", errs)
	}
}

func TestValidate_ModeOnNonLoadTask(t *testing.T) {
	cfg := &config.ProjectConfig{
		DAG: config.DAGConfig{Name: "test"},
		Tasks: []config.TaskConfig{
			{Name: "regular_task", Mode: "append"},
		},
	}
	errs := Validate(cfg, t.TempDir())
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "mode is only valid on") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Validate() expected 'mode is only valid on' error, got: %v", errs)
	}
}

func TestValidate_TransformValid(t *testing.T) {
	cfg := loadTestdata(t, "transform_valid")
	errs := Validate(cfg, cfg.Dir())
	if len(errs) != 0 {
		t.Errorf("Validate() returned %d errors, want 0:", len(errs))
		for _, e := range errs {
			t.Errorf("  %s", e)
		}
	}
}

func TestValidate_TransformNoSQL(t *testing.T) {
	dir := t.TempDir()
	// Create models/ dir so that check passes
	os.MkdirAll(filepath.Join(dir, "models"), 0o755)

	cfg := &config.ProjectConfig{}
	cfg.DAG.Name = "test"
	cfg.DAG.Transform = &config.TransformConfig{Dialect: "mssql"}
	// No [dag.sql] connection

	errs := Validate(cfg, dir)
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "connection") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected validation error about missing SQL connection, got: %v", errs)
	}
}

func TestValidate_TransformNoDialect(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "models"), 0o755)

	cfg := &config.ProjectConfig{}
	cfg.DAG.Name = "test"
	cfg.DAG.SQL.Connection = "test_db"
	cfg.DAG.Transform = &config.TransformConfig{} // empty dialect

	errs := Validate(cfg, dir)
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "dialect") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected validation error about missing dialect, got: %v", errs)
	}
}

func TestValidate_TransformNoModelsDir(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.ProjectConfig{}
	cfg.DAG.Name = "test"
	cfg.DAG.SQL.Connection = "test_db"
	cfg.DAG.Transform = &config.TransformConfig{Dialect: "mssql"}
	// No models/ directory exists

	errs := Validate(cfg, dir)
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "models") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected validation error about missing models directory, got: %v", errs)
	}
}

// loadTestdata loads a ProjectConfig from testdata/<name>/pit.toml.
func loadTestdata(t *testing.T, name string) *config.ProjectConfig {
	t.Helper()
	cfg, err := config.Load("testdata/" + name + "/pit.toml")
	if err != nil {
		t.Fatalf("loading testdata/%s: %v", name, err)
	}
	return cfg
}
