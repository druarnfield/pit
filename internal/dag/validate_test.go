package dag

import (
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

// loadTestdata loads a ProjectConfig from testdata/<name>/pit.toml.
func loadTestdata(t *testing.T, name string) *config.ProjectConfig {
	t.Helper()
	cfg, err := config.Load("testdata/" + name + "/pit.toml")
	if err != nil {
		t.Fatalf("loading testdata/%s: %v", name, err)
	}
	return cfg
}
