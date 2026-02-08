package cli

import (
	"sort"
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func TestParseRunArg(t *testing.T) {
	tests := []struct {
		name     string
		arg      string
		wantDAG  string
		wantTask string
		wantErr  bool
	}{
		{name: "dag only", arg: "my_dag", wantDAG: "my_dag", wantTask: ""},
		{name: "dag and task", arg: "my_dag/hello", wantDAG: "my_dag", wantTask: "hello"},
		{name: "trailing slash", arg: "my_dag/", wantErr: true},
		{name: "empty string", arg: "", wantErr: true},
		{name: "slash only", arg: "/task", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag, task, err := parseRunArg(tt.arg)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseRunArg(%q) expected error, got nil", tt.arg)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRunArg(%q) unexpected error: %v", tt.arg, err)
			}
			if dag != tt.wantDAG {
				t.Errorf("parseRunArg(%q) dag = %q, want %q", tt.arg, dag, tt.wantDAG)
			}
			if task != tt.wantTask {
				t.Errorf("parseRunArg(%q) task = %q, want %q", tt.arg, task, tt.wantTask)
			}
		})
	}
}

func TestAvailableDAGs(t *testing.T) {
	configs := map[string]*config.ProjectConfig{
		"charlie": {},
		"alpha":   {},
		"bravo":   {},
	}

	result := availableDAGs(configs)

	// Should be sorted
	expected := "alpha, bravo, charlie"
	if result != expected {
		t.Errorf("availableDAGs() = %q, want %q", result, expected)
	}
}

func TestAvailableDAGs_Empty(t *testing.T) {
	configs := map[string]*config.ProjectConfig{}
	result := availableDAGs(configs)
	if result != "" {
		t.Errorf("availableDAGs() = %q, want empty string", result)
	}
}

func TestAvailableDAGs_Sorted(t *testing.T) {
	// Verify the output is always sorted regardless of map iteration order
	configs := map[string]*config.ProjectConfig{
		"z": {}, "a": {}, "m": {}, "b": {},
	}

	result := availableDAGs(configs)

	// Parse and verify sorted
	names := splitCSV(result)
	if !sort.StringsAreSorted(names) {
		t.Errorf("availableDAGs() names not sorted: %v", names)
	}
}

// splitCSV splits a comma-separated string, trimming spaces.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			result = append(result, trimSpace(s[start:i]))
			start = i + 1
		}
	}
	result = append(result, trimSpace(s[start:]))
	return result
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && s[start] == ' ' {
		start++
	}
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}
