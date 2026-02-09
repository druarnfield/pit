package runner

import (
	"testing"
)

func TestResolve_ExplicitRunner(t *testing.T) {
	tests := []struct {
		name       string
		runner     string
		script     string
		wantType   string
		wantErr    bool
		errContain string
	}{
		{name: "python", runner: "python", script: "x.py", wantType: "*runner.PythonRunner"},
		{name: "bash", runner: "bash", script: "x.sh", wantType: "*runner.ShellRunner"},
		{name: "sql", runner: "sql", script: "x.sql", wantType: "*runner.SQLRunner"},
		{name: "custom", runner: "$ node", script: "x.js", wantType: "*runner.CustomRunner"},
		{name: "custom with args", runner: "$ dbt run --target", script: "x.sql", wantType: "*runner.CustomRunner"},
		{name: "empty custom", runner: "$ ", script: "x.sh", wantErr: true, errContain: "empty"},
		{name: "unknown", runner: "ruby", script: "x.rb", wantErr: true, errContain: "unknown runner"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Resolve(tt.runner, tt.script)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Resolve(%q, %q) expected error containing %q, got nil",
						tt.runner, tt.script, tt.errContain)
				} else if tt.errContain != "" && !containsStr(err.Error(), tt.errContain) {
					t.Errorf("Resolve() error = %q, want it to contain %q", err, tt.errContain)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve(%q, %q) unexpected error: %v", tt.runner, tt.script, err)
			}
			got := typeName(r)
			if got != tt.wantType {
				t.Errorf("Resolve(%q, %q) type = %s, want %s", tt.runner, tt.script, got, tt.wantType)
			}
		})
	}
}

func TestResolve_ExtensionDispatch(t *testing.T) {
	tests := []struct {
		name     string
		script   string
		wantType string
		wantErr  bool
	}{
		{name: "py", script: "tasks/hello.py", wantType: "*runner.PythonRunner"},
		{name: "sh", script: "tasks/hello.sh", wantType: "*runner.ShellRunner"},
		{name: "sql", script: "tasks/query.sql", wantType: "*runner.SQLRunner"},
		{name: "unknown ext", script: "tasks/run.rb", wantErr: true},
		{name: "no ext", script: "tasks/Makefile", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Resolve("", tt.script)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Resolve('', %q) expected error, got nil", tt.script)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve('', %q) unexpected error: %v", tt.script, err)
			}
			got := typeName(r)
			if got != tt.wantType {
				t.Errorf("Resolve('', %q) type = %s, want %s", tt.script, got, tt.wantType)
			}
		})
	}
}

func TestResolve_Singletons(t *testing.T) {
	// Stateless runners should return the same instance
	r1, _ := Resolve("python", "x.py")
	r2, _ := Resolve("python", "y.py")
	if r1 != r2 {
		t.Error("Resolve('python', ...) should return the same PythonRunner instance")
	}

	r3, _ := Resolve("", "x.sh")
	r4, _ := Resolve("bash", "y.sh")
	if r3 != r4 {
		t.Error("Resolve for shell should return the same ShellRunner instance")
	}
}

func TestValidateScript(t *testing.T) {
	tests := []struct {
		name        string
		snapshotDir string
		scriptPath  string
		wantErr     bool
	}{
		{
			name:        "valid path within snapshot",
			snapshotDir: "/runs/123/project",
			scriptPath:  "/runs/123/project/tasks/hello.sh",
			wantErr:     false,
		},
		{
			name:        "path traversal escapes snapshot",
			snapshotDir: "/runs/123/project",
			scriptPath:  "/runs/123/project/../../etc/passwd",
			wantErr:     true,
		},
		{
			name:        "completely outside snapshot",
			snapshotDir: "/runs/123/project",
			scriptPath:  "/etc/passwd",
			wantErr:     true,
		},
		{
			name:        "snapshot root itself",
			snapshotDir: "/runs/123/project",
			scriptPath:  "/runs/123/project",
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := RunContext{
				SnapshotDir: tt.snapshotDir,
				ScriptPath:  tt.scriptPath,
			}
			err := rc.ValidateScript()
			if tt.wantErr && err == nil {
				t.Errorf("ValidateScript() expected error for script %q in snapshot %q", tt.scriptPath, tt.snapshotDir)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidateScript() unexpected error: %v", err)
			}
		})
	}
}

// typeName returns the type name of a value as a string for comparison.
func typeName(v interface{}) string {
	return typeNameFmt(v)
}

func typeNameFmt(v interface{}) string {
	switch v.(type) {
	case *ShellRunner:
		return "*runner.ShellRunner"
	case *PythonRunner:
		return "*runner.PythonRunner"
	case *SQLRunner:
		return "*runner.SQLRunner"
	case *CustomRunner:
		return "*runner.CustomRunner"
	default:
		return "unknown"
	}
}

func TestDetectDriver(t *testing.T) {
	tests := []struct {
		name       string
		connStr    string
		wantDriver string
		wantErr    bool
	}{
		{name: "sqlserver uri", connStr: "sqlserver://user:pass@host:1433?database=db", wantDriver: "mssql"},
		{name: "mssql uri", connStr: "mssql://user:pass@host/db", wantDriver: "mssql"},
		{name: "sqlserver uppercase", connStr: "SQLSERVER://HOST/DB", wantDriver: "mssql"},
		{name: "unknown scheme", connStr: "postgres://host/db", wantErr: true},
		{name: "plain string", connStr: "just-a-string", wantErr: true},
		{name: "duckdb uri", connStr: "duckdb:///path/to/db", wantErr: true},
		{name: "db file path", connStr: "/data/warehouse.db", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := DetectDriver(tt.connStr)
			if tt.wantErr {
				if err == nil {
					t.Errorf("DetectDriver(%q) expected error, got nil", tt.connStr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DetectDriver(%q) unexpected error: %v", tt.connStr, err)
			}
			if driver != tt.wantDriver {
				t.Errorf("DetectDriver(%q) = %q, want %q", tt.connStr, driver, tt.wantDriver)
			}
		})
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
