---
name: write-tests
description: Write Go tests for the Pit project following established patterns. Use when adding tests for new or existing code in any internal package.
user-invocable: true
argument-hint: [package-or-file]
allowed-tools: Read, Grep, Glob, Bash(go test *), Bash(go vet *), Write, Edit
---

# Write Tests for Pit

Write Go tests for `$ARGUMENTS` following the project's established testing conventions.

## Step 1: Understand what to test

Read the target source file(s) to understand:
- All public functions, methods, and types
- All unexported functions that have non-trivial logic
- Error paths and edge cases
- Any concurrency or file I/O

## Step 2: Read existing tests in the same package

Check if `*_test.go` files already exist in the package. If so, read them to understand:
- What's already covered
- What helpers are available (e.g., `mkTestProject`)
- The style and conventions in use

## Step 3: Determine test category

For each function to test, classify it:

**Unit test** (no build tag):
- Pure logic: parsing, validation, type dispatch, string manipulation
- Functions that take all inputs as parameters (no global state)
- Functions that operate on `io.Writer`, `io.Reader` (use `bytes.Buffer`)
- File I/O tests that can use `t.TempDir()` or `testdata/` fixtures

**Integration test** (`//go:build integration`):
- Tests that execute external processes (bash, uv, python, custom runners)
- Tests that require a full `engine.Execute()` end-to-end run
- Tests that depend on external tools being installed

## Step 4: Write the tests

Follow these patterns exactly:

### File naming
- Test file goes in the same directory as the source: `foo_test.go` next to `foo.go`
- Use the same package name (not `package foo_test`)
- Integration test files: `foo_integration_test.go` with `//go:build integration` at the top

### Test structure
```go
func TestFunctionName(t *testing.T) {
    // single case
}

func TestFunctionName_Variant(t *testing.T) {
    // specific scenario
}
```

### Table-driven tests (use for 3+ cases)
```go
func TestFunctionName(t *testing.T) {
    tests := []struct {
        name    string
        input   InputType
        want    OutputType
        wantErr bool
    }{
        {name: "descriptive name", input: ..., want: ...},
        {name: "error case", input: ..., wantErr: true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := FunctionName(tt.input)
            if tt.wantErr {
                if err == nil {
                    t.Errorf("FunctionName(%v) expected error, got nil", tt.input)
                }
                return
            }
            if err != nil {
                t.Fatalf("FunctionName(%v) unexpected error: %v", tt.input, err)
            }
            if got != tt.want {
                t.Errorf("FunctionName(%v) = %v, want %v", tt.input, got, tt.want)
            }
        })
    }
}
```

### Assertions — stdlib only
```go
// Use t.Fatalf for setup failures (stops the test)
if err != nil {
    t.Fatalf("setup failed: %v", err)
}

// Use t.Errorf for assertion failures (continues)
if got != want {
    t.Errorf("Function() = %v, want %v", got, want)
}

// For error content
if !strings.Contains(err.Error(), "expected") {
    t.Errorf("error = %q, want it to contain %q", err, "expected")
}
```

### File I/O tests
```go
// For tests that READ files — use testdata/
cfg, err := Load(filepath.Join("testdata", "valid_minimal.toml"))

// For tests that WRITE files — use t.TempDir()
root := t.TempDir()
// ... create files in root ...
```

### Test helpers
```go
func helperName(t *testing.T, args ...) ReturnType {
    t.Helper()  // REQUIRED — makes failures report at caller's line
    // ... helper logic ...
}
```

### Functions with io.Writer
```go
var buf bytes.Buffer
functionThatWrites(&buf, args...)
output := buf.String()
if !strings.Contains(output, "expected content") {
    t.Errorf("output = %q, want it to contain %q", output, "expected content")
}
```

## Step 5: Per-package specifics

### config
- Load testdata TOML files and verify parsed struct fields
- Test Duration.UnmarshalText with table-driven tests
- Test Discover using t.TempDir() with programmatic project dirs
- Helper: `mkTestProject(t, dir, tomlContent string)`

### dag
- Load testdata project dirs (`testdata/<scenario>/pit.toml`)
- Each scenario dir must include `tasks/*.sh` stubs since Validate checks file existence
- Test both `Validate()` with loaded configs and with inline `config.ProjectConfig{}`
- Check error messages with `strings.Contains`

### runner
- Test `Resolve()` dispatch: explicit runner field, extension fallback, error cases
- Test `ValidateScript()` with path traversal scenarios
- Verify singleton behavior: `Resolve("python", "x.py")` returns same instance
- Actual `.Run()` methods go in integration tests

### engine
- Test `topoSort()`: linear chain, parallel tasks, cycles, single task
- Test `hasUpstreamFailure()` with pre-built status maps
- Test `prefixWriter` with single lines, partial lines, multi-line writes
- Test `printSummary()` writing to `bytes.Buffer`
- Test `Snapshot()` and `copyFile()` with t.TempDir()
- Full `Execute()` goes in integration tests

### scaffold
- Test `Create()` with t.TempDir() for all project types
- Verify expected files exist after creation
- Test `ValidType()` with table-driven tests
- Test name validation: valid and invalid names
- Test duplicate project detection

### cli
- Test `parseRunArg()`: valid inputs, empty dag, trailing slash
- Test `availableDAGs()`: sorting, empty map
- Full command tests go in integration tests

## Step 6: Verify

After writing tests, run:
```bash
go test -race -v ./internal/<package>
go vet ./internal/<package>
```

Both must pass clean. The race detector is non-negotiable for this project.

## Rules
- No testify or other assertion libraries — stdlib only
- No `//nolint` comments to suppress issues — fix them
- Every test helper must call `t.Helper()`
- Use `t.Fatalf` for "stop the test" failures, `t.Errorf` for "record and continue"
- Prefer table-driven tests when there are 3+ cases for the same function
- Test error messages, not just that errors occurred
- Integration tests must have `//go:build integration` on line 1
