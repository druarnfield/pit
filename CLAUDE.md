deferring planned tasks without permission is NOT allowed.  Even if you have a good reason for deferring the task, you must always ask for permission before deferring a task!

## Testing Conventions

### Commands
- Run all tests: `go test ./...`
- Run with race detector: `go test -race ./...`
- Run a single package: `go test ./internal/config`
- Run integration tests: `go test -tags integration ./...`
- Verbose output: `go test -v ./internal/engine`
- Vet: `go vet ./...`

### Test Structure
- **Test files**: `*_test.go` in the same package (not `_test` suffix packages)
- **Naming**: `TestFunctionName`, `TestFunctionName_Variant`, subtests via `t.Run("case", ...)`
- **Table-driven tests**: Use for functions with multiple input/output combinations
- **Assertions**: stdlib only (`if got != want { t.Errorf(...) }`), no testify

### Fixtures
- **Static fixtures**: `testdata/` directories within each package (read-only TOML files, project dirs)
- **Dynamic fixtures**: `t.TempDir()` for tests that write files (snapshot, scaffold, log files)
- **Test helpers**: Package-local `mkTestProject(t, ...)` style helpers with `t.Helper()` call

### Build Tags
- **Unit tests** (default): No build tag needed. Must not exec external processes or require network.
- **Integration tests**: Use `//go:build integration` tag. These exec real processes (bash, uv, etc). Gate with `go test -tags integration`.

### Per-Package Patterns

| Package | Test approach | Fixtures |
|---------|--------------|----------|
| `config` | Parse testdata TOMLs, verify struct fields. Use `t.TempDir()` for Discover. | `testdata/*.toml` |
| `dag` | Validate testdata project dirs. Construct `config.ProjectConfig` inline for edge cases. | `testdata/<scenario>/pit.toml + tasks/` |
| `runner` | Test `Resolve()` dispatch and `ValidateScript()` as unit tests. Test actual process execution behind `//go:build integration`. | None (inline) |
| `engine` | Test `topoSort`, `hasUpstreamFailure`, `prefixWriter`, `printSummary` as unit tests. Test `Snapshot` and `copyFile` with `t.TempDir()`. Full `Execute` behind integration tag. | `testdata/sample_project/` |
| `scaffold` | Test `Create` with `t.TempDir()`, verify file existence. Test `ValidType` inline. | None (uses t.TempDir) |
| `cli` | Test `parseRunArg`, `availableDAGs` as pure functions. CLI integration via `//go:build integration`. | None (inline) |

### Error Checking Pattern
```go
// For expected errors — check err != nil
if err == nil {
    t.Errorf("Function() expected error, got nil")
}

// For unexpected errors — use t.Fatalf to stop
if err != nil {
    t.Fatalf("Function() unexpected error: %v", err)
}

// For error message content — use strings.Contains
if !strings.Contains(err.Error(), "expected substring") {
    t.Errorf("error = %q, want it to contain %q", err, "expected substring")
}
```

### Rules
- Always run `go test -race ./...` before considering tests complete
- Never skip race detector — this project has concurrent task execution
- Test helpers must call `t.Helper()` so failures report at the caller's line
- Use `t.Fatalf` for setup failures, `t.Errorf` for assertion failures
- Keep tests fast: unit tests should complete in under 5 seconds total
