package runner

import (
	"testing"
)

func TestFormatDBTLine(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			name: "LogStartLine",
			line: `{"info":{"name":"LogStartLine","msg":"running model","level":"info"},"data":{"description":"model staging.stg_orders"}}`,
			want: "Running: model staging.stg_orders",
		},
		{
			name: "LogStartLine with msg fallback",
			line: `{"info":{"name":"LogStartLine","msg":"Running with dbt=1.9.1","level":"info"},"data":{}}`,
			want: "Running: Running with dbt=1.9.1",
		},
		{
			name: "LogModelResult",
			line: `{"info":{"name":"LogModelResult","msg":"OK","level":"info"},"data":{"execution_time":2.5,"rows_affected":1500,"node":{"name":"stg_orders","path":"models/staging/stg_orders.sql"}}}`,
			want: "OK stg_orders (2.5s, 1500 rows)",
		},
		{
			name: "LogModelResult with path fallback",
			line: `{"info":{"name":"LogModelResult","msg":"OK","level":"info"},"data":{"execution_time":1.2,"rows_affected":100,"node":{"name":"","path":"models/dim_customer.sql"}}}`,
			want: "OK models/dim_customer.sql (1.2s, 100 rows)",
		},
		{
			name: "LogTestResult pass",
			line: `{"info":{"name":"LogTestResult","msg":"PASS","level":"info"},"data":{"execution_time":0.3,"status":"pass","node":{"name":"not_null_orders_id","path":"tests/not_null_orders_id.sql"}}}`,
			want: "PASS not_null_orders_id (0.3s)",
		},
		{
			name: "LogTestResult fail",
			line: `{"info":{"name":"LogTestResult","msg":"FAIL","level":"info"},"data":{"execution_time":0.5,"status":"fail","node":{"name":"unique_orders_id","path":"tests/unique_orders_id.sql"}}}`,
			want: "FAIL unique_orders_id (0.5s)",
		},
		{
			name: "LogTestResult error",
			line: `{"info":{"name":"LogTestResult","msg":"ERROR","level":"info"},"data":{"execution_time":0.1,"status":"error","node":{"name":"bad_test","path":"tests/bad.sql"}}}`,
			want: "FAIL bad_test (0.1s)",
		},
		{
			name: "LogFreshnessResult fresh",
			line: `{"info":{"name":"LogFreshnessResult","msg":"","level":"info"},"data":{"status":"fresh","source":{"name":"raw_orders"}}}`,
			want: "FRESH raw_orders",
		},
		{
			name: "LogFreshnessResult stale",
			line: `{"info":{"name":"LogFreshnessResult","msg":"","level":"info"},"data":{"status":"stale","source":{"name":"raw_orders"}}}`,
			want: "STALE raw_orders",
		},
		{
			name: "CommandCompleted",
			line: `{"info":{"name":"CommandCompleted","msg":"","level":"info"},"data":{"elapsed":15.7}}`,
			want: "Completed in 15.7s",
		},
		{
			name: "unknown event suppressed",
			line: `{"info":{"name":"SomeInternalEvent","msg":"debug info","level":"debug"},"data":{}}`,
			want: "",
		},
		{
			name: "non-JSON passthrough",
			line: `Running with dbt=1.9.1`,
			want: "Running with dbt=1.9.1",
		},
		{
			name: "malformed JSON passthrough",
			line: `{"info": broken`,
			want: `{"info": broken`,
		},
		{
			name: "empty line",
			line: "",
			want: "",
		},
		{
			name: "whitespace line",
			line: "   ",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDBTLine([]byte(tt.line))
			if got != tt.want {
				t.Errorf("formatDBTLine() = %q, want %q", got, tt.want)
			}
		})
	}
}
