package transform

import (
	"strings"
	"testing"
)

func TestSplitCTEPrefix(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantCTE   string
		wantQuery string
	}{
		{
			name:      "no CTE",
			sql:       "SELECT * FROM orders",
			wantCTE:   "",
			wantQuery: "SELECT * FROM orders",
		},
		{
			name:      "single CTE",
			sql:       "WITH cte AS (\n    SELECT 1 AS n\n)\nSELECT * FROM cte",
			wantCTE:   "WITH cte AS (\n    SELECT 1 AS n\n)",
			wantQuery: "SELECT * FROM cte",
		},
		{
			name:      "multiple CTEs",
			sql:       "WITH a AS (SELECT 1), b AS (SELECT 2)\nSELECT * FROM a JOIN b ON 1=1",
			wantCTE:   "WITH a AS (SELECT 1), b AS (SELECT 2)",
			wantQuery: "SELECT * FROM a JOIN b ON 1=1",
		},
		{
			name:      "nested parens inside CTE",
			sql:       "WITH cte AS (SELECT CASE WHEN (x > 0) THEN 1 ELSE 0 END AS n FROM t)\nSELECT * FROM cte",
			wantCTE:   "WITH cte AS (SELECT CASE WHEN (x > 0) THEN 1 ELSE 0 END AS n FROM t)",
			wantQuery: "SELECT * FROM cte",
		},
		{
			name:      "leading whitespace",
			sql:       "  WITH cte AS (SELECT 1)\nSELECT * FROM cte",
			wantCTE:   "WITH cte AS (SELECT 1)",
			wantQuery: "SELECT * FROM cte",
		},
		{
			name:      "plain SELECT not confused with WITH",
			sql:       "SELECT * FROM t WHERE col = 'WITH'",
			wantCTE:   "",
			wantQuery: "SELECT * FROM t WHERE col = 'WITH'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCTE, gotQuery := SplitCTEPrefix(tt.sql)
			if gotCTE != tt.wantCTE {
				t.Errorf("cteBlock = %q, want %q", gotCTE, tt.wantCTE)
			}
			if gotQuery != tt.wantQuery {
				t.Errorf("selectSQL = %q, want %q", gotQuery, tt.wantQuery)
			}
		})
	}
}

func TestRenderModel_RefResolution(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders": {Schema: "staging"},
		"fct_orders": {Schema: "analytics"},
	}

	sql := `SELECT * FROM {{ ref "stg_orders" }}`
	got, err := RenderModel("fct_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() unexpected error: %v", err)
	}

	want := `SELECT * FROM [staging].[stg_orders]`
	if got != want {
		t.Errorf("RenderModel() = %q, want %q", got, want)
	}
}

func TestRenderModel_ThisResolution(t *testing.T) {
	models := map[string]*ModelConfig{
		"fct_orders": {Schema: "analytics"},
	}

	sql := `INSERT INTO {{ this }} SELECT 1`
	got, err := RenderModel("fct_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() unexpected error: %v", err)
	}

	want := `INSERT INTO [analytics].[fct_orders] SELECT 1`
	if got != want {
		t.Errorf("RenderModel() = %q, want %q", got, want)
	}
}

func TestRenderModel_UnknownRef(t *testing.T) {
	models := map[string]*ModelConfig{
		"fct_orders": {Schema: "analytics"},
	}

	sql := `SELECT * FROM {{ ref "nonexistent" }}`
	_, err := RenderModel("fct_orders", sql, models)
	if err == nil {
		t.Errorf("RenderModel() expected error for unknown ref, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error = %q, want it to contain %q", err, "nonexistent")
	}
}

func TestRenderModelWithEphemerals(t *testing.T) {
	models := map[string]*ModelConfig{
		"helper":      {Schema: "staging", Materialization: "ephemeral"},
		"stg_orders":  {Schema: "staging", Materialization: "view"},
		"fact_orders": {Schema: "analytics", Materialization: "table"},
	}

	ephemeralSQL := map[string]string{
		"helper": "SELECT order_id, amount * 1.1 AS adjusted FROM raw.orders",
	}

	sql := `SELECT h.order_id, h.adjusted, o.customer_id
FROM {{ ref "helper" }} h
JOIN {{ ref "stg_orders" }} o ON h.order_id = o.order_id`

	got, err := RenderModelWithEphemerals("fact_orders", sql, models, ephemeralSQL)
	if err != nil {
		t.Fatalf("RenderModelWithEphemerals() error: %v", err)
	}

	// Should contain CTE
	if !strings.Contains(got, "WITH __pit_ephemeral_helper AS") {
		t.Errorf("expected WITH CTE clause, got:\n%s", got)
	}
	// Ephemeral ref should resolve to CTE name
	if !strings.Contains(got, "__pit_ephemeral_helper h") {
		t.Errorf("expected ephemeral ref to resolve to CTE name, got:\n%s", got)
	}
	// Non-ephemeral ref should still resolve to qualified name
	if !strings.Contains(got, "[staging].[stg_orders]") {
		t.Errorf("expected non-ephemeral ref to resolve normally, got:\n%s", got)
	}
}

func TestRenderModel_SingleQuotedRef(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders": {Schema: "staging"},
		"fct_orders": {Schema: "analytics"},
	}

	sql := `SELECT * FROM {{ ref 'stg_orders' }}`
	got, err := RenderModel("fct_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() unexpected error: %v", err)
	}

	want := `SELECT * FROM [staging].[stg_orders]`
	if got != want {
		t.Errorf("RenderModel() = %q, want %q", got, want)
	}
}

func TestRenderModel_SQLWithLiteralBraces(t *testing.T) {
	// SQL containing JSON or other literal {{ }} must not be treated as template
	// directives. Only {{ ref }} and {{ this }} patterns should be replaced.
	models := map[string]*ModelConfig{
		"fct_orders": {Schema: "analytics"},
	}

	// A query that selects a JSON field — literal {{ }} in the SQL must be preserved.
	sql := `SELECT JSON_VALUE(payload, '$.key') AS val, {{ this }} AS tbl FROM src`
	got, err := RenderModel("fct_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() unexpected error: %v", err)
	}

	if !strings.Contains(got, "[analytics].[fct_orders]") {
		t.Errorf("RenderModel() missing resolved this, got:\n%s", got)
	}
	// The JSON expression should be unchanged.
	if !strings.Contains(got, "JSON_VALUE(payload, '$.key')") {
		t.Errorf("RenderModel() altered non-template content, got:\n%s", got)
	}
}

func TestRenderModelWithEphemerals_NestedEphemeral(t *testing.T) {
	// base_helper is ephemeral; mid_helper is ephemeral and depends on base_helper.
	// fact_orders depends on mid_helper. CTEs must be emitted in dependency order:
	// base_helper first, then mid_helper.
	models := map[string]*ModelConfig{
		"base_helper": {Schema: "staging", Materialization: "ephemeral"},
		"mid_helper":  {Schema: "staging", Materialization: "ephemeral"},
		"fact_orders": {Schema: "analytics", Materialization: "table"},
	}

	ephemeralSQL := map[string]string{
		"base_helper": `SELECT id, amount FROM raw.orders`,
		"mid_helper":  `SELECT id, amount * 1.1 AS adj FROM {{ ref "base_helper" }}`,
	}

	sql := `SELECT id, adj FROM {{ ref "mid_helper" }}`
	got, err := RenderModelWithEphemerals("fact_orders", sql, models, ephemeralSQL)
	if err != nil {
		t.Fatalf("RenderModelWithEphemerals() error: %v", err)
	}

	// Both CTEs must be present.
	if !strings.Contains(got, "__pit_ephemeral_base_helper AS") {
		t.Errorf("expected base_helper CTE, got:\n%s", got)
	}
	if !strings.Contains(got, "__pit_ephemeral_mid_helper AS") {
		t.Errorf("expected mid_helper CTE, got:\n%s", got)
	}

	// base_helper must appear before mid_helper (dependency order).
	baseIdx := strings.Index(got, "__pit_ephemeral_base_helper AS")
	midIdx := strings.Index(got, "__pit_ephemeral_mid_helper AS")
	if baseIdx >= midIdx {
		t.Errorf("base_helper CTE (pos %d) must precede mid_helper CTE (pos %d)", baseIdx, midIdx)
	}

	// mid_helper's SQL should reference base_helper by CTE alias.
	if !strings.Contains(got, "FROM __pit_ephemeral_base_helper") {
		t.Errorf("mid_helper CTE should reference base_helper CTE alias, got:\n%s", got)
	}

	// Main query should reference mid_helper by CTE alias.
	if !strings.Contains(got, "FROM __pit_ephemeral_mid_helper") {
		t.Errorf("main query should reference mid_helper CTE alias, got:\n%s", got)
	}
}

func TestRenderModel_MultipleRefs(t *testing.T) {
	models := map[string]*ModelConfig{
		"stg_orders":    {Schema: "staging"},
		"stg_customers": {Schema: "staging"},
		"fct_orders":    {Schema: "analytics"},
	}

	sql := `SELECT o.*, c.name
FROM {{ ref "stg_orders" }} o
JOIN {{ ref "stg_customers" }} c ON o.customer_id = c.id`

	got, err := RenderModel("fct_orders", sql, models)
	if err != nil {
		t.Fatalf("RenderModel() unexpected error: %v", err)
	}

	if !strings.Contains(got, "[staging].[stg_orders]") {
		t.Errorf("result missing [staging].[stg_orders], got %q", got)
	}
	if !strings.Contains(got, "[staging].[stg_customers]") {
		t.Errorf("result missing [staging].[stg_customers], got %q", got)
	}
}
