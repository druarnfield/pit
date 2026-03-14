package transform

import (
	"strings"
	"testing"
)

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
