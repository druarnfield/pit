package transform

import (
	"strings"
	"testing"
)

func TestGenerateStoredProcedure_Basic(t *testing.T) {
	result := &CompileResult{
		Models: map[string]*CompiledModel{
			"stg_orders": {
				Name:        "stg_orders",
				Config:      &ModelConfig{Materialization: "view", Schema: "staging"},
				CompiledSQL: "CREATE OR ALTER VIEW [staging].[stg_orders] AS\nSELECT id FROM raw.orders",
			},
			"fact_orders": {
				Name:        "fact_orders",
				Config:      &ModelConfig{Materialization: "table", Schema: "marts"},
				CompiledSQL: "BEGIN TRANSACTION;\nSELECT * INTO [marts].[fact_orders] FROM [staging].[stg_orders];\nCOMMIT TRANSACTION;",
			},
		},
		Order: []string{"stg_orders", "fact_orders"},
	}

	sql, err := GenerateStoredProcedure("pit_my_transforms", "dbo", result)
	if err != nil {
		t.Fatalf("GenerateStoredProcedure() error: %v", err)
	}

	// Check procedure header
	if !strings.Contains(sql, "CREATE OR ALTER PROCEDURE [dbo].[pit_my_transforms]") {
		t.Errorf("missing procedure header, got:\n%s", sql)
	}

	// Check SET options
	if !strings.Contains(sql, "SET NOCOUNT ON;") {
		t.Errorf("missing SET NOCOUNT ON, got:\n%s", sql)
	}
	if !strings.Contains(sql, "SET XACT_ABORT ON;") {
		t.Errorf("missing SET XACT_ABORT ON, got:\n%s", sql)
	}

	// Check TRY/CATCH
	if !strings.Contains(sql, "BEGIN TRY") {
		t.Errorf("missing BEGIN TRY, got:\n%s", sql)
	}
	if !strings.Contains(sql, "END TRY") {
		t.Errorf("missing END TRY, got:\n%s", sql)
	}
	if !strings.Contains(sql, "THROW;") {
		t.Errorf("missing THROW, got:\n%s", sql)
	}

	// Check model comments appear in order
	stgIdx := strings.Index(sql, "-- [1] stg_orders (view)")
	factIdx := strings.Index(sql, "-- [2] fact_orders (table)")
	if stgIdx < 0 {
		t.Errorf("missing stg_orders comment, got:\n%s", sql)
	}
	if factIdx < 0 {
		t.Errorf("missing fact_orders comment, got:\n%s", sql)
	}
	if stgIdx >= factIdx {
		t.Errorf("stg_orders comment (pos %d) should come before fact_orders (pos %d)", stgIdx, factIdx)
	}

	// Check compiled SQL is included
	if !strings.Contains(sql, "CREATE OR ALTER VIEW [staging].[stg_orders]") {
		t.Errorf("missing stg_orders compiled SQL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "[marts].[fact_orders]") {
		t.Errorf("missing fact_orders compiled SQL, got:\n%s", sql)
	}

	// Check procedure footer
	if !strings.HasSuffix(strings.TrimSpace(sql), "END") {
		t.Errorf("expected END at end of procedure, got:\n%s", sql)
	}
}

func TestGenerateStoredProcedure_SkipsEphemerals(t *testing.T) {
	result := &CompileResult{
		Models: map[string]*CompiledModel{
			"stg_orders": {
				Name:        "stg_orders",
				Config:      &ModelConfig{Materialization: "view", Schema: "dbo"},
				CompiledSQL: "CREATE OR ALTER VIEW [dbo].[stg_orders] AS\nSELECT 1",
			},
		},
		// "helper" is ephemeral and in Order but not in Models
		Order: []string{"helper", "stg_orders"},
	}

	sql, err := GenerateStoredProcedure("pit_test", "dbo", result)
	if err != nil {
		t.Fatalf("GenerateStoredProcedure() error: %v", err)
	}

	if strings.Contains(sql, "helper") {
		t.Errorf("ephemeral model 'helper' should not appear in stored procedure, got:\n%s", sql)
	}
	if !strings.Contains(sql, "stg_orders") {
		t.Errorf("missing stg_orders in stored procedure, got:\n%s", sql)
	}
}

func TestGenerateStoredProcedure_EmptyModels(t *testing.T) {
	result := &CompileResult{
		Models: map[string]*CompiledModel{},
		Order:  []string{},
	}

	_, err := GenerateStoredProcedure("pit_test", "dbo", result)
	if err == nil {
		t.Fatal("expected error for empty models, got nil")
	}
	if !strings.Contains(err.Error(), "no compiled models") {
		t.Errorf("error = %q, want it to contain %q", err, "no compiled models")
	}
}

func TestGenerateStoredProcedure_CustomSchema(t *testing.T) {
	result := &CompileResult{
		Models: map[string]*CompiledModel{
			"m1": {
				Name:        "m1",
				Config:      &ModelConfig{Materialization: "view", Schema: "dbo"},
				CompiledSQL: "SELECT 1",
			},
		},
		Order: []string{"m1"},
	}

	sql, err := GenerateStoredProcedure("my_proc", "etl", result)
	if err != nil {
		t.Fatalf("GenerateStoredProcedure() error: %v", err)
	}

	if !strings.Contains(sql, "CREATE OR ALTER PROCEDURE [etl].[my_proc]") {
		t.Errorf("expected custom schema in procedure name, got:\n%s", sql)
	}
}
