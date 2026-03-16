package transform

import (
	"strings"
	"testing"
)

func TestMaterialize_View(t *testing.T) {
	ctx := &MaterializeContext{
		This: "[analytics].[fct_orders]",
		SQL:  "SELECT * FROM [staging].[stg_orders]",
	}

	got, err := Materialize("mssql", "view", ctx)
	if err != nil {
		t.Fatalf("Materialize() unexpected error: %v", err)
	}

	if !strings.Contains(got, "CREATE OR ALTER VIEW") {
		t.Errorf("result missing CREATE OR ALTER VIEW, got %q", got)
	}
	if !strings.Contains(got, "[analytics].[fct_orders]") {
		t.Errorf("result missing qualified name, got %q", got)
	}
}

func TestMaterialize_Table(t *testing.T) {
	ctx := &MaterializeContext{
		This: "[analytics].[fct_orders]",
		SQL:  "SELECT * FROM [staging].[stg_orders]",
	}

	got, err := Materialize("mssql", "table", ctx)
	if err != nil {
		t.Fatalf("Materialize() unexpected error: %v", err)
	}

	for _, want := range []string{"BEGIN TRANSACTION", "DROP TABLE", "SELECT *", "INTO", "COMMIT TRANSACTION"} {
		if !strings.Contains(got, want) {
			t.Errorf("result missing %q, got %q", want, got)
		}
	}
}

func TestMaterialize_IncrementalMerge(t *testing.T) {
	ctx := &MaterializeContext{
		This:          "[analytics].[fct_orders]",
		SQL:           "SELECT id, amount, status FROM [staging].[stg_orders]",
		UniqueKey:     []string{"id"},
		Columns:       []string{"id", "amount", "status"},
		UpdateColumns: []string{"amount", "status"},
	}

	got, err := Materialize("mssql", "incremental_merge", ctx)
	if err != nil {
		t.Fatalf("Materialize() unexpected error: %v", err)
	}

	for _, want := range []string{
		"MERGE",
		"target.[id] = source.[id]",
		"UPDATE SET",
		"target.[amount] = source.[amount]",
		"INSERT",
		"VALUES",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("result missing %q, got:\n%s", want, got)
		}
	}
}

func TestMaterialize_IncrementalDeleteInsert(t *testing.T) {
	ctx := &MaterializeContext{
		This:      "[analytics].[fct_orders]",
		SQL:       "SELECT id, amount FROM [staging].[stg_orders]",
		UniqueKey: []string{"id"},
	}

	got, err := Materialize("mssql", "incremental_delete_insert", ctx)
	if err != nil {
		t.Fatalf("Materialize() unexpected error: %v", err)
	}

	for _, want := range []string{
		"DELETE FROM",
		"INSERT INTO",
		"[analytics].[fct_orders].[id] = __pit_source.[id]",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("result missing %q, got:\n%s", want, got)
		}
	}
}

func TestMaterialize_UnknownDialect(t *testing.T) {
	ctx := &MaterializeContext{
		This: "[test].[model]",
		SQL:  "SELECT 1",
	}

	_, err := Materialize("mysql", "view", ctx)
	if err == nil {
		t.Errorf("Materialize() expected error for unknown dialect, got nil")
	}
}

func TestMaterialize_UnknownMaterialization(t *testing.T) {
	ctx := &MaterializeContext{
		This: "[test].[model]",
		SQL:  "SELECT 1",
	}

	_, err := Materialize("mssql", "snapshot", ctx)
	if err == nil {
		t.Errorf("Materialize() expected error for unknown materialization, got nil")
	}
}
