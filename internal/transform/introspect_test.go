package transform

import "testing"

func TestBuildUpdateColumns(t *testing.T) {
	allColumns := []string{"order_id", "customer_id", "amount", "updated_at"}
	uniqueKey := []string{"order_id"}
	got := BuildUpdateColumns(allColumns, uniqueKey)
	want := []string{"customer_id", "amount", "updated_at"}
	if len(got) != len(want) {
		t.Fatalf("BuildUpdateColumns() = %v, want %v", got, want)
	}
	for i, col := range got {
		if col != want[i] {
			t.Errorf("col[%d] = %q, want %q", i, col, want[i])
		}
	}
}

func TestBuildUpdateColumns_CompositeKey(t *testing.T) {
	allColumns := []string{"order_id", "line_id", "amount", "qty"}
	uniqueKey := []string{"order_id", "line_id"}
	got := BuildUpdateColumns(allColumns, uniqueKey)
	want := []string{"amount", "qty"}
	if len(got) != len(want) {
		t.Fatalf("BuildUpdateColumns() = %v, want %v", got, want)
	}
	for i, col := range got {
		if col != want[i] {
			t.Errorf("col[%d] = %q, want %q", i, col, want[i])
		}
	}
}

func TestTableExistsQuery(t *testing.T) {
	q := TableExistsQuery("analytics", "fact_orders")
	if q == "" {
		t.Error("TableExistsQuery() returned empty string")
	}
}

func TestColumnsQuery(t *testing.T) {
	q := ColumnsQuery("analytics", "fact_orders")
	if q == "" {
		t.Error("ColumnsQuery() returned empty string")
	}
}
