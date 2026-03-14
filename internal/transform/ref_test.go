package transform

import (
	"testing"
)

func TestExtractRefs(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "no refs",
			sql:  "SELECT * FROM orders",
			want: nil,
		},
		{
			name: "single ref",
			sql:  `SELECT * FROM {{ ref "stg_orders" }}`,
			want: []string{"stg_orders"},
		},
		{
			name: "multiple refs",
			sql:  `SELECT o.id, c.name FROM {{ ref "stg_orders" }} o JOIN {{ ref "stg_customers" }} c ON o.customer_id = c.id`,
			want: []string{"stg_orders", "stg_customers"},
		},
		{
			name: "extra whitespace",
			sql:  `SELECT * FROM {{   ref   "stg_orders"   }}`,
			want: []string{"stg_orders"},
		},
		{
			name: "trim markers",
			sql:  `SELECT * FROM {{- ref "stg_orders" -}}`,
			want: []string{"stg_orders"},
		},
		{
			name: "left trim marker only",
			sql:  `SELECT * FROM {{- ref "stg_orders" }}`,
			want: []string{"stg_orders"},
		},
		{
			name: "right trim marker only",
			sql:  `SELECT * FROM {{ ref "stg_orders" -}}`,
			want: []string{"stg_orders"},
		},
		{
			name: "deduplicated refs",
			sql:  `SELECT * FROM {{ ref "stg_orders" }} UNION ALL SELECT * FROM {{ ref "stg_orders" }}`,
			want: []string{"stg_orders"},
		},
		{
			name: "multiple refs with dedup",
			sql:  `SELECT * FROM {{ ref "stg_orders" }} JOIN {{ ref "stg_customers" }} ON 1=1 UNION SELECT * FROM {{ ref "stg_orders" }}`,
			want: []string{"stg_orders", "stg_customers"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractRefs(tt.sql)
			if tt.want == nil {
				if got != nil {
					t.Errorf("ExtractRefs() = %v, want nil", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("ExtractRefs() returned %d refs, want %d: got %v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("ExtractRefs()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
