package ftp

import "testing"

func TestMatchGlob(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		{pattern: "sales_*.csv", name: "sales_2024.csv", want: true},
		{pattern: "sales_*.csv", name: "sales_jan_2024.csv", want: true},
		{pattern: "sales_*.csv", name: "purchases_2024.csv", want: false},
		{pattern: "sales_*.csv", name: "sales_2024.txt", want: false},
		{pattern: "*.csv", name: "anything.csv", want: true},
		{pattern: "*.csv", name: "anything.txt", want: false},
		{pattern: "data_???.csv", name: "data_001.csv", want: true},
		{pattern: "data_???.csv", name: "data_0001.csv", want: false},
		{pattern: "exact.csv", name: "exact.csv", want: true},
		{pattern: "exact.csv", name: "other.csv", want: false},
		{pattern: "[a-z]*.csv", name: "abc.csv", want: true},
		{pattern: "[a-z]*.csv", name: "123.csv", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.name, func(t *testing.T) {
			got, err := MatchGlob(tt.pattern, tt.name)
			if err != nil {
				t.Fatalf("MatchGlob(%q, %q) error: %v", tt.pattern, tt.name, err)
			}
			if got != tt.want {
				t.Errorf("MatchGlob(%q, %q) = %v, want %v", tt.pattern, tt.name, got, tt.want)
			}
		})
	}
}

func TestMatchGlob_InvalidPattern(t *testing.T) {
	_, err := MatchGlob("[invalid", "file.csv")
	if err == nil {
		t.Error("MatchGlob() expected error for invalid pattern, got nil")
	}
}
