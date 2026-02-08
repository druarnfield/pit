package cli

import (
	"testing"

	"github.com/druarnfield/pit/internal/config"
)

func testConfigs() map[string]*config.ProjectConfig {
	return map[string]*config.ProjectConfig{
		"claims_pipeline": {
			Outputs: []config.Output{
				{Name: "claims_staging", Type: "table", Location: "warehouse.staging.claims"},
				{Name: "claim_lines_staging", Type: "table", Location: "warehouse.staging.claim_lines"},
			},
		},
		"monthly_reports": {
			Outputs: []config.Output{
				{Name: "daily_report", Type: "file", Location: "//sftp/reports/daily.csv"},
			},
		},
		"empty_project": {},
	}
}

func TestCollectOutputs_NoFilter(t *testing.T) {
	rows := collectOutputs(testConfigs(), "", "", "")
	if len(rows) != 3 {
		t.Fatalf("len(rows) = %d, want 3", len(rows))
	}
	// Should be sorted by project then name
	if rows[0].Project != "claims_pipeline" || rows[0].Name != "claim_lines_staging" {
		t.Errorf("rows[0] = {%s, %s}, want {claims_pipeline, claim_lines_staging}", rows[0].Project, rows[0].Name)
	}
	if rows[1].Project != "claims_pipeline" || rows[1].Name != "claims_staging" {
		t.Errorf("rows[1] = {%s, %s}, want {claims_pipeline, claims_staging}", rows[1].Project, rows[1].Name)
	}
	if rows[2].Project != "monthly_reports" || rows[2].Name != "daily_report" {
		t.Errorf("rows[2] = {%s, %s}, want {monthly_reports, daily_report}", rows[2].Project, rows[2].Name)
	}
}

func TestCollectOutputs_ProjectFilter(t *testing.T) {
	rows := collectOutputs(testConfigs(), "claims_pipeline", "", "")
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Project != "claims_pipeline" {
			t.Errorf("row.Project = %q, want 'claims_pipeline'", r.Project)
		}
	}
}

func TestCollectOutputs_TypeFilter(t *testing.T) {
	rows := collectOutputs(testConfigs(), "", "file", "")
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	if rows[0].Name != "daily_report" {
		t.Errorf("rows[0].Name = %q, want 'daily_report'", rows[0].Name)
	}
}

func TestCollectOutputs_LocationGlob(t *testing.T) {
	rows := collectOutputs(testConfigs(), "", "", "warehouse.*")
	// filepath.Match treats * as matching non-separator chars.
	// "warehouse.*" matches "warehouse.staging.claims" only if . is not a separator.
	// On Unix, filepath.Separator is '/', so . is just a normal char, and * matches it.
	// Wait — filepath.Match's * does NOT match separator, but . is not filepath.Separator.
	// However, * in filepath.Match matches any sequence of non-Separator characters.
	// So "warehouse.*" will match "warehouse.staging.claims" because . is not /.
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Project != "claims_pipeline" {
			t.Errorf("row.Project = %q, want 'claims_pipeline'", r.Project)
		}
	}
}

func TestCollectOutputs_CombinedFilters(t *testing.T) {
	rows := collectOutputs(testConfigs(), "claims_pipeline", "table", "")
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}
}

func TestCollectOutputs_NoOutputs(t *testing.T) {
	configs := map[string]*config.ProjectConfig{
		"empty": {},
	}
	rows := collectOutputs(configs, "", "", "")
	if len(rows) != 0 {
		t.Errorf("len(rows) = %d, want 0", len(rows))
	}
}

func TestCollectOutputs_NonexistentProject(t *testing.T) {
	rows := collectOutputs(testConfigs(), "nonexistent", "", "")
	if len(rows) != 0 {
		t.Errorf("len(rows) = %d, want 0", len(rows))
	}
}

func TestCollectOutputs_SortOrder(t *testing.T) {
	configs := map[string]*config.ProjectConfig{
		"z_project": {
			Outputs: []config.Output{
				{Name: "beta", Type: "table", Location: "loc"},
				{Name: "alpha", Type: "table", Location: "loc"},
			},
		},
		"a_project": {
			Outputs: []config.Output{
				{Name: "gamma", Type: "file", Location: "loc"},
			},
		},
	}

	rows := collectOutputs(configs, "", "", "")
	if len(rows) != 3 {
		t.Fatalf("len(rows) = %d, want 3", len(rows))
	}
	// a_project should come first
	if rows[0].Project != "a_project" {
		t.Errorf("rows[0].Project = %q, want 'a_project'", rows[0].Project)
	}
	// Within z_project, alpha before beta
	if rows[1].Name != "alpha" {
		t.Errorf("rows[1].Name = %q, want 'alpha'", rows[1].Name)
	}
	if rows[2].Name != "beta" {
		t.Errorf("rows[2].Name = %q, want 'beta'", rows[2].Name)
	}
}
