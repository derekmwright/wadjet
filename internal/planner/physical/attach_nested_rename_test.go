package physical

import (
	"strings"
	"testing"
)

// stageByType returns the first stage of the given type, or nil.
func stageByType(stages []Stage, typ string) *Stage {
	for i := range stages {
		if string(stages[i].Type) == typ {
			return &stages[i]
		}
	}
	return nil
}

// TestAttachScanSelectProjections_NestedRename is the #386 regression: an
// outer SELECT that merely forwards a NESTED subquery rename used to slip
// past attachScanSelectProjections (no outer alias, no expression), so the
// sort stage keyed on a column no stage emitted and the ORDER BY silently
// no-oped. The pass must now materialize the alias at the producing fragment
// — spec Expr carries the SOURCE column the streams have, spec Name the
// alias the sort keys on — and re-point the gather rename at the alias.
func TestAttachScanSelectProjections_NestedRename(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantExpr string // ProjectExprSpec.Expr on the producing stage
		wantName string // ProjectExprSpec.Name on the producing stage
		sortKey  string // the sort stage's key, which must equal wantName
		target   string // stage type carrying the projection
	}{
		{name: "desc over nested rename",
			sql:      `SELECT k FROM (SELECT r_regionkey AS k FROM region) t ORDER BY k DESC`,
			wantExpr: "r_regionkey", wantName: "k", sortKey: "k", target: "scan"},
		{name: "shadowing alias sorts by the aliased source",
			sql:      `SELECT r_name FROM (SELECT r_comment AS r_name FROM region) t ORDER BY r_name`,
			wantExpr: "r_comment", wantName: "r_name", sortKey: "r_name", target: "scan"},
		{name: "chained rename",
			sql:      `SELECT a FROM (SELECT b AS a FROM (SELECT r_regionkey AS b FROM region) u) t ORDER BY a DESC`,
			wantExpr: "r_regionkey", wantName: "a", sortKey: "a", target: "scan"},
		{name: "outer alias of a nested rename",
			sql:      `SELECT k AS j FROM (SELECT r_regionkey AS k FROM region) t ORDER BY j DESC`,
			wantExpr: "r_regionkey", wantName: "j", sortKey: "j", target: "scan"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stages := planStagesForRenameTest(t, tc.sql)
			target := stageByType(stages, tc.target)
			if target == nil {
				t.Fatalf("no %s stage in plan", tc.target)
			}
			var spec *ProjectExprSpec
			for i := range target.ProjectExprs {
				if strings.EqualFold(target.ProjectExprs[i].Name, tc.wantName) {
					spec = &target.ProjectExprs[i]
					break
				}
			}
			if spec == nil {
				t.Fatalf("%s stage carries no projection named %q: %+v",
					tc.target, tc.wantName, target.ProjectExprs)
			}
			if !strings.EqualFold(spec.Expr, tc.wantExpr) {
				t.Errorf("projection %q reads %q, want source column %q",
					tc.wantName, spec.Expr, tc.wantExpr)
			}
			sort := stageByType(stages, "sort")
			if sort == nil {
				t.Fatal("no sort stage in plan")
			}
			if len(sort.SortKeys) != 1 || !strings.EqualFold(sort.SortKeys[0].Column, tc.sortKey) {
				t.Errorf("sort keys = %+v, want single key %q", sort.SortKeys, tc.sortKey)
			}
			// The gather must rename FROM the name the stage now emits.
			for _, r := range gatherRenames(t, stages) {
				if strings.EqualFold(r.To, tc.wantName) && !strings.EqualFold(r.From, tc.wantName) {
					t.Errorf("gather rename %q -> %q is stale: the fragment already emits %q",
						r.From, r.To, tc.wantName)
				}
			}
		})
	}
}

// TestAttachScanSelectProjections_NestedRenameDeclines guards the leanness
// half: when no sort key names the forwarded alias, the gather's rename
// (post-#385) already covers the query, and attaching a projection would be
// pure cost — the pass must keep declining.
func TestAttachScanSelectProjections_NestedRenameDeclines(t *testing.T) {
	for _, sql := range []string{
		// No ORDER BY at all.
		`SELECT k FROM (SELECT r_regionkey AS k FROM region) t`,
		// ORDER BY names a passthrough source column, not the alias.
		`SELECT k, r_name FROM (SELECT r_regionkey AS k, r_name FROM region) t ORDER BY r_name`,
	} {
		stages := planStagesForRenameTest(t, sql)
		if scan := stageByType(stages, "scan"); scan != nil && len(scan.ProjectExprs) > 0 {
			t.Errorf("%s\n  scan stage got ProjectExprs %+v; the gather rename already covers this shape",
				sql, scan.ProjectExprs)
		}
	}
}
