// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"
	"testing"
)

// TestAttachScanSelectProjections_ComputedOverNestedRename is the #387
// regression at the plan level: the attached spec's Expr must reference the
// SOURCE column (compiled against the scan's real schema), its Name must
// keep the outer SELECT's spelling (what the gather renames and the sort
// keys against), and the gather renames must point at names the fragment
// emits — including on the direct scan→gather path, where the #385
// resolution had already re-pointed them at source names.
func TestAttachScanSelectProjections_ComputedOverNestedRename(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantExprs map[string]string // spec Name (lower) -> spec Expr (lower)
	}{
		{name: "computed mix with sort",
			sql:       `SELECT k, k + 1 AS m FROM (SELECT r_regionkey AS k FROM region) t ORDER BY k`,
			wantExprs: map[string]string{"k": "r_regionkey", "m": "r_regionkey + 1"}},
		{name: "computed mix without sort",
			sql:       `SELECT k, k + 1 AS m FROM (SELECT r_regionkey AS k FROM region) t`,
			wantExprs: map[string]string{"k": "r_regionkey", "k + 1": "r_regionkey + 1"}},
		{name: "expression over a chained rename",
			sql: `SELECT a + 1 AS m FROM (SELECT b AS a FROM (SELECT r_regionkey AS b FROM region) u) t
				ORDER BY m`,
			wantExprs: map[string]string{"m": "r_regionkey + 1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stages := planStagesForRenameTest(t, tc.sql)
			scan := stageByType(stages, "scan")
			if scan == nil {
				t.Fatal("no scan stage in plan")
			}
			got := map[string]string{}
			for _, sp := range scan.ProjectExprs {
				got[strings.ToLower(sp.Name)] = strings.ToLower(sp.Expr)
			}
			for name, wantExpr := range tc.wantExprs {
				if got[name] != wantExpr {
					t.Errorf("spec %q reads %q, want %q (all: %v)", name, got[name], wantExpr, got)
				}
			}
			// Every gather rename From must name a column the fragment
			// emits, or the rename misses and degrades to full width.
			for _, r := range gatherRenames(t, stages) {
				if r.Expr != nil {
					continue
				}
				if _, ok := got[strings.ToLower(r.From)]; !ok {
					t.Errorf("gather rename %q -> %q: the fragment emits %v, not %q",
						r.From, r.To, got, r.From)
				}
			}
		})
	}
}
