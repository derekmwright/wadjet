package logical

import (
	"strings"
	"testing"
)

// A scan answers two different questions, and #489 separated them: which
// RELATION it is (TableAlias, what tells one arm of a self-join from the
// other) and which derived table's SCOPE it sits in (DerivedAliases, what lets
// `u.a` drop its qualifier there and nowhere else).
//
// Every consumer that asks the SECOND question has to ask it through these
// helpers. The correlated-subquery collectors did not, so once TableAlias
// stopped being overwritten with the derived alias, `u` was a name none of
// them knew: `WHERE EXISTS (… WHERE t.k = u.did)` was no longer recognized as
// correlated and the subquery stayed per-row — 0 rows silently on the
// single-process pipeline, loud on the DAG.

func TestScopeNamesCoverTheDerivedTable(t *testing.T) {
	n := &Node{
		Type: NodeScan, TableName: "nation", TableAlias: "n1",
		DerivedAliases: []string{"x", "y"},
	}
	got := n.ScopeNames()
	for _, want := range []string{"nation", "n1", "x", "y"} {
		if !containsFold(got, want) {
			t.Errorf("ScopeNames() = %v, missing %q", got, want)
		}
	}
	if len(got) != 4 {
		t.Errorf("ScopeNames() = %v, want exactly the four distinct names", got)
	}
}

func TestScopeNamesDeduplicates(t *testing.T) {
	// The common single-table derived case: setSubtreeAlias takes the alias
	// AND records it, so the same name arrives twice.
	n := &Node{Type: NodeScan, TableName: "nation", TableAlias: "u", DerivedAliases: []string{"u"}}
	if got := n.ScopeNames(); len(got) != 2 {
		t.Errorf("ScopeNames() = %v, want [nation u]", got)
	}
}

func TestScopeNamesEmptyForNonScans(t *testing.T) {
	if got := (&Node{Type: NodeProject}).ScopeNames(); got != nil {
		t.Errorf("ScopeNames() on a Project = %v, want nil", got)
	}
	var nilNode *Node
	if got := nilNode.ScopeNames(); got != nil {
		t.Errorf("ScopeNames() on nil = %v, want nil", got)
	}
}

// TestOuterTableIDIsTheOutermostDerivedAlias: the enclosing query can only
// write the OUTERMOST name, so that is what a bare column of this scan is
// attributed to.
func TestOuterTableIDIsTheOutermostDerivedAlias(t *testing.T) {
	for _, tc := range []struct {
		name string
		node *Node
		want string
	}{
		{"nested derived tables take the outermost",
			&Node{Type: NodeScan, TableName: "nation", TableAlias: "n1",
				DerivedAliases: []string{"x", "y"}}, "y"},
		{"one derived table", &Node{Type: NodeScan, TableName: "nation", TableAlias: "n1",
			DerivedAliases: []string{"u"}}, "u"},
		// No derived table: exactly what every caller had before
		// DerivedAliases existed.
		{"plain aliased scan", &Node{Type: NodeScan, TableName: "nation", TableAlias: "n1"}, "n1"},
		{"unaliased scan", &Node{Type: NodeScan, TableName: "nation"}, "nation"},
		{"not a scan", &Node{Type: NodeProject}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.OuterTableID(); got != tc.want {
				t.Errorf("OuterTableID() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestCorrelatedRefIntoDerivedTableIsSeenAsOuter drives the collector that
// actually regressed: the derived table's alias has to be in the set of names
// an outer reference may be qualified by, and the scan's columns have to be
// attributed to it.
func TestCorrelatedRefIntoDerivedTableIsSeenAsOuter(t *testing.T) {
	plan, err := planOf(t,
		`SELECT COUNT(*) AS c FROM (SELECT n1.n_nationkey AS did FROM nation n1) u
		 WHERE EXISTS (SELECT 1 FROM region WHERE region.r_regionkey = u.did)`)
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	tables := map[string]bool{}
	collectTableNames(plan, tables)
	for _, want := range []string{"u", "n1", "nation"} {
		if !tables[want] {
			t.Errorf("collectTableNames = %v, missing %q — a reference qualified by it "+
				"is not recognized as outer, so the subquery stays per-row", tables, want)
		}
	}

}

// TestCollectScanInfoAttributesColumnsToTheDerivedTable is the colToTable half.
// ScanColumns is filled from the catalog by AnnotateScanColumns, so the
// subtree is built by hand rather than parsed.
func TestCollectScanInfoAttributesColumnsToTheDerivedTable(t *testing.T) {
	subtree := &Node{Type: NodeProject,
		Projections: []Projection{{Column: "n_nationkey", Expr: "n1.n_nationkey", Alias: "did"}},
		Children: []*Node{{
			Type: NodeScan, TableName: "nation", TableAlias: "n1",
			ScanColumns: []string{"n_nationkey", "n_name"}, DerivedAliases: []string{"u"},
		}},
	}
	tables, colToTable := collectScanInfo(subtree)
	for _, want := range []string{"u", "n1", "nation"} {
		if !tables[want] {
			t.Errorf("collectScanInfo tables = %v, missing %q", tables, want)
		}
	}
	if got := colToTable["n_nationkey"]; got != "u" {
		t.Errorf("collectScanInfo attributes n_nationkey to %q, want %q — the enclosing "+
			"query calls this scan by the derived table's name", got, "u")
	}
}

func containsFold(hay []string, needle string) bool {
	for _, h := range hay {
		if strings.EqualFold(h, needle) {
			return true
		}
	}
	return false
}
