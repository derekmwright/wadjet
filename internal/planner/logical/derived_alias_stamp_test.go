package logical

import (
	"strings"
	"testing"
)

// A derived table's alias answers "which derived table's scope is this scan
// in", and a scan's own alias answers "which relation is this scan". Writing
// the first into the second erased the second, and a self-join inside a
// derived table then had two arms with the same name — after which nothing
// could say which `n_name` was which.
//
// PostgreSQL 17 on the equivalent fixture answers 3 groups for
//
//	SELECT u.b, COUNT(*) FROM (SELECT n1.nm AS a, n2.nm AS b
//	  FROM n n1 JOIN n n2 ON n1.r = n2.id) u GROUP BY u.b
//
// where this engine answered one group per row of the OTHER arm (#489).

// TestDerivedAliasDoesNotEraseAnInnerScanAlias is the structural half.
func TestDerivedAliasDoesNotEraseAnInnerScanAlias(t *testing.T) {
	plan, err := planOf(t,
		"SELECT u.a, u.b FROM (SELECT n1.nm AS a, n2.nm AS b FROM n n1 JOIN n n2 ON n1.r = n2.id) u")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	var scans []*Node
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeScan {
			scans = append(scans, n)
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
	if len(scans) != 2 {
		t.Fatalf("plan has %d scans, want 2:\n%s", len(scans), plan.PrettyPrint(0))
	}
	if strings.EqualFold(scans[0].TableAlias, scans[1].TableAlias) {
		t.Errorf("both arms of the self-join are aliased %q — the derived alias overwrote the query's own:\n%s",
			scans[0].TableAlias, plan.PrettyPrint(0))
	}
	for _, s := range scans {
		if s.TableAlias != "n1" && s.TableAlias != "n2" {
			t.Errorf("scan alias %q, want the query's own n1/n2", s.TableAlias)
		}
		// The scope question still has to be answerable: `u.a` may drop its
		// qualifier inside this subtree and nowhere else.
		var sawU bool
		for _, d := range s.DerivedAliases {
			if strings.EqualFold(d, "u") {
				sawU = true
			}
		}
		if !sawU {
			t.Errorf("scan %s AS %s records derived aliases %v, want it to name u",
				s.TableName, s.TableAlias, s.DerivedAliases)
		}
	}
}

// TestDerivedAliasStillNamesAnUnaliasedScan keeps the long-standing spelling:
// where the query wrote no alias of its own, the derived one is the scan's
// name, exactly as before. Every plan that is not a self-join inside a derived
// table is byte-identical after the change.
func TestDerivedAliasStillNamesAnUnaliasedScan(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"SELECT k FROM (SELECT s_suppkey AS k FROM supplier) x", "x"},
		// A scan whose only name is its TABLE name is not a name the query
		// wrote, so the derived alias still takes it.
		{"SELECT k FROM (SELECT s_suppkey AS k FROM supplier supplier) x", "x"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			plan, err := planOf(t, tc.sql)
			if err != nil {
				t.Fatalf("BuildFromSelect: %v", err)
			}
			scan := findNodeType(plan, NodeScan)
			if scan == nil {
				t.Fatalf("plan carries no scan:\n%s", plan.PrettyPrint(0))
			}
			if !strings.EqualFold(scan.TableAlias, tc.want) {
				t.Errorf("scan alias %q, want %q:\n%s", scan.TableAlias, tc.want, plan.PrettyPrint(0))
			}
		})
	}
}

// TestNestedDerivedAliasesAccumulate: two levels of derived table both name
// the same scan's scope, and a reference qualified by either must resolve.
func TestNestedDerivedAliasesAccumulate(t *testing.T) {
	plan, err := planOf(t,
		"SELECT y.j FROM (SELECT k AS j FROM (SELECT s_suppkey AS k FROM supplier s1) x) y")
	if err != nil {
		t.Fatalf("BuildFromSelect: %v", err)
	}
	scan := findNodeType(plan, NodeScan)
	if scan == nil {
		t.Fatalf("plan carries no scan:\n%s", plan.PrettyPrint(0))
	}
	if scan.TableAlias != "s1" {
		t.Errorf("scan alias %q, want the query's own s1", scan.TableAlias)
	}
	want := map[string]bool{"x": false, "y": false}
	for _, d := range scan.DerivedAliases {
		want[strings.ToLower(d)] = true
	}
	for name, saw := range want {
		if !saw {
			t.Errorf("scan records derived aliases %v, want it to name %q too",
				scan.DerivedAliases, name)
		}
	}
}
