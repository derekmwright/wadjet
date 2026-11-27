// SPDX-License-Identifier: MIT

package logical

import (
	"sort"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// #1678 line k: a derived table, a CTE reference and a LATERAL arm show the
// join around them ONE relation — their own name — and their OUTPUT columns.
// Before the fix the side attribution descended into the block and published
// its scans' aliases, table names and columns, so an outer `a.s` "belonged"
// to a derived side whose body declared its own `a`, and the predicate was
// pushed inside it (a cross product on every arm, PostgreSQL 17.11: 5 rows).

func ekAnnotate(plan *Node) {
	schema := map[string][]string{
		"ej_t": {"id", "s", "n"},
		"ej_u": {"id", "s", "n"},
		"ej_v": {"id", "s"},
	}
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.Type == NodeScan && len(n.ScanColumns) == 0 {
			n.ScanColumns = schema[strings.ToLower(n.TableName)]
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
}

func ekBuild(t *testing.T, sql string) *Node {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := BuildFromSelectWithCTEs(info, info.CTEs)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ekAnnotate(plan)
	return plan
}

func ekFirstJoin(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeJoin && len(n.Children) == 2 {
		return n
	}
	for _, c := range n.Children {
		if j := ekFirstJoin(c); j != nil {
			return j
		}
	}
	return nil
}

func ekKeys[V any](m map[string]V) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// TestJoinSideAttributionStopsAtTheBlock pins what each side of the first
// join answers to, built and before any rewrite: the derived (CTE, LATERAL)
// side answers to its alias and its output names only.
func TestJoinSideAttributionStopsAtTheBlock(t *testing.T) {
	cases := []struct {
		name, sql               string
		leftTables, rightTables string
		rightCols               string
	}{
		{"innerAliasIsOuterAlias",
			"SELECT a.id FROM ej_t a JOIN (SELECT a.id, upper(a.s) AS k FROM ej_u a) b ON upper(a.s) = b.k",
			"a", "b", "id,k"},
		{"innerTableIsOuterTable",
			"SELECT ej_t.id FROM ej_t JOIN (SELECT ej_t.id, upper(ej_t.s) AS k FROM ej_t) b ON upper(ej_t.s) = b.k",
			"ej_t", "b", "id,k"},
		{"bareColumnNotPublished",
			"SELECT a.id FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1",
			"a", "b", "bid,bs"},
		{"cteReference",
			"WITH b AS (SELECT a.id, upper(a.s) AS k FROM ej_u a) SELECT a.id FROM ej_t a JOIN b ON upper(a.s) = b.k",
			"a", "b", "id,k"},
		{"lateral",
			"SELECT a.id FROM ej_t a JOIN LATERAL (SELECT a.id, upper(a.s) AS k FROM ej_u a) b ON upper(a.s) = b.k",
			"a", "b", "id,k"},
		{"nested",
			"SELECT a.id FROM ej_t a JOIN (SELECT d1.id, d1.k FROM (SELECT a.id, upper(a.s) AS k FROM ej_u a) d1) b ON upper(a.s) = b.k",
			"a", "b", "id,k"},
		// Control: a join INSIDE a derived table still sees its own relations.
		{"joinInsideTheBlock",
			"SELECT q.x FROM (SELECT a.id AS x FROM ej_t a JOIN ej_u c ON a.id = c.id) q",
			"a", "c", "id,n,s"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j := ekFirstJoin(ekBuild(t, c.sql))
			if j == nil {
				t.Fatal("no join")
			}
			tables, cols := joinSidesScanInfo(j.Children[0], j.Children[1])
			if got := ekKeys(tables[0]); got != c.leftTables {
				t.Errorf("left side answers to %q, want %q", got, c.leftTables)
			}
			if got := ekKeys(tables[1]); got != c.rightTables {
				t.Errorf("right side answers to %q, want %q", got, c.rightTables)
			}
			if got := ekKeys(cols[1]); got != c.rightCols {
				t.Errorf("right side's bare columns %q, want %q", got, c.rightCols)
			}
		})
	}
}

// TestOuterPredicateStaysOutsideTheDerivedTable: after optimization no
// predicate under the derived table's root reads the OUTER relation.
func TestOuterPredicateStaysOutsideTheDerivedTable(t *testing.T) {
	for _, sql := range []string{
		"SELECT a.id, b.id FROM ej_t a JOIN (SELECT a.id, upper(a.s) AS k FROM ej_u a) b ON upper(a.s) = b.k",
		"SELECT ej_t.id, b.id FROM ej_t JOIN (SELECT ej_t.id, upper(ej_t.s) AS k FROM ej_t) b ON lower(ej_t.s) = lower(b.k)",
		"SELECT a.id, b.bid FROM ej_t a JOIN (SELECT id AS bid, lower(s) AS bs FROM ej_u) b ON upper(a.s) = upper(b.bs) AND n > 1",
	} {
		plan := Optimize(ekBuild(t, sql), ekAnnotate)
		j := ekFirstJoin(plan)
		if j == nil {
			t.Fatalf("%s: no join", sql)
		}
		var walk func(*Node) string
		walk = func(n *Node) string {
			if n == nil {
				return ""
			}
			for _, p := range n.Predicates {
				raw := p.Raw
				if p.ASTExpr != nil {
					raw = p.ASTExpr.String()
				}
				// Every predicate inside the block reads only the block's
				// own relation: the outer one is a, ej_t's outer instance
				// (spelled like the inner one in the first two) or the
				// bare n.
				if strings.Contains(raw, "upper(a.s)") && strings.Contains(raw, "=") ||
					strings.Contains(raw, "lower(ej_t.s)") ||
					strings.Contains(raw, "n > 1") {
					return raw
				}
			}
			for _, c := range n.Children {
				if r := walk(c); r != "" {
					return r
				}
			}
			return ""
		}
		block := ekFindDerived(plan, "b")
		if block == nil {
			t.Fatalf("%s: no derived table b in the plan", sql)
		}
		if r := walk(block); r != "" {
			t.Errorf("%s\n  the outer predicate %q was pushed inside the derived table", sql, r)
		}
	}
}

// ekFindDerived returns the root of the derived table called alias.
func ekFindDerived(n *Node, alias string) *Node {
	if n == nil {
		return nil
	}
	if n.DerivedAlias == alias {
		return n
	}
	for _, c := range n.Children {
		if r := ekFindDerived(c, alias); r != nil {
			return r
		}
	}
	return nil
}
