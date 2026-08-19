package sql

import (
	"reflect"
	"strings"
	"testing"
)

// OuterColumnCandidates is what keeps a correlated subquery's outer columns
// alive through column pruning (#347). Under-reporting is the wrong answer it
// exists to prevent, so the cases below pin the recursion and the one
// exclusion it is allowed to make.
func TestOuterColumnCandidates(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			// The #347 subquery. c_acctbal is the subquery's own and
			// unqualified — reported anyway, because attributing an
			// unqualified name needs a catalog this package does not have,
			// and a name the outer scan lacks is dropped downstream.
			name: "qualified_outer_ref",
			sql:  "SELECT AVG(c_acctbal) FROM customer c2 WHERE c2.c_nationkey < c1.c_nationkey",
			want: []string{"c_acctbal", "c_nationkey"},
		},
		{
			// A reference qualified by the subquery's OWN alias is the one
			// case that can be ruled out, and is.
			name: "own_alias_excluded",
			sql:  "SELECT AVG(c2.c_acctbal) FROM customer c2 WHERE c2.c_nationkey < 5",
			want: nil,
		},
		{
			// Same, via the table name rather than the alias.
			name: "own_table_name_excluded",
			sql:  "SELECT 1 FROM customer WHERE customer.c_nationkey < 5",
			want: nil,
		},
		{
			// Two levels down. Missing this is the nested half of the bug:
			// pruning drops the column, and the outer batch cannot supply
			// what the innermost SELECT asks for.
			name: "nested_two_deep",
			sql: `SELECT AVG(c2.c_acctbal) FROM customer c2
				WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
					WHERE c3.c_nationkey < c1.c_nationkey)`,
			want: []string{"c_nationkey"},
		},
		{
			name: "nested_exists",
			sql: `SELECT AVG(c2.c_acctbal) FROM customer c2
				WHERE EXISTS (SELECT 1 FROM nation n WHERE n.n_nationkey = c2.c_nationkey
					AND n.n_nationkey < c1.c_nationkey)`,
			want: []string{"c_nationkey"},
		},
		{
			// HAVING and the SELECT list are walked too — a correlation can
			// sit in either.
			name: "having_and_select_list",
			sql: `SELECT MAX(o.o_totalprice) - c1.c_acctbal FROM orders o
				GROUP BY o.o_custkey HAVING COUNT(*) > c1.c_custkey`,
			want: []string{"c_acctbal", "c_custkey"},
		},
		{
			// Unparseable text yields nothing rather than a guess. The
			// expression compiler parses the same text and declines to build
			// a correlated evaluator for it, and readOuterValues fails loudly
			// if one is built anyway.
			name: "unparseable",
			sql:  "NOT A QUERY",
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := OuterColumnCandidates(tc.sql)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("OuterColumnCandidates(%q) = %v, want %v", tc.sql, got, tc.want)
			}
		})
	}
}

// The correlation analysis has to see a reference bound two levels up, or the
// evaluator never substitutes it and the inner SQL reaches the runner still
// naming a table that is not in its FROM.
func TestFindCorrelatedRefsDescendsIntoNestedSubqueries(t *testing.T) {
	outer := map[string]bool{"c1": true, "customer": true}
	sql := `SELECT AVG(c2.c_acctbal) FROM customer c2
		WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
			WHERE c3.c_nationkey < c1.c_nationkey)`

	refs, err := FindCorrelatedRefs(sql, outer)
	if err != nil {
		t.Fatalf("FindCorrelatedRefs: %v", err)
	}
	want := []OuterRef{{Table: "c1", Column: "c_nationkey"}}
	if !reflect.DeepEqual(refs, want) {
		t.Fatalf("refs = %v, want %v", refs, want)
	}
}

// And the substitution has to reach the same depth. Before it did, the
// rewritten SQL still read `c1.c_nationkey`.
func TestRewriteOuterRefsRewritesNestedSubqueries(t *testing.T) {
	outer := map[string]bool{"c1": true}
	vals := map[string]any{"c1.c_nationkey": int64(7)}

	parsed, err := Parse(`SELECT AVG(c2.c_acctbal) FROM customer c2
		WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
			WHERE c3.c_nationkey < c1.c_nationkey)`)
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	got := RewriteOuterRefs(info.WhereExpr, outer, vals).String()
	if want := "c3.c_nationkey < 7"; !strings.Contains(got, want) {
		t.Errorf("rewritten WHERE = %q, want it to contain %q", got, want)
	}
	if strings.Contains(got, "c1.c_nationkey") {
		t.Errorf("rewritten WHERE = %q still names the outer column", got)
	}
}

// A nested subquery with nothing to substitute keeps its ORIGINAL text: the
// rewrite must not round-trip every subquery through RebuildSQL just because
// it walked one.
func TestRewriteOuterRefsLeavesUntouchedNestedSubqueryVerbatim(t *testing.T) {
	original := "SELECT AVG(c3.c_acctbal) FROM customer c3 WHERE c3.c_nationkey < 5"
	node := &SubqueryNode{SQL: original}

	got := RewriteOuterRefs(node, map[string]bool{"c1": true}, map[string]any{"c1.c_nationkey": int64(7)})
	sq, ok := got.(*SubqueryNode)
	if !ok {
		t.Fatalf("got %T, want *SubqueryNode", got)
	}
	if sq.SQL != original {
		t.Errorf("SQL = %q, want the original %q untouched", sq.SQL, original)
	}
	if sq != node {
		t.Error("an unchanged subquery should be returned as the same node")
	}
}
