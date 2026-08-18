package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Regression for #313: the stage DAG lost ORDER BY whenever the SELECT list
// renamed the grouped column.
//
// The logical builder resolves ORDER BY to the SELECT list's OUTPUT name, so
// `o_orderpriority AS p ... ORDER BY p` arrives at the physical planner as a
// Sort keyed on "p". walkStages treats the Project as a passthrough, so the
// aggregate stage below still emits "o_orderpriority" — the sort matched no
// column, did nothing, and the rows came back in arbitrary order while the
// same query without `AS p` sorted correctly. TPC-H Q09 lost its ORDER BY the
// same way (`n_name AS nation`, `SUBSTR(o_orderdate,1,4) AS o_year`).
//
// The invariant asserted here: every sort key the DAG plans must name a
// column the stage it sorts actually produces, and renaming a SELECT column
// must not change the physical sort keys at all.

// stageSortKeys collects every sort key the plan carries, in stage order —
// standalone sort/merge_sort stages plus keys fused onto a producing stage by
// fuseSortIntoPredecessor.
func stageSortKeys(stages []Stage) []SortKeySpec {
	var out []SortKeySpec
	for _, s := range stages {
		out = append(out, s.SortKeys...)
	}
	return out
}

func TestAliasedSortKeyResolvesToGroupedColumn(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	tests := []struct {
		name string
		sql  string
		// control is the same query with the SELECT-list rename removed. Its
		// sort keys are the reference: an alias must plan identically.
		control string
		want    []SortKeySpec
	}{
		{
			name:    "grouped column renamed",
			sql:     "SELECT o_orderpriority AS p, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY p",
			control: "SELECT o_orderpriority, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY o_orderpriority",
			want:    []SortKeySpec{{Column: "o_orderpriority", NullsLast: true}},
		},
		{
			name:    "grouped column renamed, DESC",
			sql:     "SELECT o_orderpriority AS p, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY p DESC",
			control: "SELECT o_orderpriority, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY o_orderpriority DESC",
			want:    []SortKeySpec{{Column: "o_orderpriority", Desc: true}},
		},
		{
			// Q09's shape: one renamed bare column, one renamed grouped
			// expression. The expression's group key is spelled by its text,
			// which is what the aggregate stage emits.
			name: "grouped expression renamed",
			sql: `SELECT n_name AS nation, SUBSTR(o_orderdate, 1, 4) AS o_year, COUNT(*) AS c
				FROM orders JOIN nation ON o_custkey = n_nationkey
				GROUP BY n_name, SUBSTR(o_orderdate, 1, 4)
				ORDER BY nation, o_year DESC`,
			control: `SELECT n_name, SUBSTR(o_orderdate, 1, 4), COUNT(*) AS c
				FROM orders JOIN nation ON o_custkey = n_nationkey
				GROUP BY n_name, SUBSTR(o_orderdate, 1, 4)
				ORDER BY n_name, SUBSTR(o_orderdate, 1, 4) DESC`,
			want: []SortKeySpec{
				{Column: "n_name", NullsLast: true},
				{Column: "substr(o_orderdate, 1, 4)", Desc: true},
			},
		},
		{
			// An aggregate's alias IS its output column name, so it must be
			// left alone — resolving it away would break the sort instead.
			name:    "aggregate alias is left alone",
			sql:     "SELECT o_orderpriority AS p, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY c DESC, p",
			control: "SELECT o_orderpriority, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY c DESC, o_orderpriority",
			want: []SortKeySpec{
				{Column: "c", Desc: true},
				{Column: "o_orderpriority", NullsLast: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tt.sql, 3)
			got := stageSortKeys(stages)
			if len(got) == 0 {
				t.Fatal("plan carries no sort keys at all — the ORDER BY was dropped outright")
			}
			// Every stage carrying keys carries the same ones (sort, merge
			// tree levels, or a fused producer), so compare against the
			// expected list repeated per key-bearing stage.
			for i, k := range got {
				w := tt.want[i%len(tt.want)]
				if k.Column != w.Column || k.Desc != w.Desc {
					t.Errorf("sort key %d = %+v, want column %q desc=%v", i, k, w.Column, w.Desc)
				}
			}
			// The physical sort keys must not depend on the SELECT-list
			// rename: same query, alias removed, same keys.
			ctrl := stageSortKeys(sqlToStages(t, cat, ctx, tt.control, 3))
			if len(ctrl) != len(got) {
				t.Fatalf("aliased plan carries %d sort keys, un-aliased control carries %d", len(got), len(ctrl))
			}
			for i := range got {
				if got[i] != ctrl[i] {
					t.Errorf("sort key %d differs from the un-aliased control: got %+v, control %+v", i, got[i], ctrl[i])
				}
			}
			// The key must name a column the stage it sorts really emits.
			assertSortKeysProduced(t, stages)
		})
	}
}

// assertSortKeysProduced checks that every sort key on an aggregate-producing
// stage names one of that stage's outputs (a group key or an aggregate output
// column). A key that names nothing is exactly the #313 failure: the sort
// finds no column and silently returns its input untouched.
func assertSortKeysProduced(tb testing.TB, stages []Stage) {
	tb.Helper()
	byID := make(map[string]*Stage, len(stages))
	for i := range stages {
		byID[stages[i].ID] = &stages[i]
	}
	for i := range stages {
		s := &stages[i]
		if len(s.SortKeys) == 0 {
			continue
		}
		// The stage whose output the keys are read from: the stage itself
		// when the sort was fused into a producer, otherwise its dependency.
		src := s
		if s.Type == "sort" || s.Type == "merge_sort" {
			for _, dep := range s.Dependencies {
				if d, ok := byID[dep]; ok && len(d.GroupByCols) > 0 {
					src = d
					break
				}
			}
		}
		if len(src.GroupByCols) == 0 && len(src.FusedAggGroupBy) == 0 {
			continue // not an aggregate producer: naming is decided elsewhere
		}
		produced := append(append([]string(nil), src.GroupByCols...), src.FusedAggGroupBy...)
		for _, a := range src.AggSpecs {
			produced = append(produced, a.OutputCol)
		}
		for _, a := range src.FusedAggSpecs {
			produced = append(produced, a.OutputCol)
		}
		for _, k := range s.SortKeys {
			found := false
			for _, p := range produced {
				if strings.EqualFold(p, k.Column) {
					found = true
					break
				}
			}
			if !found {
				tb.Errorf("stage %s (%s) sorts on %q, which %s does not produce (outputs: %v)",
					s.ID, s.Type, k.Column, src.ID, produced)
			}
		}
	}
}

func TestResolveSortKeyColumn(t *testing.T) {
	agg := func(groupBy []string, outs ...string) *logical.Node {
		n := &logical.Node{Type: logical.NodeAggregate, GroupBy: groupBy}
		for _, o := range outs {
			n.AggExprs = append(n.AggExprs, logical.AggExpr{Func: "count", OutputCol: o})
		}
		return n
	}
	project := func(child *logical.Node, projs ...logical.Projection) *logical.Node {
		return &logical.Node{Type: logical.NodeProject, Projections: projs, Children: []*logical.Node{child}}
	}

	tests := []struct {
		name  string
		key   string
		child *logical.Node
		want  string
	}{
		{
			name: "bare rename resolves to the grouped column",
			key:  "p",
			child: project(agg([]string{"o_orderpriority"}, "c"),
				logical.Projection{Alias: "p", Column: "o_orderpriority", Expr: "o_orderpriority"},
				logical.Projection{Alias: "c", Expr: "count(*)", IsAgg: true}),
			want: "o_orderpriority",
		},
		{
			name: "renamed grouped expression resolves to its group-key text",
			key:  "o_year",
			child: project(agg([]string{"substr(o_orderdate, 1, 4)"}, "c"),
				logical.Projection{Alias: "o_year", Expr: "substr(o_orderdate, 1, 4)"}),
			want: "substr(o_orderdate, 1, 4)",
		},
		{
			name: "aggregate alias is already the output column",
			key:  "c",
			child: project(agg([]string{"o_orderpriority"}, "c"),
				logical.Projection{Alias: "c", Expr: "count(*)", IsAgg: true}),
			want: "c",
		},
		{
			name: "unresolvable key is left untouched",
			key:  "nope",
			child: project(agg([]string{"o_orderpriority"}, "c"),
				logical.Projection{Alias: "p", Column: "o_orderpriority"}),
			want: "nope",
		},
		{
			name: "swapped names substitute once, never chase each other",
			key:  "a",
			child: project(agg([]string{"b"}, "c"),
				logical.Projection{Alias: "a", Column: "b"},
				logical.Projection{Alias: "b", Column: "a"}),
			want: "b",
		},
		{
			name: "descends through an order-preserving passthrough",
			key:  "p",
			child: &logical.Node{Type: logical.NodeDistinct, Children: []*logical.Node{
				project(agg([]string{"o_orderpriority"}, "c"),
					logical.Projection{Alias: "p", Column: "o_orderpriority"}),
			}},
			want: "o_orderpriority",
		},
		{
			// No aggregate below: the producing fragment's naming is settled
			// later (attachScanSelectProjections may alias its output), so
			// the key must be left as the builder resolved it.
			name:  "scan below the project is left alone",
			key:   "p",
			child: project(&logical.Node{Type: logical.NodeScan, TableName: "orders"}, logical.Projection{Alias: "p", Column: "o_orderpriority"}),
			want:  "p",
		},
		{
			name:  "nil child is a no-op",
			key:   "p",
			child: nil,
			want:  "p",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveSortKeyColumn(tt.key, tt.child); got != tt.want {
				t.Errorf("resolveSortKeyColumn(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}
