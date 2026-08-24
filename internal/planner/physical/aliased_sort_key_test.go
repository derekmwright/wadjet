package physical

import (
	"reflect"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// Regression for #316: the aggregate-free sibling of the above. With no
// aggregate below the Sort there is nothing for resolveSortKeyColumn to
// resolve against — the sort key stays spelled as the SELECT alias, and the
// only thing that can make it resolve is attachScanSelectProjections
// materializing that alias on the producing fragment. That pass used to fire
// only when the SELECT list carried a scalar expression, so
// `SELECT o_orderpriority AS p FROM orders ORDER BY p` planned a sort on "p"
// over a scan emitting "o_orderpriority": the sort matched no column, did
// nothing, and only the gather's rename made the output look right.
//
// The invariant asserted here: every sort key names a column the stage below
// it actually EMITS — its alias-naming projection when it carries one, its
// column set otherwise — and the alias resolves to the SELECT item that owns
// it even when it shadows another column of the same input.
func TestAliasedSortKeyWithoutAggregateResolves(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	tests := []struct {
		name string
		sql  string
		want []string
		// wantProjected is the alias-materializing projection the producing
		// stage must carry, as expr→name pairs in order. Nil means the plan
		// must be left alone: nothing needs the alias, so narrowing the
		// fragment's schema would be pure cost.
		wantProjected []ProjectExprSpec
	}{
		{
			// #316 verbatim.
			name:          "bare column alias",
			sql:           "SELECT o_orderpriority AS p FROM orders ORDER BY p",
			want:          []string{"p"},
			wantProjected: []ProjectExprSpec{{Expr: "o_orderpriority", Name: "p"}},
		},
		{
			// No rename: the scan already emits the sort key, so the plan
			// must come out byte-identical to what it was before #316.
			name: "unaliased bare column",
			sql:  "SELECT o_orderpriority FROM orders ORDER BY o_orderpriority",
			want: []string{"o_orderpriority"},
		},
		{
			// The sharp one: the alias names a DIFFERENT column of the same
			// table. Sorting on the raw input column would find a real
			// "n_comment" and order by the wrong values — a divergence no
			// row-count or unordered compare can see. The projection must
			// emit "n_comment" FROM n_name.
			name: "alias shadows another column",
			sql:  "SELECT n_name AS n_comment, n_comment AS c FROM nation ORDER BY n_comment",
			want: []string{"n_comment"},
			wantProjected: []ProjectExprSpec{
				{Expr: "n_name", Name: "n_comment"},
				{Expr: "n_comment", Name: "c"},
			},
		},
		{
			// Materializing the aliased key must not cost the plan the
			// un-aliased one: OpProject narrows the schema to exactly its
			// outputs, so every key has to survive the projection.
			name: "multiple keys, only one aliased",
			sql:  "SELECT o_orderpriority AS p, o_orderstatus FROM orders ORDER BY p, o_orderstatus",
			want: []string{"p", "o_orderstatus"},
			wantProjected: []ProjectExprSpec{
				{Expr: "o_orderpriority", Name: "p"},
				{Expr: "o_orderstatus", Name: "o_orderstatus"},
			},
		},
		{
			// Same shape one hop up: the producer is a join, not a scan.
			name:          "alias over a join",
			sql:           "SELECT n_name AS nm FROM nation JOIN region ON n_regionkey = r_regionkey ORDER BY nm",
			want:          []string{"nm"},
			wantProjected: []ProjectExprSpec{{Expr: "n_name", Name: "nm"}},
		},
		{
			// The accidental-correctness control from the issue: an
			// expression in the SELECT list already fired this pass. It must
			// keep planning exactly as it did.
			name: "expression in the select list",
			sql:  "SELECT n_name AS nm, UPPER(n_comment) AS uc FROM nation ORDER BY nm",
			want: []string{"nm"},
			wantProjected: []ProjectExprSpec{
				{Expr: "n_name", Name: "nm"},
				{Expr: "upper(n_comment)", Name: "uc", Type: parquet.TypeString, TypeKnown: true},
			},
		},
		{
			// No ORDER BY: nothing needs the alias materialized, and the
			// gather's rename covers the output schema on its own.
			name: "renamed column with no sort",
			sql:  "SELECT o_orderpriority AS p FROM orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := sqlToStages(t, cat, ctx, tt.sql, 3)
			byID := make(map[string]*Stage, len(stages))
			for i := range stages {
				byID[stages[i].ID] = &stages[i]
			}
			producer := sortProducer(t, stages, byID)

			if got := stageSortKeys(stages); len(tt.want) == 0 {
				if len(got) != 0 {
					t.Errorf("plan carries sort keys %+v for a query with no ORDER BY", got)
				}
			} else {
				if len(got) == 0 {
					t.Fatal("plan carries no sort keys at all — the ORDER BY was dropped outright")
				}
				for i, k := range got {
					if w := tt.want[i%len(tt.want)]; k.Column != w {
						t.Errorf("sort key %d = %q, want %q", i, k.Column, w)
					}
				}
				// The bug: a key naming something the producer does not emit.
				emitted := stageOutputNames(producer)
				for _, k := range got {
					if !containsFold(emitted, k.Column) {
						t.Errorf("sort key %q names no output of %s (%s), which emits %v — "+
							"the sort finds no column and silently returns its input unsorted",
							k.Column, producer.ID, producer.Type, emitted)
					}
				}
			}

			if len(tt.wantProjected) == 0 {
				if len(producer.ProjectExprs) != 0 {
					t.Errorf("%s carries projection %+v; this shape needs none",
						producer.ID, producer.ProjectExprs)
				}
			} else if !reflect.DeepEqual(producer.ProjectExprs, tt.wantProjected) {
				t.Errorf("%s projection = %+v, want %+v", producer.ID, producer.ProjectExprs, tt.wantProjected)
			}

			assertGatherRenamesResolve(t, stages, byID)
		})
	}
}

// sortProducer returns the stage whose output the plan's sort keys are read
// from: the first sort/merge_sort stage's transitive dependency, skipping the
// sort tree itself. Falls back to the gather's dependency when the query has
// no ORDER BY.
func sortProducer(tb testing.TB, stages []Stage, byID map[string]*Stage) *Stage {
	tb.Helper()
	var cur *Stage
	for i := range stages {
		if t := stages[i].Type; t == "sort" || t == "merge_sort" || t == StageExchangeGather {
			cur = &stages[i]
			break
		}
	}
	if cur == nil {
		tb.Fatal("plan has neither a sort nor a gather stage")
	}
	for range stages {
		if len(cur.Dependencies) != 1 {
			return cur
		}
		dep, ok := byID[cur.Dependencies[0]]
		if !ok {
			return cur
		}
		if t := dep.Type; t != "sort" && t != "merge_sort" && t != StageExchangeGather {
			return dep
		}
		cur = dep
	}
	return cur
}

// stageOutputNames is the column set a stage emits. A stage carrying
// ProjectExprs emits exactly those (OpProject narrows the fragment's schema
// to its projections and drops everything else); otherwise it passes its
// input columns through.
func stageOutputNames(s *Stage) []string {
	if len(s.ProjectExprs) > 0 {
		out := make([]string, len(s.ProjectExprs))
		for i, p := range s.ProjectExprs {
			out[i] = p.Name
		}
		return out
	}
	return s.Columns
}

func containsFold(names []string, want string) bool {
	for _, n := range names {
		if strings.EqualFold(n, want) {
			return true
		}
	}
	return false
}

// assertGatherRenamesResolve checks the gather's source→alias pairs against
// the columns the stage below it really emits: each source must resolve, and
// no two may resolve to the same column. A stale pair left behind after the
// producer was given an alias-naming projection renames an ALREADY-renamed
// column a second time — `n_name AS n_comment, n_comment AS c` came back with
// both output columns named "c".
func assertGatherRenamesResolve(tb testing.TB, stages []Stage, byID map[string]*Stage) {
	tb.Helper()
	var gather *Stage
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			gather = &stages[i]
			break
		}
	}
	if gather == nil || len(gather.OutputRenames) == 0 {
		return
	}
	emitted := stageOutputNames(sortProducer(tb, stages, byID))
	if len(emitted) == 0 {
		return // producer's column set not modeled at plan time
	}
	taken := make(map[string]string, len(gather.OutputRenames))
	for _, r := range gather.OutputRenames {
		if r.Expr != nil {
			continue // evaluated at the gather, not read from a column
		}
		if !containsFold(emitted, r.From) {
			tb.Errorf("gather renames %q → %q, but the stage below emits %v",
				r.From, r.To, emitted)
			continue
		}
		key := strings.ToLower(r.From)
		if prev, dup := taken[key]; dup {
			tb.Errorf("gather renames column %q twice (→ %q and → %q): "+
				"one alias shadows another item's source and the output loses a column name",
				r.From, prev, r.To)
		}
		taken[key] = r.To
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
