package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Regression for #355: an aggregate over a RENAMED subquery column answered
// NULL on the stage DAG.
//
// walkStages treats an ordinary Project as a passthrough — it emits no stage —
// so a subquery's SELECT-list rename never happens on the DAG. `SELECT MAX(n)
// FROM (SELECT o_custkey AS n FROM orders)` dispatched a scan reading
// o_custkey and an aggregate asking for `n`; exec.HashAggregate resolved `n`
// to index -1 and answered NULL, while the single-process path (where the
// Project really runs) and DuckDB both answered 1499.
//
// A renamed GROUP BY key is the louder half of the same miss: an unresolvable
// key serializes as a NULL key, so every row collapses into one NULL group.
//
// resolveAggInputName is the mapping walkStages applies so the aggregate reads
// the name the stage below it actually emits — the aggregate's counterpart to
// resolveShuffleKey (join keys) and resolveSortKeyColumn (ORDER BY terms).
func TestResolveAggInputName(t *testing.T) {
	scan := func(table string, cols ...string) *logical.Node {
		n := logical.NewScan(table, "")
		n.ScanColumns = cols
		return n
	}
	project := func(child *logical.Node, projs ...logical.Projection) *logical.Node {
		return &logical.Node{Type: logical.NodeProject, Projections: projs, Children: []*logical.Node{child}}
	}
	filter := func(child *logical.Node) *logical.Node {
		return logical.NewFilter(child, []logical.Predicate{{Raw: "1 = 1"}})
	}

	tests := []struct {
		name      string
		in        string
		child     *logical.Node
		want      string
		wantExpr  string // "" = no derived expression
		wantAlias bool
	}{
		{
			name: "a plain rename resolves to its source column",
			in:   "n",
			child: project(scan("orders", "o_custkey"),
				logical.Projection{Alias: "n", Column: "o_custkey", Expr: "o_custkey"}),
			want: "o_custkey", wantAlias: true,
		},
		{
			name: "a name the projection does not rename is left alone",
			in:   "o_custkey",
			child: project(scan("orders", "o_custkey"),
				logical.Projection{Column: "o_custkey", Expr: "o_custkey"}),
			want: "o_custkey",
		},
		{
			name:  "no projection at all is left alone",
			in:    "o_custkey",
			child: scan("orders", "o_custkey"),
			want:  "o_custkey",
		},
		{
			name: "the rename is found through an intervening filter",
			in:   "n",
			child: filter(project(scan("orders", "o_custkey"),
				logical.Projection{Alias: "n", Column: "o_custkey", Expr: "o_custkey"})),
			want: "o_custkey", wantAlias: true,
		},
		{
			name: "nested renames chase down to the base column",
			in:   "m",
			child: project(project(scan("orders", "o_custkey"),
				logical.Projection{Alias: "n", Column: "o_custkey", Expr: "o_custkey"}),
				logical.Projection{Alias: "m", Column: "n", Expr: "n"}),
			want: "o_custkey", wantAlias: true,
		},
		{
			name: "an alias over an expression comes back as a derived expression",
			in:   "n",
			child: project(scan("orders", "o_custkey"),
				logical.Projection{Alias: "n", Expr: "o_custkey * 2",
					ASTExpr: &plansql.BinaryOp{
						Left:  &plansql.ColRef{Column: "o_custkey"},
						Op:    "*",
						Right: &plansql.Lit{Value: "2", Kind: plansql.LitNumber},
					}}),
			want: "n", wantExpr: "o_custkey * 2", wantAlias: true,
		},
		{
			name: "an aggregate below stops the walk — its outputs are its own names",
			in:   "n",
			child: &logical.Node{Type: logical.NodeAggregate,
				AggExprs: []logical.AggExpr{{Func: "max", InputCol: "o_custkey", OutputCol: "n"}},
				Children: []*logical.Node{project(scan("orders", "o_custkey"),
					logical.Projection{Alias: "n", Column: "o_custkey", Expr: "o_custkey"})}},
			want: "n",
		},
		{
			name: "a projection list is simultaneous: swapped names never chase each other",
			in:   "a",
			child: project(scan("t", "a", "b"),
				logical.Projection{Alias: "a", Column: "b", Expr: "b"},
				logical.Projection{Alias: "b", Column: "a", Expr: "a"}),
			want: "b", wantAlias: true,
		},
		{
			name: "a rename under one arm of a join is found",
			in:   "n",
			child: logical.NewJoin(
				scan("nation", "n_nationkey"),
				project(scan("orders", "o_custkey"),
					logical.Projection{Alias: "n", Column: "o_custkey", Expr: "o_custkey"}),
				"inner", "n_nationkey = o_custkey"),
			want: "o_custkey", wantAlias: true,
		},
		{
			name:  "an empty name is not a lookup",
			in:    "",
			child: project(scan("orders", "o_custkey"), logical.Projection{Alias: "n", Column: "o_custkey"}),
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, expr, _, alias := resolveAggInputName(tt.in, tt.child)
			if got != tt.want {
				t.Errorf("resolveAggInputName(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if tt.wantExpr == "" {
				if expr != nil {
					t.Errorf("expr = %q, want none", expr.String())
				}
			} else if expr == nil {
				t.Errorf("expr = none, want %q — with no expression the aggregate has nothing to read "+
					"and answers NULL", tt.wantExpr)
			} else if expr.String() != tt.wantExpr {
				t.Errorf("expr = %q, want %q", expr.String(), tt.wantExpr)
			}
			if alias != tt.wantAlias {
				t.Errorf("alias = %v, want %v", alias, tt.wantAlias)
			}
		})
	}
}

// TestAggregateOutputNameFollowsRename pins the other half: what a sort keyed
// on the alias must name.
//
// The answer is the key's PUBLISHED name, and it changed with ADR-0026 §2's
// two-name carrier. It used to be the source column `o_orderstatus`, because a
// stage published its keys under the same spelling the worker computed them
// from and a rename Project emits no stage of its own (#355). Now the
// resolution spelling rides beside the published one, so the aggregate finds
// the key by `o_orderstatus` and emits it as `k` — the same column name the
// single-process aggregate emits for the same query, which is what lets one
// sort key be resolved on both engines (§2b).
//
// Both halves are asserted here. Asserting the published name alone would pass
// just as happily if the stage had stopped carrying a resolution at all, and
// the key would then reach the worker spelled over a column the fragment's
// input does not have.
func TestAggregateOutputNameFollowsRename(t *testing.T) {
	scan := logical.NewScan("orders", "")
	scan.ScanColumns = []string{"o_orderstatus"}
	inner := &logical.Node{Type: logical.NodeProject,
		Projections: []logical.Projection{{Alias: "k", Column: "o_orderstatus", Expr: "o_orderstatus"}},
		Children:    []*logical.Node{scan}}
	agg := &logical.Node{Type: logical.NodeAggregate, GroupBy: []string{"k"},
		AggExprs: []logical.AggExpr{{Func: "count", OutputCol: "c"}},
		Children: []*logical.Node{inner}}

	got, ok := aggregateOutputName(agg, "k")
	if !ok {
		t.Fatal("group key \"k\" not recognized as an aggregate output")
	}
	if got != "k" {
		t.Errorf("aggregateOutputName = %q, want %q — the aggregate publishes the key under the "+
			"name the query wrote, so a sort keyed on the alias resolves to it", got, "k")
	}
	published, resolve := stageGroupKeyNames(agg, inner)
	if len(published) != 1 || published[0] != "k" {
		t.Errorf("published names %v, want [k]", published)
	}
	if len(resolve) != 1 || resolve[0].Expr != "o_orderstatus" || resolve[0].Computed {
		t.Errorf("resolution %v, want [{o_orderstatus false}] — the fragment reads the source "+
			"column, because the rename Project emits no stage of its own (#355)", resolve)
	}
}
