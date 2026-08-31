package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

func planStagesForRenameTest(t *testing.T, sql string) []Stage {
	t.Helper()
	cat, ctx := setupTPCHCatalog(t)
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = 3
	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}
	return stages
}

func gatherRenames(t *testing.T, stages []Stage) []OutputRename {
	t.Helper()
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return stages[i].OutputRenames
		}
	}
	t.Fatal("no Gather stage emitted")
	return nil
}

// TestResolveOutputRenameSource covers the walk itself over constructed
// logical trees: plain rename, chained rename, self-rename, computed alias
// stop, aggregate stop, and join recursion.
func TestResolveOutputRenameSource(t *testing.T) {
	scan := func() *logical.Node { return &logical.Node{Type: logical.NodeScan, TableName: "region"} }
	renameProj := func(col, alias string, child *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeProject,
			Projections: []logical.Projection{{Column: col, Expr: col, Alias: alias}},
			Children:    []*logical.Node{child}}
	}

	tests := []struct {
		name  string
		in    string
		child *logical.Node
		want  string
	}{
		{name: "plain rename", in: "k",
			child: renameProj("r_regionkey", "k", scan()), want: "r_regionkey"},
		{name: "not an alias", in: "r_name",
			child: renameProj("r_regionkey", "k", scan()), want: "r_name"},
		{name: "chained through nested projects", in: "a",
			child: renameProj("b", "a", renameProj("r_regionkey", "b", scan())),
			want:  "r_regionkey"},
		{name: "self-rename stops", in: "k",
			child: renameProj("k", "k", scan()), want: "k"},
		{name: "through a filter", in: "k",
			child: &logical.Node{Type: logical.NodeFilter,
				Children: []*logical.Node{renameProj("r_regionkey", "k", scan())}},
			want: "r_regionkey"},
		{name: "computed alias stops (materialized under its own name)", in: "rk2",
			child: &logical.Node{Type: logical.NodeProject,
				Projections: []logical.Projection{{Column: "", Expr: "nullif(r_regionkey, 2)", Alias: "rk2"}},
				Children:    []*logical.Node{scan()}},
			want: "rk2"},
		{name: "aggregate stops the walk", in: "k",
			child: &logical.Node{Type: logical.NodeAggregate,
				Children: []*logical.Node{renameProj("r_regionkey", "k", scan())}},
			want: "k"},
		{name: "join recurses into build side", in: "k",
			child: &logical.Node{Type: logical.NodeJoin, JoinType: "inner",
				Children: []*logical.Node{scan(), renameProj("r_regionkey", "k", scan())}},
			want: "r_regionkey"},
		{name: "join recurses into probe side first", in: "k",
			child: &logical.Node{Type: logical.NodeJoin, JoinType: "inner",
				Children: []*logical.Node{renameProj("n_regionkey", "k", scan()), scan()}},
			want: "n_regionkey"},
		{name: "semi join skips the build side", in: "k",
			child: &logical.Node{Type: logical.NodeJoin, JoinType: "semi",
				Children: []*logical.Node{scan(), renameProj("r_regionkey", "k", scan())}},
			want: "k"},
		{name: "nil child", in: "k", child: nil, want: "k"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveOutputRenameSource(tc.in, tc.child); got != tc.want {
				t.Errorf("resolveOutputRenameSource(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestResolveJoinNeededColumns verifies alias resolution and that plans with
// nothing to resolve keep the identical slice (byte-identical plans).
func TestResolveJoinNeededColumns(t *testing.T) {
	scan := &logical.Node{Type: logical.NodeScan, TableName: "nation"}
	sub := &logical.Node{Type: logical.NodeProject,
		Projections: []logical.Projection{{Column: "r_regionkey", Expr: "r_regionkey", Alias: "k"}},
		Children:    []*logical.Node{&logical.Node{Type: logical.NodeScan, TableName: "region"}}}
	join := &logical.Node{Type: logical.NodeJoin, JoinType: "inner",
		Children:      []*logical.Node{scan, sub},
		NeededColumns: []string{"k", "n_name", "n_regionkey"}}

	got := resolveJoinNeededColumns(join)
	want := []string{"r_regionkey", "n_name", "n_regionkey"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("resolved = %v, want %v", got, want)
	}

	// Alias AND its source both needed: the mapping introduces a duplicate,
	// which must collapse.
	join.NeededColumns = []string{"k", "r_regionkey", "n_name"}
	got = resolveJoinNeededColumns(join)
	want = []string{"r_regionkey", "n_name"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("deduped = %v, want %v", got, want)
	}

	// Nothing resolves: the ORIGINAL slice comes back untouched.
	join.NeededColumns = []string{"n_name", "n_regionkey"}
	got = resolveJoinNeededColumns(join)
	if len(got) != 2 || &got[0] != &join.NeededColumns[0] {
		t.Errorf("unchanged plan must return the original slice, got %v", got)
	}
}

// TestPlanDistributed_NestedRenameResolvesGatherSource is the end-to-end
// planner check for #385: when the outer SELECT merely forwards a subquery's
// rename, the Gather's OutputRenames must source the COLUMN the streams carry
// (r_regionkey), not the alias no stage ever emits.
func TestPlanDistributed_NestedRenameResolvesGatherSource(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string]string // From -> To
	}{
		// No ORDER BY: with a sort in the plan the #386 pass materializes
		// the alias at the scan and re-points the rename to {k -> k}
		// instead (covered by TestAttachScanSelectProjections_NestedRename).
		{name: "bare forward",
			sql:  `SELECT k FROM (SELECT r_regionkey AS k FROM region) t`,
			want: map[string]string{"r_regionkey": "k"}},
		{name: "multi rename",
			sql:  `SELECT k1, k2 FROM (SELECT r_regionkey AS k1, r_name AS k2 FROM region) t`,
			want: map[string]string{"r_regionkey": "k1", "r_name": "k2"}},
		{name: "chained",
			sql:  `SELECT a FROM (SELECT b AS a FROM (SELECT r_regionkey AS b FROM region) u) t`,
			want: map[string]string{"r_regionkey": "a"}},
		{name: "rename above an aggregate subquery",
			sql: `SELECT k FROM (SELECT n_regionkey AS k, COUNT(*) AS c FROM nation
				GROUP BY n_regionkey) t ORDER BY k`,
			// The aggregate carries the ORDER BY, fused onto its own stage
			// and keyed on `n_regionkey`. A projection materializing `k`
			// above it would rename the key out from under the consumer that
			// reads the ordering off its direct dependency, so the SELECT
			// list is not attached there and the gather resolves the rename
			// through the source name (#656 R5).
			want: map[string]string{"n_regionkey": "k"}},
		{name: "rename under a join build side",
			sql: `SELECT n_name, k FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
				ON n_regionkey = k ORDER BY n_name`,
			want: map[string]string{"n_name": "n_name", "r_regionkey": "k"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			renames := gatherRenames(t, planStagesForRenameTest(t, tc.sql))
			got := map[string]string{}
			for _, r := range renames {
				got[r.From] = r.To
			}
			for from, to := range tc.want {
				if got[from] != to {
					t.Errorf("missing rename %q -> %q (got map %v)", from, to, got)
				}
			}
			if len(got) != len(tc.want) {
				t.Errorf("rename map %v, want exactly %v", got, tc.want)
			}
		})
	}
}

// TestPlanDistributed_JoinColumnsResolveNestedRename asserts the join
// stage's Columns (the worker's output filter) carry the SOURCE column, not
// the subquery alias the streams never contain — the width half of #385's
// join face.
func TestPlanDistributed_JoinColumnsResolveNestedRename(t *testing.T) {
	stages := planStagesForRenameTest(t,
		`SELECT n_name, k FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
			ON n_regionkey = k ORDER BY n_name`)
	var join *Stage
	for i := range stages {
		if strings.Contains(stages[i].Type, "join") {
			join = &stages[i]
			break
		}
	}
	if join == nil {
		t.Fatal("no join stage emitted")
	}
	has := func(col string) bool {
		for _, c := range join.Columns {
			if strings.EqualFold(c, col) {
				return true
			}
		}
		return false
	}
	if has("k") {
		t.Errorf("join Columns %v still carry the alias k — no stream contains it", join.Columns)
	}
	if !has("r_regionkey") {
		t.Errorf("join Columns %v missing the resolved source r_regionkey", join.Columns)
	}
}

// The two walks in this file are one question asked twice, and they must not
// disagree about what a SCOPE is.
//
// `resolveRenameSource` maps a name to the column the streams carry, and
// `relationScopeSubtree` finds the subtree a QUALIFIER names so that map can be
// asked of the right arm. Both descend a logical tree. Where one descends
// through a node and the other stops at it, a qualified reference is resolved
// in a scope wider than the one it named — and a bare lookup in a scope that
// spans a join takes the first arm that answers, which is #742's silent
// capture. That is exactly how WINDOW was missed: `resolveRenameSource` has no
// case for it and descended, `scopePreservingWrapper` did not list it and
// stopped, and `SUM(y.w) OVER ()` in the SELECT list put one between the outer
// Project and the join.
//
// So the agreement is asserted rather than argued, over EVERY logical node
// type — a new node kind fails the completeness check below rather than
// silently resolving one way in one walk and the other way in the other.
//
// Two node types are stops in `relationScopeSubtree` and NOT in
// `resolveRenameSource`, deliberately and for stated reasons, so they are
// listed as such instead of being cross-checked:
//
//   - Project IS the scope boundary — its projection list is the scope's own
//     SELECT list, and the caller looks a name up INSIDE the scope, not
//     through it;
//   - Aggregate replaces its child's schema with its GROUP BY keys and
//     aggregate outputs, so a bare name resolved below it answers from a
//     schema the stream does not carry. `resolveRenameSource` stops there too;
//     it is listed here only because the probe below cannot distinguish "stops
//     at the aggregate" from "descends and finds nothing".
func TestScopePreservingWrapperMatchesTheRenameWalk(t *testing.T) {
	// A rename-only Project over a scan: `w` resolves to `a` below it, and
	// stays `w` above anything that does not descend.
	inner := func() *logical.Node {
		return &logical.Node{Type: logical.NodeProject,
			Projections: []logical.Projection{{Column: "a", Expr: "a", Alias: "w"}},
			Children: []*logical.Node{{Type: logical.NodeScan,
				TableName: "region", TableAlias: "x"}}}
	}

	type expectation struct {
		wrapper bool   // what scopePreservingWrapper must answer
		crossed bool   // whether the rename-walk probe is meaningful here
		why     string // why, in one line, for the failure message
	}
	table := map[logical.NodeType]expectation{
		logical.NodeFilter: {true, true, "narrows rows; renames nothing"},
		logical.NodeSort:   {true, true, "reorders rows; renames nothing"},
		logical.NodeLimit:  {true, true, "drops rows; renames nothing"},
		logical.NodeDistinct: {true, true,
			"dedups rows; renames nothing"},
		logical.NodeWindow: {true, true,
			"APPENDS its output columns; renames no existing one"},
		logical.NodeProject: {false, false,
			"IS the scope boundary — the caller looks up inside it"},
		logical.NodeAggregate: {false, false,
			"replaces the child's schema with its keys and outputs"},
		logical.NodeJoin: {false, false,
			"the walk SPLITS here rather than descending, so the probe cannot " +
				"tell the two apart"},
		logical.NodeUnion: {false, true,
			"two arms, and the output naming is re-rooted onto the first"},
		logical.NodeIntersect: {false, true, "as Union"},
		logical.NodeExcept:    {false, true, "as Union"},
		logical.NodeScan:      {false, true, "a leaf: nothing below it"},
		logical.NodeDual:      {false, true, "a leaf: nothing below it"},
	}
	// The arity a node really has. A one-child probe under a Join or a set
	// operation is not that node at all: `resolveRenameSource`'s generic
	// single-child descent fires and the probe reports a descent no real plan
	// ever performs. The first cut of this test did exactly that and said the
	// two walks disagreed about four node types when they do not.
	twoArmed := map[logical.NodeType]bool{
		logical.NodeJoin: true, logical.NodeUnion: true,
		logical.NodeIntersect: true, logical.NodeExcept: true,
	}

	// Completeness: every NodeType the logical package declares is in the
	// table, so adding one without deciding this question fails here. The
	// upper bound is found by walking until String() stops recognising the
	// value, which is what a new constant extends.
	for id := logical.NodeType(0); ; id++ {
		if strings.HasPrefix(id.String(), "Unknown(") {
			break
		}
		if _, ok := table[id]; !ok {
			t.Fatalf("logical node type %s (%d) is not in this table — decide whether a "+
				"scope walk may descend through it and say why", id, int(id))
		}
	}

	for id, want := range table {
		id, want := id, want
		t.Run(id.String(), func(t *testing.T) {
			n := &logical.Node{Type: id, Children: []*logical.Node{inner()}}
			switch {
			case id == logical.NodeScan || id == logical.NodeDual:
				n.Children = nil // leaves carry no child
			case twoArmed[id]:
				n.Children = append(n.Children, inner())
			}
			if got := scopePreservingWrapper(n); got != want.wrapper {
				t.Errorf("scopePreservingWrapper(%s) = %v, want %v — %s",
					id, got, want.wrapper, want.why)
			}
			if !want.crossed {
				return
			}
			// The rename walk descends iff it resolves `w` to the source
			// column the Project below renames.
			descends := resolveOutputRenameSource("w", n) == "a"
			if descends != want.wrapper {
				t.Errorf("resolveRenameSource descends through %s = %v while "+
					"scopePreservingWrapper says %v — the two walks disagree about "+
					"what a scope is, which resolves a qualified reference in the "+
					"wrong arm (#742). %s",
					id, descends, want.wrapper, want.why)
			}
		})
	}
}
