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
