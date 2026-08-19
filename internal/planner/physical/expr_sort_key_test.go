package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// Regression for #320 on the stage DAG: an ORDER BY term the SELECT list does
// not carry.
//
// The logical builder materializes such a term as a hidden column on the
// SELECT-list projection and keys the sort on it. On the DAG that only works
// if the fragment BELOW the sort really computes the column —
// attachScanSelectProjections has to carry the hidden projection through to
// the producing stage, and the gather's rename list has to leave it out so the
// column never reaches the client.
//
// Both halves are asserted here: every sort key names a column its producing
// stage emits (the invariant #313 and #316 each restored for their own
// spelling), and the gather returns exactly the SELECT-list columns.
func TestExpressionSortKeyIsProducedOnTheDAG(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	tests := []struct {
		name string
		sql  string
		// wantKeys is the sort key spelling every key-bearing stage carries.
		wantKeys []string
		// wantProjected is the projection the producing stage must carry, as
		// expr→name pairs in order.
		wantProjected []ProjectExprSpec
		// wantOutputs is the gather's output column list — the SELECT list,
		// and nothing else.
		wantOutputs []string
	}{
		{
			// #320 verbatim: nothing in the plan computed `length(n_name)`,
			// so the sort matched no column and passed its input through.
			name:     "function of a selected column",
			sql:      "SELECT n_name FROM nation ORDER BY LENGTH(n_name)",
			wantKeys: []string{"__sortkey_0"},
			wantProjected: []ProjectExprSpec{
				{Expr: "n_name", Name: "n_name"},
				{Expr: "length(n_name)", Name: "__sortkey_0"},
			},
			wantOutputs: []string{"n_name"},
		},
		{
			// The same failure with no expression: a column the SELECT-list
			// projection had already dropped.
			name:     "column not in the select list",
			sql:      "SELECT n_comment FROM nation ORDER BY n_name",
			wantKeys: []string{"__sortkey_0"},
			wantProjected: []ProjectExprSpec{
				{Expr: "n_comment", Name: "n_comment"},
				{Expr: "n_name", Name: "__sortkey_0"},
			},
			wantOutputs: []string{"n_comment"},
		},
		{
			// Mixed keys: one carried by the select list, one materialized.
			// Materializing the second must not cost the plan the first —
			// OpProject narrows the schema to exactly its outputs.
			name:     "mixed carried and materialized keys",
			sql:      "SELECT n_name, n_regionkey FROM nation ORDER BY n_regionkey, LENGTH(n_name)",
			wantKeys: []string{"n_regionkey", "__sortkey_0"},
			wantProjected: []ProjectExprSpec{
				{Expr: "n_name", Name: "n_name"},
				{Expr: "n_regionkey", Name: "n_regionkey"},
				{Expr: "length(n_name)", Name: "__sortkey_0"},
			},
			wantOutputs: []string{"n_name", "n_regionkey"},
		},
		{
			// One hop up: the producer is a join, not a scan.
			name:     "materialized key over a join",
			sql:      "SELECT n_name FROM nation JOIN region ON n_regionkey = r_regionkey ORDER BY LENGTH(n_name)",
			wantKeys: []string{"__sortkey_0"},
			wantProjected: []ProjectExprSpec{
				{Expr: "n_name", Name: "n_name"},
				{Expr: "length(n_name)", Name: "__sortkey_0"},
			},
			wantOutputs: []string{"n_name"},
		},
		{
			// The control: an ORDER BY the select list already carries plans
			// exactly as it did before — no hidden column, no projection.
			name:        "select-list column needs nothing",
			sql:         "SELECT n_name FROM nation ORDER BY n_name",
			wantKeys:    []string{"n_name"},
			wantOutputs: []string{"n_name"},
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

			keys := stageSortKeys(stages)
			if len(keys) == 0 {
				t.Fatal("plan carries no sort keys at all — the ORDER BY was dropped outright")
			}
			for i, k := range keys {
				if w := tt.wantKeys[i%len(tt.wantKeys)]; k.Column != w {
					t.Errorf("sort key %d = %q, want %q", i, k.Column, w)
				}
			}
			// The #313/#316/#320 invariant: a key naming nothing the producer
			// emits is a sort that silently returns its input unsorted.
			emitted := stageOutputNames(producer)
			for _, k := range keys {
				if !containsFold(emitted, k.Column) {
					t.Errorf("sort key %q names no output of %s (%s), which emits %v — "+
						"the sort finds no column and silently returns its input unsorted",
						k.Column, producer.ID, producer.Type, emitted)
				}
			}

			if len(tt.wantProjected) == 0 {
				if len(producer.ProjectExprs) != 0 {
					t.Errorf("%s carries projection %+v; this shape needs none", producer.ID, producer.ProjectExprs)
				}
			} else {
				if len(producer.ProjectExprs) != len(tt.wantProjected) {
					t.Fatalf("%s projection = %+v, want %+v", producer.ID, producer.ProjectExprs, tt.wantProjected)
				}
				for i, w := range tt.wantProjected {
					if got := producer.ProjectExprs[i]; got.Expr != w.Expr || got.Name != w.Name {
						t.Errorf("%s projection %d = %+v, want expr %q name %q", producer.ID, i, got, w.Expr, w.Name)
					}
				}
			}

			// The hidden column must not reach the client: the gather projects
			// to exactly the names it lists, so leaving it unlisted is what
			// drops it.
			gather := gatherStage(t, stages)
			var outputs []string
			for _, r := range gather.OutputRenames {
				outputs = append(outputs, r.To)
				if logical.IsHiddenSortColumn(r.To) || logical.IsHiddenSortColumn(r.From) {
					t.Errorf("gather emits %q → %q; a materialized sort column must be dropped before the result", r.From, r.To)
				}
			}
			if len(tt.wantProjected) > 0 && len(outputs) == 0 {
				t.Fatal("gather carries no output renames, so nothing drops the materialized sort column")
			}
			if len(outputs) > 0 {
				if len(outputs) != len(tt.wantOutputs) {
					t.Fatalf("gather emits %v, want the select list %v", outputs, tt.wantOutputs)
				}
				for i, w := range tt.wantOutputs {
					if !strings.EqualFold(outputs[i], w) {
						t.Errorf("gather output %d = %q, want %q", i, outputs[i], w)
					}
				}
			}
			assertGatherRenamesResolve(t, stages, byID)
		})
	}
}

func gatherStage(tb testing.TB, stages []Stage) *Stage {
	tb.Helper()
	for i := range stages {
		if stages[i].Type == StageExchangeGather {
			return &stages[i]
		}
	}
	tb.Fatal("plan carries no gather stage")
	return nil
}

// TestHiddenSortTrimOpDropsMaterializedColumns covers the single-process half:
// there is no gather to project the result, so the pipeline needs its own
// trim. Without it the Sort hands __sortkey_N to the client next to the
// columns the query asked for.
func TestHiddenSortTrimOpDropsMaterializedColumns(t *testing.T) {
	visible := logical.Projection{Column: "n_name", Expr: "n_name", Alias: "n_name"}
	hidden := logical.Projection{Expr: "length(n_name)", Alias: "__sortkey_0", Hidden: true}

	plan := &logical.Node{Type: logical.NodeSort, Children: []*logical.Node{
		{Type: logical.NodeProject, Projections: []logical.Projection{visible, hidden}},
	}}
	op := hiddenSortTrimOp(plan)
	if op == nil {
		t.Fatal("no trim operator for a plan carrying a materialized sort column")
	}
	trim, ok := op.(*exec.Project)
	if !ok {
		t.Fatalf("trim operator is a %T, want *exec.Project", op)
	}
	if len(trim.Projections) != 1 || trim.Projections[0].Name != "n_name" {
		t.Errorf("trim projects %+v, want exactly the select list [n_name]", trim.Projections)
	}

	// Without a hidden column the plan must be left alone — a trim there
	// would be pure cost on every sorted query in the corpus.
	plain := &logical.Node{Type: logical.NodeSort, Children: []*logical.Node{
		{Type: logical.NodeProject, Projections: []logical.Projection{visible}},
	}}
	if hiddenSortTrimOp(plain) != nil {
		t.Error("plan with no materialized sort column grew a trim operator")
	}

	// An unexpanded star has no column list to trim to; the plan is left
	// alone rather than projected to nulls.
	star := &logical.Node{Type: logical.NodeSort, Children: []*logical.Node{
		{Type: logical.NodeProject, Projections: []logical.Projection{{Expr: "*", Column: "*"}, hidden}},
	}}
	if hiddenSortTrimOp(star) != nil {
		t.Error("unexpanded star grew a trim operator; it would project every row to nulls")
	}
}
