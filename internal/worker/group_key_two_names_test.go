package worker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The worker's half of ADR-0026 §2: a fragment RESOLVES each GROUP BY key by
// the spelling the planner sent and PUBLISHES it under the name the planner
// sent, and it decides nothing about either by parsing text.
//
// `derivedGroupKeys` used to recover the second name from the first, and a
// text cannot say which one it is: `GROUP BY "g + 1"` names a column and
// `GROUP BY g + 1` is arithmetic, and both are recorded as `g + 1` (§2c). Every
// shape whose two names differ was answered from the wrong one — one NULL group
// over the whole table, silently, on both DAG arms (#736, #781, #794).

// TestFragmentResolvesAndPublishesTheTwoNames drives the real aggregate builder
// and asserts BOTH names on the batch it emits.
func TestFragmentResolvesAndPublishesTheTwoNames(t *testing.T) {
	e := &Executor{}
	ctx := context.Background()
	slot0 := plansql.SlotName(plansql.SlotGroupKey, 0)

	for _, tc := range []struct {
		name string
		spec distributed.OpSpec
		// resolve is what exec.HashAggregate must look the key up by, and
		// publish is the output column name it must emit.
		resolve, publish []string
	}{
		{
			// The shape the second name exists for: the query wrote `x.w`, the
			// JOIN's stream carries `w`, and consumers above read the key under
			// the name the query wrote.
			name: "a-derived-alias-resolves-by-the-stream-and-publishes-the-query's-name",
			spec: distributed.OpSpec{
				Type:           distributed.OpHashAggregate,
				GroupByCols:    []string{"x.w"},
				GroupByResolve: []distributed.GroupKeyResolveSpec{{Expr: "w"}},
				Aggregates:     []distributed.AggSpec{{Func: "count", OutputCol: "n"}},
				BuildProject:   true,
			},
			resolve: []string{"w"},
			// exec strips a qualifier it is not told to keep, on BOTH engines.
			publish: []string{"w"},
		},
		{
			// The AMBIGUOUS pair: two arms publish `w`, so the join qualified
			// the build's. The key naming the build arm resolves by `y.w`, and
			// binding it to the bare `w` would answer the OTHER arm's values.
			name: "a-qualified-alias-resolves-by-the-qualified-column",
			spec: distributed.OpSpec{
				Type:           distributed.OpHashAggregate,
				GroupByCols:    []string{"y.w"},
				GroupByResolve: []distributed.GroupKeyResolveSpec{{Expr: "y.w"}},
				Aggregates:     []distributed.AggSpec{{Func: "count", OutputCol: "n"}},
				BuildProject:   true,
			},
			resolve: []string{"y.w"},
			publish: []string{"w"},
		},
		{
			// A COMPUTED key: resolved by a hidden slot no query can spell,
			// published under its own canonical text (§2/§2a).
			name: "a-computed-key-resolves-by-a-slot",
			spec: distributed.OpSpec{
				Type:           distributed.OpHashAggregate,
				GroupByCols:    []string{"g + 1"},
				GroupByResolve: []distributed.GroupKeyResolveSpec{{Expr: "g + 1", Computed: true}},
				Aggregates:     []distributed.AggSpec{{Func: "count", OutputCol: "n"}},
				BuildProject:   true,
			},
			resolve: []string{slot0},
			publish: []string{"g + 1"},
		},
		{
			// The key an aggregate DIRECTLY BELOW already publishes. Its text
			// is arithmetic and its meaning is a COLUMN, and the flag is the
			// only thing that says so — parsing it re-derives `g` against a
			// schema that no longer has one (#736's first refusal).
			name: "a-key-published-below-is-a-column-not-arithmetic",
			spec: distributed.OpSpec{
				Type:           distributed.OpHashAggregate,
				GroupByCols:    []string{"g + 1"},
				GroupByResolve: []distributed.GroupKeyResolveSpec{{Expr: "g + 1"}},
				BuildProject:   true,
			},
			resolve: []string{"g + 1"},
			publish: []string{"g + 1"},
		},
		{
			// A MERGE carries no resolution list and wants none: its input is a
			// partial's output, where the key is already a column under its
			// published name (#794).
			name: "a-merge-resolves-by-the-published-name",
			spec: distributed.OpSpec{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"x.w"},
				Aggregates:  []distributed.AggSpec{{Func: "sum", InputCol: "n", OutputCol: "n"}},
				MergeMode:   true,
			},
			resolve: []string{"x.w"},
			publish: []string{"w"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg, err := e.buildFragmentHashAggregate(ctx, tc.spec)
			if err != nil {
				t.Fatalf("buildFragmentHashAggregate: %v", err)
			}
			if !equalStringSlices(agg.GroupByCols, tc.resolve) {
				t.Errorf("the aggregate RESOLVES its keys by %v, want %v — a key looked up under "+
					"a name the input does not carry lands every row in one NULL group",
					agg.GroupByCols, tc.resolve)
			}
			got := exec.PublishedGroupKeyNames(agg.GroupByCols, agg.GroupByOutNames, agg.GroupByAll)
			if !equalStringSlices(got, tc.publish) {
				t.Errorf("the aggregate PUBLISHES its keys as %v, want %v — this is the name every "+
					"consumer above the stage reads, and the single-process aggregate's own",
					got, tc.publish)
			}
		})
	}
}

// TestFragmentFallsBackWhenTheCoordinatorSendsNoResolution is the compatibility
// decision, asserted rather than described: an OpSpec with no GroupByResolve is
// an OLDER coordinator, and the worker recovers the second name the way it
// always did — by parsing the first. It is the same tolerance GroupByTypes'
// absence already gets, and it is the behaviour, not a degraded one, for every
// shape whose two names are the same string.
func TestFragmentFallsBackWhenTheCoordinatorSendsNoResolution(t *testing.T) {
	e := &Executor{}
	spec := distributed.OpSpec{
		Type:         distributed.OpHashAggregate,
		GroupByCols:  []string{"g + 1"},
		Aggregates:   []distributed.AggSpec{{Func: "count", OutputCol: "n"}},
		BuildProject: true,
	}
	agg, err := e.buildFragmentHashAggregate(context.Background(), spec)
	if err != nil {
		t.Fatalf("buildFragmentHashAggregate: %v", err)
	}
	slot0 := plansql.SlotName(plansql.SlotGroupKey, 0)
	if !equalStringSlices(agg.GroupByCols, []string{slot0}) {
		t.Errorf("with no resolution list the aggregate resolves by %v, want the parsed-out slot "+
			"[%s] — the fallback IS the old behaviour", agg.GroupByCols, slot0)
	}
	if !equalStringSlices(agg.GroupByOutNames, []string{"g + 1"}) {
		t.Errorf("with no resolution list the aggregate publishes %v, want [g + 1]",
			agg.GroupByOutNames)
	}
}

// TestFragmentPublishesWhatTheSingleProcessOperatorPublishes is §2b as a
// property rather than a table: for any pair of names the planner may send, the
// column the worker's aggregate EMITS is the one exec's own rule gives — the
// same call, on the same inputs, that the single-process planner's operator
// makes. A copy of the rule on either side is how the two would drift.
func TestFragmentPublishesWhatTheSingleProcessOperatorPublishes(t *testing.T) {
	e := &Executor{}
	spec := distributed.OpSpec{
		Type:        distributed.OpHashAggregate,
		GroupByCols: []string{"n1.n_name", "n2.n_name", "g + 1"},
		GroupByResolve: []distributed.GroupKeyResolveSpec{
			{Expr: "n1.n_name"}, {Expr: "n2.n_name"}, {Expr: "g + 1", Computed: true},
		},
		Aggregates:   []distributed.AggSpec{{Func: "count", OutputCol: "n"}},
		BuildProject: true,
	}
	agg, err := e.buildFragmentHashAggregate(context.Background(), spec)
	if err != nil {
		t.Fatalf("buildFragmentHashAggregate: %v", err)
	}
	// What the SINGLE-process planner feeds the same operator: the key's own
	// normalized name for a bare reference, the slot for a materialized one,
	// with the canonical text as the override.
	slot0 := plansql.SlotName(plansql.SlotGroupKey, 0)
	single := exec.PublishedGroupKeyNames(
		[]string{"n1.n_name", "n2.n_name", slot0}, []string{"", "", "g + 1"}, false)
	got := exec.PublishedGroupKeyNames(agg.GroupByCols, agg.GroupByOutNames, agg.GroupByAll)
	if !equalStringSlices(got, single) {
		t.Errorf("the DAG's aggregate emits %v where the single-process aggregate emits %v — one "+
			"HAVING predicate, one sort key and one projection are resolved on both engines, and "+
			"they cannot be if the two schemas differ (ADR-0026 §2b)", got, single)
	}
	// …and the ambiguity rule really did fire, so this is not a pair of
	// trivially-equal one-element answers.
	if len(got) != 3 || got[0] != "n1.n_name" || got[1] != "n2.n_name" || got[2] != "g + 1" {
		t.Errorf("emitted names %v — two keys that strip to one name must keep their qualifiers",
			got)
	}
}

// TestPreAggregateProjectionMaterializesOnlyTheComputedKeys asserts the other
// half of the pair on the operator that builds the values: a COMPUTED key gets
// a projection column under its slot, and a key that merely NAMES a column gets
// a pass-through under that name and no evaluation at all.
func TestPreAggregateProjectionMaterializesOnlyTheComputedKeys(t *testing.T) {
	spec := distributed.OpSpec{
		Type:        distributed.OpHashAggregate,
		GroupByCols: []string{"x.w", "g + 1"},
		GroupByResolve: []distributed.GroupKeyResolveSpec{
			{Expr: "w"}, {Expr: "g + 1", Computed: true},
		},
		Aggregates:   []distributed.AggSpec{{Func: "count", OutputCol: "n"}},
		BuildProject: true,
	}
	keys, err := fragmentGroupKeyPlan(spec)
	if err != nil || keys == nil {
		t.Fatalf("fragmentGroupKeyPlan declined a spec carrying both names: %v", err)
	}
	project, _, err := buildAggInputProjection(spec.GroupByCols, spec.Aggregates, nil,
		spec.GroupByTypes, spec.GroupByDecimal, keys)
	if err != nil {
		t.Fatalf("buildAggInputProjection: %v", err)
	}
	if project == nil {
		t.Fatal("no projection built — the computed key has to be materialized")
	}
	slot0 := plansql.SlotName(plansql.SlotGroupKey, 0)
	byName := map[string]exec.ProjectColumn{}
	for _, pc := range project.Projections {
		byName[pc.Name] = pc
	}
	if pc, has := byName["w"]; !has || pc.DirectCopy != "w" {
		t.Errorf("the projection does not pass `w` through — a key the stream already carries is "+
			"read, not recomputed (projections: %v)", projectionNames(project))
	}
	if _, has := byName["g + 1"]; has {
		t.Errorf("the projection materialized the computed key under its OWN TEXT — it must land " +
			"in the hidden slot, or it shadows an input column of that spelling (ADR-0026 §2)")
	}
	if pc, has := byName[slot0]; !has || pc.Expr == nil {
		t.Errorf("the computed key is not materialized into %s (projections: %v)",
			slot0, projectionNames(project))
	}
	// And the values really are two different columns of one batch.
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "w", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
	}, 1)
	b.Columns[0].SetValue(0, int64(7))
	b.Columns[1].SetValue(0, int64(41))
	if err := project.Init(context.Background()); err != nil {
		t.Fatalf("project init: %v", err)
	}
	out, err := project.Execute(context.Background(), b)
	if err != nil {
		t.Fatalf("project execute: %v", err)
	}
	wi := out.ColumnIndex("w")
	si := out.ColumnIndex(slot0)
	if wi < 0 || si < 0 {
		t.Fatalf("output batch carries %v, want both `w` and %s", batchNames(out), slot0)
	}
	if got := fmt.Sprintf("%v", out.Columns[wi].GetValue(0)); got != "7" {
		t.Errorf("the passed-through key is %s, want 7", got)
	}
	if got := fmt.Sprintf("%v", out.Columns[si].GetValue(0)); got != "42" {
		t.Errorf("the materialized key is %s, want 42 (g + 1)", got)
	}
}

func projectionNames(p *exec.Project) []string {
	out := make([]string, 0, len(p.Projections))
	for _, pc := range p.Projections {
		out = append(out, pc.Name)
	}
	return out
}

func batchNames(b *batch.RecordBatch) []string {
	out := make([]string, 0, len(b.Schema))
	for _, c := range b.Schema {
		out = append(out, c.Name)
	}
	return out
}

// TestFragmentRefusesAMisalignedResolutionList is P4's half of the
// compatibility contract, and the reason the fallback is not a catch-all.
//
// An ABSENT resolution list is a VERSION — an older coordinator — and the
// worker recovers the second name the way it always did. A list that is
// PRESENT but does not line up with the published one is not a version: a
// coordinator that sends the field sends it index-aligned, and
// `physical.TestStageCarriesOneGroupKeyList` asserts that at plan time.
// Falling back there would answer the query by the pre-arc rule with no signal
// at all, which is the silent branch this arc exists to remove.
func TestFragmentRefusesAMisalignedResolutionList(t *testing.T) {
	e := &Executor{}
	for _, tc := range []struct {
		name string
		spec distributed.OpSpec
		want string
	}{
		{
			name: "a-shorter-resolution-list",
			spec: distributed.OpSpec{
				Type:           distributed.OpHashAggregate,
				GroupByCols:    []string{"g + 1", "h"},
				GroupByResolve: []distributed.GroupKeyResolveSpec{{Expr: "g + 1", Computed: true}},
				BuildProject:   true,
			},
			want: "index-aligned",
		},
		{
			// The window call's own rendering: `WindowFuncNode.String()` emits
			// `sum(a) OVER (...)`, which is not SQL. A spec carrying one is a
			// coordinator sending something this worker cannot evaluate.
			name: "a-computed-resolution-that-does-not-parse",
			spec: distributed.OpSpec{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"w"},
				GroupByResolve: []distributed.GroupKeyResolveSpec{
					{Expr: "sum(a) OVER (...) + 0", Computed: true},
				},
				BuildProject: true,
			},
			want: "does not parse",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.buildFragmentHashAggregate(context.Background(), tc.spec)
			if err == nil {
				t.Fatalf("the fragment ACCEPTED %v and fell back to the text re-parse — that "+
					"answers the query by the pre-arc rule with no signal at all",
					tc.spec.GroupByResolve)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name the defect (%q)", err, tc.want)
			}
		})
	}
}
