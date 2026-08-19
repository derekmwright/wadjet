package worker

import (
	"context"
	"io"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// emptyFinalAggTask builds the fragment shape a final aggregate takes when
// its whole input turned out to be empty — an upstream join that matched
// nothing, so no stage output file exists to read:
//
//	[OpShuffleSource(no files), OpHashAggregate(ungrouped), OpUnpartitionedSink]
func emptyFinalAggTask(id string, bucket string, agg distributed.OpSpec) distributed.Task {
	return distributed.Task{
		ID:           id,
		QueryID:      "q-" + id,
		StageID:      "final_aggregate-0",
		Type:         distributed.TaskTypeStage,
		StageType:    "final_aggregate",
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/" + id + "/",
		Operators: []distributed.OpSpec{
			{Type: distributed.OpShuffleSource, InputAlias: "upstream", InputBucket: bucket},
			agg,
			{Type: distributed.OpUnpartitionedSink},
		},
	}
}

// readSingleOutputBatch reads the fragment's one .wshf output and returns its
// single batch.
func readSingleOutputBatch(t *testing.T, store objstore.Store, bucket string, files []string) *batch.RecordBatch {
	t.Helper()
	if len(files) != 1 {
		t.Fatalf("expected exactly 1 output file, got %d: %v", len(files), files)
	}
	rc, _, err := store.Get(context.Background(), bucket, files[0])
	if err != nil {
		t.Fatalf("get output: %v", err)
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	r, err := newShuffleChunkReader(raw)
	if err != nil {
		t.Fatalf("parse output: %v", err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatalf("chunk next: %v", err)
	}
	if b == nil {
		t.Fatal("output file carries no batch")
	}
	next, err := r.Next()
	if err != nil {
		t.Fatalf("chunk next (second): %v", err)
	}
	if next != nil {
		t.Fatalf("output carries more than one batch — an ungrouped aggregate owes exactly one row")
	}
	return b
}

// TestExecuteFragment_EmptySourceUngroupedIdentityRow is the #329 regression.
//
// An ungrouped aggregate returns exactly one row for any input, including
// none: COUNT is 0, SUM/MIN/MAX/AVG are NULL. On the stage DAG that row was
// lost whenever the aggregate's input was an empty JOIN result — the fragment
// short-circuited on "no input files" before the pipeline existed, and the
// carve-out that let it through covered COUNT-family aggregates only, because
// nothing on the wire told the worker what type the other families' NULL
// should wear. distributed.AggSpec.OutputType now carries it.
//
// Each subtest asserts the VALUE and the declared TYPE of every output
// column: a float64-typed NULL where a string belongs is the failure mode
// the old carve-out was avoiding, so a test that only checked for nil would
// pass on the very thing that made this hard.
func TestExecuteFragment_EmptySourceUngroupedIdentityRow(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-frag-empty-ungrouped-agg"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	executor := NewExecutor(store, NewLRUCache(1<<20), nil)

	// One aggregate of every family the planner can type, all at once —
	// which is also the shape the old carve-out failed hardest on: a single
	// SUM alongside COUNT(*) disqualified the whole fragment, so even the
	// COUNT came back missing rather than 0.
	task := emptyFinalAggTask("frag-empty-allfam", bucket, distributed.OpSpec{
		Type:      distributed.OpHashAggregate,
		MergeMode: true,
		FoldAvg:   true,
		Aggregates: []distributed.AggSpec{
			{Func: "sum", InputCol: "price", OutputCol: "s", OutputType: int(parquet.TypeFloat64)},
			{Func: "min", InputCol: "name", OutputCol: "mn", OutputType: int(parquet.TypeString)},
			{Func: "max", InputCol: "shipdate", OutputCol: "mx", OutputType: int(parquet.TypeDate)},
			{Func: "sum", InputCol: "price", OutputCol: "__avg_sum#av", OutputType: int(parquet.TypeFloat64)},
			{Func: "count", InputCol: "price", OutputCol: "__avg_count#av", OutputType: int(parquet.TypeInt64)},
			{Func: "count", OutputCol: "c", OutputType: int(parquet.TypeInt64)},
		},
		EmitEmptyIdentity: true,
	})
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 1 {
		t.Fatalf("NumRows = %d, want 1 — an ungrouped aggregate owes SQL one row over the empty set", result.NumRows)
	}
	b := readSingleOutputBatch(t, store, bucket, result.ResultFiles)
	if b.ActiveLen() != 1 {
		t.Fatalf("output batch has %d rows, want 1", b.ActiveLen())
	}

	byName := map[string]int{}
	for i, c := range b.Schema {
		byName[c.Name] = i
	}
	// AVG is reassembled by the fold, so its synthetic legs must be gone.
	for _, synthetic := range []string{"__avg_sum#av", "__avg_count#av"} {
		if _, present := byName[synthetic]; present {
			t.Errorf("output still carries AVG synthetic %q — the fold did not run: %v", synthetic, b.Schema)
		}
	}
	wantTypes := map[string]parquet.TypeID{
		"s":  parquet.TypeFloat64,
		"mn": parquet.TypeString,
		"mx": parquet.TypeDate,
		"av": parquet.TypeFloat64,
		"c":  parquet.TypeInt64,
	}
	for name, want := range wantTypes {
		idx, ok := byName[name]
		if !ok {
			t.Errorf("output has no column %q: %v", name, b.Schema)
			continue
		}
		if got := b.Schema[idx].Type; got != want {
			t.Errorf("column %q type: got %v, want %v — the identity row must wear the type "+
				"a non-empty run would have produced, not the aggregate's default", name, got, want)
		}
	}
	// COUNT over the empty set is 0, never NULL — including here, where the
	// final aggregate is in merge mode and would otherwise re-count its
	// (absent) partials with SUM, whose identity is NULL.
	if idx, ok := byName["c"]; ok {
		if b.Columns[idx].Nulls.IsNullFast(0) {
			t.Error("COUNT(*) over the empty set came back NULL, want 0")
		} else if got := b.Columns[idx].Int64Data[0]; got != 0 {
			t.Errorf("COUNT(*) over the empty set = %d, want 0", got)
		}
	}
	for _, name := range []string{"s", "mn", "mx", "av"} {
		idx, ok := byName[name]
		if !ok {
			continue
		}
		if !b.Columns[idx].Nulls.IsNullFast(0) {
			t.Errorf("column %q over the empty set is not NULL", name)
		}
	}
}

// TestExecuteFragment_EmptySourceIdentityRowGates covers the cases that must
// NOT produce a row, so the widened carve-out stays as narrow as its
// justification: only an ungrouped FINAL aggregate, and only when the planner
// declared every output type.
func TestExecuteFragment_EmptySourceIdentityRowGates(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-frag-empty-agg-gates"

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	executor := NewExecutor(store, NewLRUCache(1<<20), nil)

	typedSum := distributed.AggSpec{
		Func: "sum", InputCol: "v", OutputCol: "s", OutputType: int(parquet.TypeFloat64),
	}
	untypedSum := distributed.AggSpec{Func: "sum", InputCol: "v", OutputCol: "s"}
	untypedCount := distributed.AggSpec{Func: "count", OutputCol: "c"}

	tests := []struct {
		name    string
		agg     distributed.OpSpec
		wantRow bool
	}{{
		// The wire-compatibility contract: a spec from a coordinator that
		// predates OutputType must behave exactly as it did before, which
		// means declining rather than guessing float64.
		name: "undeclared output type declines",
		agg: distributed.OpSpec{
			Type: distributed.OpHashAggregate, MergeMode: true,
			Aggregates: []distributed.AggSpec{untypedSum}, EmitEmptyIdentity: true,
		},
		wantRow: false,
	}, {
		// One undeclared spec disqualifies the fragment: emitting the row
		// with a hole in it is worse than not emitting it.
		name: "one undeclared spec among declared ones declines",
		agg: distributed.OpSpec{
			Type: distributed.OpHashAggregate, MergeMode: true,
			Aggregates:        []distributed.AggSpec{typedSum, untypedSum},
			EmitEmptyIdentity: true,
		},
		wantRow: false,
	}, {
		// A partial aggregate's empty output is absorbed by the final above
		// it, which emits the row instead. Emitting here too would put a
		// planner-typed partial next to runtime-typed siblings.
		name: "partial aggregate declines even when fully declared",
		agg: distributed.OpSpec{
			Type:       distributed.OpHashAggregate,
			Aggregates: []distributed.AggSpec{typedSum},
		},
		wantRow: false,
	}, {
		// Unchanged from before #329: COUNT-family output types are
		// input-independent, so they never needed a declaration and emit at
		// any stage. #292's scalar-subquery substitution depends on it.
		name: "count-family emits with no declaration and no final marker",
		agg: distributed.OpSpec{
			Type:       distributed.OpHashAggregate,
			Aggregates: []distributed.AggSpec{untypedCount},
		},
		wantRow: true,
	}, {
		// A GROUP BY over no rows has no groups, so it owes no rows.
		name: "grouped aggregate emits nothing",
		agg: distributed.OpSpec{
			Type: distributed.OpHashAggregate, MergeMode: true,
			GroupByCols:       []string{"g"},
			Aggregates:        []distributed.AggSpec{typedSum},
			EmitEmptyIdentity: true,
		},
		wantRow: false,
	}}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := emptyFinalAggTask("frag-gate-"+string(rune('a'+i)), bucket, tt.agg)
			result := &distributed.ResultNotification{TaskID: task.ID}
			if err := executor.executeStage(ctx, task, result); err != nil {
				t.Fatalf("executeStage: %v", err)
			}
			want := int64(0)
			if tt.wantRow {
				want = 1
			}
			if result.NumRows != want {
				t.Errorf("NumRows = %d, want %d", result.NumRows, want)
			}
			if !tt.wantRow && len(result.ResultFiles) != 0 {
				t.Errorf("expected no output files, got %v", result.ResultFiles)
			}
		})
	}
}
