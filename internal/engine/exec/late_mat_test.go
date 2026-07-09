package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

var lateMatBuildSchema = []parquet.Column{
	{Name: "o_orderkey", Type: parquet.TypeInt64},
	{Name: "o_comment", Type: parquet.TypeString},
}

var lateMatProbeSchema = []parquet.Column{
	{Name: "l_orderkey", Type: parquet.TypeInt64},
	{Name: "l_price", Type: parquet.TypeFloat64},
	{Name: "l_ship", Type: parquet.TypeString},
}

func lateMatBuildRows() []map[string]any {
	return []map[string]any{
		{"o_orderkey": int64(1), "o_comment": "one"},
		{"o_orderkey": int64(2), "o_comment": "two"},
		{"o_orderkey": int64(3), "o_comment": ""},
	}
}

func lateMatProbeRows() []map[string]any {
	return []map[string]any{
		{"l_orderkey": int64(2), "l_price": 20.5, "l_ship": "AIR"},
		{"l_orderkey": int64(9), "l_price": 90.0, "l_ship": "RAIL"}, // no match
		{"l_orderkey": int64(1), "l_price": 10.0, "l_ship": ""},
		{"l_orderkey": int64(2), "l_price": 21.0, "l_ship": "SHIP"}, // dup key: 1:N with row 0
	}
}

// runLateMatJoin builds the join once per invocation and probes a fresh
// batch, returning the output rows. With lateMat the output must arrive as
// views and match the eager rows exactly after FlattenViews.
func runLateMatJoin(t *testing.T, joinType JoinType, lateMat bool) []map[string]any {
	t.Helper()
	hj := NewHashJoin(joinType, []string{"l_orderkey"}, []string{"o_orderkey"})
	if err := hj.Build(context.Background(), NewSliceSource(lateMatBuildSchema, lateMatBuildRows())); err != nil {
		t.Fatal(err)
	}
	hj.FixKeyAssignment()
	probe := hj.Probe()
	probe.LateMaterialize = lateMat
	if err := probe.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := probe.Execute(context.Background(), batch.FromRows(lateMatProbeSchema, lateMatProbeRows()))
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("expected output batch")
	}
	if lateMat {
		if !out.HasViews() {
			t.Fatal("late-mat output has no view columns")
		}
		out.FlattenViews()
		if out.HasViews() {
			t.Fatal("FlattenViews left views")
		}
	} else if out.HasViews() {
		t.Fatal("eager output unexpectedly contains views")
	}
	return out.ToRows()
}

func TestLateMatInnerJoinParity(t *testing.T) {
	before := LateMatBatchesEmitted.Load()
	eager := runLateMatJoin(t, InnerJoin, false)
	if LateMatBatchesEmitted.Load() != before {
		t.Fatal("dormancy violated: counter moved with flag off")
	}
	lazy := runLateMatJoin(t, InnerJoin, true)
	if LateMatBatchesEmitted.Load() == before {
		t.Fatal("engagement marker did not move with flag on")
	}
	if !reflect.DeepEqual(eager, lazy) {
		t.Fatalf("inner join parity broken:\neager=%v\nlazy =%v", eager, lazy)
	}
	if len(lazy) != 3 {
		t.Fatalf("expected 3 inner rows, got %d", len(lazy))
	}
}

func TestLateMatLeftJoinParity(t *testing.T) {
	eager := runLateMatJoin(t, LeftJoin, false)
	lazy := runLateMatJoin(t, LeftJoin, true)
	if !reflect.DeepEqual(eager, lazy) {
		t.Fatalf("left join parity broken:\neager=%v\nlazy =%v", eager, lazy)
	}
	if len(lazy) != 4 {
		t.Fatalf("expected 4 left rows, got %d", len(lazy))
	}
	// The unmatched probe row must carry NULL build columns via the view's
	// own null bitmap.
	var sawUnmatched bool
	for _, r := range lazy {
		if r["l_orderkey"] == int64(9) {
			sawUnmatched = true
			if r["o_orderkey"] != nil || r["o_comment"] != nil {
				t.Fatalf("unmatched row build cols not null: %v", r)
			}
		}
	}
	if !sawUnmatched {
		t.Fatal("unmatched probe row missing from left join output")
	}
}

func TestLateMatSemiAntiUnaffected(t *testing.T) {
	// Semi/anti already return the input with a selection vector; the flag
	// must not change that path (no views, no counter movement).
	for _, jt := range []JoinType{SemiJoin, AntiJoin} {
		hj := NewHashJoin(jt, []string{"l_orderkey"}, []string{"o_orderkey"})
		if err := hj.Build(context.Background(), NewSliceSource(lateMatBuildSchema, lateMatBuildRows())); err != nil {
			t.Fatal(err)
		}
		hj.FixKeyAssignment()
		probe := hj.Probe()
		probe.LateMaterialize = true
		if err := probe.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		before := LateMatBatchesEmitted.Load()
		in := batch.FromRows(lateMatProbeSchema, lateMatProbeRows())
		out, err := probe.Execute(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		if out != nil && out.HasViews() {
			t.Fatalf("%v emitted views", jt)
		}
		if LateMatBatchesEmitted.Load() != before {
			t.Fatalf("%v moved the late-mat counter", jt)
		}
	}
}

// TestLateMatPipelineFlattensForSink runs a probe inside a Pipeline whose
// sink is not view-aware: the defensive flatten must hand the sink an owned
// batch, and the flatten counter must record the deferred gather.
func TestLateMatPipelineFlattensForSink(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"l_orderkey"}, []string{"o_orderkey"})
	if err := hj.Build(context.Background(), NewSliceSource(lateMatBuildSchema, lateMatBuildRows())); err != nil {
		t.Fatal(err)
	}
	hj.FixKeyAssignment()
	probe := hj.Probe()
	probe.LateMaterialize = true

	sink := &CollectSink{}
	pl := &Pipeline{
		Source: NewSliceSource(lateMatProbeSchema, lateMatProbeRows()),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	flattensBefore := LateMatFlattens.Load()
	if err := pl.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if LateMatFlattens.Load() == flattensBefore {
		t.Fatal("pipeline did not flatten for the non-view-aware sink")
	}
	for _, b := range sink.Batches() {
		if b.HasViews() {
			t.Fatal("sink retained a batch with live views")
		}
	}
	rows := sink.ToRows()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows through pipeline, got %d", len(rows))
	}
}

// TestLateMatProbeInputDetached verifies the probe severs its input from the
// pool when views reference it — a pooled recycle would truncate the shared
// arenas out from under the views.
func TestLateMatProbeInputDetached(t *testing.T) {
	hj := NewHashJoin(InnerJoin, []string{"l_orderkey"}, []string{"o_orderkey"})
	if err := hj.Build(context.Background(), NewSliceSource(lateMatBuildSchema, lateMatBuildRows())); err != nil {
		t.Fatal(err)
	}
	hj.FixKeyAssignment()
	probe := hj.Probe()
	probe.LateMaterialize = true
	if err := probe.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	pool := batch.NewBatchPool(lateMatProbeSchema, batch.DefaultBatchSize)
	in := pool.Get()
	for i, r := range lateMatProbeRows() {
		for j, col := range lateMatProbeSchema {
			in.Columns[j].SetValue(i, r[col.Name])
		}
	}
	in.Len = len(lateMatProbeRows())
	for _, c := range in.Columns {
		c.Len = in.Len
	}

	out, err := probe.Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !out.HasViews() {
		t.Fatal("expected view output")
	}
	// The pipeline would call in.Release() here; after Detach it must be a
	// no-op — the pool must NOT hand the batch out again while views live.
	in.Release()
	recycled := pool.Get()
	if recycled == in {
		t.Fatal("probe input recycled into the pool while views reference it")
	}
	out.FlattenViews()
	if got := out.ToRows(); len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got))
	}
}
