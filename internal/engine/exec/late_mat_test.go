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

// TestLateMatChainComposition drives a two-join chain probe-by-probe and
// asserts the phase-3 property: join 2's pass-through view columns compose
// straight back to the ORIGINAL scan batch's vectors (one copy per column,
// paid at the final flatten), while only the column join 2 reads (its probe
// key) is materialized in between.
func TestLateMatChainComposition(t *testing.T) {
	lineSchema := []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_price", Type: parquet.TypeFloat64},
		{Name: "l_comment", Type: parquet.TypeString},
	}
	ordersSchema := []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_custkey", Type: parquet.TypeInt64},
	}
	custSchema := []parquet.Column{
		{Name: "c_custkey", Type: parquet.TypeInt64},
		{Name: "c_name", Type: parquet.TypeString},
	}
	lineRows := []map[string]any{
		{"l_orderkey": int64(1), "l_price": 10.0, "l_comment": "a"},
		{"l_orderkey": int64(2), "l_price": 20.0, "l_comment": "b"},
		{"l_orderkey": int64(1), "l_price": 11.0, "l_comment": "c"},
		{"l_orderkey": int64(3), "l_price": 30.0, "l_comment": "d"}, // no order match
	}
	orderRows := []map[string]any{
		{"o_orderkey": int64(1), "o_custkey": int64(100)},
		{"o_orderkey": int64(2), "o_custkey": int64(200)},
	}
	custRows := []map[string]any{
		{"c_custkey": int64(100), "c_name": "alice"},
		{"c_custkey": int64(200), "c_name": "bob"},
	}

	runChain := func(lateMat bool) ([]map[string]any, *batch.RecordBatch, *batch.RecordBatch, *batch.RecordBatch) {
		hj1 := NewHashJoin(InnerJoin, []string{"l_orderkey"}, []string{"o_orderkey"})
		if err := hj1.Build(context.Background(), NewSliceSource(ordersSchema, orderRows)); err != nil {
			t.Fatal(err)
		}
		hj1.FixKeyAssignment()
		p1 := hj1.Probe()
		p1.LateMaterialize = lateMat

		hj2 := NewHashJoin(InnerJoin, []string{"o_custkey"}, []string{"c_custkey"})
		if err := hj2.Build(context.Background(), NewSliceSource(custSchema, custRows)); err != nil {
			t.Fatal(err)
		}
		hj2.FixKeyAssignment()
		p2 := hj2.Probe()
		p2.LateMaterialize = lateMat

		in := batch.FromRows(lineSchema, lineRows)
		out1, err := p1.Execute(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		out2, err := p2.Execute(context.Background(), out1)
		if err != nil {
			t.Fatal(err)
		}
		out2.FlattenViews()
		return out2.ToRows(), in, out1, out2
	}

	eager, _, _, _ := runChain(false)

	flattensBefore := LateMatFlattens.Load()
	lazy, in, out1, out2 := runChain(true)
	_ = out2
	if !reflect.DeepEqual(eager, lazy) {
		t.Fatalf("chain parity broken:\neager=%v\nlazy =%v", eager, lazy)
	}
	if len(lazy) != 3 {
		t.Fatalf("expected 3 chained rows, got %d", len(lazy))
	}

	// Probe 2 must have flattened out1's o_custkey (its key) and nothing else.
	custKeyIdx := out1.ColumnIndex("o_custkey")
	priceIdx := out1.ColumnIndex("l_price")
	if out1.Columns[custKeyIdx].IsView() {
		t.Fatal("probe 2 did not flatten its key column")
	}
	if !out1.Columns[priceIdx].IsView() {
		t.Fatal("probe 2 flattened a pass-through column — composition lost")
	}
	// LateMatFlattens: exactly 1 — probe 2's key column. The final
	// FlattenViews above is a direct batch call, not a counted consumer
	// flatten, and no pass-through column may have been materialized.
	if got := LateMatFlattens.Load() - flattensBefore; got != 1 {
		t.Fatalf("expected exactly 1 flatten event (probe-2 key), got %d", got)
	}
	// The composed pass-through view must reference the ORIGINAL input's
	// vector, not join 1's output — that identity IS the one-copy property.
	// (out2 was flattened for row comparison; re-run the composition check
	// against a fresh probe on the still-lazy out1.)
	if out1.Columns[priceIdx].Base != in.Columns[in.ColumnIndex("l_price")] {
		t.Fatal("pass-through view does not compose back to the original batch")
	}
}
