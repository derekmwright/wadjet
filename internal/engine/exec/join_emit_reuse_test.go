package exec

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The reuse of join-emit vector backing (join_emit_reuse.go) is only sound if
// every consumer downstream of a probe either copies what it needs before
// returning or claims the batch with Detach. A reused backing a downstream
// still references is silent data corruption, so this file drives the probe
// into every consumer KIND the engine has — a copying sink (the stage/shuffle
// sink shape), a retaining sink, HashAggregate, Sort, CollectSink, the
// spillable collector, a second fused probe — under both drivers (the
// release-inputs pipeline driver and the worker's non-releasing one), with a
// ColumnPrune in the chain for the derived-batch hazard, and asserts the
// output is identical with reuse ON and OFF.

var emitReuseBuildSchema = []parquet.Column{
	{Name: "b_key", Type: parquet.TypeInt64},
	{Name: "b_name", Type: parquet.TypeString, Nullable: true},
	{Name: "b_amt", Type: parquet.TypeFloat64, Nullable: true},
	{Name: "b_flag", Type: parquet.TypeBool, Nullable: true},
}

var emitReuseProbeSchema = []parquet.Column{
	{Name: "p_key", Type: parquet.TypeInt64},
	{Name: "p_note", Type: parquet.TypeString, Nullable: true},
	{Name: "p_val", Type: parquet.TypeInt64, Nullable: true},
}

// emitReuseBuildRows produces a build side with NULLs scattered through every
// nullable column and byte widths that vary from empty to long, so a reused
// bytes arena has to grow, shrink and be re-cleared across batches.
func emitReuseBuildRows(n int) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		r := map[string]any{"b_key": int64(i % 1500)}
		switch i % 5 {
		case 0:
			r["b_name"] = nil
		case 1:
			r["b_name"] = ""
		default:
			r["b_name"] = fmt.Sprintf("name-%s", string(make([]byte, i%37)))
		}
		if i%7 == 0 {
			r["b_amt"] = nil
		} else {
			r["b_amt"] = float64(i) * 1.5
		}
		if i%11 == 0 {
			r["b_flag"] = nil
		} else {
			r["b_flag"] = i%2 == 0
		}
		rows = append(rows, r)
	}
	return rows
}

func emitReuseProbeRows(n int) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		r := map[string]any{"p_key": int64(i % 2000)} // some keys never match
		switch i % 4 {
		case 0:
			r["p_note"] = nil
		case 1:
			r["p_note"] = ""
		default:
			r["p_note"] = fmt.Sprintf("note-%d-%s", i, string(make([]byte, i%23)))
		}
		if i%13 == 0 {
			r["p_val"] = nil
		} else {
			r["p_val"] = int64(i)
		}
		rows = append(rows, r)
	}
	return rows
}

// copyingSink models the stage and shuffle sinks: it reads every row out
// during Consume and never keeps the batch, so it does NOT Detach — exactly
// the case the reuse depends on.
type copyingSink struct {
	rows []map[string]any
}

func (s *copyingSink) Init(context.Context) error { return nil }
func (s *copyingSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	FlattenForConsumer(b, nil)
	s.rows = append(s.rows, b.ToRows()...)
	return nil
}
func (s *copyingSink) Finalize(context.Context) error { return nil }
func (s *copyingSink) Close() error                   { return nil }

// retainingSink models a generic pipeline breaker: it keeps every batch until
// Finalize and therefore claims it with Detach.
type retainingSink struct {
	batches []*batch.RecordBatch
	rows    []map[string]any
}

func (s *retainingSink) Init(context.Context) error { return nil }
func (s *retainingSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	FlattenForConsumer(b, nil)
	b.Detach()
	s.batches = append(s.batches, b)
	return nil
}
func (s *retainingSink) Finalize(context.Context) error {
	for _, b := range s.batches {
		s.rows = append(s.rows, b.ToRows()...)
	}
	return nil
}
func (s *retainingSink) Close() error { return nil }

// prunedRetainingSink is retainingSink reached through a ColumnPrune, which
// mints a DERIVED batch over the probe's own *Vector pointers. Detach on the
// derived batch has to reach the probe's storage — that is what the per-vector
// claim is for.
type emitReuseConsumer struct {
	name string
	// build returns the sink plus a func that extracts its result rows after
	// Finalize, and the operator chain to run above it.
	build func(t *testing.T, probe *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any)
}

func emitReuseConsumers() []emitReuseConsumer {
	sortKeys := []SortKey{{Column: "p_key"}, {Column: "p_val"}}
	return []emitReuseConsumer{
		{
			name: "copying-sink",
			build: func(_ *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := &copyingSink{}
				return []UnaryOperator{p}, s, func() []map[string]any { return s.rows }
			},
		},
		{
			name: "retaining-sink",
			build: func(_ *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := &retainingSink{}
				return []UnaryOperator{p}, s, func() []map[string]any { return s.rows }
			},
		},
		{
			name: "prune-then-retaining-sink",
			build: func(_ *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := &retainingSink{}
				prune := NewColumnPrune([]string{"p_key", "b_name", "b_amt"})
				return []UnaryOperator{p, prune}, s, func() []map[string]any { return s.rows }
			},
		},
		{
			name: "prune-then-copying-sink",
			build: func(_ *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := &copyingSink{}
				prune := NewColumnPrune([]string{"p_note", "b_name"})
				return []UnaryOperator{p, prune}, s, func() []map[string]any { return s.rows }
			},
		},
		{
			name: "collect-sink",
			build: func(_ *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := &CollectSink{}
				return []UnaryOperator{p}, s, func() []map[string]any { return s.ToRows() }
			},
		},
		{
			name: "sort",
			build: func(t *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := NewSort(sortKeys)
				return []UnaryOperator{p}, s, func() []map[string]any { return drainSource(t, s) }
			},
		},
		{
			name: "spillable-collector",
			build: func(t *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				s := &SpillableBatchCollector{}
				return []UnaryOperator{p}, s, func() []map[string]any {
					var out []map[string]any
					if err := s.Iterate(func(b *batch.RecordBatch) error {
						out = append(out, b.ToRows()...)
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					return out
				}
			},
		},
		{
			name: "hash-aggregate",
			build: func(t *testing.T, p *HashJoinProbe) ([]UnaryOperator, Sink, func() []map[string]any) {
				agg := NewHashAggregate([]string{"b_name"}, []AggColumn{
					{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64},
					{Func: AggSum, InputCol: "p_val", OutputCol: "sv", OutputType: parquet.TypeInt64},
					{Func: AggMin, InputCol: "b_amt", OutputCol: "mn", OutputType: parquet.TypeFloat64},
					{Func: AggMax, InputCol: "p_note", OutputCol: "mx", OutputType: parquet.TypeString},
				})
				return []UnaryOperator{p}, agg, func() []map[string]any { return drainSource(t, agg) }
			},
		},
	}
}

// drainSource pulls a pipeline-breaker sink dry through its Source half.
func drainSource(t *testing.T, src Source) []map[string]any {
	t.Helper()
	ctx := context.Background()
	var out []map[string]any
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return out
		}
		FlattenForConsumer(b, nil)
		out = append(out, b.ToRows()...)
	}
}

type emitReuseCase struct {
	joinType   JoinType
	multiBuild bool // force >1 build batch so the eager gather branch runs
	release    bool // driver releases inputs (exec.Pipeline) vs not (worker)
	fused      bool // a second probe above the first
}

// runEmitReuseCase runs the probe chain end to end and returns the consumer's
// rows rendered and sorted, so the comparison does not depend on emission
// order.
func runEmitReuseCase(t *testing.T, c emitReuseCase, cons emitReuseConsumer, reuse bool) []string {
	t.Helper()
	prev := vectorReuse.Set(reuse)
	defer vectorReuse.Set(prev)

	ctx := context.Background()
	hj := NewHashJoin(c.joinType, []string{"p_key"}, []string{"b_key"})
	if c.multiBuild {
		// Consolidation is skipped once the shared tracker is >30% used, which
		// is the only in-test lever that keeps buildBatches > 1 — and >1 is
		// what routes build columns through gatherBuildVector instead of a
		// view, i.e. through the reused gather storage.
		tr := memory.NewTracker("emit-reuse", 1<<30)
		tr.ForceReserve(1 << 29)
		hj.MemTracker = tr
	}
	if err := hj.Build(ctx, NewSliceSource(emitReuseBuildSchema, emitReuseBuildRows(5000))); err != nil {
		t.Fatal(err)
	}
	hj.FixKeyAssignment()
	if c.multiBuild && len(hj.buildBatches) < 2 {
		t.Fatalf("multi-batch build not achieved: %d batches", len(hj.buildBatches))
	}
	if !c.multiBuild && len(hj.buildBatches) != 1 {
		t.Fatalf("single-batch build not achieved: %d batches", len(hj.buildBatches))
	}

	probe := hj.Probe()
	probe.LateMaterialize = true
	if err := probe.Init(ctx); err != nil {
		t.Fatal(err)
	}

	ops, sink, rows := cons.build(t, probe)
	if c.fused {
		// A second join above the first: its output views compose over the
		// first probe's columns, and its in.DetachPool must not poison the
		// first probe's reuse.
		hj2 := NewHashJoin(InnerJoin, []string{"p_key"}, []string{"c_key"})
		if err := hj2.Build(ctx, NewSliceSource(emitReuseBuildSchema2(), emitReuseBuildRows2(2500))); err != nil {
			t.Fatal(err)
		}
		hj2.FixKeyAssignment()
		p2 := hj2.Probe()
		p2.LateMaterialize = true
		if err := p2.Init(ctx); err != nil {
			t.Fatal(err)
		}
		ops = append([]UnaryOperator{ops[0], p2}, ops[1:]...)
	}
	if err := sink.Init(ctx); err != nil {
		t.Fatal(err)
	}
	EnableBoundedOutput(ops)

	driver := NewChainDriver(ops, func(ctx context.Context, b *batch.RecordBatch) error {
		FlattenForConsumer(b, sink)
		if err := sink.Consume(ctx, b); err != nil {
			return err
		}
		if c.release {
			b.Release()
		}
		return nil
	})
	if c.release {
		driver = driver.ReleaseInputs()
	}

	src := NewSliceSource(emitReuseProbeSchema, emitReuseProbeRows(3000))
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		if _, err := driver.Push(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	return sortRowKeys(rows())
}

func emitReuseBuildSchema2() []parquet.Column {
	return []parquet.Column{
		{Name: "c_key", Type: parquet.TypeInt64},
		{Name: "b2_tag", Type: parquet.TypeString, Nullable: true},
	}
}

func emitReuseBuildRows2(n int) []map[string]any {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		r := map[string]any{"c_key": int64(i % 1500)}
		if i%3 == 0 {
			r["b2_tag"] = nil
		} else {
			r["b2_tag"] = fmt.Sprintf("t%d%s", i, string(make([]byte, i%17)))
		}
		rows = append(rows, r)
	}
	return rows
}

// sortRowKeys renders each row once and sorts the renderings, so the
// comparison is order-independent without paying fmt.Sprint per comparison.
func sortRowKeys(rows []map[string]any) []string {
	keys := make([]string, len(rows))
	for i, r := range rows {
		keys[i] = fmt.Sprint(r)
	}
	sort.Strings(keys)
	return keys
}

func TestJoinEmitReuseMatchesFreshAllocation(t *testing.T) {
	cases := []emitReuseCase{
		{joinType: InnerJoin, multiBuild: false, release: true},
		{joinType: InnerJoin, multiBuild: false, release: false},
		{joinType: InnerJoin, multiBuild: true, release: true},
		{joinType: InnerJoin, multiBuild: true, release: false},
		{joinType: LeftJoin, multiBuild: false, release: false},
		{joinType: LeftJoin, multiBuild: true, release: false},
		{joinType: InnerJoin, multiBuild: true, release: false, fused: true},
		{joinType: InnerJoin, multiBuild: false, release: true, fused: true},
	}
	for _, c := range cases {
		for _, cons := range emitReuseConsumers() {
			name := fmt.Sprintf("%v/multiBuild=%v/release=%v/fused=%v/%s",
				c.joinType, c.multiBuild, c.release, c.fused, cons.name)
			t.Run(name, func(t *testing.T) {
				off := runEmitReuseCase(t, c, cons, false)
				on := runEmitReuseCase(t, c, cons, true)
				if len(off) == 0 {
					t.Fatal("no output rows — the case exercises nothing")
				}
				if len(on) != len(off) {
					t.Fatalf("row count differs: reuse-on %d, reuse-off %d", len(on), len(off))
				}
				for i := range on {
					if on[i] != off[i] {
						t.Fatalf("row %d differs\n reuse-on:  %v\n reuse-off: %v", i, on[i], off[i])
					}
				}
			})
		}
	}
}

// TestJoinEmitReuseActuallyReuses guards the optimization itself: without it
// the test above passes trivially. The probe must hand back the SAME batch
// pointer and the same gather storage on consecutive emits when the consumer
// only copies, and a fresh one once a consumer claims it.
func TestJoinEmitReuseActuallyReuses(t *testing.T) {
	ctx := context.Background()
	prev := vectorReuse.Set(true)
	defer vectorReuse.Set(prev)

	newProbe := func() *HashJoinProbe {
		hj := NewHashJoin(InnerJoin, []string{"p_key"}, []string{"b_key"})
		tr := memory.NewTracker("emit-reuse", 1<<30)
		tr.ForceReserve(1 << 29)
		hj.MemTracker = tr
		if err := hj.Build(ctx, NewSliceSource(emitReuseBuildSchema, emitReuseBuildRows(5000))); err != nil {
			t.Fatal(err)
		}
		hj.FixKeyAssignment()
		if len(hj.buildBatches) < 2 {
			t.Fatalf("want multi-batch build, got %d", len(hj.buildBatches))
		}
		p := hj.Probe()
		p.LateMaterialize = true
		if err := p.Init(ctx); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Copying consumer: storage is reused.
	p := newProbe()
	var first, second *batch.RecordBatch
	var firstGather, secondGather *batch.Vector
	for i := 0; i < 2; i++ {
		out, err := p.Execute(ctx, batch.FromRows(emitReuseProbeSchema, emitReuseProbeRows(500)))
		if err != nil {
			t.Fatal(err)
		}
		if out == nil {
			t.Fatal("no output")
		}
		g := gatheredColumn(out)
		if g == nil {
			t.Fatal("no eagerly gathered build column in the output")
		}
		if i == 0 {
			first, firstGather = out, g
		} else {
			second, secondGather = out, g
		}
	}
	if first != second {
		t.Error("output batch shell was not reused across emits")
	}
	if firstGather != secondGather {
		t.Error("gathered build column was not reused across emits")
	}

	// Claiming consumer: storage is surrendered.
	p = newProbe()
	out1, err := p.Execute(ctx, batch.FromRows(emitReuseProbeSchema, emitReuseProbeRows(500)))
	if err != nil {
		t.Fatal(err)
	}
	claimed := gatheredColumn(out1)
	out1.Detach()
	out2, err := p.Execute(ctx, batch.FromRows(emitReuseProbeSchema, emitReuseProbeRows(500)))
	if err != nil {
		t.Fatal(err)
	}
	if out1 == out2 {
		t.Error("a Detach'd batch was reused")
	}
	if g := gatheredColumn(out2); g == claimed {
		t.Error("a claimed gather column was reused")
	}
	if !claimed.Claimed() {
		t.Error("Detach did not claim the batch's column vectors")
	}
}

// gatheredColumn returns the first owned (non-view) column of a probe output,
// i.e. one produced by the eager gather branch of emitViewOutput.
func gatheredColumn(b *batch.RecordBatch) *batch.Vector {
	for _, c := range b.Columns {
		if !c.IsView() {
			return c
		}
	}
	return nil
}

// TestDetachClaimsThroughDerivedBatch pins the mechanism the reuse rests on:
// a consumer that claims a DERIVED batch (ColumnPrune's shallow copy over the
// same vectors, or a view minted over one of them) has to reach the original
// producer's storage.
func TestDetachClaimsThroughDerivedBatch(t *testing.T) {
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeInt64}}
	src := batch.NewRecordBatch(schema, 4)

	derived := &batch.RecordBatch{Schema: schema[:1], Columns: []*batch.Vector{src.Columns[0]}, Len: 4}
	derived.Detach()
	if !src.Columns[0].Claimed() {
		t.Error("claim did not reach the shared vector through a derived batch")
	}
	if src.Columns[1].Claimed() {
		t.Error("claim leaked to a column the derived batch did not carry")
	}

	// Through a view's Base.
	src2 := batch.NewRecordBatch(schema, 4)
	view := batch.NewViewVector(src2.Columns[0], []uint32{0, 1})
	holder := &batch.RecordBatch{Schema: schema[:1], Columns: []*batch.Vector{view}, Len: 2}
	holder.Detach()
	if !src2.Columns[0].Claimed() {
		t.Error("claim did not propagate through Vector.Base")
	}

	// DetachPool severs the pool link without claiming.
	src3 := batch.NewRecordBatch(schema, 4)
	src3.DetachPool()
	if src3.Retained() || src3.Columns[0].Claimed() {
		t.Error("DetachPool must not claim")
	}
}
