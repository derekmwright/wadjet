package exec

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// probeShapeSink records the shape of every batch the join emits without
// retaining it: the row count of each output batch plus a checksum over the
// probe- and build-side key columns. Retaining batches would itself hold the
// output alive and defeat the point of the bound.
type probeShapeSink struct {
	batches   int
	rows      int
	maxRows   int
	nullBuild int   // rows where the build side is NULL (outer-join fill)
	sum       int64 // checksum over the first column
	col       string
	buildCol  string
}

func (s *probeShapeSink) Init(context.Context) error { return nil }

func (s *probeShapeSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	n := b.ActiveLen()
	s.batches++
	s.rows += n
	if n > s.maxRows {
		s.maxRows = n
	}
	ci, bi := -1, -1
	for i, c := range b.Schema {
		if c.Name == s.col {
			ci = i
		}
		if c.Name == s.buildCol {
			bi = i
		}
	}
	for i := 0; i < b.Len; i++ {
		if b.Sel != nil {
			break
		}
		if ci >= 0 {
			v := b.Columns[ci]
			if !v.Nulls.IsNullFast(i) {
				s.sum += v.Int64Data[i]
			}
		}
		if bi >= 0 && b.Columns[bi].Nulls.IsNullFast(i) {
			s.nullBuild++
		}
	}
	return nil
}

func (s *probeShapeSink) Finalize(context.Context) error { return nil }
func (s *probeShapeSink) Close() error                   { return nil }

func rowsToBatchesForProbe(schema []parquet.Column, rows []map[string]any) []*batch.RecordBatch {
	var out []*batch.RecordBatch
	for i := 0; i < len(rows); i += batch.DefaultBatchSize {
		end := i + batch.DefaultBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		out = append(out, batch.FromRows(schema, rows[i:end]))
	}
	return out
}

// TestHashJoinProbeBoundsFanOut is the regression test for #317: the probe
// used to materialise an entire input batch's fan-out before emitting
// anything, so a single 2048-row probe batch against a low-cardinality build
// side expanded to hundreds of millions of match pairs in one live
// allocation. GOMEMLIMIT cannot help there — the memory is live — so the
// process was kernel-OOM-killed.
//
// The shape below is the same one, scaled to a unit test: every probe row
// matches every build row, so one 512-row probe batch fans out to 512 x 2000
// = 1,024,000 output rows. Before the fix the join emitted that as ONE batch;
// the assertion on maxRows fails. After the fix the fan-out is suspended and
// resumed, so the output arrives as many batches, none larger than the bound.
func TestHashJoinProbeBoundsFanOut(t *testing.T) {
	const buildN = 2000 // all share one join key
	const probeN = 512

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}

	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rk": int64(7), "rv": int64(i)}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"k": int64(7), "amount": int64(i)}
	}

	ctx := context.Background()
	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}

	sink := &probeShapeSink{col: "amount"}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, probeRows)),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wantRows := buildN * probeN
	if sink.rows != wantRows {
		t.Fatalf("got %d joined rows, want %d", sink.rows, wantRows)
	}
	// Every probe row's `amount` appears buildN times.
	var wantSum int64
	for i := 0; i < probeN; i++ {
		wantSum += int64(i) * buildN
	}
	if sink.sum != wantSum {
		t.Fatalf("checksum %d, want %d (rows joined to the wrong partners)", sink.sum, wantSum)
	}
	if sink.maxRows > maxProbeOutputRows {
		t.Fatalf("probe emitted a %d-row batch; the per-call bound is %d rows "+
			"(the fan-out of a whole input batch was materialised at once)",
			sink.maxRows, maxProbeOutputRows)
	}
	if sink.batches < 2 {
		t.Fatalf("expected the %d-row fan-out to arrive as multiple bounded batches, got %d",
			wantRows, sink.batches)
	}
}

// TestHashJoinProbeBoundedOuterSemantics checks that suspending and resuming
// a fan-out does not disturb outer-join semantics: an unmatched probe row is
// emitted exactly once, no matter how many times the operator suspended while
// working through the batch it lives in.
func TestHashJoinProbeBoundedOuterSemantics(t *testing.T) {
	const buildPerKey = 700
	const probeN = 300

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}

	// Build side: keys 0 and 1, buildPerKey rows each.
	buildRows := make([]map[string]any, 0, 2*buildPerKey)
	for k := 0; k < 2; k++ {
		for i := 0; i < buildPerKey; i++ {
			buildRows = append(buildRows, map[string]any{"rk": int64(k), "rv": int64(i)})
		}
	}
	// Probe side: a third of the rows carry key 99, which has no build match.
	probeRows := make([]map[string]any, probeN)
	matched, unmatched := 0, 0
	for i := range probeRows {
		k := int64(99)
		if i%3 != 0 {
			k = int64(i % 2)
			matched++
		} else {
			unmatched++
		}
		probeRows[i] = map[string]any{"k": k, "amount": int64(i)}
	}

	ctx := context.Background()
	hj := NewHashJoin(LeftJoin, []string{"k"}, []string{"rk"})
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}

	sink := &probeShapeSink{col: "amount", buildCol: "rv"}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, probeRows)),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wantRows := matched*buildPerKey + unmatched
	if sink.rows != wantRows {
		t.Fatalf("got %d joined rows, want %d (matched=%d unmatched=%d)",
			sink.rows, wantRows, matched, unmatched)
	}
	if sink.nullBuild != unmatched {
		t.Fatalf("got %d null-filled rows, want %d — unmatched probe rows must be "+
			"emitted exactly once per row, not once per resumption",
			sink.nullBuild, unmatched)
	}
	if sink.maxRows > maxProbeOutputRows {
		t.Fatalf("probe emitted a %d-row batch; the per-call bound is %d rows", sink.maxRows, maxProbeOutputRows)
	}
}

// TestHashJoinProbeBoundedKeyShapes runs the same high-fan-out shape through
// every probe variant — single int (inline), dual int (inline), and the
// serialized-key path used for string and composite keys — so a bounded fast
// path with an unbounded slow path cannot slip through.
func TestHashJoinProbeBoundedKeyShapes(t *testing.T) {
	const buildN = 1200
	const probeN = 256

	cases := []struct {
		name         string
		buildSchema  []parquet.Column
		probeSchema  []parquet.Column
		leftKeys     []string
		rightKeys    []string
		buildRow     func(i int) map[string]any
		probeRow     func(i int) map[string]any
		checksumCol  string
		wantChecksum func() int64
	}{
		{
			name:        "single-int",
			buildSchema: []parquet.Column{{Name: "rk", Type: parquet.TypeInt64}, {Name: "rv", Type: parquet.TypeInt64}},
			probeSchema: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "amount", Type: parquet.TypeInt64}},
			leftKeys:    []string{"k"},
			rightKeys:   []string{"rk"},
			buildRow:    func(i int) map[string]any { return map[string]any{"rk": int64(1), "rv": int64(i)} },
			probeRow:    func(i int) map[string]any { return map[string]any{"k": int64(1), "amount": int64(i)} },
			checksumCol: "amount",
		},
		{
			name: "dual-int",
			buildSchema: []parquet.Column{
				{Name: "rk1", Type: parquet.TypeInt64}, {Name: "rk2", Type: parquet.TypeInt64},
				{Name: "rv", Type: parquet.TypeInt64},
			},
			probeSchema: []parquet.Column{
				{Name: "k1", Type: parquet.TypeInt64}, {Name: "k2", Type: parquet.TypeInt64},
				{Name: "amount", Type: parquet.TypeInt64},
			},
			leftKeys:  []string{"k1", "k2"},
			rightKeys: []string{"rk1", "rk2"},
			buildRow: func(i int) map[string]any {
				return map[string]any{"rk1": int64(3), "rk2": int64(4), "rv": int64(i)}
			},
			probeRow: func(i int) map[string]any {
				return map[string]any{"k1": int64(3), "k2": int64(4), "amount": int64(i)}
			},
			checksumCol: "amount",
		},
		{
			name:        "string-key",
			buildSchema: []parquet.Column{{Name: "rk", Type: parquet.TypeString}, {Name: "rv", Type: parquet.TypeInt64}},
			probeSchema: []parquet.Column{{Name: "k", Type: parquet.TypeString}, {Name: "amount", Type: parquet.TypeInt64}},
			leftKeys:    []string{"k"},
			rightKeys:   []string{"rk"},
			buildRow:    func(i int) map[string]any { return map[string]any{"rk": "region-1", "rv": int64(i)} },
			probeRow:    func(i int) map[string]any { return map[string]any{"k": "region-1", "amount": int64(i)} },
			checksumCol: "amount",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buildRows := make([]map[string]any, buildN)
			for i := range buildRows {
				buildRows[i] = tc.buildRow(i)
			}
			probeRows := make([]map[string]any, probeN)
			for i := range probeRows {
				probeRows[i] = tc.probeRow(i)
			}

			ctx := context.Background()
			hj := NewHashJoin(InnerJoin, tc.leftKeys, tc.rightKeys)
			if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(tc.buildSchema, buildRows))); err != nil {
				t.Fatalf("build: %v", err)
			}
			sink := &probeShapeSink{col: tc.checksumCol}
			pipe := &Pipeline{
				Source: NewBatchSource(rowsToBatchesForProbe(tc.probeSchema, probeRows)),
				Ops:    []UnaryOperator{hj.Probe()},
				Sink:   sink,
			}
			if err := pipe.Run(ctx); err != nil {
				t.Fatalf("run: %v", err)
			}
			if err := pipe.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			if want := buildN * probeN; sink.rows != want {
				t.Fatalf("got %d joined rows, want %d", sink.rows, want)
			}
			var wantSum int64
			for i := 0; i < probeN; i++ {
				wantSum += int64(i) * buildN
			}
			if sink.sum != wantSum {
				t.Fatalf("checksum %d, want %d", sink.sum, wantSum)
			}
			if sink.maxRows > maxProbeOutputRows {
				t.Fatalf("probe emitted a %d-row batch; the per-call bound is %d rows",
					sink.maxRows, maxProbeOutputRows)
			}
		})
	}
}

// TestCrossJoinBoundsFanOut covers the other unbounded probe path: a cross
// join's fan-out is the whole build side per probe row.
func TestCrossJoinBoundsFanOut(t *testing.T) {
	const buildN = 3000
	const probeN = 400

	buildSchema := []parquet.Column{{Name: "rv", Type: parquet.TypeInt64}}
	probeSchema := []parquet.Column{{Name: "amount", Type: parquet.TypeInt64}}

	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rv": int64(i)}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"amount": int64(i)}
	}

	ctx := context.Background()
	hj := NewHashJoin(CrossJoin, nil, nil)
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}
	sink := &probeShapeSink{col: "amount"}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, probeRows)),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if want := buildN * probeN; sink.rows != want {
		t.Fatalf("got %d joined rows, want %d", sink.rows, want)
	}
	var wantSum int64
	for i := 0; i < probeN; i++ {
		wantSum += int64(i) * buildN
	}
	if sink.sum != wantSum {
		t.Fatalf("checksum %d, want %d", sink.sum, wantSum)
	}
	if sink.maxRows > maxProbeOutputRows {
		t.Fatalf("cross join emitted a %d-row batch; the per-call bound is %d rows", sink.maxRows, maxProbeOutputRows)
	}
}

// lockedShapeSink is probeShapeSink for parallel pipelines, where several
// cloned probes consume into one sink concurrently.
type lockedShapeSink struct {
	mu    sync.Mutex
	inner probeShapeSink
}

func (s *lockedShapeSink) Init(context.Context) error { return nil }
func (s *lockedShapeSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.Consume(ctx, b)
}
func (s *lockedShapeSink) Finalize(context.Context) error { return nil }
func (s *lockedShapeSink) Close() error                   { return nil }

// TestHashJoinProbeBoundedParallel drives a high-fan-out join through a
// multi-worker pipeline. Each worker gets its own cloned probe, so each has
// its own fan-out cursor; a cursor shared across clones would interleave two
// workers' suspensions and produce a wrong row count.
func TestHashJoinProbeBoundedParallel(t *testing.T) {
	const buildN = 1500
	const probeN = 6 * batch.DefaultBatchSize

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}

	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rk": int64(i % 3), "rv": int64(i)}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"k": int64(i % 3), "amount": int64(1)}
	}

	ctx := context.Background()
	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}

	sink := &lockedShapeSink{inner: probeShapeSink{col: "amount"}}
	pipe := &Pipeline{
		// SliceSource, not BatchSource: only the former guards its cursor
		// with a mutex, and parallel workers pull from a shared source.
		Source:  NewSliceSource(probeSchema, probeRows),
		Ops:     []UnaryOperator{hj.Probe()},
		Sink:    sink,
		Workers: 4,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Every probe row matches buildN/3 build rows (500 per key).
	want := probeN * (buildN / 3)
	if sink.inner.rows != want {
		t.Fatalf("got %d joined rows, want %d", sink.inner.rows, want)
	}
	if sink.inner.sum != int64(want) {
		t.Fatalf("checksum %d, want %d", sink.inner.sum, want)
	}
	if sink.inner.maxRows > maxProbeOutputRows {
		t.Fatalf("probe emitted a %d-row batch; the per-call bound is %d rows",
			sink.inner.maxRows, maxProbeOutputRows)
	}
}

// TestHashJoinProbeBoundedGraceSpill sends the same high-fan-out shape
// through the Grace Hash Join flush path: the build side is spilled by a
// tight budget, so the joined rows come out of per-partition probes replayed
// from disk. Those probes suspend too, and NextFlush has to drain a partition
// probe's pending fan-out before it reads the next spilled probe batch —
// otherwise the spill reader recycles the batch the cursor still points into.
func TestHashJoinProbeBoundedGraceSpill(t *testing.T) {
	const buildN = 20000
	const probeN = 2 * batch.DefaultBatchSize

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeString},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}

	// 20 distinct keys over 20k build rows: 1000 build rows per key, so one
	// 2048-row probe batch fans out to ~2M rows — well past the bound.
	buildRows := make([]map[string]any, buildN)
	for i := range buildRows {
		buildRows[i] = map[string]any{"rk": int64(i % 20), "rv": "build-row-padding-for-size"}
	}
	probeRows := make([]map[string]any, probeN)
	for i := range probeRows {
		probeRows[i] = map[string]any{"k": int64(i % 20), "amount": int64(1)}
	}

	ctx := context.Background()
	tracker := memory.NewTracker("bounded-probe-spill", 400_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
	hj.Spill = sm
	hj.MemTracker = tracker
	if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}
	if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
		t.Fatal("test setup: build did not spill — budget too generous")
	}

	sink := &probeShapeSink{col: "amount"}
	pipe := &Pipeline{
		Source: NewBatchSource(rowsToBatchesForProbe(probeSchema, probeRows)),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := pipe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	want := probeN * (buildN / 20)
	if sink.rows != want {
		t.Fatalf("got %d joined rows, want %d", sink.rows, want)
	}
	if sink.sum != int64(want) {
		t.Fatalf("checksum %d, want %d", sink.sum, want)
	}
	if sink.maxRows > maxProbeOutputRows {
		t.Fatalf("spilled-partition probe emitted a %d-row batch; the per-call bound is %d rows",
			sink.maxRows, maxProbeOutputRows)
	}
}

// BenchmarkHashJoinProbeFanout measures the probe hot loop across the fan-out
// range that matters: 1:1 (dimension lookup), 1:4 (TPC-H lineitem->orders),
// and 1:64 (a low-cardinality key, where the per-call bound engages and the
// operator suspends). Build cost is excluded from the timer; the pipeline is
// the production driver, so the resume protocol's cost is included.
func BenchmarkHashJoinProbeFanout(b *testing.B) {
	const probeN = 8 * batch.DefaultBatchSize

	buildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeInt64},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	probeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "amount", Type: parquet.TypeInt64},
	}
	strBuildSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeString},
		{Name: "rv", Type: parquet.TypeInt64},
	}
	strProbeSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeInt64},
	}

	ctx := context.Background()

	runInt := func(b *testing.B, fanout, keys int) {
		buildN := fanout * keys
		buildRows := make([]map[string]any, buildN)
		for i := range buildRows {
			buildRows[i] = map[string]any{"rk": int64(i % keys), "rv": int64(i)}
		}
		probeRows := make([]map[string]any, probeN)
		for i := range probeRows {
			probeRows[i] = map[string]any{"k": int64(i % keys), "amount": int64(i)}
		}
		hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
		if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(buildSchema, buildRows))); err != nil {
			b.Fatal(err)
		}
		probeBatches := rowsToBatchesForProbe(probeSchema, probeRows)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink := &benchCountSink{}
			pipe := &Pipeline{Source: NewBatchSource(append([]*batch.RecordBatch(nil), probeBatches...)),
				Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
			if err := pipe.Run(ctx); err != nil {
				b.Fatal(err)
			}
			if sink.rows != probeN*fanout {
				b.Fatalf("got %d rows, want %d", sink.rows, probeN*fanout)
			}
		}
	}

	runStr := func(b *testing.B, fanout, keys int) {
		buildN := fanout * keys
		buildRows := make([]map[string]any, buildN)
		for i := range buildRows {
			buildRows[i] = map[string]any{"rk": fmt.Sprintf("key-%06d", i%keys), "rv": int64(i)}
		}
		probeRows := make([]map[string]any, probeN)
		for i := range probeRows {
			probeRows[i] = map[string]any{"k": fmt.Sprintf("key-%06d", i%keys), "amount": int64(i)}
		}
		hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"rk"})
		if err := hj.Build(ctx, NewBatchSource(rowsToBatchesForProbe(strBuildSchema, buildRows))); err != nil {
			b.Fatal(err)
		}
		probeBatches := rowsToBatchesForProbe(strProbeSchema, probeRows)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sink := &benchCountSink{}
			pipe := &Pipeline{Source: NewBatchSource(append([]*batch.RecordBatch(nil), probeBatches...)),
				Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
			if err := pipe.Run(ctx); err != nil {
				b.Fatal(err)
			}
			if sink.rows != probeN*fanout {
				b.Fatalf("got %d rows, want %d", sink.rows, probeN*fanout)
			}
		}
	}

	b.Run("int/fanout=1", func(b *testing.B) { runInt(b, 1, 65536) })
	b.Run("int/fanout=4", func(b *testing.B) { runInt(b, 4, 16384) })
	b.Run("int/fanout=64", func(b *testing.B) { runInt(b, 64, 1024) })
	b.Run("str/fanout=1", func(b *testing.B) { runStr(b, 1, 65536) })
	b.Run("str/fanout=4", func(b *testing.B) { runStr(b, 4, 16384) })
}
