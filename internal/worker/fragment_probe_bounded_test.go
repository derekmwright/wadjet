package worker

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// probeShapeFragmentSink records the shape of every batch a fragment emits
// without retaining it: batch count, the largest batch, and a checksum over
// the probe-side value column. Retaining the batches would itself hold the
// whole fan-out alive and defeat the point of the bound. Safe for the
// morsel-parallel paths, which consume from k goroutines.
type probeShapeFragmentSink struct {
	mu      sync.Mutex
	batches int
	rows    int
	maxRows int
	sum     int64
	col     string
}

func (s *probeShapeFragmentSink) consume(_ context.Context, b *batch.RecordBatch) error {
	exec.FlattenForConsumer(b, nil)
	s.mu.Lock()
	defer s.mu.Unlock()
	n := b.ActiveLen()
	s.batches++
	s.rows += n
	if n > s.maxRows {
		s.maxRows = n
	}
	ci := -1
	for i, c := range b.Schema {
		if c.Name == s.col {
			ci = i
		}
	}
	if ci < 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		row := i
		if b.Sel != nil {
			row = int(b.Sel[i])
		}
		if !b.Columns[ci].Nulls.IsNullFast(row) {
			s.sum += b.Columns[ci].Int64Data[row]
		}
	}
	return nil
}

func (s *probeShapeFragmentSink) finalize(context.Context, distributed.Task, *distributed.ResultNotification) error {
	return nil
}

func (s *probeShapeFragmentSink) close() {}

// fanOutJoin builds the #317 shape at unit-test scale: every probe row matches
// every build row, so one probe batch fans out to probeN x buildN rows. It
// returns the built join plus the probe-side batches.
func fanOutJoin(t *testing.T, buildN, probeN int) (*exec.HashJoin, []*batch.RecordBatch) {
	t.Helper()
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
	toBatches := func(schema []parquet.Column, rows []map[string]any) []*batch.RecordBatch {
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
	hj := exec.NewHashJoin(exec.InnerJoin, []string{"k"}, []string{"rk"})
	if err := hj.Build(context.Background(), exec.NewBatchSource(toBatches(buildSchema, buildRows))); err != nil {
		t.Fatalf("build: %v", err)
	}
	return hj, toBatches(probeSchema, probeRows)
}

// checkFanOut asserts the join emitted every row exactly once, joined to the
// right partners, in batches no larger than the per-call bound.
func checkFanOut(t *testing.T, sink *probeShapeFragmentSink, buildN, probeN int) {
	t.Helper()
	wantRows := buildN * probeN
	if sink.rows != wantRows {
		t.Fatalf("got %d joined rows, want %d (rows lost or duplicated across a suspension)", sink.rows, wantRows)
	}
	var wantSum int64
	for i := 0; i < probeN; i++ {
		wantSum += int64(i) * int64(buildN)
	}
	if sink.sum != wantSum {
		t.Fatalf("checksum %d, want %d (rows joined to the wrong partners)", sink.sum, wantSum)
	}
	if sink.maxRows > exec.MaxProbeOutputRows {
		t.Fatalf("fragment emitted a %d-row batch; the per-call bound is %d rows "+
			"(the fan-out of a whole input batch was materialised at once)",
			sink.maxRows, exec.MaxProbeOutputRows)
	}
	if sink.batches < 2 {
		t.Fatalf("expected the %d-row fan-out to arrive as multiple bounded batches, got %d",
			wantRows, sink.batches)
	}
}

func boundedFragmentExecutor(t *testing.T) *Executor {
	t.Helper()
	return NewExecutor(objstore.NewMemStore(), NewLRUCache(1024*1024), nil)
}

// TestFragmentLinearBoundsProbeFanOut is the distributed half of #317. The
// probe suspends its fan-out only for a driver that promises to drain
// NextOutput before supplying the next input; exec.Pipeline made that promise,
// the worker's fragment chain drivers did not, so a join running as a DAG
// fragment still materialised O(batch x fan-out) rows in one live allocation
// and still OOM-killed the worker. Both fragment widths are covered: the
// serial consumer loop and the morsel-parallel chains, whose clones each carry
// their own cursor.
func TestFragmentLinearBoundsProbeFanOut(t *testing.T) {
	// Four probe batches of 2048 rows against a 500-row build side that
	// shares one key: 1,024,000 output rows per input batch, 31x the
	// per-call bound, and enough morsels for the parallel path to hand work
	// to more than one consumer.
	const buildN = 500
	const probeN = 4 * batch.DefaultBatchSize

	t.Run("serial", func(t *testing.T) {
		ctx := context.Background()
		hj, probeBatches := fanOutJoin(t, buildN, probeN)
		probe := hj.Probe()
		if err := probe.Init(ctx); err != nil {
			t.Fatalf("probe init: %v", err)
		}
		sink := &probeShapeFragmentSink{col: "amount"}
		e := boundedFragmentExecutor(t)
		task := distributed.Task{ID: "frag-bound-serial", StageID: "join-0"}
		result := &distributed.ResultNotification{TaskID: task.ID}
		err := e.runFragmentLinear(ctx, task, exec.NewBatchSource(probeBatches),
			[]exec.UnaryOperator{probe}, sink, result)
		if err != nil {
			t.Fatalf("runFragmentLinear: %v", err)
		}
		if result.NumRows != int64(buildN*probeN) {
			t.Fatalf("result.NumRows = %d, want %d", result.NumRows, buildN*probeN)
		}
		checkFanOut(t, sink, buildN, probeN)
	})

	t.Run("morsel parallel", func(t *testing.T) {
		ctx := context.Background()
		hj, probeBatches := fanOutJoin(t, buildN, probeN)
		probe := hj.Probe()
		if err := probe.Init(ctx); err != nil {
			t.Fatalf("probe init: %v", err)
		}
		sink := &probeShapeFragmentSink{col: "amount"}
		e := boundedFragmentExecutor(t)
		e.SetMorselWorkers(4) // fixed width: bypasses the fragment size gate
		task := distributed.Task{ID: "frag-bound-parallel", StageID: "join-0"}
		result := &distributed.ResultNotification{TaskID: task.ID}
		err := e.runFragmentLinear(ctx, task, exec.NewBatchSource(probeBatches),
			[]exec.UnaryOperator{probe}, sink, result)
		if err != nil {
			t.Fatalf("runFragmentLinear (parallel): %v", err)
		}
		if result.NumRows != int64(buildN*probeN) {
			t.Fatalf("result.NumRows = %d, want %d", result.NumRows, buildN*probeN)
		}
		checkFanOut(t, sink, buildN, probeN)
	})
}

// TestDrainThroughBreakerBoundsProbeFanOut covers the breaker phase's chain
// driver: a probe sitting between two pipeline breakers is driven by
// drainThroughBreaker, not by the linear loop.
func TestDrainThroughBreakerBoundsProbeFanOut(t *testing.T) {
	const buildN = 2000
	const probeN = 512

	ctx := context.Background()
	hj, probeBatches := fanOutJoin(t, buildN, probeN)
	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		t.Fatalf("probe init: %v", err)
	}
	sink := &probeShapeSink{col: "amount"}
	err := drainThroughBreaker(ctx, exec.NewBatchSource(probeBatches), nil,
		[]exec.UnaryOperator{probe}, sink)
	if err != nil {
		t.Fatalf("drainThroughBreaker: %v", err)
	}
	if sink.rows != buildN*probeN {
		t.Fatalf("got %d joined rows, want %d", sink.rows, buildN*probeN)
	}
	if sink.maxRows > exec.MaxProbeOutputRows {
		t.Fatalf("breaker drain emitted a %d-row batch; the per-call bound is %d rows",
			sink.maxRows, exec.MaxProbeOutputRows)
	}
}

// probeShapeSink is the exec.Sink flavour of probeShapeFragmentSink, for the
// drivers whose downstream is a pipeline breaker rather than a fragment sink.
// It is also a MergeableSink, so it can stand in for the HashAggregate the
// morsel-parallel breaker consume phase clones: every clone records into the
// same counters, and merging is therefore a no-op.
type probeShapeSink struct {
	mu      *sync.Mutex
	shared  *probeShapeSink // nil on the primary; clones point at it
	batches int
	rows    int
	maxRows int
	col     string
}

func (s *probeShapeSink) Init(context.Context) error { return nil }

func (s *probeShapeSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	dst := s
	if s.shared != nil {
		dst = s.shared
	}
	if dst.mu != nil {
		dst.mu.Lock()
		defer dst.mu.Unlock()
	}
	n := b.ActiveLen()
	dst.batches++
	dst.rows += n
	if n > dst.maxRows {
		dst.maxRows = n
	}
	return nil
}

func (s *probeShapeSink) Finalize(context.Context) error                   { return nil }
func (s *probeShapeSink) Close() error                                     { return nil }
func (s *probeShapeSink) Next(context.Context) (*batch.RecordBatch, error) { return nil, nil }
func (s *probeShapeSink) CloneSink() exec.SinkSource {
	return &probeShapeSink{shared: s, col: s.col}
}
func (s *probeShapeSink) MergeSink(exec.SinkSource) {}

// TestBreakerConsumeParallelBoundsProbeFanOut covers the fragment shape the
// issue reported: a join feeding a pipeline breaker (scan → probe →
// aggregate) inside one fragment, run morsel-parallel. Each cloned chain has
// its own probe and therefore its own fan-out cursor.
func TestBreakerConsumeParallelBoundsProbeFanOut(t *testing.T) {
	const buildN = 500
	const probeN = 4 * batch.DefaultBatchSize

	ctx := context.Background()
	hj, probeBatches := fanOutJoin(t, buildN, probeN)
	probe := hj.Probe()
	if err := probe.Init(ctx); err != nil {
		t.Fatalf("probe init: %v", err)
	}
	sink := &probeShapeSink{mu: &sync.Mutex{}, col: "amount"}
	e := boundedFragmentExecutor(t)
	task := distributed.Task{ID: "frag-bound-breaker", StageID: "join-agg-0"}
	err := e.runBreakerConsumeParallel(ctx, task, exec.NewBatchSource(probeBatches),
		[]exec.UnaryOperator{probe}, sink, 2, nil)
	if err != nil {
		t.Fatalf("runBreakerConsumeParallel: %v", err)
	}
	if want := buildN * probeN; sink.rows != want {
		t.Fatalf("got %d joined rows, want %d (rows lost or duplicated across a suspension)", sink.rows, want)
	}
	if sink.maxRows > exec.MaxProbeOutputRows {
		t.Fatalf("breaker consume emitted a %d-row batch; the per-call bound is %d rows",
			sink.maxRows, exec.MaxProbeOutputRows)
	}
}

// TestExecuteFragmentHighFanOutJoinRowCount drives the same shape through the
// whole fragment path — task spec, shuffle-file source, spec-built probe,
// stage sink — so that suspension is exercised where it actually runs, and
// asserts the row count survives it.
func TestExecuteFragmentHighFanOutJoinRowCount(t *testing.T) {
	ctx := context.Background()
	const bucket = "test-fragment-fanout"
	const buildN = 500
	const probeN = 512

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	buildRows := make([][2]int64, buildN)
	for i := range buildRows {
		buildRows[i] = [2]int64{7, int64(i)} // every build row shares key 7
	}
	put := func(key string, data []byte) {
		t.Helper()
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	buildKey := "in/fanout/build.wshf"
	put(buildKey, makeBuildWshf(t, buildRows))

	probeRows := make([]struct {
		ID   int64
		Name string
	}, probeN)
	for i := range probeRows {
		probeRows[i] = struct {
			ID   int64
			Name string
		}{ID: 7, Name: "row-" + strconv.Itoa(i)}
	}
	probeKey := "in/fanout/probe.wshf"
	put(probeKey, makeProbeWshf(t, probeRows))

	executor := NewExecutor(store, NewLRUCache(4*1024*1024), nil)
	task := distributed.Task{
		ID:           "frag-fanout",
		QueryID:      "q-frag-fanout",
		StageID:      "join-0",
		Type:         distributed.TaskTypeStage,
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "out/join-0/",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpShuffleSource,
				InputAlias:  "probe",
				InputFiles:  []string{probeKey},
				InputBucket: bucket,
			},
			{
				Type:        distributed.OpBroadcastProbe,
				JoinType:    "inner",
				LeftKeys:    []string{"id"},
				RightKeys:   []string{"id"},
				BuildAlias:  "build",
				BuildFiles:  []string{buildKey},
				BuildBucket: bucket,
			},
			{Type: distributed.OpUnpartitionedSink},
		},
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != int64(buildN*probeN) {
		t.Fatalf("NumRows = %d, want %d (fan-out rows lost or duplicated across a suspension)",
			result.NumRows, buildN*probeN)
	}
}
