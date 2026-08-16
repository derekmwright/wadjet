package worker

import (
	"context"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

type countingSink struct {
	mu   sync.Mutex
	rows int
}

func (s *countingSink) consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	s.rows += b.ActiveLen()
	s.mu.Unlock()
	return nil
}
func (s *countingSink) finalize(context.Context, distributed.Task, *distributed.ResultNotification) error {
	return nil
}
func (s *countingSink) close() {}

func intBatch(t *testing.T, col string, keys []int64) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{{Name: col, Type: parquet.TypeInt64}}
	b := batch.NewRecordBatch(schema, len(keys))
	for i, k := range keys {
		b.Columns[0].Int64Data[i] = k
	}
	return b
}

// emitCapturingSink must accumulate the OUTPUT stream's keys — including
// under concurrent consume (parallel fragments) — and pass every batch to
// the inner sink unchanged.
func TestEmitCapturingSinkAccumulatesConcurrently(t *testing.T) {
	op := exec.NewDynamicFilterEmitOp("f1", "k", "int64", 1<<16)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	inner := &countingSink{}
	sink := &emitCapturingSink{inner: inner, ops: []*exec.DynamicFilterEmitOp{op}}

	const goroutines = 8
	const batches = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < batches; i++ {
				keys := []int64{int64(g*1000 + i)}
				if err := sink.consume(context.Background(), intBatch(t, "k", keys)); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if inner.rows != goroutines*batches {
		t.Fatalf("inner sink saw %d rows, want %d", inner.rows, goroutines*batches)
	}
	snap := op.Snapshot()
	if snap.RowCount != goroutines*batches {
		t.Fatalf("RowCount = %d, want %d", snap.RowCount, goroutines*batches)
	}
	// Zero false negatives even under concurrency (the mutex must make
	// bloom read-modify-writes atomic).
	for g := 0; g < goroutines; g++ {
		for i := 0; i < batches; i++ {
			k := int64(g*1000 + i)
			if !exec.BloomContains(snap.Bloom, snap.BloomMask, exec.BloomHashInt(k)) {
				t.Fatalf("bloom missing key %d — concurrent accumulation lost a write", k)
			}
		}
	}
}

type sliceSource struct {
	batches []*batch.RecordBatch
	i       int
}

func (s *sliceSource) Init(context.Context) error { return nil }
func (s *sliceSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.i >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.i]
	s.i++
	return b, nil
}
func (s *sliceSource) Close() error { return nil }

// bloomFilteredSource must drop rows whose key is not in the bloom (via
// selection vector) and skip batches that reject entirely — while never
// dropping a key that IS in the bloom (no false negatives).
func TestBloomFilteredSourceFilters(t *testing.T) {
	// Bloom of {10, 20, 30}.
	bloom, mask := exec.NewBloomSized(3)
	for _, k := range []int64{10, 20, 30} {
		h := exec.BloomHashInt(k)
		bloom[h&mask] |= 1 << (h & 63)
		bloom[(h>>17)&mask] |= 1 << ((h >> 6) & 63)
	}
	op := exec.NewBloomFilterOp(bloom, mask, []string{"k"}, true)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	src := &bloomFilteredSource{
		inner: &sliceSource{batches: []*batch.RecordBatch{
			intBatch(t, "k", []int64{10, 999, 20, 998}),
			intBatch(t, "k", []int64{500, 501, 502}), // all rejected → skipped
			intBatch(t, "k", []int64{30}),
		}},
		ops: []*exec.BloomFilterOp{op},
	}

	var got []int64
	for {
		b, err := src.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		col := b.Columns[0]
		if b.Sel != nil {
			for _, idx := range b.Sel {
				got = append(got, col.Int64Data[idx])
			}
		} else {
			for i := 0; i < b.Len; i++ {
				got = append(got, col.Int64Data[i])
			}
		}
	}
	for _, k := range []int64{10, 20, 30} {
		found := false
		for _, g := range got {
			if g == k {
				found = true
			}
		}
		if !found {
			t.Fatalf("key %d in bloom was dropped (false negative)", k)
		}
	}
	for _, g := range got {
		if g >= 500 {
			t.Fatalf("key %d survived a bloom that excludes it (possible but suspicious at this size)", g)
		}
	}
}

// Regression: SF100 Q04 2026-08-04 worker panic ("index out of range
// [1057*64+6]"). BloomFilterOp reuses an internal selBuf across Execute
// calls; bloomFilteredSource runs in the morsel PRODUCER while consumers
// still hold earlier batches, so each emitted batch's Sel must be
// detached from the op's scratch. This asserts (a) no aliasing between
// successive batches' Sel backing arrays and (b) batch N's Sel content
// survives producing batch N+1.
func TestBloomFilteredSourceSelDetached(t *testing.T) {
	bloom, mask := exec.NewBloomSized(4)
	for _, k := range []int64{10, 20, 30, 40} {
		h := exec.BloomHashInt(k)
		bloom[h&mask] |= 1 << (h & 63)
		bloom[(h>>17)&mask] |= 1 << ((h >> 6) & 63)
	}
	op := exec.NewBloomFilterOp(bloom, mask, []string{"k"}, true)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	src := &bloomFilteredSource{
		inner: &sliceSource{batches: []*batch.RecordBatch{
			intBatch(t, "k", []int64{10, 999, 20}), // Sel -> [0 2]
			intBatch(t, "k", []int64{998, 30, 40}), // Sel -> [1 2]
		}},
		ops: []*exec.BloomFilterOp{op},
	}
	b1, err := src.Next(context.Background())
	if err != nil || b1 == nil || b1.Sel == nil {
		t.Fatalf("batch 1: b=%v err=%v", b1, err)
	}
	sel1 := append([]uint32(nil), b1.Sel...)
	b2, err := src.Next(context.Background())
	if err != nil || b2 == nil || b2.Sel == nil {
		t.Fatalf("batch 2: b=%v err=%v", b2, err)
	}
	if &b1.Sel[0] == &b2.Sel[0] {
		t.Fatal("batch 1 and batch 2 share Sel backing storage (op scratch not detached)")
	}
	for i, v := range sel1 {
		if b1.Sel[i] != v {
			t.Fatalf("batch 1 Sel[%d] mutated by producing batch 2: %d != %d", i, b1.Sel[i], v)
		}
	}
}
