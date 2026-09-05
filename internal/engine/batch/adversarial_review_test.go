package batch_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A retaining consumer downstream of ColumnPrune keeps the values it was
// handed, across as many pool cycles as the producer runs (#897).
//
// The composition is the whole point: ColumnPrune mints a NEW RecordBatch over
// the input's *Vector pointers, so the consumer's Detach claims vectors the
// ORIGINAL pooled batch still owns — and ChainDriver.ReleaseInputs releases
// that original. Before the fix the pool ignored the claim, the next Get reset
// the same vectors, and a retained [11 22] read back as [22 22].
func TestRetainedRowsSurvivePoolCyclesThroughAPrunedBatch(t *testing.T) {
	schema := []parquet.Column{
		{Name: "keep", Type: parquet.TypeInt64},
		{Name: "drop", Type: parquet.TypeInt64},
	}
	// Every retaining consumer class the issue names reaches the pool through
	// the same Detach, so the gate drives Detach directly and names the class.
	for _, consumer := range []string{"collector", "sort-input", "join-build"} {
		t.Run(consumer, func(t *testing.T) {
			pool := batch.NewBatchPool(schema, 1)
			vetoesBefore := batch.PoolRetentionVetoes()
			var held []*batch.RecordBatch
			driver := exec.NewChainDriver(
				[]exec.UnaryOperator{exec.NewColumnPrune([]string{"keep"})},
				func(_ context.Context, b *batch.RecordBatch) error {
					b.Detach()
					held = append(held, b)
					return nil
				},
			).ReleaseInputs()

			want := []any{}
			for _, value := range []int64{11, 22, 33, 44, 55} {
				b := pool.Get()
				b.Columns[0].SetValue(0, value)
				b.Columns[1].SetValue(0, value*1000)
				if _, err := driver.Push(context.Background(), b); err != nil {
					t.Fatal(err)
				}
				want = append(want, value)
			}
			got := make([]any, 0, len(held))
			for _, b := range held {
				got = append(got, b.Columns[0].GetValue(0))
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("retained rows=%v; want %v", got, want)
			}
			// Engagement: the veto is what makes the values survive, so a run
			// in which no veto fired proves nothing about the fix.
			if batch.PoolRetentionVetoes() == vetoesBefore {
				t.Errorf("no pool retention veto fired; the gate exercised nothing")
			}
		})
	}
}

// The control the issue asks for: detaching the ORIGINAL pooled batch (no
// derived shell in between) always worked, and still does.
func TestRetainedRowsSurvivePoolCyclesWithADirectDetach(t *testing.T) {
	schema := []parquet.Column{{Name: "keep", Type: parquet.TypeInt64}}
	pool := batch.NewBatchPool(schema, 1)
	var held []*batch.RecordBatch
	want := []any{}
	for _, value := range []int64{11, 22, 33} {
		b := pool.Get()
		b.Columns[0].SetValue(0, value)
		b.Detach()
		held = append(held, b)
		b.Release() // a no-op after Detach
		want = append(want, value)
	}
	// Churn the pool so any recycled storage would be overwritten.
	for i := 0; i < 8; i++ {
		b := pool.Get()
		b.Columns[0].SetValue(0, int64(-1))
		b.Release()
	}
	got := make([]any, 0, len(held))
	for _, b := range held {
		got = append(got, b.Columns[0].GetValue(0))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("directly detached rows=%v; want %v", got, want)
	}
}

// The claim rides DOWN into nested children and view bases, so the pool's veto
// has to look there too: a consumer that keeps a view over a ROW column's
// child holds storage whose top-level column carries no claim of its own.
func TestPoolVetoSeesAClaimOnANestedChildAndOnAViewBase(t *testing.T) {
	t.Run("nested-child", func(t *testing.T) {
		field := parquet.Column{Name: "f", Type: parquet.TypeInt64}
		schema := []parquet.Column{{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{field}}}
		pool := batch.NewBatchPool(schema, 1)
		b := pool.Get()
		b.Columns[0].SetValue(0, map[string]any{"f": int64(7)})
		child := b.Columns[0].Children[0]
		child.Claim() // what a consumer keeping only the child would do
		b.Release()
		next := pool.Get()
		if next.Columns[0].Children[0] == child {
			t.Fatalf("pool recycled a batch whose ROW child a consumer claimed")
		}
		if got, _ := child.GetInt64(0); got != 7 {
			t.Errorf("claimed child value=%d; want 7", got)
		}
	})

	t.Run("view-base", func(t *testing.T) {
		schema := []parquet.Column{{Name: "c", Type: parquet.TypeInt64}}
		pool := batch.NewBatchPool(schema, 2)
		b := pool.Get()
		b.Columns[0].SetValue(0, int64(11))
		b.Columns[0].SetValue(1, int64(22))
		base := b.Columns[0]
		view := batch.NewViewVector(base, []uint32{1})
		view.Claim() // Claim propagates through Base
		b.Release()
		next := pool.Get()
		if next.Columns[0] == base {
			t.Fatalf("pool recycled a batch whose column a view's Claim reached")
		}
		if got := view.GetValue(0); !reflect.DeepEqual(got, int64(22)) {
			t.Errorf("view over a claimed base=%v; want 22", got)
		}
	})
}

// Two derived batches over ONE pooled input, both retained: the claim is on
// the shared vectors, so neither shell being pooled is what protects them.
func TestTwoDerivedBatchesOverOneInputBothKeepTheirRows(t *testing.T) {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
	}
	pool := batch.NewBatchPool(schema, 1)
	var held []*batch.RecordBatch
	for _, value := range []int64{101, 202, 303} {
		in := pool.Get()
		in.Columns[0].SetValue(0, value)
		in.Columns[1].SetValue(0, value+1)
		for _, keep := range [][]string{{"a"}, {"b"}} {
			prune := exec.NewColumnPrune(keep)
			out, err := prune.Execute(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			out.Detach()
			held = append(held, out)
		}
		in.Release()
	}
	want := []any{int64(101), int64(102), int64(202), int64(203), int64(303), int64(304)}
	got := make([]any, 0, len(held))
	for _, b := range held {
		got = append(got, b.Columns[0].GetValue(0))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("retained rows=%v; want %v", got, want)
	}
}

// The veto runs on the release path, which is concurrent: run it under -race
// with several producers sharing one pool, half of them retaining.
func TestPoolStaysCorrectUnderConcurrentGetAndPut(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeInt64}}
	pool := batch.NewBatchPool(schema, 1)
	const workers, iters = 8, 200
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var held []*batch.RecordBatch
			for i := 0; i < iters; i++ {
				b := pool.Get()
				want := int64(w*iters + i)
				b.Columns[0].SetValue(0, want)
				if i%3 == 0 {
					b.Detach()
					held = append(held, b)
					continue
				}
				if got, _ := b.Columns[0].GetInt64(0); got != want {
					errs <- fmt.Sprintf("worker %d: in-flight value %d != %d", w, got, want)
					return
				}
				b.Release()
			}
			for k, b := range held {
				want := int64(w*iters + k*3)
				if got, _ := b.Columns[0].GetInt64(0); got != want {
					errs <- fmt.Sprintf("worker %d: retained value %d != %d", w, got, want)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// The veto refuses admission; it must not cost reuse where nobody retains.
// An unretained workload cycles the SAME batch forever.
func TestTheVetoDoesNotCostReuseWhenNobodyRetains(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeInt64}}
	pool := batch.NewBatchPool(schema, 1)
	first := pool.Get()
	first.Release()
	for i := 0; i < 50; i++ {
		b := pool.Get()
		if b != first {
			t.Fatalf("cycle %d handed back a different batch: an unretained release must reuse", i)
		}
		b.Release()
	}
}

// --- helpers ---

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

func renderAny(v any) string { return fmt.Sprintf("%v", v) }

// recoverVectorWrite runs a write that reports by PANICKING with a typed query
// error (the #361 guard's contract, shared by the integer-range and the
// vector-width refusals) and returns that error, or nil when it succeeded.
func recoverVectorWrite(write func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				return
			}
			err = fmt.Errorf("%v", r)
		}
	}()
	write()
	return nil
}

func sqlStateOf(err error) string {
	var s interface{ SQLState() string }
	if errors.As(err, &s) {
		return s.SQLState()
	}
	return ""
}
