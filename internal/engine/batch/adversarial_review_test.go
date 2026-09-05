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

// Checked value conversion reaches every leaf, however deeply nested (#898).
// SetValueChecked used to handle only a vector whose OWN type is DECIMAL and
// delegate a container to SetValue, which reinterprets an integer as a raw
// unscaled carrier, saturates an over-wide text and stores 0.00 for text that
// names no number — all with no error.
func TestCheckedConversionRefusesTheSameDecimalBoxAtEveryNestingDepth(t *testing.T) {
	d := parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 2}
	dval := parquet.Column{Name: "value", Type: parquet.TypeDecimal, Precision: 18, Scale: 2}
	mapEntry := parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "key", Type: parquet.TypeString}, dval,
	}}

	shapes := []struct {
		name string
		col  parquet.Column
		wrap func(any) any
		// path is the fragment the error must name so the failing leaf can be
		// found inside the container.
		path string
	}{
		{
			name: "scalar",
			col:  d,
			wrap: func(v any) any { return v },
		},
		{
			name: "row",
			col:  parquet.Column{Name: "x", Type: parquet.TypeRow, Fields: []parquet.Column{d}},
			wrap: func(v any) any { return map[string]any{"d": v} },
			path: `in "d"`,
		},
		{
			name: "array",
			col:  parquet.Column{Name: "x", Type: parquet.TypeArray, ElementType: &d},
			wrap: func(v any) any { return []any{v} },
			path: `in "[0]"`,
		},
		{
			name: "map",
			col:  parquet.Column{Name: "x", Type: parquet.TypeMap, ElementType: &mapEntry},
			wrap: func(v any) any { return map[string]any{"a": v} },
			path: `in "[0].value"`,
		},
		{
			name: "row-of-array",
			col: parquet.Column{Name: "x", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "a", Type: parquet.TypeArray, ElementType: &d},
			}},
			wrap: func(v any) any { return map[string]any{"a": []any{v}} },
			path: `in "a[0]"`,
		},
	}

	bad := []struct {
		name string
		val  any
		want string // a fragment of the refusal, so the REASON is asserted too
	}{
		{"invalid-text", "not-a-number", "invalid input syntax"},
		{"raw-int-carrier", int64(42), "raw unscaled carrier"},
		{"overflow", "99999999999999999999999999999999999999999999999999999", "numeric field overflow"},
	}

	for _, sh := range shapes {
		for _, b := range bad {
			t.Run(sh.name+"/"+b.name, func(t *testing.T) {
				rows := []map[string]any{{sh.col.Name: sh.wrap(b.val)}}
				out, err := batch.FromRowsChecked([]parquet.Column{sh.col}, rows)
				if err == nil {
					t.Fatalf("checked write accepted %v (%T) and stored %v", b.val, b.val, out.ToRows())
				}
				if !contains(err.Error(), b.want) {
					t.Errorf("error %q does not name the reason %q", err, b.want)
				}
				if sh.path != "" && !contains(err.Error(), sh.path) {
					t.Errorf("error %q does not name the leaf path %q", err, sh.path)
				}
			})
		}
		// The control: a valid decimal text stores the exact value at every
		// nesting, so the fix refuses without also refusing the good case.
		t.Run(sh.name+"/valid", func(t *testing.T) {
			rows := []map[string]any{{sh.col.Name: sh.wrap("12.34")}}
			out, err := batch.FromRowsChecked([]parquet.Column{sh.col}, rows)
			if err != nil {
				t.Fatalf("checked write refused a valid decimal: %v", err)
			}
			if got := out.Columns[0].GetValue(0); !contains(renderAny(got), "12.34") {
				t.Errorf("stored %v; want a 12.34 somewhere in it", got)
			}
		})
	}
}

// Every append helper reserves a VECTOR row's components, NULL rows included
// (#899). appendToVector had no TypeVector arm, so a nested vector leaf and a
// null-through-a-view append both left the component storage short and the
// next read sliced past it.
func TestVectorComponentsAreReservedByEveryAppendPath(t *testing.T) {
	t.Run("array-of-vector", func(t *testing.T) {
		defer failOnPanic(t)
		elem := parquet.Column{Name: "element", Type: parquet.TypeVector, Dimension: 2}
		col := parquet.Column{Name: "x", Type: parquet.TypeArray, ElementType: &elem}
		value := []any{[]float32{1, 2}, []float32{3, 4}}
		b := batch.FromRows([]parquet.Column{col}, []map[string]any{{"x": value}})
		if got := b.Columns[0].GetValue(0); !reflect.DeepEqual(got, value) {
			t.Errorf("ARRAY<VECTOR(2)> = %v; want %v", got, value)
		}
	})

	t.Run("array-of-vector-with-null-element", func(t *testing.T) {
		defer failOnPanic(t)
		elem := parquet.Column{Name: "element", Type: parquet.TypeVector, Dimension: 2}
		col := parquet.Column{Name: "x", Type: parquet.TypeArray, ElementType: &elem}
		value := []any{nil, []float32{3, 4}}
		b := batch.FromRows([]parquet.Column{col}, []map[string]any{{"x": value}})
		if got := b.Columns[0].GetValue(0); !reflect.DeepEqual(got, value) {
			t.Errorf("ARRAY<VECTOR(2)> with a NULL element = %v; want %v", got, value)
		}
	})

	t.Run("row-of-vector", func(t *testing.T) {
		defer failOnPanic(t)
		col := parquet.Column{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "v", Type: parquet.TypeVector, Dimension: 3},
		}}
		want := map[string]any{"v": []float32{1, 2, 3}}
		b := batch.FromRows([]parquet.Column{col}, []map[string]any{{"r": want}})
		if got := b.Columns[0].GetValue(0); !reflect.DeepEqual(got, want) {
			t.Errorf("ROW{VECTOR(3)} = %v; want %v", got, want)
		}
	})

	t.Run("map-to-vector", func(t *testing.T) {
		defer failOnPanic(t)
		entry := parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "key", Type: parquet.TypeString},
			{Name: "value", Type: parquet.TypeVector, Dimension: 2},
		}}
		col := parquet.Column{Name: "m", Type: parquet.TypeMap, ElementType: &entry}
		b := batch.FromRows([]parquet.Column{col}, []map[string]any{{"m": map[string]any{"a": []float32{5, 6}}}})
		got := b.Columns[0].GetValue(0)
		if !contains(renderAny(got), "[5 6]") {
			t.Errorf("MAP<STRING,VECTOR(2)> = %v; want a [5 6] value", got)
		}
	})

	t.Run("null-view-append-then-value", func(t *testing.T) {
		defer failOnPanic(t)
		src := batch.NewVectorVector(2, 2)
		src.SetVector(0, []float32{1, 2})
		src.SetVector(1, []float32{3, 4})
		view := batch.NewViewVector(src, []uint32{0, 1})
		view.Nulls.SetNull(0)
		dst := batch.NewVectorLike(src)
		dst.AppendFrom(view, 0)
		dst.AppendFrom(view, 1)
		got := []any{dst.GetValue(0), dst.GetValue(1)}
		if !reflect.DeepEqual(got, []any{nil, []float32{3, 4}}) {
			t.Errorf("view append = %v; want [<nil> [3 4]]", got)
		}
	})

	t.Run("present-before-null", func(t *testing.T) {
		defer failOnPanic(t)
		src := batch.NewVectorVector(2, 2)
		src.SetVector(0, []float32{1, 2})
		src.Nulls.SetNull(1)
		dst := batch.NewVectorLike(src)
		dst.AppendFrom(src, 0)
		dst.AppendFrom(src, 1)
		dst.AppendFrom(src, 0)
		got := []any{dst.GetValue(0), dst.GetValue(1), dst.GetValue(2)}
		want := []any{[]float32{1, 2}, nil, []float32{1, 2}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("append = %v; want %v", got, want)
		}
	})

	// The reservation is per DIMENSION, so the narrowest and a wide one both
	// have to keep row i at [i*dim, (i+1)*dim).
	for _, dim := range []int{1, 1024} {
		t.Run(fmt.Sprintf("dim-%d-through-an-array", dim), func(t *testing.T) {
			defer failOnPanic(t)
			elem := parquet.Column{Name: "element", Type: parquet.TypeVector, Dimension: dim}
			col := parquet.Column{Name: "x", Type: parquet.TypeArray, ElementType: &elem}
			first := make([]float32, dim)
			second := make([]float32, dim)
			for j := range first {
				first[j] = float32(j)
				second[j] = float32(j) + 0.5
			}
			value := []any{first, nil, second}
			b := batch.FromRows([]parquet.Column{col}, []map[string]any{{"x": value}})
			if got := b.Columns[0].GetValue(0); !reflect.DeepEqual(got, value) {
				t.Errorf("ARRAY<VECTOR(%d)> round trip lost values", dim)
			}
		})
	}

	t.Run("owned-null-append-then-value", func(t *testing.T) {
		defer failOnPanic(t)
		src := batch.NewVectorVector(2, 2)
		src.Nulls.SetNull(0)
		src.SetVector(1, []float32{3, 4})
		dst := batch.NewVectorLike(src)
		dst.AppendFrom(src, 0)
		dst.AppendFrom(src, 1)
		got := []any{dst.GetValue(0), dst.GetValue(1)}
		if !reflect.DeepEqual(got, []any{nil, []float32{3, 4}}) {
			t.Errorf("owned append = %v; want [<nil> [3 4]]", got)
		}
	})
}

// A VECTOR(N) value has exactly N components, and pool reuse can never show a
// previous batch's (#900). SetVector copied however many components fit and
// initialized nothing, so the same input answered [1 0] on a fresh batch and
// [1 8] on one that had held [7 8] — no error either way.
func TestVectorWriteIsExactlyTheDeclaredWidth(t *testing.T) {
	schema := []parquet.Column{{Name: "x", Type: parquet.TypeVector, Dimension: 2}}

	// The headline: identical input, identical output, whatever the pool has
	// been carrying. Compared component for component, not row for row.
	t.Run("fresh-and-reused-agree", func(t *testing.T) {
		fresh := batch.NewRecordBatch(schema, 1)
		fresh.Columns[0].SetVector(0, []float32{1, 9})

		pool := batch.NewBatchPool(schema, 1)
		prior := pool.Get()
		prior.Columns[0].SetVector(0, []float32{7, 8})
		prior.Release()
		reused := pool.Get()
		if reused != prior {
			t.Skip("the pool minted new storage; this cell needs the same batch back")
		}
		reused.Columns[0].SetVector(0, []float32{1, 9})

		if got, want := reused.Columns[0].GetValue(0), fresh.Columns[0].GetValue(0); !reflect.DeepEqual(got, want) {
			t.Errorf("same input: reused=%v fresh=%v", got, want)
		}
		if got, want := reused.Columns[0].Float32Data, fresh.Columns[0].Float32Data; !reflect.DeepEqual(got, want) {
			t.Errorf("component storage: reused=%v fresh=%v", got, want)
		}
	})

	// Every write path that reaches a VECTOR row, fresh against reused.
	t.Run("every-write-path-agrees-fresh-and-reused", func(t *testing.T) {
		writes := map[string]func(*batch.Vector){
			"SetVector": func(v *batch.Vector) { v.SetVector(0, []float32{3, 4}) },
			"SetValue":  func(v *batch.Vector) { v.SetValue(0, []float32{3, 4}) },
			"SetValueAny": func(v *batch.Vector) {
				v.SetValue(0, []any{float64(3), float64(4)})
			},
			"AppendFrom": func(v *batch.Vector) {
				src := batch.NewVectorVector(1, 2)
				src.SetVector(0, []float32{3, 4})
				v.Len = 0
				v.Float32Data = v.Float32Data[:0]
				v.AppendFrom(src, 0)
			},
		}
		for name, write := range writes {
			t.Run(name, func(t *testing.T) {
				fresh := batch.NewRecordBatch(schema, 1)
				write(fresh.Columns[0])

				pool := batch.NewBatchPool(schema, 1)
				prior := pool.Get()
				prior.Columns[0].SetVector(0, []float32{7, 8})
				prior.Release()
				reused := pool.Get()
				if reused != prior {
					t.Skip("the pool minted new storage; this cell needs the same batch back")
				}
				write(reused.Columns[0])

				if got, want := reused.Columns[0].GetValue(0), fresh.Columns[0].GetValue(0); !reflect.DeepEqual(got, want) {
					t.Errorf("%s: reused=%v fresh=%v", name, got, want)
				}
			})
		}
	})

	t.Run("null-then-write-leaves-no-stale-component", func(t *testing.T) {
		v := batch.NewVectorVector(1, 3)
		v.SetVector(0, []float32{7, 8, 9})
		v.Nulls.SetNull(0)
		v.SetVector(0, []float32{1, 2, 3})
		if got := v.GetValue(0); !reflect.DeepEqual(got, []float32{1, 2, 3}) {
			t.Errorf("rewritten row=%v; want [1 2 3]", got)
		}
	})

	// A short or long write is refused at the API — never padded, never
	// truncated — and the refused write leaves the row as it was.
	for _, bad := range [][]float32{{}, {1}, {1, 2, 3}, {1, 2, 3, 4}} {
		t.Run(fmt.Sprintf("refuses-%d-components", len(bad)), func(t *testing.T) {
			v := batch.NewVectorVector(1, 2)
			v.SetVector(0, []float32{7, 8})
			err := recoverVectorWrite(func() { v.SetVector(0, bad) })
			if err == nil {
				t.Fatalf("SetVector accepted %d components into a VECTOR(2) and stored %v",
					len(bad), v.GetValue(0))
			}
			if !contains(err.Error(), "dimensions") {
				t.Errorf("refusal %q does not name the dimension", err)
			}
			if got := sqlStateOf(err); got != "22000" {
				t.Errorf("refusal SQLSTATE %q; want 22000 (data_exception, what pgvector raises)", got)
			}
			if got := v.GetValue(0); !reflect.DeepEqual(got, []float32{7, 8}) {
				t.Errorf("row after a refused write=%v; want the old [7 8]", got)
			}
		})
	}

	t.Run("setvalue-arms-refuse-the-same-widths", func(t *testing.T) {
		for _, box := range []any{
			[]float32{1},
			[]float32{1, 2, 3},
			[]any{float32(1)},
			[]any{float64(1), float64(2), float64(3)},
		} {
			v := batch.NewVectorVector(1, 2)
			if err := recoverVectorWrite(func() { v.SetValue(0, box) }); err == nil {
				t.Errorf("SetValue accepted %v into a VECTOR(2), storing %v", box, v.GetValue(0))
			}
		}
	})

	// An []any component box the arm does not recognize used to leave the slot
	// holding whatever was there; it is a type mismatch like any other now.
	t.Run("setvalue-refuses-an-unconvertible-component", func(t *testing.T) {
		v := batch.NewVectorVector(1, 2)
		v.SetVector(0, []float32{7, 8})
		if err := recoverVectorWrite(func() { v.SetValue(0, []any{float64(1), "two"}) }); err == nil {
			t.Errorf("SetValue accepted a string component, storing %v", v.GetValue(0))
		}
	})

	t.Run("nested-arms-refuse-the-same-widths", func(t *testing.T) {
		elem := parquet.Column{Name: "element", Type: parquet.TypeVector, Dimension: 2}
		arr := parquet.Column{Name: "x", Type: parquet.TypeArray, ElementType: &elem}
		if err := recoverVectorWrite(func() {
			batch.FromRows([]parquet.Column{arr}, []map[string]any{{"x": []any{[]float32{1}}}})
		}); err == nil {
			t.Errorf("a short vector inside an ARRAY was accepted")
		}
		row := parquet.Column{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "v", Type: parquet.TypeVector, Dimension: 2},
		}}
		if err := recoverVectorWrite(func() {
			batch.FromRows([]parquet.Column{row}, []map[string]any{{"r": map[string]any{"v": []float32{1, 2, 3}}}})
		}); err == nil {
			t.Errorf("a long vector inside a ROW was accepted")
		}
		key := parquet.Column{Name: "key", Type: parquet.TypeString}
		val := parquet.Column{Name: "value", Type: parquet.TypeVector, Dimension: 2}
		entry := parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{key, val}}
		m := parquet.Column{Name: "m", Type: parquet.TypeMap, ElementType: &entry}
		if err := recoverVectorWrite(func() {
			batch.FromRows([]parquet.Column{m}, []map[string]any{{"m": map[string]any{"a": []float32{1}}}})
		}); err == nil {
			t.Errorf("a short vector inside a MAP was accepted")
		}
	})
}

// The O(1) pool check reads ONE flag, and it is only sound while every vector
// under a pooled batch carries that batch's claim state. This asserts THAT —
// the invariant the test is named for — and not merely that a well-formed
// batch vetoes, which is what its first version checked (round-2 review P1).
//
// Three parts: the invariant holds on a batch out of the pool; a claim at any
// depth vetoes; and a FOREIGN vector — one minted somewhere else, which is the
// only way to break the invariant — is adopted by SetColumn and vetoes like any
// other, from all three origins the codebase actually mints.
func TestAPooledBatchOwnsItsColumns(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "c", Type: parquet.TypeRow, Fields: []parquet.Column{{Name: "d", Type: parquet.TypeFloat64}}},
		}},
		{Name: "arr", Type: parquet.TypeArray, ElementType: &elem},
	}
	pool := batch.NewBatchPool(schema, 4)

	// The invariant itself, at mint and after a reuse cycle.
	t.Run("invariant", func(t *testing.T) {
		b := pool.Get()
		if !b.OwnsItsColumns() {
			t.Fatalf("a freshly minted pooled batch does not own its columns")
		}
		b.Release()
		again := pool.Get()
		if !again.OwnsItsColumns() {
			t.Errorf("a recycled pooled batch does not own its columns")
		}
		again.Release()
	})

	// A claim on ANY vector under the batch — however deep — has to reach the
	// batch's own check, which is what the flag is for.
	for _, at := range []string{"column", "row-child", "row-grandchild", "array-child"} {
		t.Run("claim-at/"+at, func(t *testing.T) {
			b := pool.Get()
			target := b.Columns[0]
			switch at {
			case "row-child":
				target = b.Columns[1].Children[0]
			case "row-grandchild":
				target = b.Columns[1].Children[1].Children[0]
			case "array-child":
				target = b.Columns[2].Child
			}
			if target == nil {
				t.Fatalf("schema did not produce the %s vector", at)
			}
			vetoes := batch.PoolRetentionVetoes()
			target.Claim()
			b.Release()
			if batch.PoolRetentionVetoes() == vetoes {
				t.Errorf("a claim on the %s did not veto the batch's pool admission", at)
			}
			next := pool.Get()
			if next == b {
				t.Errorf("the pool handed back a batch whose %s a consumer claimed", at)
			}
			next.Release()
		})
	}

	// A FOREIGN column. Each of these is a shape the tree already mints, and
	// each carries a claim state that is not this batch's — so before SetColumn
	// existed, a claim on one set somebody else's flag and the batch was
	// recycled underneath it. Both orders: claimed before joining, and claimed
	// after.
	flat := []parquet.Column{{Name: "c", Type: parquet.TypeInt64}}
	foreigners := map[string]func() *batch.Vector{
		"NewVectorLike":   func() *batch.Vector { return batch.NewVectorLike(batch.NewRecordBatch(flat, 4).Columns[0]) },
		"NewColumnVector": func() *batch.Vector { return batch.NewColumnVector(flat[0], 4) },
		"another-pooled-batch": func() *batch.Vector {
			return batch.NewBatchPool(flat, 4).Get().Columns[0]
		},
	}
	for name, mint := range foreigners {
		for _, order := range []string{"claim-after-join", "claim-before-join"} {
			t.Run("foreign/"+name+"/"+order, func(t *testing.T) {
				p := batch.NewBatchPool(flat, 4)
				b := p.Get()
				alien := mint()
				if order == "claim-before-join" {
					alien.Claim()
				}
				b.SetColumn(0, alien)
				if !b.OwnsItsColumns() {
					t.Fatalf("SetColumn did not adopt the %s vector", name)
				}
				if order == "claim-after-join" {
					alien.Claim()
				}
				vetoes := batch.PoolRetentionVetoes()
				b.Release()
				if batch.PoolRetentionVetoes() == vetoes {
					t.Errorf("a claim on an adopted %s vector did not veto", name)
				}
				if next := p.Get(); next == b {
					t.Errorf("the pool handed back a batch whose adopted %s column is claimed", name)
				}
			})
		}
	}

	// And the hazard SetColumn exists to prevent, stated as a measurement: a
	// raw assignment leaves the batch not owning its columns, which is exactly
	// what OwnsItsColumns reports and what a future operator pooling a
	// column-swapping accumulator would have to answer for.
	t.Run("raw-assignment-breaks-the-invariant", func(t *testing.T) {
		p := batch.NewBatchPool(flat, 4)
		b := p.Get()
		b.Columns[0] = batch.NewColumnVector(flat[0], 4)
		if b.OwnsItsColumns() {
			t.Errorf("a raw assignment of a foreign column went undetected; " +
				"OwnsItsColumns can no longer catch the hazard SetColumn exists for")
		}
	})
}

// A batch built by hand over another batch's vectors carries no claim state of
// its own, so the predicate has to fall back to the walk rather than answer
// "unclaimed" because a pointer is nil.
func TestADerivedShellStillSeesAClaimThroughTheWalk(t *testing.T) {
	schema := []parquet.Column{{Name: "c", Type: parquet.TypeInt64}}
	pool := batch.NewBatchPool(schema, 1)
	src := pool.Get()
	src.Columns[0].SetValue(0, int64(7))

	// The shape ColumnPrune and the set-op emitter mint: a fresh shell over
	// the same *Vector pointers, with a pool of its own attached.
	derived := &batch.RecordBatch{
		Schema:  schema,
		Columns: []*batch.Vector{src.Columns[0]},
		Len:     1,
	}
	vetoes := batch.PoolRetentionVetoes()
	src.Columns[0].Claim()
	pool.Put(derived)
	if batch.PoolRetentionVetoes() == vetoes {
		t.Errorf("a hand-built shell over a claimed vector was admitted to the pool")
	}
}

// --- helpers ---

// failOnPanic turns a panic inside a subtest into that subtest's failure, so
// one broken cell reports instead of taking the whole binary down with it.
func failOnPanic(t *testing.T) {
	t.Helper()
	if r := recover(); r != nil {
		t.Errorf("panicked: %v", r)
	}
}

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
