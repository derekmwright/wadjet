package expr

import (
	"fmt"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// nullBearingBatch carries one all-NULL row alongside rows that make the
// predicates in these tests answer both TRUE and FALSE. The NULL row is what
// makes an agreement check meaningful: without it the two boolean protocols
// cannot disagree.
func nullBearingBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "s", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, 5)
	vals := []struct {
		d int32
		n int64
		f float64
		s string
	}{
		{10000, 5, 1.5, ""},
		{10500, 50, 50.0, "abcdef"},
		{11000, 500, 500.0, "zz"},
		{0, 0, 0, ""},
		{12000, 5000, 5000.0, "abcdef"},
	}
	for i, v := range vals {
		b.Columns[0].SetValue(i, v.d)
		b.Columns[1].SetValue(i, v.n)
		b.Columns[2].SetValue(i, v.f)
		b.Columns[3].SetValue(i, v.s)
	}
	// Row 3 is NULL in every column.
	for c := range b.Columns {
		b.Columns[c].Nulls.SetNull(3)
	}
	return b
}

// TestLazyResolutionUnderConcurrency exercises the publication of the
// lazily-resolved node state (ColRef's column index/type, BinOpNumeric's
// arithmetic mode, BinOpFloat64/BinOpInt64's opCode, FuncCall's fn) from
// many goroutines at once, the way parallel pipeline workers share one
// compiled expression through a captured closure. Run under -race this is
// the guard on the double-checked resolution that replaced sync.Once at
// each of those sites.
func TestLazyResolutionUnderConcurrency(t *testing.T) {
	for _, sql := range []string{
		"n * 2 + 1",
		"f * 2 + 1",
		"n / 3",
		"d >= '1980-01-01'",
		"n IN (5, 50, 500)",
		"f * 2.5",   // BinOpFloat64, built directly by compileBinOp
		"2 * 3 + 4", // BinOpInt64, built directly (both operands int-native)
		"upper(s)",  // FuncCall.fnOnce
	} {
		t.Run(sql, func(t *testing.T) {
			// Reference answers from a separate, serially-resolved tree.
			ref := compileExprSQL(t, sql)
			refBatch := nullBearingBatch()
			want := make([]any, refBatch.Len)
			for row := range want {
				want[row] = ref.Eval(refBatch, row)
			}

			e := compileExprSQL(t, sql)
			const workers = 16
			var wg sync.WaitGroup
			errs := make(chan string, workers)
			start := make(chan struct{})
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					// Each worker reads its own batch, as pipeline clones do.
					b := nullBearingBatch()
					<-start
					for i := 0; i < 200; i++ {
						for row := 0; row < b.Len; row++ {
							if got := e.Eval(b, row); got != want[row] {
								errs <- fmt.Sprintf("row %d: got %v (%T), want %v (%T)",
									row, got, got, want[row], want[row])
								return
							}
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			close(errs)
			for msg := range errs {
				t.Fatal(msg)
			}
		})
	}
}

// TestBinOpInt64LazyResolutionUnderConcurrency covers BinOpInt64 with a
// COLUMN operand racing its own opCode guard alongside the operand ColRef's
// resolve guard — a shape compileBinOp never emits (isIntNative rejects a
// bare ColRef; see BenchmarkBinOpInt64Eval in dispatch_bench_test.go), but
// one the type itself does not forbid, and the one every other typed BinOp
// in this file is exercised with. Built by hand since SQL compilation
// cannot reach it.
func TestBinOpInt64LazyResolutionUnderConcurrency(t *testing.T) {
	newTree := func() Expr {
		return &BinOpInt64{Left: &ColRef{Name: "n"}, Right: &Lit{Val: int64(7)}, Op: "+"}
	}

	ref := newTree()
	refBatch := nullBearingBatch()
	want := make([]any, refBatch.Len)
	for row := range want {
		want[row] = ref.Eval(refBatch, row)
	}

	e := newTree()
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := nullBearingBatch()
			<-start
			for i := 0; i < 200; i++ {
				for row := 0; row < b.Len; row++ {
					if got := e.Eval(b, row); got != want[row] {
						errs <- fmt.Sprintf("row %d: got %v (%T), want %v (%T)",
							row, got, got, want[row], want[row])
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Fatal(msg)
	}
}
