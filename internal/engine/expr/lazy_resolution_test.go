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
// arithmetic mode) from many goroutines at once, the way parallel pipeline
// workers share one compiled expression through a captured closure. Run
// under -race this is the guard on the double-checked resolution that
// replaced sync.Once.
func TestLazyResolutionUnderConcurrency(t *testing.T) {
	for _, sql := range []string{
		"n * 2 + 1",
		"f * 2 + 1",
		"n / 3",
		"d >= '1980-01-01'",
		"n IN (5, 50, 500)",
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
