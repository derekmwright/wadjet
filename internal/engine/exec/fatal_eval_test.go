package exec

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The pipeline half of the #347 guard: an expression that cannot produce a
// correct answer raises rather than returning NULL, and the pipeline turns
// that into a query ERROR instead of letting it take the process down or —
// worse — letting a wrong answer through.
//
// The value used here is the real *expr.MissingOuterColumnError, so this also
// pins that the two packages actually connect: expr raises it, exec's
// FatalEvalPanic interface recognizes it. A test with a locally declared
// stand-in would pass while the two drifted apart.

func fatalEvalSchema() []parquet.Column {
	return []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
}

func fatalEvalRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}
	return rows
}

func missingOuterColumnError() *expr.MissingOuterColumnError {
	return &expr.MissingOuterColumnError{
		Ref:       plansql.OuterRef{Table: "c1", Column: "c_nationkey"},
		Available: []string{"c_custkey"},
	}
}

func TestPipelineConvertsFatalEvalPanicToError(t *testing.T) {
	for _, workers := range []int{1, 4} {
		name := "serial"
		if workers > 1 {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			raised := missingOuterColumnError()
			// The parallel driver runs the FIRST batch through the original
			// operator chain on the caller's goroutine (the warm-up), so a
			// predicate that panics immediately would be caught by Run's own
			// defer and never reach a worker. Panicking from the second
			// batch on is what puts the raise on a worker goroutine, where
			// only the per-worker recover can see it.
			firstID := int64(0)
			if workers > 1 {
				firstID = int64(batch.DefaultBatchSize)
			}
			p := &Pipeline{
				Source: NewSliceSource(fatalEvalSchema(), fatalEvalRows(4*batch.DefaultBatchSize)),
				Ops: []UnaryOperator{NewFilter(func(b *batch.RecordBatch, row int) bool {
					if v, ok := b.ColumnByName("id").GetValue(row).(int64); ok && v >= firstID {
						panic(raised)
					}
					return true
				})},
				Sink:    &CollectSink{},
				Workers: workers,
			}
			err := p.Run(context.Background())
			if err == nil {
				t.Fatal("Run returned nil; a FatalEvalPanic must surface as a query error, " +
					"not be swallowed and not crash the process")
			}
			var missing *expr.MissingOuterColumnError
			if !errors.As(err, &missing) {
				t.Fatalf("Run returned %v, want the raised *expr.MissingOuterColumnError", err)
			}
			if !strings.Contains(err.Error(), "c_nationkey") {
				t.Errorf("error text %q does not name the unresolved column", err)
			}
		})
	}
}

// Anything that is not a FatalEvalPanic keeps propagating as a panic: the
// recover is a narrow conversion, not a blanket catch that would turn every
// engine bug into a quiet error return.
func TestPipelineRepanicsOrdinaryPanics(t *testing.T) {
	p := &Pipeline{
		Source: NewSliceSource(fatalEvalSchema(), fatalEvalRows(4)),
		Ops: []UnaryOperator{NewFilter(func(*batch.RecordBatch, int) bool {
			panic("some engine bug")
		})},
		Sink: &CollectSink{},
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("an ordinary panic was swallowed by the FatalEvalPanic recover")
		}
		if s, ok := r.(string); !ok || s != "some engine bug" {
			t.Fatalf("re-raised %#v, want the original panic value", r)
		}
	}()
	_ = p.Run(context.Background())
}
