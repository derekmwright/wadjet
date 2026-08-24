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

// An ordinary panic is NOT quietly turned into a nil error, and it is not
// re-raised into a process exit either. #511 replaced the second half of that
// contract: the recover used to be a narrow conversion that re-panicked
// everything else, on the reasoning that a blanket catch would hide engine
// bugs. It hid nothing and cost everything — the re-panic reached no further
// recovery, so one query's bug ended the server for every connection.
//
// The bug stays loud without that price. It comes back as an internal error
// (XX000) whose message carries the panic value, the full stack is logged at
// error level, and exec.QueryPanicsRecovered counts it so the process-killer
// gate fails CI on any query that reaches one.
func TestPipelineReportsOrdinaryPanicsAsInternalErrors(t *testing.T) {
	p := &Pipeline{
		Source: NewSliceSource(fatalEvalSchema(), fatalEvalRows(4)),
		Ops: []UnaryOperator{NewFilter(func(*batch.RecordBatch, int) bool {
			panic("some engine bug")
		})},
		Sink: &CollectSink{},
	}
	before := QueryPanicsRecovered()

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("the panic escaped Pipeline.Run (%v) — it would end the process", r)
			}
		}()
		return p.Run(context.Background())
	}()

	if err == nil {
		t.Fatal("Run returned nil; the panic must surface as a query error")
	}
	if !strings.Contains(err.Error(), "some engine bug") {
		t.Errorf("error %q lost the panic value", err)
	}
	var qp *QueryPanic
	if !errors.As(err, &qp) {
		t.Fatalf("error %v is not a *QueryPanic, so it carries no SQLSTATE", err)
	}
	if qp.SQLState() != SQLStateInternalError {
		t.Errorf("SQLSTATE = %q, want %q", qp.SQLState(), SQLStateInternalError)
	}
	if after := QueryPanicsRecovered(); after != before+1 {
		t.Errorf("QueryPanicsRecovered %d -> %d, want +1: an engine bug the gate cannot "+
			"see is an engine bug the gate cannot fail on", before, after)
	}
}
