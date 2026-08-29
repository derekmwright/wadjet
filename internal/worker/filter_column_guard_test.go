package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The #147 guard reaching the DAG's ROW evaluator (#653).
//
// A scan stage's FilterExprs are written by the planner against the CATALOG's
// schema, so a predicate naming a column the scan's batches do not carry names
// nothing at all — `expr.ColRef.Eval` answers nil, the predicate is UNKNOWN on
// every row, and a WHERE that admits only TRUE returns no rows. That is
// indistinguishable from empty data, and it is how a renamed CTE column
// answered ZERO on the DAG while the single-process engine answered correctly.
//
// The guard is deliberately NOT applied above a join: a hash-join partition
// with an empty build side emits its probe rows with only the join keys
// declared for the missing side, so a legitimately-NULL build column is
// genuinely absent from that batch's schema (TPC-H Q20's
// `ps_availqty > 0.5 * __scalar_0` over a LEFT join). Hence the scanSchema
// argument, and hence both halves of this test.
func guardBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
	}, 2)
	b.Columns[0].SetValue(0, int64(1))
	b.Columns[0].SetValue(1, int64(2))
	b.Columns[1].SetValue(0, int64(10))
	b.Columns[1].SetValue(1, nil) // a legitimately NULL value, not an absent column
	b.Len = 2
	return b
}

func TestScanFilterRefusesAColumnTheScanDoesNotCarry(t *testing.T) {
	ops, _, err := compileFilterExprs([]string{"v > 0"}, true)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("got %d ops, want 1", len(ops))
	}
	_, err = ops[0].Execute(context.Background(), guardBatch(t))
	if err == nil {
		t.Fatal("the scan filter ANSWERED for a column the scan does not carry; " +
			"that is the #147 failure mode and must be a query error")
	}
	if !strings.Contains(err.Error(), `"v"`) {
		t.Fatalf("error %q does not name the missing column", err)
	}
}

func TestScanFilterAdmitsRowsOverANullColumn(t *testing.T) {
	// c_i64 is present and NULL on one row. The guard is a SCHEMA test, so it
	// must not fire — the NULL row simply fails the predicate.
	ops, _, err := compileFilterExprs([]string{"c_i64 > 5"}, true)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := ops[0].Execute(context.Background(), guardBatch(t))
	if err != nil {
		t.Fatalf("a NULL VALUE is not an absent COLUMN: %v", err)
	}
	if out == nil || len(out.Sel) != 1 || out.Sel[0] != 0 {
		t.Fatalf("selected %v, want row 0 only", out)
	}
}

func TestNonScanFilterDoesNotJudgeItsInputSchema(t *testing.T) {
	// The same predicate above a JOIN, where an empty build side legitimately
	// leaves a column out of the batch's schema. It must answer (no rows),
	// not refuse.
	ops, _, err := compileFilterExprs([]string{"v > 0"}, false)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := ops[0].Execute(context.Background(), guardBatch(t))
	if err != nil {
		t.Fatalf("a filter above a join must not judge its input schema: %v", err)
	}
	if out != nil {
		t.Fatalf("an unresolvable reference is UNKNOWN on every row, so nothing qualifies; got %v", out.Sel)
	}
}
