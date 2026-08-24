package coordinator

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The probe-split merge is the third path a DECIMAL aggregate can take: not
// the single process and not the stage DAG, but the coordinator folding one
// partial row per worker (mergeProbePartials → mergeScalarAggregates /
// reAggregatePartials).
//
// It folds through ToRows, whose DECIMAL box is TEXT, so before #455 the
// float arms rounded the value and compareAnyValues ordered the STRINGS —
// "9.5" after "10.2". These pin the exact fold, and that AVG is NOT folded:
// a mean of means is not the mean, and the partials carry no count to
// recover it from.

func psdCol(name string, scale int) parquet.Column {
	return parquet.Column{Name: name, Type: parquet.TypeDecimal, Precision: 38, Scale: scale, Nullable: true}
}

func TestProbeSplitScalarDecimalMerge(t *testing.T) {
	schema := []parquet.Column{psdCol("s", 10), psdCol("lo", 10), psdCol("hi", 10), psdCol("a", 14)}
	mk := func(vals ...string) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 1)
		for i, v := range vals {
			b.Columns[i].DecimalData.Data[0] = batch.ParseDecimalString(v, schema[i].Scale)
			b.Columns[i].Nulls.SetValid(0)
		}
		b.Len = 1
		return b
	}
	// Two partials whose SUM is 25 digits wide, and MIN/MAX pairs a string
	// comparison gets backwards.
	b1 := mk("977777777887777.7577887713", "-100.5000000000", "9.5000000000", "1.00000000000000")
	b2 := mk("22222222112222.2422112287", "-9.5000000000", "10.2000000000", "3.00000000000000")
	mi := &logical.MergeInfo{HasAggregate: true, AggExprs: []logical.AggExpr{
		{Func: "sum", OutputCol: "s"},
		{Func: "min", OutputCol: "lo"},
		{Func: "max", OutputCol: "hi"},
		{Func: "avg", OutputCol: "a"},
	}}
	rows := mergeScalarAggHelper(t, []*batch.RecordBatch{b1, b2},
		[]string{"s", "lo", "hi", "a"}, mi)[0].ToRows()
	for _, tc := range []struct{ col, want string }{
		{"s", "1000000000000000.0000000000"},
		{"lo", "-100.5000000000"},
		{"hi", "10.2000000000"},
	} {
		if got := rows[0][tc.col]; got != tc.want {
			t.Errorf("%s = %#v, want %q", tc.col, got, tc.want)
		}
	}
	// AVG keeps this path's long-standing approximation (one partial's own
	// answer). Folding it as a SUM — which the DECIMAL arm did until
	// decimalFoldable gated it — turns "approximate" into "1+3=4".
	if got := rows[0]["a"]; got != "1.00000000000000" && got != "3.00000000000000" {
		t.Errorf("AVG = %#v, want one partial's own value; a SUM of the partials is not an average", got)
	}
}

func TestProbeSplitGroupedDecimalMerge(t *testing.T) {
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		psdCol("s", 10), psdCol("lo", 10), psdCol("hi", 10),
	}
	mk := func(g int64, s, lo, hi string) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 1)
		b.Columns[0].Int64Data[0] = g
		b.Columns[0].Nulls.SetValid(0)
		for i, v := range []string{s, lo, hi} {
			b.Columns[i+1].DecimalData.Data[0] = batch.ParseDecimalString(v, 10)
			b.Columns[i+1].Nulls.SetValid(0)
		}
		b.Len = 1
		return b
	}
	mi := &logical.MergeInfo{HasAggregate: true, GroupBy: []string{"g"}, AggExprs: []logical.AggExpr{
		{Func: "sum", OutputCol: "s"},
		{Func: "min", OutputCol: "lo"},
		{Func: "max", OutputCol: "hi"},
	}}
	c := &Coordinator{}
	merged, err := c.reAggregatePartials([]*batch.RecordBatch{
		mk(1, "977777777887777.7577887713", "-100.5000000000", "9.5000000000"),
		mk(1, "22222222112222.2422112287", "-9.5000000000", "10.2000000000"),
	}, []string{"g", "s", "lo", "hi"}, map[string]int{"g": 0, "s": 1, "lo": 2, "hi": 3}, mi)
	if err != nil {
		t.Fatalf("reAggregatePartials: %v", err)
	}
	rows := merged[0].ToRows()
	if len(rows) != 1 {
		t.Fatalf("got %d groups, want 1", len(rows))
	}
	for _, tc := range []struct{ col, want string }{
		{"s", "1000000000000000.0000000000"},
		{"lo", "-100.5000000000"},
		{"hi", "10.2000000000"},
	} {
		if got := rows[0][tc.col]; got != tc.want {
			t.Errorf("%s = %#v, want %q", tc.col, got, tc.want)
		}
	}
}
