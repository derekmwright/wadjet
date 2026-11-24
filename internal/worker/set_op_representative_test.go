// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An INTERSECT / EXCEPT counting aggregate keeps a LEFT member as each
// group's representative whenever the group has one (#1713): the worker
// arms it from the emit operator that follows it in the fragment, and the
// shuffle sender's precombine keeps the same invariant for the rows it ships.
func setOpTagBatch(rows [][2]any) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
		{Name: dagplan.SetOpLeftCountCol, Type: parquet.TypeInt64},
		{Name: dagplan.SetOpRightCountCol, Type: parquet.TypeInt64},
	}
	b := batch.NewRecordBatch(schema, len(rows))
	for i, r := range rows {
		b.Columns[0].SetValue(i, r[0])
		b.Columns[0].Nulls.SetValid(i)
		l := r[1].(int64)
		b.Columns[1].Int64Data[i] = l
		b.Columns[2].Int64Data[i] = 1 - l
		b.Columns[1].Nulls.SetValid(i)
		b.Columns[2].Nulls.SetValid(i)
	}
	return b
}

func setOpCountingAggs() []exec.AggColumn {
	return []exec.AggColumn{
		{Func: exec.AggSum, InputCol: dagplan.SetOpLeftCountCol, OutputCol: dagplan.SetOpLeftCountCol, OutputType: parquet.TypeInt64},
		{Func: exec.AggSum, InputCol: dagplan.SetOpRightCountCol, OutputCol: dagplan.SetOpRightCountCol, OutputType: parquet.TypeInt64},
	}
}

// The right arm's 7 meets the left arm's 7.00 in both orders: the group
// prints the left member, as PostgreSQL's INTERSECT does.
func TestSetOpCountingAggregateKeepsALeftRepresentative(t *testing.T) {
	ctx := context.Background()
	for _, rows := range [][][2]any{
		{{"7", int64(0)}, {"7.00", int64(1)}},
		{{"7.00", int64(1)}, {"7", int64(0)}},
	} {
		agg := exec.NewHashAggregate([]string{"v"}, setOpCountingAggs())
		setOpRepresentativeEligibility(agg, []distributed.OpSpec{{
			Type: distributed.OpSetOpEmit, SetOp: "intersect",
			SetOpLeftCol: dagplan.SetOpLeftCountCol, SetOpRightCol: dagplan.SetOpRightCountCol,
		}})
		if err := agg.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if err := agg.Consume(ctx, setOpTagBatch(rows)); err != nil {
			t.Fatal(err)
		}
		if err := agg.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		out, err := agg.Next(ctx)
		if err != nil || out == nil {
			t.Fatalf("next: %v %v", out, err)
		}
		if got := out.Columns[0].GetValueOf(0, false); got != "7.00" {
			t.Errorf("arrival %v: representative %v, want the left member 7.00", rows, got)
		}
	}
}

func TestShufflePrecombineKeepsALeftRepresentative(t *testing.T) {
	specs := []distributed.AggSpec{
		{Func: "sum", InputCol: dagplan.SetOpLeftCountCol, OutputCol: dagplan.SetOpLeftCountCol},
		{Func: "sum", InputCol: dagplan.SetOpRightCountCol, OutputCol: dagplan.SetOpRightCountCol},
	}
	for _, rows := range [][][2]any{
		{{"7", int64(0)}, {"7.00", int64(1)}},
		{{"7.00", int64(1)}, {"7", int64(0)}},
	} {
		p := newCappedPartialAgg([]string{"v"}, specs, 1<<30)
		out := drainAll(t, p, setOpTagBatch(rows))
		if len(out) != 1 {
			t.Fatalf("arrival %v: %d rows", rows, len(out))
		}
		if got := out[0]["v"]; got != "7.00" {
			t.Errorf("arrival %v: precombined representative %v, want the left member 7.00", rows, got)
		}
	}
}
