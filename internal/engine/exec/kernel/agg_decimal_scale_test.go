package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A DECIMAL accumulator counts in ONE scale, and it is the scale of the values
// it holds. A batch that contributed nothing must not change it.
//
// This is the value half of #685, isolated from the query that found it. On
// the stage DAG an ungrouped aggregate whose filter matched no rows emits an
// identity row — one all-NULL row — and before that row carried its stage's
// declared (p,s) it shipped a scale-0 DECIMAL column. The merging aggregate
// consumed it alongside the partials that held real values, and the batch-level
// DECIMAL kernels assigned `acc.DecScale = vec.DecimalData.Scale` OUTSIDE their
// null guard, so the empty batch redefined the scale of an Int128 it had not
// touched. FinalSum then rendered 3824 at scale 0 and the emit re-parsed
// "3824" into a scale-2 column: 3824.00 for 38.24, exactly 10^scale out.
//
// The row-level updaters (sumRowDecimal and friends) already set the scale
// only inside the guard, so this is also the two kernels agreeing on one rule.
//
// The identity row's declaration is fixed too (exec.HashAggregate.outputSchema
// carries the plan-time (p,s) now), which means the DAG no longer produces the
// scale-0 batch this test hands the kernel directly. That is why the test is
// here and not only end-to-end: the two fixes are independent, and this one is
// the one that holds for ANY producer of a non-contributing batch.
func TestDecimalBatchKernelsKeepTheContributingScale(t *testing.T) {
	// Two rows at scale 2: unscaled 3825 and -1, i.e. 38.25 and -0.01.
	valued := batch.NewVectorWithScale(batch.TypeDecimal, 2, 2)
	valued.DecimalData.Data[0] = batch.Int128From(3825)
	valued.DecimalData.Data[1] = batch.Int128From(-1)
	valued.Nulls.SetValid(0)
	valued.Nulls.SetValid(1)

	// One all-NULL row at scale 0 — the identity row's shape.
	empty := batch.NewVectorWithScale(batch.TypeDecimal, 1, 0)
	empty.Nulls.SetNull(0)

	for _, tc := range []struct {
		name string
		kern BatchAggKernel
		want string
		read func(a *Accumulator) any
	}{
		{"sum", ResolveBatchSum(batch.TypeDecimal), "38.24", (*Accumulator).FinalSum},
		{"min", ResolveBatchMin(batch.TypeDecimal), "-0.01", (*Accumulator).FinalMin},
		{"max", ResolveBatchMax(batch.TypeDecimal), "38.25", (*Accumulator).FinalMax},
	} {
		// Both orders: the empty batch may reach the merge before or after the
		// ones that carry values, and the merge order across partial files is
		// not deterministic.
		for _, order := range []struct {
			name string
			vecs []*batch.Vector
		}{
			{"valued_then_empty", []*batch.Vector{valued, empty}},
			{"empty_then_valued", []*batch.Vector{empty, valued}},
			// And an empty batch on BOTH sides, which is the two-empty-tasks
			// shape a filter matching only one file produces.
			{"empty_valued_empty", []*batch.Vector{empty, valued, empty}},
		} {
			t.Run(tc.name+"_"+order.name, func(t *testing.T) {
				var acc Accumulator
				for _, v := range order.vecs {
					tc.kern(&acc, v, nil, v.Len)
				}
				got := tc.read(&acc)
				if got != tc.want {
					t.Errorf("%s = %#v, want %q — an all-NULL batch at a different scale "+
						"redefined the scale of an Int128 it never touched, so the right "+
						"integer renders as a different number (#685)", tc.name, got, tc.want)
				}
			})
		}
	}

	// AVG's reader too: it divides at batch.AvgScale(DecScale), so a
	// redefined DecScale moves the quotient's scale as well as its digits.
	for _, order := range []struct {
		name string
		vecs []*batch.Vector
	}{
		{"empty_then_valued", []*batch.Vector{empty, valued}},
		{"valued_then_empty", []*batch.Vector{valued, empty}},
	} {
		t.Run("avg_"+order.name, func(t *testing.T) {
			var acc Accumulator
			sum := ResolveBatchSum(batch.TypeDecimal)
			for _, v := range order.vecs {
				sum(&acc, v, nil, v.Len)
			}
			if got := acc.FinalAvg(); got != "19.120000" {
				t.Errorf("FinalAvg = %#v, want %q", got, "19.120000")
			}
		})
	}

	// The control: a genuinely scale-0 DECIMAL column still reports scale 0,
	// so the rule costs nothing where every batch agrees.
	t.Run("scale_zero_column", func(t *testing.T) {
		z := batch.NewVectorWithScale(batch.TypeDecimal, 1, 0)
		z.DecimalData.Data[0] = batch.Int128From(7)
		z.Nulls.SetValid(0)
		var acc Accumulator
		ResolveBatchSum(batch.TypeDecimal)(&acc, z, nil, z.Len)
		if got := acc.FinalSum(); got != "7" {
			t.Errorf("FinalSum = %#v, want %q", got, "7")
		}
	})
}
