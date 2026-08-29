package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The morsel-parallel half of #685: a CLONE that consumed only NULLs must not
// give the primary its scale.
//
// exec runs a scalar aggregate across scanParallelism() clones and merges them
// (CloneSink / MergeSink -> mergeSinkState -> kernel.Accumulator.Merge). A
// clone whose morsel held no non-NULL value is carrying no DECIMAL at all, so
// it has no scale to contribute — but Merge adopted its DecScale
// unconditionally, and the batch kernels set IsDecimal from the COLUMN, so
// `other.IsDecimal` was true for a clone that had contributed nothing. The
// primary's scale went to whatever that clone happened to hold, and a right
// Int128 rendered as a different number: SUM over four 1.00 rows plus an
// all-NULL morsel answered 400.00.
//
// It is not specific to a scale-0 batch, which is why BOTH scales are here: an
// all-NULL clone at the column's OWN scale 2 poisoned the merge just the same,
// because a non-contributing accumulator's DecScale is 0 whatever its input
// vector said. That is the shape a plain two-file table reaches with no filter
// anywhere — no identity row, no DAG, no selectivity.
func TestScalarDecimalMergeIgnoresANonContributingClone(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true}}

	// valued: four rows of 1.00 (unscaled 100 at scale 2).
	valued := func() *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 4)
		for i := 0; i < 4; i++ {
			b.Columns[0].DecimalData.Data[i] = batch.Int128From(100)
			b.Columns[0].Nulls.SetValid(i)
		}
		return b
	}
	// allNull at a chosen vector scale: four NULL rows.
	allNull := func(scale int) *batch.RecordBatch {
		s := []parquet.Column{{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: scale, Nullable: true}}
		b := batch.NewRecordBatch(s, 4)
		for i := 0; i < 4; i++ {
			b.Columns[0].Nulls.SetNull(i)
		}
		return b
	}

	for _, tc := range []struct {
		name       string
		nullScale  int
		nullsFirst bool
	}{
		{"null clone at scale 0", 0, false},
		{"null clone at the column's scale", 2, false},
		{"null clone merged first", 0, true},
		{"null clone at the column's scale, merged first", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := NewHashAggregate(nil, []AggColumn{
				{Func: AggSum, InputCol: "a", OutputCol: "s",
					OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision, OutputScale: 2},
				{Func: AggMin, InputCol: "a", OutputCol: "lo",
					OutputType: parquet.TypeDecimal, OutputPrecision: 9, OutputScale: 2},
				{Func: AggMax, InputCol: "a", OutputCol: "hi",
					OutputType: parquet.TypeDecimal, OutputPrecision: 9, OutputScale: 2},
				{Func: AggAvg, InputCol: "a", OutputCol: "av",
					OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision,
					OutputScale: batch.AvgScale(2)},
				{Func: AggCount, InputCol: "a", OutputCol: "n", OutputType: parquet.TypeInt64},
			})
			if err := parent.Init(ctx); err != nil {
				t.Fatalf("parent init: %v", err)
			}

			valuedClone := parent.CloneSink().(*HashAggregate)
			nullClone := parent.CloneSink().(*HashAggregate)
			for _, c := range []*HashAggregate{valuedClone, nullClone} {
				if err := c.Init(ctx); err != nil {
					t.Fatalf("clone init: %v", err)
				}
			}
			if err := valuedClone.Consume(ctx, valued()); err != nil {
				t.Fatalf("valued clone consume: %v", err)
			}
			if err := nullClone.Consume(ctx, allNull(tc.nullScale)); err != nil {
				t.Fatalf("null clone consume: %v", err)
			}

			order := []*HashAggregate{valuedClone, nullClone}
			if tc.nullsFirst {
				order = []*HashAggregate{nullClone, valuedClone}
			}
			for _, c := range order {
				parent.MergeSink(c)
			}

			out, err := parent.Next(ctx)
			if err != nil {
				t.Fatalf("next: %v", err)
			}
			if out == nil {
				t.Fatal("no output batch")
			}
			rows := out.ToRows()
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			r := rows[0]
			for _, c := range []struct{ col, want string }{
				{"s", "4.00"}, {"lo", "1.00"}, {"hi", "1.00"}, {"av", "1.000000"},
			} {
				if got, _ := r[c.col].(string); got != c.want {
					t.Errorf("%s = %#v, want %q — a clone that contributed no DECIMAL value "+
						"gave the merge its scale (#685 review, F1)", c.col, r[c.col], c.want)
				}
			}
			if n, _ := r["n"].(int64); n != 4 {
				t.Errorf("n = %v, want 4", r["n"])
			}
		})
	}
}

// The soundness half of the same rule: two clones that BOTH contributed, at
// two different scales, is a merge whose Int128s are not counted in one scale.
// It must be refused, not resolved in whichever direction the merge order
// picks — 12.75 + 0.1275 is 25.50 under one reading and 0.2550 under the other,
// and neither is the answer.
//
// Nothing in the tree should be able to deliver such a pair (the planner
// reconciles set-operation arms, the shuffle writer refuses a cross-scale
// chunk, and the shuffle reader refuses a cross-scale stage input), so this
// pins the LAST door rather than a reachable query.
func TestScalarDecimalMergeRefusesTwoContributingScales(t *testing.T) {
	ctx := context.Background()
	at := func(scale int, unscaled int64) *batch.RecordBatch {
		s := []parquet.Column{{Name: "a", Type: parquet.TypeDecimal, Precision: 18, Scale: scale, Nullable: true}}
		b := batch.NewRecordBatch(s, 1)
		b.Columns[0].DecimalData.Data[0] = batch.Int128From(unscaled)
		b.Columns[0].Nulls.SetValid(0)
		return b
	}

	parent := NewHashAggregate(nil, []AggColumn{
		{Func: AggSum, InputCol: "a", OutputCol: "s",
			OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision, OutputScale: 2},
	})
	if err := parent.Init(ctx); err != nil {
		t.Fatalf("parent init: %v", err)
	}
	c1 := parent.CloneSink().(*HashAggregate)
	c2 := parent.CloneSink().(*HashAggregate)
	for _, c := range []*HashAggregate{c1, c2} {
		if err := c.Init(ctx); err != nil {
			t.Fatalf("clone init: %v", err)
		}
	}
	if err := c1.Consume(ctx, at(2, 1275)); err != nil { // 12.75
		t.Fatalf("consume: %v", err)
	}
	if err := c2.Consume(ctx, at(4, 1275)); err != nil { // 0.1275
		t.Fatalf("consume: %v", err)
	}
	parent.MergeSink(c1)
	parent.MergeSink(c2)

	out, err := parent.Next(ctx)
	if err == nil {
		var got any
		if out != nil && len(out.ToRows()) == 1 {
			got = out.ToRows()[0]["s"]
		}
		t.Fatalf("the merge ANSWERED %#v for two contributing scales; 12.75 + 0.1275 has no "+
			"reading under one scale, so this must be a 22003 refusal", got)
	}
	if !containsAll(err.Error(), "two different scales") {
		t.Errorf("refusal does not name the disagreement: %v", err)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
