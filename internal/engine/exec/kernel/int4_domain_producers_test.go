package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// EVERY SUM/AVG PRODUCER ANSWERS FOR PORT AND PROTOCOL, AND ANSWERS THE SAME
// NUMBER AS INT32 (#953, round-1 blocker B1).
//
// PORT and PROTOCOL are int4-domain values in an int32 array and declare
// `integer` on the wire (#834), so every producer that sums an INT32 must sum
// them — and sum them identically. `TypeProtocol` was missing from four of
// them, which is why the GROUPED `SUM(c_proto)` answered NULL while the
// windowed spelling answered the total.
//
// Round 1 found that the QUERY-level census could not defend all of it:
// reverting `ResolveBatchSum`'s arm, or the exact-carrier arms, changes no
// answer because a NIL kernel makes HashAggregate fall back to another
// producer that is now also correct. Redundancy is not coverage — the day the
// fallback changes, a nil arm is a wrong answer — so the resolvers are
// asserted HERE, one per producer, where a nil is visible on its own. No
// plan, no budget, no cluster: the seam gate's rule.
func TestEverySumProducerAnswersForTheInt4DomainTypes(t *testing.T) {
	// Values a PORT and a PROTOCOL can both hold, with a NULL row: SQL
	// excludes it from the sum AND from AVG's divisor.
	vals := []int32{80, 443, 6, 17, 0}
	const wantSum = int64(546)
	const wantCount = int64(5)

	col := func(typ parquet.TypeID) (*batch.Vector, parquet.Column) {
		c := parquet.Column{Name: "v", Type: typ, Nullable: true}
		v := batch.NewColumnVector(c, len(vals)+1)
		for i, x := range vals {
			v.SetValue(i, x)
		}
		v.WriteNullAt(len(vals))
		return v, c
	}

	for _, typ := range []parquet.TypeID{parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol} {
		t.Run(typ.String(), func(t *testing.T) {
			v, _ := col(typ)
			n := len(vals) + 1

			// Producer 1: the ROW updater — HashAggregate's generic map path
			// and the fallback whenever a batch kernel declines.
			for _, nn := range []bool{false, true} {
				u := ResolveRowSum(typ)
				if nn {
					u = ResolveRowSumNoNulls(typ)
				}
				if u == nil {
					t.Fatalf("ResolveRowSum(noNulls=%v) is nil — this column cannot be summed at all", nn)
				}
				var acc Accumulator
				for r := 0; r < len(vals); r++ { // the NoNulls variant may not test
					u(&acc, v, r)
				}
				if acc.SumI64 != wantSum || acc.Count != wantCount {
					t.Errorf("row updater (noNulls=%v): sum=%d count=%d, want %d/%d",
						nn, acc.SumI64, acc.Count, wantSum, wantCount)
				}
			}

			// Producer 2: the BATCH kernel — the ungrouped scalar fast path.
			bk := ResolveBatchSum(typ)
			if bk == nil {
				t.Fatal("ResolveBatchSum is nil — the scalar fast path declines this column")
			}
			var bacc Accumulator
			bk(&bacc, v, nil, n)
			if bacc.SumI64 != wantSum || bacc.Count != wantCount {
				t.Errorf("batch kernel: sum=%d count=%d, want %d/%d",
					bacc.SumI64, bacc.Count, wantSum, wantCount)
			}

			// Producer 3: the AVG resolvers, which delegate to SUM for this
			// class and would be nil with it.
			if ResolveRowAvg(typ) == nil || ResolveRowAvgNoNulls(typ) == nil || ResolveBatchAvg(typ) == nil {
				t.Error("an AVG resolver is nil for a type whose SUM resolvers answer")
			}

			// Producer 4: the EXACT Int128 arms, which AVG(int4) takes
			// because its declaration is numeric (#784). A nil here does not
			// change an answer today — HashAggregate falls back to producer 1
			// — and that is exactly why it is asserted directly.
			re := ResolveRowSumIntExact(typ, false)
			if re == nil {
				t.Fatal("ResolveRowSumIntExact is nil — the exact carrier declines this column")
			}
			var eacc Accumulator
			for r := 0; r < n; r++ {
				re(&eacc, v, r)
			}
			if !eacc.SumDec.Equal(batch.Int128From(wantSum)) || eacc.Count != wantCount {
				t.Errorf("exact row updater: sum=%s count=%d, want %d/%d",
					eacc.SumDec.String(), eacc.Count, wantSum, wantCount)
			}
			be := ResolveBatchSumIntExact(typ)
			if be == nil {
				t.Fatal("ResolveBatchSumIntExact is nil — the exact scalar path declines this column")
			}
			var beacc Accumulator
			be(&beacc, v, nil, n)
			if !beacc.SumDec.Equal(batch.Int128From(wantSum)) || beacc.Count != wantCount {
				t.Errorf("exact batch kernel: sum=%s count=%d, want %d/%d",
					beacc.SumDec.String(), beacc.Count, wantSum, wantCount)
			}
		})
	}

	// The BOUNDARY, from the other side: DATE, TIMESTAMP and DURATION are
	// int-backed too and are deliberately NOT in the exact-carrier set —
	// PostgreSQL has no `sum(date)`/`sum(timestamp)` and an interval's sum is
	// an interval. They keep their ordinary resolvers and decline the exact
	// ones, which is what makes their float64 declaration consistent.
	for _, typ := range []parquet.TypeID{parquet.TypeDate, parquet.TypeTimestamp, parquet.TypeDuration} {
		t.Run("boundary/"+typ.String(), func(t *testing.T) {
			if ResolveRowSum(typ) == nil {
				t.Error("the ordinary row updater declines a type that used to sum")
			}
			if ResolveRowSumIntExact(typ, false) != nil || ResolveBatchSumIntExact(typ) != nil {
				t.Error("an int-backed wadjet type joined the exact carrier — " +
					"exec.IntegerAccOutputType does not name it, so its declaration " +
					"and its carrier would disagree")
			}
		})
	}
}
