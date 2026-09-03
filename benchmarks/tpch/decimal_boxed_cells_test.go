package tpch

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// The DECIMAL hot path is a RATCHET on boxed cells, not on ns/op.
//
// A computed DECIMAL cell had two possible routes. The exact one writes
// unscaled Int128 carriers straight into the output vector and allocates
// nothing. The other renders the value to decimal TEXT and parses it back —
// Int128 → FormatDecimal → FormatUint → an `any` box → SetComputedChecked →
// parquet.DecimalTextParts — four allocations per cell, between two exact
// kernels, for a conversion nothing asked for. pprof at SF0.01 put those
// four sites at 77.4% of every object the process allocated.
//
// The kernel existed. `expr.BinOpNumeric` implements `DecimalVecExpr` and
// the planner attached it in exactly ONE place, the SELECT-list projection
// builder — so every DECIMAL AGGREGATE INPUT took the box (#705), and so
// did every DECIMAL projection under a live selection vector (#825).
// Measured with a probe at SF0.01: Q01 120,000 boxed cells (2 computed
// columns x 60,000 rows — every row), Q06 1,236, Q15 2,482, Q11 480, and
// Q15 a further 1 through exec.Project's sel branch. At SF1 that is
// 48,026,572 allocs/op on Q01 against the FLOAT64 arm's 26,614: 1804x.
//
// The assertion is a boxed-CELL count and not an alloc count because the
// alloc number moves with everything else in the process; the cell count
// moves only when a value takes the wrong route. The recorded number is
// now ZERO for these four shapes, which makes the ratchet exact: any cell
// that takes the box again fails this gate and names the query.
//
// The BYTE ratio is deliberately NOT asserted. `batch.NewDecimalColumn` is
// 55% of allocated bytes on this arm and the DECIMAL/FLOAT64 byte ratio is
// ~2.0x, because the Int128 carrier is 16 bytes against float64's 8. That
// is ADR-0024's predicted, accepted cost, not a defect, and a gate that
// asserted on it would be asserting against the type system.
func TestDecimalComputedCellsTakeTheExactKernel(t *testing.T) {
	if testing.Short() {
		t.Skip("loads the DECIMAL fixture")
	}
	ctx := context.Background()
	data := decimalFixtureRows(t)
	db := ingestDecimalFixture(t, ctx, data)

	// The boxed-cell counts these shapes produced before the kernel was
	// attached, kept so a failure can say how far back it has slipped.
	cases := []struct {
		name     string
		sql      string
		wasBoxed int64  // what this shape boxed before the kernel was attached
		want     int64  // the ratchet: boxed cells that must remain
		why      string // required whenever want > 0
	}{
		{"q01", queryByName(t, "Q01"), 120000, 0, ""},
		{"q06", queryByName(t, "Q06"), 1236, 0, ""},
		{"q11", queryByName(t, "Q11"), 480, 0, ""},
		{
			"q15", queryByName(t, "Q15"), 2482, 1,
			// ONE cell survives, and it is not arithmetic. It is the
			// projection of `total_revenue` — a plain DECIMAL column
			// reference out of the revenue view — on a one-row batch whose
			// source index did not resolve, so it falls to the generic
			// per-row Expr route. No exact kernel exists for a column
			// reference and none is needed: the fix for that cell is a
			// direct gather, which is a projection-resolution question,
			// not a decimal-arithmetic one. Recorded rather than forced.
			"a column reference, not arithmetic: no vector kernel applies",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := exec.DecimalBoxedCells.Load()
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if len(res.Rows) == 0 {
				t.Fatalf("%s returned no rows — this cell would prove nothing", tc.name)
			}
			boxed := exec.DecimalBoxedCells.Load() - before
			if boxed > tc.want {
				t.Fatalf("%s wrote %d DECIMAL cells through the boxed text round trip "+
					"(four allocations each), want at most %d; the exact fixed-point "+
					"kernel must take them.\n"+
					"  Before the kernel was attached this shape boxed %d cells.",
					tc.name, boxed, tc.want, tc.wasBoxed)
			}
			if boxed < tc.want {
				t.Fatalf("%s now boxes %d DECIMAL cells, fewer than the recorded %d. "+
					"Set the constant to the new number (and delete the reason with it "+
					"when it reaches 0) — a stale ratchet stops measuring.",
					tc.name, boxed, tc.want)
			}
			if tc.want > 0 && tc.why == "" {
				t.Fatalf("%s tolerates %d boxed cells with no recorded reason", tc.name, tc.want)
			}
		})
	}
}

// queryByName returns the corpus SQL for a TPC-H query, so the gate reads
// the same text the correctness gate does rather than a copy that can drift.
func queryByName(t *testing.T, name string) string {
	t.Helper()
	for _, c := range decimalCorpus() {
		if c.name == name {
			return c.sql
		}
	}
	t.Fatalf("query %s is not in the decimal corpus", name)
	return ""
}

// BenchmarkDecimalComputedCells reports allocs/op for the two TPC-H shapes
// whose aggregate inputs are computed DECIMAL expressions. It is the
// -benchmem side of the ratchet above: the counter says WHICH route a cell
// took, this says what that route costs. Wall time on a shared box is
// meaningless; read allocs/op.
func BenchmarkDecimalComputedCells(b *testing.B) {
	ctx := context.Background()
	data := decimalFixtureRows(b)
	db := ingestDecimalFixture(b, ctx, data)
	for _, name := range []string{"Q01", "Q06"} {
		sql := benchQueryByName(b, name)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Query(ctx, sql); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchQueryByName(b *testing.B, name string) string {
	b.Helper()
	for _, c := range decimalCorpus() {
		if c.name == name {
			return c.sql
		}
	}
	b.Fatalf("query %s is not in the decimal corpus", name)
	return ""
}
