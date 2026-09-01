package exec

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// migrationSpecBatch builds one batch of (nullable INT32 key, DECIMAL value,
// FLOAT64 value). A key row is NULL when keys[i] < 0 — the NULL key is what
// migrates the int-keyed fast path to the generic map, which is the condition
// this file's gates are about.
func migrationSpecBatch(t *testing.T, keys []int32, decs []*big.Int, f64s []float64, scale int) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: scale, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		// The NEGATED twins. Which half of the loss is observable depends on
		// the SIGN of the data: a lost accumulator reads back 0, and 0 WINS the
		// merge as the maximum of an all-negative column exactly as it wins as
		// the minimum of an all-positive one. With only non-negative values —
		// which is all typematrix has — the lost-MAX half is invisible.
		{Name: "nd", Type: parquet.TypeDecimal, Precision: 38, Scale: scale, Nullable: true},
		{Name: "nf", Type: parquet.TypeFloat64, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, len(keys))
	for i := range keys {
		if keys[i] >= 0 {
			b.Columns[0].Int32Data[i] = keys[i]
			b.Columns[0].Nulls.SetValid(i)
		} else {
			b.Columns[0].Nulls.SetNull(i)
		}
		b.Columns[1].DecimalData.Data[i] = decSpillInt128(decs[i])
		b.Columns[1].Nulls.SetValid(i)
		b.Columns[2].Float64Data[i] = f64s[i]
		b.Columns[2].Nulls.SetValid(i)
		b.Columns[3].DecimalData.Data[i] = decSpillInt128(new(big.Int).Neg(decs[i]))
		b.Columns[3].Nulls.SetValid(i)
		b.Columns[4].Float64Data[i] = -f64s[i] - 1
		b.Columns[4].Nulls.SetValid(i)
	}
	b.Len = len(keys)
	return b
}

// A drain that lands on the batch that migrated the key path must still write
// DECIMAL and FLOAT accumulators as DECIMAL and FLOAT (#782, second symptom).
//
// Mechanism. buildPartialAggSpecs read IsFloat/IsDecimal/DecScale out of
// h.intFlatAccs, the SoA flat arrays. A NULL group key migrates the int-keyed
// path to the generic map, and migrateToGenericMap -> materializeFlatAccums
// NILS intFlatAccs on the way. Consume checks canUseExternalMerge BEFORE
// consuming the batch, so the batch that migrates can still take the
// external-merge drain afterwards — and by then the specs come back
// IsDecimal=false, so emitAcc writes the (never populated) SumI64 int field
// instead of the Int128, readAcc puts a zero back, and the whole run's
// contribution to every group in it disappears. The run file is well formed
// and the query answers a plausible smaller number.
//
// Determinism. Under a memory budget alone, whether a drain lands on the
// migrating batch follows tracker timing: the same query answered right three
// times out of five. ForceAggDrainEvery(1) puts a drain on every Consume, so
// the drain on the migrating batch is certain.
//
// Reverting the fix (buildPartialAggSpecs reading intFlatAccs alone) fails this
// with s = 0 for the groups whose rows were in the drained run.
func TestDrainOnTheMigratingBatchKeepsDecimalAndFloatSpecs(t *testing.T) {
	const rowsPerBatch, numGroups = 64, 5
	step, _ := new(big.Int).SetString("1234567890123456789", 10)
	mkBatches := func() []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for bi := 0; bi < 6; bi++ {
			keys := make([]int32, rowsPerBatch)
			decs := make([]*big.Int, rowsPerBatch)
			f64s := make([]float64, rowsPerBatch)
			for ri := 0; ri < rowsPerBatch; ri++ {
				idx := bi*rowsPerBatch + ri
				keys[ri] = int32(idx % numGroups)
				// Batch 1 introduces the NULL key: the migration lands on a
				// batch that already carries state from batch 0, so the
				// drain that follows it has something to lose.
				if bi == 1 && ri == 3 {
					keys[ri] = -1
				}
				decs[ri] = new(big.Int).Mul(step, big.NewInt(int64(idx+1)))
				f64s[ri] = float64(idx) * 1.5
			}
			out = append(out, migrationSpecBatch(t, keys, decs, f64s, 6))
		}
		return out
	}
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "d", OutputCol: "s_dec", OutputType: parquet.TypeDecimal},
			{Func: AggAvg, InputCol: "d", OutputCol: "a_dec", OutputType: parquet.TypeDecimal},
			{Func: AggMin, InputCol: "d", OutputCol: "lo_dec", OutputType: parquet.TypeDecimal},
			{Func: AggMax, InputCol: "d", OutputCol: "hi_dec", OutputType: parquet.TypeDecimal},
			{Func: AggSum, InputCol: "f", OutputCol: "s_f64", OutputType: parquet.TypeFloat64},
			{Func: AggMax, InputCol: "f", OutputCol: "hi_f64", OutputType: parquet.TypeFloat64},
			{Func: AggMin, InputCol: "nd", OutputCol: "lo_ndec", OutputType: parquet.TypeDecimal},
			{Func: AggMax, InputCol: "nd", OutputCol: "hi_ndec", OutputType: parquet.TypeDecimal},
			{Func: AggSum, InputCol: "nd", OutputCol: "s_ndec", OutputType: parquet.TypeDecimal},
			{Func: AggMin, InputCol: "nf", OutputCol: "lo_nf64", OutputType: parquet.TypeFloat64},
			{Func: AggMax, InputCol: "nf", OutputCol: "hi_nf64", OutputType: parquet.TypeFloat64},
			{Func: AggCount, InputCol: "d", OutputCol: "n", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		return h
	}

	ref := aggRowsByInt32Key(t, runHashAggToMap(t, mk(nil), mkBatches()))

	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	before := ForcedAggDrains.Load()
	restore := ForceAggDrainEvery(1)
	defer ForceAggDrainEvery(restore)
	h := mk(sm)
	got := aggRowsByInt32Key(t, runHashAggToMap(t, h, mkBatches()))
	if ForcedAggDrains.Load() == before {
		t.Fatal("no forced drain fired: the knob did not engage, so this gate proves nothing")
	}

	if len(ref) != numGroups+1 || len(got) != numGroups+1 {
		t.Fatalf("groups: reference=%d drained=%d, want %d (%d keys plus NULL)", len(ref), len(got), numGroups+1, numGroups)
	}
	for k, want := range ref {
		have, ok := got[k]
		if !ok {
			t.Errorf("key %v: missing from the drained run", k)
			continue
		}
		for _, col := range []string{"s_dec", "a_dec", "lo_dec", "hi_dec", "s_f64", "hi_f64",
			"lo_ndec", "hi_ndec", "s_ndec", "lo_nf64", "hi_nf64", "n"} {
			if gotV, wantV := fmtAggCell(have[col]), fmtAggCell(want[col]); gotV != wantV {
				t.Errorf("key %v col %s: drained %s, want %s", k, col, gotV, wantV)
			}
		}
	}
}

// aggRowsByInt32Key indexes aggregate output rows by their "k" group key,
// with the NULL key under the nil interface.
func aggRowsByInt32Key(t *testing.T, rows []map[string]any) map[any]map[string]any {
	t.Helper()
	out := make(map[any]map[string]any, len(rows))
	for _, r := range rows {
		k := r["k"]
		if _, dup := out[k]; dup {
			t.Fatalf("group key %v emitted twice", k)
		}
		out[k] = r
	}
	return out
}

// fmtAggCell renders an aggregate output cell for comparison. A DECIMAL boxes
// as its own type, so this compares the value's own text and never rounds one
// through float64.
func fmtAggCell(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}
