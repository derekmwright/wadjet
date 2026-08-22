package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Two-level index benchmarks (G6). Two levels of measurement:
//
//   - table level (BenchmarkIntIndexGrowth / BenchmarkPackedIndexGrowth):
//     the GROW phase and the STEADY-STATE probe are timed separately, which
//     is the claim under test — bucketing buys rehash locality and must not
//     cost probe throughput. "grow" starts at the minimum capacity and
//     inserts n distinct keys, paying every doubling; "presized" starts at
//     the final capacity and pays none; "probe" re-reads a filled table.
//   - aggregate level (BenchmarkHashAggregateHighCardTwoLevel and the
//     high-card arms of BenchmarkHashAggregatePackedNearUnique): the same
//     comparison through the real consume path.
//
// BenchmarkIntIndexConvertThreshold sweeps cardinality across the
// conversion threshold and is what the twoLevelConvertAt default is set
// from: the crossover point where bucketed growth starts beating flat.
// BenchmarkIntIndexOffheapSubGate is what offheapSubMinBytes is set from.
//
// Methodology note (this machine is noisy at the ±10% level): run each pair
// as ONE interleaved window and compare minima —
// `-benchtime 1x -count 5`. Consecutive whole-benchmark runs drift by more
// than the effect being measured.

// benchIntKeys returns n distinct int64 keys. scaled mimics a hashed id
// column (ClickBench WatchID); dense mimics a surrogate key, the family
// fibHash maps bijectively.
func benchIntKeys(n int, scaled bool) []int64 {
	keys := make([]int64, n)
	for i := range keys {
		if scaled {
			keys[i] = int64(i) * 2654435761
		} else {
			keys[i] = int64(i)
		}
	}
	return keys
}

func fillFlatInt(keys []int64, presize int) *intHashTable {
	t := newIntHashTable(presize)
	for i, k := range keys {
		t.GetOrInsertNoGrow(k, int32(i))
		t.CheckGrow()
	}
	return t
}

func fillTwoLevelInt(keys []int64, presize int) *intTwoLevelTable {
	t := newIntTwoLevelTable(presize, nil)
	for i, k := range keys {
		t.GetOrInsertAt(k, fibHash(k), int32(i))
	}
	return t
}

func BenchmarkIntIndexGrowth(b *testing.B) {
	for _, n := range []int{100_000, 1 << 20, 8 << 20} {
		keys := benchIntKeys(n, true)
		b.Run(fmt.Sprintf("n=%d/flat-grow", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillFlatInt(keys, 16)
			}
		})
		b.Run(fmt.Sprintf("n=%d/twolevel-grow", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillTwoLevelInt(keys, 16)
			}
		})
		b.Run(fmt.Sprintf("n=%d/flat-presized", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillFlatInt(keys, n)
			}
		})
		b.Run(fmt.Sprintf("n=%d/twolevel-presized", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillTwoLevelInt(keys, n)
			}
		})
		flat := fillFlatInt(keys, n)
		tl := fillTwoLevelInt(keys, n)
		b.Run(fmt.Sprintf("n=%d/flat-probe", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, k := range keys {
					if _, ok := flat.Get(k); !ok {
						b.Fatal("missing key")
					}
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/twolevel-probe", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, k := range keys {
					if _, ok := tl.Get(k); !ok {
						b.Fatal("missing key")
					}
				}
			}
		})
	}
}

// BenchmarkIntIndexConvertThreshold is the evidence behind
// twoLevelConvertAt and behind offheapSubMinBytes. It fills a group index
// from empty with n distinct keys the way a real sink does — flat, then
// converted at the threshold — against a flat index that never converts,
// and it does it off-heap (the production backing) so the flat arm keeps the
// huge-page-backed reservation it really has.
//
// Read it as ONE interleaved window (`-benchtime 1x -count 5`, compare the
// minima); the arms are ordered flat, bucketed per size so a -count run
// alternates them. The numbers this produced are in two_level_hash.go.
func BenchmarkIntIndexConvertThreshold(b *testing.B) {
	// Convert as early as the structure allows, so the sweep shows the
	// STRUCTURAL crossover (where bucketed growth starts beating flat
	// growth) rather than the effect of the shipped threshold. The shipped
	// default is then chosen conservatively below that crossover.
	prev := twoLevelConvertAt
	twoLevelConvertAt = 100_000
	defer func() { twoLevelConvertAt = prev }()
	for _, n := range []int{256 << 10, 512 << 10, 1 << 20, 2 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20} {
		keys := benchIntKeys(n, true)
		b.Run(fmt.Sprintf("n=%d/flat", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reg := memory.NewOffheapRegistry()
				t := newIntHashTableReg(4096, reg)
				for j, k := range keys {
					t.GetOrInsertNoGrow(k, int32(j))
					t.CheckGrow()
				}
				reg.Close()
			}
		})
		b.Run(fmt.Sprintf("n=%d/bucketed", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reg := memory.NewOffheapRegistry()
				fillAdaptiveInt(keys, reg)
				reg.Close()
			}
		})
	}
}

// fillAdaptiveInt reproduces the production fill: flat until the conversion
// point, bucketed after it. The threshold test runs once per BATCH, as
// consumeBatchIntGroup does — testing it per key would charge the adaptive
// arm ~10% that the real path never pays — and it uses the production gate
// (convertsToTwoLevel), so the conversion lands where a flat doubling was
// already due rather than at an arbitrary live count.
func fillAdaptiveInt(keys []int64, reg *memory.OffheapRegistry) int {
	flat := newIntHashTableReg(4096, reg)
	var tl *intTwoLevelTable
	for off := 0; off < len(keys); off += batch.DefaultBatchSize {
		end := off + batch.DefaultBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		if tl == nil && convertsToTwoLevel(flat.Len(), flat.Slots(), end-off) {
			tl = convertIntHashTableToTwoLevel(flat, reg)
		}
		if tl != nil {
			for j, k := range keys[off:end] {
				tl.GetOrInsertAt(k, fibHash(k), int32(off+j))
			}
			continue
		}
		for j, k := range keys[off:end] {
			flat.GetOrInsertNoGrow(k, int32(off+j))
			flat.CheckGrow()
		}
	}
	if tl == nil {
		return flat.Len()
	}
	return tl.Len()
}

// BenchmarkIntIndexThresholdChoice picks the SHIPPED twoLevelConvertAt:
// for a table that ends at n entries, how much does converting at each
// candidate threshold cost or save against never converting? Run it as one
// interleaved window and compare minima.
func BenchmarkIntIndexThresholdChoice(b *testing.B) {
	for _, n := range []int{2 << 20, 4 << 20, 8 << 20} {
		keys := benchIntKeys(n, true)
		b.Run(fmt.Sprintf("n=%d/flat", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reg := memory.NewOffheapRegistry()
				t := newIntHashTableReg(4096, reg)
				for j, k := range keys {
					t.GetOrInsertNoGrow(k, int32(j))
					t.CheckGrow()
				}
				reg.Close()
			}
		})
		for _, at := range []int{1 << 20, 2 << 20, 4 << 20} {
			b.Run(fmt.Sprintf("n=%d/convert-at=%d", n, at), func(b *testing.B) {
				prev := twoLevelConvertAt
				twoLevelConvertAt = at
				defer func() { twoLevelConvertAt = prev }()
				for i := 0; i < b.N; i++ {
					reg := memory.NewOffheapRegistry()
					fillAdaptiveInt(keys, reg)
					reg.Close()
				}
			})
		}
	}
}

// BenchmarkIntIndexOffheapSubGate sweeps offheapSubMinBytes, the huge-page
// gate: below 2 MiB a per-bucket mapping is 4 KiB-paged and the fault + dTLB
// cost swamps everything the structure buys.
func BenchmarkIntIndexOffheapSubGate(b *testing.B) {
	prevAt := twoLevelConvertAt
	twoLevelConvertAt = 100_000 // convert early so the gate is what varies
	defer func() { twoLevelConvertAt = prevAt }()
	for _, n := range []int{4 << 20, 8 << 20} {
		keys := benchIntKeys(n, true)
		for _, gate := range []int{4 << 10, 64 << 10, 2 << 20, 1 << 30 /* never */} {
			b.Run(fmt.Sprintf("n=%d/gate=%d", n, gate), func(b *testing.B) {
				prev := offheapSubMinBytes
				offheapSubMinBytes = gate
				defer func() { offheapSubMinBytes = prev }()
				for i := 0; i < b.N; i++ {
					reg := memory.NewOffheapRegistry()
					fillAdaptiveInt(keys, reg)
					reg.Close()
				}
			})
		}
	}
}

func BenchmarkPackedIndexGrowth(b *testing.B) {
	for _, n := range []int{100_000, 1 << 20, 8 << 20} {
		lo := make([]uint64, n)
		hi := make([]uint64, n)
		for i := range lo {
			lo[i] = uint64(i) * 2654435761
			hi[i] = uint64(i)
		}
		fillFlat := func(presize int) *packedHashTable {
			t := newPackedHashTable(presize)
			for i := range lo {
				t.GetOrInsertNoGrow(lo[i], hi[i], int32(i))
				t.CheckGrow()
			}
			return t
		}
		fillTL := func(presize int) *packedTwoLevelTable {
			t := newPackedTwoLevelTable(presize, nil)
			for i := range lo {
				t.GetOrInsertAt(lo[i], hi[i], packedHash(lo[i], hi[i]), int32(i))
			}
			return t
		}
		b.Run(fmt.Sprintf("n=%d/flat-grow", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillFlat(16)
			}
		})
		b.Run(fmt.Sprintf("n=%d/twolevel-grow", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillTL(16)
			}
		})
		b.Run(fmt.Sprintf("n=%d/flat-presized", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillFlat(n)
			}
		})
		b.Run(fmt.Sprintf("n=%d/twolevel-presized", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fillTL(n)
			}
		})
		flat := fillFlat(n)
		tl := fillTL(n)
		b.Run(fmt.Sprintf("n=%d/flat-probe", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := range lo {
					if _, ok := flat.Get(lo[j], hi[j]); !ok {
						b.Fatal("missing key")
					}
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/twolevel-probe", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := range lo {
					if _, ok := tl.Get(lo[j], hi[j]); !ok {
						b.Fatal("missing key")
					}
				}
			}
		})
	}
}

// BenchmarkHashAggregateHighCardTwoLevel drives the whole consume path at
// near-unique cardinalities above the conversion threshold, flat vs
// bucketed, for the single-int and packed key modes.
func BenchmarkHashAggregateHighCardTwoLevel(b *testing.B) {
	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
			{Func: AggAvg, InputCol: "w", OutputCol: "a", OutputType: parquet.TypeFloat64},
		}
	}
	const rowsPerBatch = 2048

	for _, groups := range []int{1 << 20, 8 << 20} {
		intSchema := []parquet.Column{
			{Name: "k", Type: parquet.TypeInt64},
			{Name: "v", Type: parquet.TypeInt64},
			{Name: "w", Type: parquet.TypeInt64},
		}
		packSchema := []parquet.Column{
			{Name: "k", Type: parquet.TypeInt64},
			{Name: "k2", Type: parquet.TypeInt64},
			{Name: "v", Type: parquet.TypeInt64},
			{Name: "w", Type: parquet.TypeInt64},
		}
		nBatches := groups / rowsPerBatch
		intBatches := make([]*batch.RecordBatch, nBatches)
		packBatches := make([]*batch.RecordBatch, nBatches)
		for bi := 0; bi < nBatches; bi++ {
			ib := batch.NewRecordBatch(intSchema, rowsPerBatch)
			pb := batch.NewRecordBatch(packSchema, rowsPerBatch)
			for i := 0; i < rowsPerBatch; i++ {
				row := int64(bi*rowsPerBatch + i)
				ib.Columns[0].SetValue(i, row*2654435761)
				ib.Columns[1].SetValue(i, row&1)
				ib.Columns[2].SetValue(i, 1024+row%512)
				pb.Columns[0].SetValue(i, row*2654435761)
				pb.Columns[1].SetValue(i, row)
				pb.Columns[2].SetValue(i, row&1)
				pb.Columns[3].SetValue(i, 1024+row%512)
			}
			intBatches[bi] = ib
			packBatches[bi] = pb
		}

		run := func(b *testing.B, cols []string, batches []*batch.RecordBatch, on bool) {
			b.Helper()
			prev := twoLevelToggle.Set(on)
			defer twoLevelToggle.Set(prev)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				agg := NewHashAggregate(cols, aggs())
				if err := agg.Init(ctx); err != nil {
					b.Fatal(err)
				}
				for _, bb := range batches {
					if err := agg.Consume(ctx, bb); err != nil {
						b.Fatal(err)
					}
				}
				if agg.numIntGroups != len(batches)*rowsPerBatch {
					b.Fatalf("groups = %d, want %d", agg.numIntGroups, len(batches)*rowsPerBatch)
				}
				agg.Close()
			}
		}
		b.Run(fmt.Sprintf("single-int/groups=%d/flat", groups), func(b *testing.B) {
			run(b, []string{"k"}, intBatches, false)
		})
		b.Run(fmt.Sprintf("single-int/groups=%d/twolevel", groups), func(b *testing.B) {
			run(b, []string{"k"}, intBatches, true)
		})
		b.Run(fmt.Sprintf("packed-two-int64/groups=%d/flat", groups), func(b *testing.B) {
			run(b, []string{"k", "k2"}, packBatches, false)
		})
		b.Run(fmt.Sprintf("packed-two-int64/groups=%d/twolevel", groups), func(b *testing.B) {
			run(b, []string{"k", "k2"}, packBatches, true)
		})
	}
}
