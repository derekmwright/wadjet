package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// runSemiAntiNE builds a join over (k, v) rows and probes (k, v) rows,
// returning the emitted probe row ids. ne toggles the distinct-pair path
// vs the generic closure path — outputs must be identical.
func runSemiAntiNE(t *testing.T, jt JoinType, ne bool, keyType parquet.TypeID,
	buildRows, probeRows []map[string]any) []string {
	t.Helper()
	buildSchema := []parquet.Column{
		{Name: "k", Type: keyType},
		{Name: "v", Type: keyType},
	}
	probeSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "k", Type: keyType},
		{Name: "pv", Type: keyType},
	}
	hj := NewHashJoin(jt, []string{"k"}, []string{"k"})
	hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
	if ne {
		hj.SemiAntiNEProbeCol = "pv"
		hj.SemiAntiNEBuildCol = "v"
	}
	if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
		t.Fatal(err)
	}
	if ne && len(buildRows) > 0 && !hj.neActive {
		t.Fatal("NE path did not activate on an int-typed fixture")
	}
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(probeSchema, probeRows),
		Ops:    []UnaryOperator{hj.Probe()},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range sink.Rows {
		ids = append(ids, fmt.Sprint(r["id"]))
	}
	sort.Strings(ids)
	return ids
}

// BuildNETestFilter mirrors the production closure for `probe.pv <> build.v`
// with SQL three-valued semantics: NULL on either side → no match.
func BuildNETestFilter(probeCol, buildCol string) func(*batch.RecordBatch, int, *batch.RecordBatch, int) bool {
	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		pi := probe.ColumnIndex(probeCol)
		bi := build.ColumnIndex(buildCol)
		if pi < 0 || bi < 0 {
			return false
		}
		pc, bc := probe.Columns[pi], build.Columns[bi]
		if pc.Nulls.IsNull(probeRow) || bc.Nulls.IsNull(buildRow) {
			return false
		}
		pv, ok1 := intKeyFromVector(pc, probeRow)
		bv, ok2 := intKeyFromVector(bc, buildRow)
		if !ok1 || !ok2 {
			return false
		}
		return pv != bv
	}
}

func neVal(keyType parquet.TypeID, v int64) any {
	if keyType == parquet.TypeInt32 {
		return int32(v)
	}
	return v
}

// Differential test: the distinct-pair path must emit exactly the rows the
// generic closure path emits — semi and anti, int64 and int32, duplicate
// keys, single- vs multi-distinct values, NULL keys and NULL values on
// both sides.
func TestSemiAntiNEDifferential(t *testing.T) {
	for _, keyType := range []parquet.TypeID{parquet.TypeInt64, parquet.TypeInt32} {
		for _, jt := range []JoinType{SemiJoin, AntiJoin} {
			t.Run(fmt.Sprintf("%v_%v", keyType, jt), func(t *testing.T) {
				rng := rand.New(rand.NewSource(42))
				var buildRows []map[string]any
				for i := 0; i < 4000; i++ {
					k := int64(rng.Intn(300))
					v := int64(rng.Intn(4)) // few distinct values → 1-vs-2 pair coverage
					row := map[string]any{"k": neVal(keyType, k), "v": neVal(keyType, v)}
					if rng.Intn(50) == 0 {
						row["k"] = nil
					}
					if rng.Intn(50) == 0 {
						row["v"] = nil
					}
					buildRows = append(buildRows, row)
				}
				var probeRows []map[string]any
				for i := 0; i < 3000; i++ {
					k := int64(rng.Intn(400)) // includes keys absent from build
					v := int64(rng.Intn(4))
					row := map[string]any{"id": fmt.Sprintf("r%04d", i), "k": neVal(keyType, k), "pv": neVal(keyType, v)}
					if rng.Intn(60) == 0 {
						row["k"] = nil
					}
					if rng.Intn(60) == 0 {
						row["pv"] = nil
					}
					probeRows = append(probeRows, row)
				}
				want := runSemiAntiNE(t, jt, false, keyType, buildRows, probeRows)
				got := runSemiAntiNE(t, jt, true, keyType, buildRows, probeRows)
				if len(want) != len(got) {
					t.Fatalf("row count diverged: generic=%d ne=%d", len(want), len(got))
				}
				for i := range want {
					if want[i] != got[i] {
						t.Fatalf("row %d diverged: generic=%s ne=%s", i, want[i], got[i])
					}
				}
			})
		}
	}
}

// Single-distinct-value keys are the discriminating case: EXISTS depends on
// the probe value. Two-distinct keys always satisfy the NE for any probe.
func TestSemiAntiNESingleVsMulti(t *testing.T) {
	buildRows := []map[string]any{
		{"k": int64(1), "v": int64(10)},                                  // single distinct
		{"k": int64(1), "v": int64(10)},                                  // dup, still single
		{"k": int64(2), "v": int64(10)}, {"k": int64(2), "v": int64(20)}, // two distinct
	}
	probeRows := []map[string]any{
		{"id": "a", "k": int64(1), "pv": int64(10)}, // v1==pv, single → no other supplier
		{"id": "b", "k": int64(1), "pv": int64(99)}, // v1!=pv → exists
		{"id": "c", "k": int64(2), "pv": int64(10)}, // two distinct → exists
		{"id": "d", "k": int64(3), "pv": int64(10)}, // key absent
	}
	semi := runSemiAntiNE(t, SemiJoin, true, parquet.TypeInt64, buildRows, probeRows)
	if fmt.Sprint(semi) != "[b c]" {
		t.Fatalf("semi got %v want [b c]", semi)
	}
	anti := runSemiAntiNE(t, AntiJoin, true, parquet.TypeInt64, buildRows, probeRows)
	if fmt.Sprint(anti) != "[a d]" {
		t.Fatalf("anti got %v want [a d]", anti)
	}
}

// A non-int value column must leave the NE path inactive and fall back to
// the generic filtered build (correctness preserved).
func TestSemiAntiNEFallbackNonIntValue(t *testing.T) {
	buildSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString},
	}
	hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiNEProbeCol = "pv"
	hj.SemiAntiNEBuildCol = "v"
	hj.SemiAntiFilter = func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		return true
	}
	rows := []map[string]any{{"k": int64(1), "v": "x"}}
	if err := hj.Build(context.Background(), NewSliceSource(buildSchema, rows)); err != nil {
		t.Fatal(err)
	}
	if hj.neActive {
		t.Fatal("NE must not activate on a string value column")
	}
	if len(hj.buildBatches) == 0 {
		t.Fatal("fallback must store build batches for the closure path")
	}
}

// BenchmarkSemiAntiNEProbe compares the distinct-pair probe against the
// generic closure probe on the Q21 shape (many duplicate keys, few
// distinct values per key).
func BenchmarkSemiAntiNEProbe(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	var buildRows []map[string]any
	for i := 0; i < 200_000; i++ {
		buildRows = append(buildRows, map[string]any{
			"k": int64(rng.Intn(40_000)), "v": int64(rng.Intn(8)),
		})
	}
	var probeRows []map[string]any
	for i := 0; i < 50_000; i++ {
		probeRows = append(probeRows, map[string]any{
			"id": "x", "k": int64(rng.Intn(50_000)), "pv": int64(rng.Intn(8)),
		})
	}
	buildSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}}
	probeSchema := []parquet.Column{{Name: "id", Type: parquet.TypeString}, {Name: "k", Type: parquet.TypeInt64}, {Name: "pv", Type: parquet.TypeInt64}}
	for _, ne := range []bool{false, true} {
		name := "generic"
		if ne {
			name = "distinct-pair"
		}
		b.Run(name, func(b *testing.B) {
			hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
			hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
			if ne {
				hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
			}
			if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink := &CollectSink{}
				pipe := &Pipeline{Source: NewSliceSource(probeSchema, probeRows), Ops: []UnaryOperator{hj.Probe()}, Sink: sink}
				if err := pipe.Run(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSemiAntiNEBuild measures the build collapse: distinct-pair
// stores 24 B/key; the generic filtered build stores every batch + arena.
func BenchmarkSemiAntiNEBuild(b *testing.B) {
	rng := rand.New(rand.NewSource(9))
	var buildRows []map[string]any
	for i := 0; i < 500_000; i++ {
		buildRows = append(buildRows, map[string]any{
			"k": int64(rng.Intn(100_000)), "v": int64(rng.Intn(8)),
		})
	}
	buildSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}}
	for _, ne := range []bool{false, true} {
		name := "generic"
		if ne {
			name = "distinct-pair"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
				hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
				if ne {
					hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
				}
				if err := hj.Build(context.Background(), NewSliceSource(buildSchema, buildRows)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
