// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A SEMI/ANTI RESIDUAL WITH A NULL OPERAND DOES NOT MATCH (#1681, 1681a).
// buildSemiAntiFilter's closure is the residual of every decorrelated EXISTS /
// NOT EXISTS with a non-equality correlated comparison, on the embedded engine
// and on a DAG worker alike. A comparison with a NULL operand is UNKNOWN, and
// the residual must be TRUE for the pair to match. The typed kernels read the
// slot without the bitmap, so a NULL slot compared as its zero value: each
// "base" case below matched before the fix (the zero value satisfies the
// comparison), and the generic kernel compared the text "<nil>".
func TestSemiAntiFilterNullOperandIsUnknown(t *testing.T) {
	families := []struct {
		name    string
		typ     parquet.TypeID
		lo, hi  any
		kernels string
	}{
		{"int32", parquet.TypeInt32, int32(1), int32(5), "int32"},
		{"date", parquet.TypeDate, int32(1), int32(5), "int32"},
		{"int64", parquet.TypeInt64, int64(1), int64(5), "int64"},
		{"timestamp", parquet.TypeTimestamp, int64(1), int64(5), "int64"},
		{"float64", parquet.TypeFloat64, float64(1), float64(5), "float64"},
		{"float32", parquet.TypeFloat32, float32(1), float32(5), "float32"},
		{"string", parquet.TypeString, "b", "e", "string"},
		{"bool", parquet.TypeBool, false, true, "generic"},
	}
	cases := []struct {
		name         string
		filter       string
		probe, build func(lo, hi any) any
		want         bool
	}{
		// The zero slot is below hi: 0 < 5, "" < "e", "<nil>" < "true".
		{"probeNull", "a < b", func(_, _ any) any { return nil }, func(_, hi any) any { return hi }, false},
		// hi is above the zero slot.
		{"buildNull", "a > b", func(_, hi any) any { return hi }, func(_, _ any) any { return nil }, false},
		// Two zero slots are equal.
		{"bothNull", "a <= b", func(_, _ any) any { return nil }, func(_, _ any) any { return nil }, false},
		// A NULL in the second conjunct fails the pair although the first holds.
		{"secondConjunctNull", "a < b AND c < d", func(lo, _ any) any { return lo }, func(_, hi any) any { return hi }, false},
		// Controls: no NULL.
		{"lessThan", "a < b", func(lo, _ any) any { return lo }, func(_, hi any) any { return hi }, true},
		{"notGreater", "a > b", func(lo, _ any) any { return lo }, func(_, hi any) any { return hi }, false},
		{"notEqual", "a != b", func(lo, _ any) any { return lo }, func(_, hi any) any { return hi }, true},
	}
	for _, fam := range families {
		for _, tc := range cases {
			t.Run(fam.name+"/"+tc.name, func(t *testing.T) {
				col := func(n string) parquet.Column { return parquet.Column{Name: n, Type: fam.typ, Nullable: true} }
				probe := batch.NewRecordBatch([]parquet.Column{col("a"), col("c")}, 1)
				build := batch.NewRecordBatch([]parquet.Column{col("b"), col("d")}, 1)
				probe.Columns[0].SetValue(0, tc.probe(fam.lo, fam.hi))
				build.Columns[0].SetValue(0, tc.build(fam.lo, fam.hi))
				// c < d: c is NULL on the probe side, d is hi (the zero
				// slot is below it).
				probe.Columns[1].SetValue(0, nil)
				build.Columns[1].SetValue(0, fam.hi)
				f, err := buildSemiAntiFilter(tc.filter)
				if err != nil {
					t.Fatal(err)
				}
				if f == nil {
					t.Fatalf("filter %q compiled to nothing", tc.filter)
				}
				if got := f(probe, 0, build, 0); got != tc.want {
					t.Errorf("%s kernel, filter %q, probe %v build %v: matched=%v, want %v",
						fam.kernels, tc.filter, probe.Columns[0].GetValue(0), build.Columns[0].GetValue(0), got, tc.want)
				}
			})
		}
	}
}

// A VIEW operand reads its nullness as GetValue does: its own bit, then its
// base's through Indices.
func TestFilterOperandNullReadsAView(t *testing.T) {
	base := batch.NewVector(batch.TypeInt64, 3)
	base.Int64Data[0], base.Int64Data[2] = 7, 9
	base.Nulls.SetNull(1)
	view := &batch.Vector{Type: batch.TypeInt64, Len: 3, Nulls: batch.NewBitmap(3), Base: base, Indices: []uint32{1, 0, 2}}
	view.Nulls.SetNull(2)
	for row, want := range []bool{true, false, true} {
		if got := filterOperandNull(view, row); got != want {
			t.Errorf("view row %d: null=%v, want %v", row, got, want)
		}
	}
}

// semiAntiBenchRows is the Q21-like shape: many duplicate build keys, a small
// value domain, 1 in 16 values NULL on each side.
func semiAntiBenchRows() (build, probe []map[string]any) {
	rng := rand.New(rand.NewSource(7))
	val := func() any {
		if rng.Intn(16) == 0 {
			return nil
		}
		return int64(rng.Intn(8))
	}
	for i := 0; i < 200_000; i++ {
		build = append(build, map[string]any{"k": int64(rng.Intn(40_000)), "v": val()})
	}
	for i := 0; i < 50_000; i++ {
		probe = append(probe, map[string]any{"id": int64(i), "k": int64(rng.Intn(50_000)), "pv": val()})
	}
	return build, probe
}

// BenchmarkSemiAntiFilterProbe runs a semi / anti join whose residual is the
// production closure (buildSemiAntiFilter, `pv < v` — not the `<>` the
// distinct-pair path takes), so the probe calls it on key-chain pairs.
func BenchmarkSemiAntiFilterProbe(b *testing.B) {
	buildRows, probeRows := semiAntiBenchRows()
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	buildSchema := []parquet.Column{i64("k"), i64("v")}
	probeSchema := []parquet.Column{i64("id"), i64("k"), i64("pv")}
	for _, jt := range []exec.JoinType{exec.SemiJoin, exec.AntiJoin} {
		name := map[exec.JoinType]string{exec.SemiJoin: "semi", exec.AntiJoin: "anti"}[jt]
		b.Run(name, func(b *testing.B) {
			hj := exec.NewHashJoin(jt, []string{"k"}, []string{"k"})
			hj.SemiAntiFilter, _ = buildSemiAntiFilter("pv < v")
			if err := hj.Build(context.Background(), exec.NewSliceSource(buildSchema, buildRows)); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink := &exec.CollectSink{}
				pipe := &exec.Pipeline{Source: exec.NewSliceSource(probeSchema, probeRows), Ops: []exec.UnaryOperator{hj.Probe()}, Sink: sink}
				if err := pipe.Run(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSemiAntiFilterClosure calls the closure alone over a batch of
// pairs: the per-pair cost of the NULL test, with nothing around it.
func BenchmarkSemiAntiFilterClosure(b *testing.B) {
	const n = 2048
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	probe := batch.NewRecordBatch([]parquet.Column{i64("pv")}, n)
	build := batch.NewRecordBatch([]parquet.Column{i64("v")}, n)
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < n; i++ {
		probe.Columns[0].SetValue(i, int64(rng.Intn(8)))
		build.Columns[0].SetValue(i, int64(rng.Intn(8)))
	}
	f, _ := buildSemiAntiFilter("pv < v")
	b.ResetTimer()
	matched := 0
	for i := 0; i < b.N; i++ {
		for r := 0; r < n; r++ {
			if f(probe, r, build, n-1-r) {
				matched++
			}
		}
	}
	if matched < 0 {
		b.Fatal(matched)
	}
}
