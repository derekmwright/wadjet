package kernel

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// The float predicate kernels are the hot path this whole file exists to
// protect: every `WHERE f < c` in TPC-H and ClickBench runs one of these over
// 2048 rows at a time. PostgreSQL's float order costs at most ONE extra
// self-inequality test, and only on `>` and `>=` (see resolveFloatConstPred) —
// the other four kernels are byte-for-byte what they were.
//
// Run interleaved against the parent commit:
//
//	go test -run '^$' -bench 'FloatFilterKernel|FloatColCol' -benchmem -count=8 ./internal/engine/exec/kernel

func benchFloat64Vec(n int) *batch.Vector {
	v := batch.NewVector(batch.TypeFloat64, n)
	for i := 0; i < n; i++ {
		v.Float64Data[i] = float64(i) * 1.5
	}
	return v
}

func benchFloat32Vec(n int) *batch.Vector {
	v := batch.NewVector(batch.TypeFloat32, n)
	for i := 0; i < n; i++ {
		v.Float32Data[i] = float32(i) * 1.5
	}
	return v
}

var floatBenchOps = []struct {
	name string
	op   CompareOp
}{
	{"Eq", OpEq}, {"Ne", OpNe}, {"Lt", OpLt}, {"Le", OpLe}, {"Gt", OpGt}, {"Ge", OpGe},
}

func BenchmarkFloatFilterKernel64(b *testing.B) {
	vec := benchFloat64Vec(2048)
	outSel := make([]uint32, 0, 2048)
	for _, tc := range floatBenchOps {
		b.Run(tc.name, func(b *testing.B) {
			k := ResolveFilterKernel(batch.TypeFloat64, tc.op, float64(1000))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k(vec, nil, 2048, outSel)
			}
		})
	}
}

func BenchmarkFloatFilterKernel32(b *testing.B) {
	vec := benchFloat32Vec(2048)
	outSel := make([]uint32, 0, 2048)
	for _, tc := range floatBenchOps {
		b.Run(tc.name, func(b *testing.B) {
			k := ResolveFilterKernel(batch.TypeFloat32, tc.op, float64(1000))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k(vec, nil, 2048, outSel)
			}
		})
	}
}

func BenchmarkFloatColColKernel(b *testing.B) {
	left := benchFloat64Vec(2048)
	right := benchFloat64Vec(2048)
	for i := range right.Float64Data {
		right.Float64Data[i] = float64(2047-i) * 1.5
	}
	outSel := make([]uint32, 0, 2048)
	for _, tc := range floatBenchOps {
		b.Run(tc.name, func(b *testing.B) {
			k := ResolveColColFilterKernel(batch.TypeFloat64, tc.op)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k(left, right, nil, 2048, outSel)
			}
		})
	}
}

func BenchmarkFloatInFilterKernel(b *testing.B) {
	vec := benchFloat64Vec(2048)
	outSel := make([]uint32, 0, 2048)
	list := []any{1.5, 300.0, 1000.5, 3000.0}
	b.Run("NoNaN", func(b *testing.B) {
		k := ResolveInFilterKernel(batch.TypeFloat64, list, false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k(vec, nil, 2048, outSel)
		}
	})
	b.Run("WithNaN", func(b *testing.B) {
		k := ResolveInFilterKernel(batch.TypeFloat64, append(append([]any{}, list...), math.NaN()), false)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k(vec, nil, 2048, outSel)
		}
	})
}

// BenchmarkKeyFloatBits is the key-serializer half: one branch on top of
// Float64bits, taken on every float group / join / bloom / shuffle key.
func BenchmarkKeyFloatBits(b *testing.B) {
	data := benchFloat64Vec(2048).Float64Data
	b.ReportAllocs()
	b.ResetTimer()
	var acc uint64
	for i := 0; i < b.N; i++ {
		for _, f := range data {
			acc ^= KeyFloat64Bits(f)
		}
	}
	_ = acc
}
