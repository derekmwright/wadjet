package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// The DECIMAL batch kernels' baseline.
//
// It exists because ADR-0024 records that nothing in the tree measures DECIMAL
// at all — the TPC-H schema declares its monetary columns FLOAT64 — so a change
// to these three loops had no number to be held to.
//
// Two null densities on purpose. #685 makes the kernels ask "did this batch
// contribute a value", and the honest place to measure that is the ALL-NULL
// batch: it is the shape the question exists for, and it is where a first
// attempt that answered it with a SEPARATE bitmap pass (rather than a flag set
// in the guard body the loop already runs) paid for the scan twice. The shipped
// form adds one register store per contributing row to MIN/MAX and one int64
// compare after the SUM loop — the inner loops are otherwise byte-identical to
// their parents.
//
// What these benchmarks did NOT establish, recorded so nobody reads a claim
// into their absence: on an unpinned developer box the run-to-run spread within
// ONE arm (SUM 3704–4238 ns/op across ten samples) is wider than the gap
// between the arms, in both directions and on kernels whose loops did not
// change. The earlier claim of a 20% MIN/MAX win in this file was that noise
// read as signal. A real number for these needs a quiet, pinned machine.
// benchDecimalVec builds a full batch at scale 4. allNull makes every row NULL;
// otherwise one row in seventeen is.
func benchDecimalVec(tb testing.TB, allNull bool) *batch.Vector {
	tb.Helper()
	v := batch.NewVectorWithScale(batch.TypeDecimal, batch.DefaultBatchSize, 4)
	for i := 0; i < batch.DefaultBatchSize; i++ {
		v.DecimalData.Data[i] = batch.Int128From(int64(i)*1234567 - 1000)
		if allNull || i%17 == 0 {
			v.Nulls.SetNull(i)
		} else {
			v.Nulls.SetValid(i)
		}
	}
	return v
}

func benchDecimalKernel(b *testing.B, k BatchAggKernel, allNull bool) {
	b.Helper()
	v := benchDecimalVec(b, allNull)
	var acc Accumulator
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k(&acc, v, nil, v.Len)
	}
}

func BenchmarkBatchSumDecimal(b *testing.B) {
	benchDecimalKernel(b, ResolveBatchSum(batch.TypeDecimal), false)
}

func BenchmarkBatchMinDecimal(b *testing.B) {
	benchDecimalKernel(b, ResolveBatchMin(batch.TypeDecimal), false)
}

func BenchmarkBatchMaxDecimal(b *testing.B) {
	benchDecimalKernel(b, ResolveBatchMax(batch.TypeDecimal), false)
}

func BenchmarkBatchSumDecimalAllNull(b *testing.B) {
	benchDecimalKernel(b, ResolveBatchSum(batch.TypeDecimal), true)
}

func BenchmarkBatchMinDecimalAllNull(b *testing.B) {
	benchDecimalKernel(b, ResolveBatchMin(batch.TypeDecimal), true)
}

func BenchmarkBatchMaxDecimalAllNull(b *testing.B) {
	benchDecimalKernel(b, ResolveBatchMax(batch.TypeDecimal), true)
}
