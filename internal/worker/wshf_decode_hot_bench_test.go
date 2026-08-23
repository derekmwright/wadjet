package worker

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/wshf"
)

// BenchmarkWSHFDecodeHot measures the cursor-level hot path
// (internal/wshf.Cursor.Take/U8/U16/U32/Len32 and the ReadColumn column
// loop they back) that every shuffle tier — file, stream, pread — and the
// coordinator's inline-result decode all funnel through since #422. The
// fixture is the same 2048-row, two-column (int64 + string) WSHF payload
// buildRoundTripWSHF uses for the round-trip tests, decoded repeatedly:
// DecodeBatches walks the header once and every column's null bitmap and
// data segment through Cursor, which is exactly the code cursor.go's
// takeErr/implausibleLengthErr split targets — moving error construction
// off Take/Peek/Len32's own bodies into noinline helpers so those bodies
// carry no fmt.Errorf call-site weight of their own, only ever paid when a
// read actually fails.
func BenchmarkWSHFDecodeHot(b *testing.B) {
	raw := buildRoundTripWSHF(b)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batches, err := wshf.DecodeBatches(raw)
		if err != nil {
			b.Fatalf("DecodeBatches: %v", err)
		}
		if len(batches) != 1 || batches[0].Len != 2048 {
			b.Fatalf("unexpected decode result: %d batches", len(batches))
		}
	}
}
