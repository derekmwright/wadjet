package worker

import (
	"os"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// sinkDirectChunkEnabled gates the stage sinks' direct-chunk path: a consume
// slice large enough to trip its sink's flush threshold on its own skips the
// accumulator and encodes straight from the source batch into the .wshf
// stream OUTSIDE the sink/partition mutex (a per-stream `flushing` flag keeps
// the writer single-threaded). WADJET_SINK_DIRECT_CHUNK=0 restores the
// locked-accumulator path everywhere (A/B kill switch).
//
// Why: the 2026-08-12 SF100 block profile pinned 64.3% of ALL worker mutex
// block time on partitionedShuffleSink.appendAndMaybeFlush (2,690s blocked,
// worker-*-block.prof in the 20260812-reprofile evidence) — 98%+ of it via
// the large-consume burst path, where morsel-parallel consumers (k ≤ 8,
// morsel views up to 64K rows) each held a partition lock for a multi-
// thousand-row accumulator copy plus chunk encode. unpartitionedStageSink.
// Consume was another 11.2% with the same shape. The direct path removes
// both the copy (source→accumulator→wire becomes source→wire) and the
// encode from the critical section; the lock now covers only counter
// updates. See docs/design/sink-direct-chunk.md.
var sinkDirectChunkEnabled = os.Getenv("WADJET_SINK_DIRECT_CHUNK") != "0"

// approxRowBytes estimates the encoded bytes per row of b for the
// direct-chunk threshold gate. Fixed-width types contribute their width;
// bytes-typed columns contribute their column-mean value length (physical
// rows of the backing storage — for views, the base). It is a GATE estimate,
// not an accounting figure: over- or under-shooting by a factor only moves
// slices between the direct path and the accumulator path, both correct.
func approxRowBytes(b *batch.RecordBatch) int {
	total := 0
	for _, col := range b.Columns {
		src := col
		if src.Base != nil {
			src = src.Base
		}
		switch col.Type {
		case parquet.TypeBool:
			total++
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate, parquet.TypeFloat32:
			total += 4
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration, parquet.TypeFloat64:
			total += 8
		case parquet.TypeDecimal:
			total += 16
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			n := src.Len
			if n <= 0 {
				n = 1
			}
			total += len(src.BytesData.Data)/n + 4
		}
	}
	if total <= 0 {
		total = 1
	}
	return total
}
