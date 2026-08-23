package parquet

import (
	"encoding/binary"
	"testing"
)

// deltaStream builds a DELTA_BINARY_PACKED body carrying exactly the values
// given: one block, one miniblock, all deltas bit-packed at a width wide
// enough for the largest gap.
func deltaStream(vals []int64) []byte {
	var out []byte
	put := func(v uint64) {
		var buf [binary.MaxVarintLen64]byte
		out = append(out, buf[:binary.PutUvarint(buf[:], v)]...)
	}
	zigzag := func(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

	const blockSize, miniblocks = 128, 1
	put(blockSize)
	put(miniblocks)
	put(uint64(len(vals)))
	put(zigzag(vals[0]))
	if len(vals) == 1 {
		return out
	}
	// One block: min delta 0, bit width 8, deltas bit-packed one per byte.
	put(zigzag(0))
	out = append(out, 8) // bit width for the single miniblock
	packed := make([]byte, blockSize)
	for i := 1; i < len(vals); i++ {
		packed[i-1] = byte(vals[i] - vals[i-1])
	}
	return append(out, packed...)
}

// A DELTA_BINARY_PACKED stream carries its OWN value count, which the page
// header's count does not have to match. A stream shorter than the page's
// claim used to be honoured silently: the caller got fewer values than it
// asked for and no error, so every row past the end of the stream kept
// whatever the destination already held.
func TestDeltaBinaryPackedRefusesAStreamShorterThanThePage(t *testing.T) {
	body := deltaStream([]int64{10, 11, 12, 13})

	if _, err := DecodeDeltaBinaryPackedInt64(body, 4); err != nil {
		t.Fatalf("an honest 4-value page: %v", err)
	}
	if _, err := DecodeDeltaBinaryPackedInt64(body, 1000); err == nil {
		t.Error("a page declaring 1000 values over a 4-value stream decoded without error")
	}
	if _, err := DecodeDeltaBinaryPackedInt32(body, 1000); err == nil {
		t.Error("(int32) a page declaring 1000 values over a 4-value stream decoded without error")
	}

	// A stream carrying MORE than the page reads is legal — the caller
	// decodes a prefix — and still returns exactly what was asked for.
	v, err := DecodeDeltaBinaryPackedInt64(body, 2)
	if err != nil {
		t.Fatalf("reading a 2-value prefix of a 4-value stream: %v", err)
	}
	if got := v.Int64(); len(got) != 2 || got[0] != 10 || got[1] != 11 {
		t.Errorf("prefix decoded to %v, want [10 11]", got)
	}

	// An empty stream against a non-empty page is the same refusal.
	if _, err := DecodeDeltaBinaryPackedInt64(deltaStream([]int64{0})[:3], 8); err == nil {
		t.Error("a truncated stream decoded without error")
	}
}
