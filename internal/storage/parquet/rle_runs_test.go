package parquet

import (
	"fmt"
	"math/rand"
	"testing"
)

// --- stream builders -------------------------------------------------

// putVarint appends an unsigned LEB128 varint (the RLE header encoding).
func putVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// rleGroup appends an RLE run of count copies of val at bitWidth.
func rleGroup(dst []byte, val int32, count, bitWidth int) []byte {
	dst = putVarint(dst, uint64(count)<<1)
	for i := 0; i < (bitWidth+7)/8; i++ {
		dst = append(dst, byte(uint32(val)>>(8*uint(i))))
	}
	return dst
}

// packGroup appends a bit-packed run holding vals (len must be a multiple
// of 8), packed LSB-first exactly as DecodeBitPacked reads them.
func packGroup(dst []byte, vals []int32, bitWidth int) []byte {
	if len(vals)%8 != 0 {
		panic("bit-packed runs hold whole groups of 8")
	}
	dst = putVarint(dst, uint64(len(vals)/8)<<1|1)
	if bitWidth == 0 {
		return dst
	}
	nbytes := len(vals) * bitWidth / 8
	buf := make([]byte, nbytes+8)
	bitPos := 0
	for _, v := range vals {
		for b := 0; b < bitWidth; b++ {
			if uint32(v)>>uint(b)&1 == 1 {
				buf[(bitPos+b)/8] |= 1 << uint((bitPos+b)%8)
			}
		}
		bitPos += bitWidth
	}
	return append(dst, buf[:nbytes]...)
}

// --- equivalence harness ---------------------------------------------

// decodeAllRef is the reference: exactly what every DecodeRLEInt32* entry
// point runs, including its partial output on error.
func decodeAllRef(data []byte, bitWidth, count int) ([]int32, error) {
	dst := make([]int32, count)
	d := NewRLEDecoder(data, bitWidth, count)
	n, err := d.decodeAllBatch(dst)
	return dst[:n], err
}

// expandRuns drives RLERunIterator and materializes what it yields, so the
// two can be compared value for value.
func expandRuns(tb testing.TB, data []byte, bitWidth, count int) ([]int32, error) {
	tb.Helper()
	it := NewRLERunIterator(data, bitWidth, count)
	out := make([]int32, 0, count)
	for {
		v, n, ok := it.Next()
		if !ok {
			break
		}
		if n <= 0 {
			tb.Fatalf("iterator yielded a non-positive run length %d", n)
		}
		for i := 0; i < n; i++ {
			out = append(out, v)
		}
		if len(out) > count {
			tb.Fatalf("iterator emitted %d values past the %d-value cap", len(out), count)
		}
	}
	if it.Emitted() != len(out) {
		tb.Fatalf("Emitted() = %d, runs covered %d values", it.Emitted(), len(out))
	}
	// Next must stay terminated and must not change the error.
	if _, _, ok := it.Next(); ok {
		tb.Fatal("iterator resumed after reporting end of stream")
	}
	return out, it.Err()
}

// assertRunEquivalence fails unless the iterator and decodeAllBatch agree
// on values AND on the error.
func assertRunEquivalence(tb testing.TB, name string, data []byte, bitWidth, count int) {
	tb.Helper()
	wantVals, wantErr := decodeAllRef(data, bitWidth, count)
	gotVals, gotErr := expandRuns(tb, data, bitWidth, count)

	switch {
	case wantErr == nil && gotErr != nil:
		tb.Fatalf("%s: iterator errored (%v) where decodeAllBatch did not", name, gotErr)
	case wantErr != nil && gotErr == nil:
		tb.Fatalf("%s: decodeAllBatch errored (%v) where the iterator did not", name, wantErr)
	case wantErr != nil && gotErr != nil:
		if wantErr.Error() != gotErr.Error() {
			tb.Fatalf("%s: error mismatch\n  decodeAllBatch: %v\n  iterator:       %v", name, wantErr, gotErr)
		}
		// Errors match; both produced partial output, which the decoder's
		// callers discard. Nothing more to compare.
		return
	}
	if len(gotVals) != len(wantVals) {
		tb.Fatalf("%s: iterator produced %d values, decodeAllBatch %d", name, len(gotVals), len(wantVals))
	}
	for i := range wantVals {
		if gotVals[i] != wantVals[i] {
			tb.Fatalf("%s: value %d = %d, decodeAllBatch has %d", name, i, gotVals[i], wantVals[i])
		}
	}
}

// --- table-driven cases ----------------------------------------------

func TestRLERunIteratorEquivalence(t *testing.T) {
	// A 3-bit alphabet keeps the bit-packed cases sub-byte-aligned.
	packed24 := make([]int32, 24)
	for i := range packed24 {
		packed24[i] = int32(i % 8)
	}
	const bw3 = 3

	cases := []struct {
		name     string
		data     []byte
		bitWidth int
		count    int
	}{
		{"empty-data", nil, 8, 0},
		{"empty-data-nonzero-count", nil, 8, 16},
		{"zero-count", rleGroup(nil, 42, 10, 8), 8, 0},
		{"single-run", rleGroup(nil, 42, 1000, 8), 8, 1000},
		{"single-run-one-value", rleGroup(nil, 7, 1, 8), 8, 1},
		{"bitwidth-zero", rleGroup(nil, 0, 100, 0), 0, 100},
		{"bitwidth-32", rleGroup(nil, -1, 5, 32), 32, 5},
		{"bitwidth-16", rleGroup(nil, 30000, 5, 16), 16, 5},
		{
			"alternating-runs",
			func() []byte {
				var b []byte
				for i := 0; i < 20; i++ {
					b = rleGroup(b, int32(i%3), 7, 8)
				}
				return b
			}(),
			8, 140,
		},
		{"zero-length-run-then-run",
			rleGroup(rleGroup(nil, 5, 0, 8), 9, 4, 8), 8, 4},
		{"pure-bit-packed", packGroup(nil, packed24, bw3), bw3, 24},
		{
			"bit-packed-multi-group",
			packGroup(packGroup(nil, packed24, bw3), packed24, bw3),
			bw3, 48,
		},
		{
			"bit-packed-all-equal",
			packGroup(nil, make([]int32, 24), bw3),
			bw3, 24,
		},
		{
			"bit-packed-window-boundary", // > 64 values: crosses the decode window
			packGroup(nil, func() []int32 {
				v := make([]int32, 200)
				for i := range v {
					v[i] = int32(i % 8)
				}
				return v
			}(), bw3),
			bw3, 200,
		},
		{
			"bit-packed-equal-across-window", // constant run spanning windows
			packGroup(nil, make([]int32, 200), bw3),
			bw3, 200,
		},
		{
			"mixed",
			func() []byte {
				b := rleGroup(nil, 1, 50, bw3)
				b = packGroup(b, packed24, bw3)
				b = rleGroup(b, 6, 300, bw3)
				b = packGroup(b, packed24, bw3)
				return b
			}(),
			bw3, 398,
		},
		{"run-longer-than-count", rleGroup(nil, 3, 1000, 8), 8, 10},
		{"bit-packed-longer-than-count", packGroup(nil, packed24, bw3), bw3, 5},
		{"stream-shorter-than-count", rleGroup(nil, 3, 4, 8), 8, 100},
		{"truncated-varint", []byte{0x80}, 8, 10},
		{"truncated-varint-mid-stream",
			append(rleGroup(nil, 1, 4, 8), 0x80), 8, 100},
		{"truncated-rle-value", []byte{0x08}, 16, 10}, // header only, value bytes missing
		{"truncated-rle-value-partial",
			[]byte{0x08, 0x01}, 32, 10}, // 1 of 4 value bytes
		{"truncated-bit-packed", []byte{0x03, 0x55}, bw3, 24}, // 3 groups declared, 1 byte
		{"bit-packed-truncated-mid",
			packGroup(nil, packed24, bw3)[:5], bw3, 24},
		{"varint-overflow",
			[]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 8, 10},
		{"huge-group-count-overflow",
			// header for a bit-packed run whose value count overflows int.
			putVarint(nil, uint64(1)<<62|1), 8, 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertRunEquivalence(t, tc.name, tc.data, tc.bitWidth, tc.count)
		})
	}
}

// TestRLERunIteratorRunStructure pins the property the scan filter relies
// on: an RLE-shaped stream is delivered as a handful of runs, not one run
// per value.
func TestRLERunIteratorRunStructure(t *testing.T) {
	data := rleGroup(nil, 42, 1_000_000, 8)
	it := NewRLERunIterator(data, 8, 1_000_000)
	v, n, ok := it.Next()
	if !ok || v != 42 || n != 1_000_000 {
		t.Fatalf("one-run page yielded (%d, %d, %v), want (42, 1000000, true)", v, n, ok)
	}
	if _, _, ok := it.Next(); ok {
		t.Fatal("one-run page yielded a second run")
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestRLERunIteratorRandomEquivalence is the property test: randomized
// streams across the whole shape space must expand to exactly what
// decodeAllBatch produces, error included.
func TestRLERunIteratorRandomEquivalence(t *testing.T) {
	for seed := int64(0); seed < 400; seed++ {
		rng := rand.New(rand.NewSource(seed))
		bitWidth := rng.Intn(17) // 0..16
		maxVal := int32(1)
		if bitWidth > 0 && bitWidth < 31 {
			maxVal = int32(1) << uint(bitWidth)
		}
		var data []byte
		total := 0
		groups := rng.Intn(12)
		for g := 0; g < groups; g++ {
			if rng.Intn(2) == 0 {
				n := rng.Intn(300)
				data = rleGroup(data, rng.Int31n(maxVal), n, bitWidth)
				total += n
			} else {
				k := rng.Intn(10) * 8
				vals := make([]int32, k)
				for i := range vals {
					vals[i] = rng.Int31n(maxVal)
				}
				data = packGroup(data, vals, bitWidth)
				total += k
			}
		}
		// Truncate a third of the streams to exercise the error and
		// zero-fill paths against the reference.
		if len(data) > 0 && rng.Intn(3) == 0 {
			data = data[:rng.Intn(len(data))]
		}
		// count above, at, and below the stream's natural length.
		for _, count := range []int{total, total / 2, total + 37} {
			if count < 0 {
				continue
			}
			assertRunEquivalence(t, fmt.Sprintf("seed=%d bw=%d count=%d", seed, bitWidth, count),
				data, bitWidth, count)
		}
	}
}

// TestCountRLERuns checks the census bound and its short circuit.
func TestCountRLERuns(t *testing.T) {
	packed24 := make([]int32, 24)
	for i := range packed24 {
		packed24[i] = int32(i % 8)
	}
	cases := []struct {
		name     string
		data     []byte
		bitWidth int
		count    int
		limit    int
		want     int
	}{
		{"one-run", rleGroup(nil, 42, 1000, 8), 8, 1000, 100, 1},
		{"three-runs", rleGroup(rleGroup(rleGroup(nil, 1, 10, 8), 2, 10, 8), 3, 10, 8), 8, 30, 100, 3},
		{"empty-runs-skipped", rleGroup(rleGroup(nil, 1, 0, 8), 2, 10, 8), 8, 10, 100, 1},
		{"bit-packed-counts-values", packGroup(nil, packed24, 3), 3, 24, 100, 24},
		{"clamped-by-count", rleGroup(rleGroup(nil, 1, 10, 8), 2, 10, 8), 8, 10, 100, 1},
		{"no-data", nil, 8, 100, 100, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CountRLERuns(tc.data, tc.bitWidth, tc.count, tc.limit)
			if err != nil {
				t.Fatalf("CountRLERuns: %v", err)
			}
			if got != tc.want {
				t.Fatalf("CountRLERuns = %d, want %d", got, tc.want)
			}
		})
	}

	// Short circuit: a big bit-packed stream must stop counting just past
	// the limit rather than walking the whole thing.
	big := make([]int32, 8000)
	for i := range big {
		big[i] = int32(i % 8)
	}
	got, err := CountRLERuns(packGroup(nil, big, 3), 3, 8000, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got <= 100 {
		t.Fatalf("census = %d, want > limit(100) for a bit-packed stream", got)
	}
}

// TestCountRLERunsIsUpperBound is the safety property the threshold rests
// on: the census must never under-count what the iterator yields, or the
// run path would be chosen for a stream it is slow on.
func TestCountRLERunsIsUpperBound(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		rng := rand.New(rand.NewSource(seed + 9000))
		bitWidth := 1 + rng.Intn(12)
		maxVal := int32(1) << uint(bitWidth)
		var data []byte
		total := 0
		for g := 0; g < rng.Intn(10); g++ {
			if rng.Intn(2) == 0 {
				n := rng.Intn(200)
				data = rleGroup(data, rng.Int31n(maxVal), n, bitWidth)
				total += n
			} else {
				k := rng.Intn(8) * 8
				vals := make([]int32, k)
				for i := range vals {
					vals[i] = rng.Int31n(maxVal)
				}
				data = packGroup(data, vals, bitWidth)
				total += k
			}
		}
		bound, err := CountRLERuns(data, bitWidth, total, 1<<30)
		if err != nil {
			t.Fatalf("seed %d: census: %v", seed, err)
		}
		it := NewRLERunIterator(data, bitWidth, total)
		actual := 0
		for {
			if _, _, ok := it.Next(); !ok {
				break
			}
			actual++
		}
		if it.Err() != nil {
			t.Fatalf("seed %d: iterate: %v", seed, it.Err())
		}
		if actual > bound {
			t.Fatalf("seed %d: iterator yielded %d runs, census bound was %d", seed, actual, bound)
		}
	}
}

// TestDecodeBitPackedRangeMatchesWhole checks the windowed decoder against
// the allocating whole-group one, including reads past the end of data.
func TestDecodeBitPackedRangeMatchesWhole(t *testing.T) {
	for bitWidth := 0; bitWidth <= 20; bitWidth++ {
		for _, n := range []int{0, 1, 7, 8, 9, 64, 65, 130} {
			nbytes := (n*bitWidth + 7) / 8
			data := make([]byte, nbytes)
			for i := range data {
				data[i] = byte(i*37 + bitWidth)
			}
			for _, truncate := range []int{0, 1, 3} {
				d := data
				if truncate < len(d) {
					d = d[:len(d)-truncate]
				}
				want := DecodeBitPacked(d, bitWidth, n)
				got := make([]int32, n)
				// Decode in windows of 7 to exercise arbitrary `from`.
				for from := 0; from < n; from += 7 {
					k := n - from
					if k > 7 {
						k = 7
					}
					decodeBitPackedRange(got[from:from+k], d, bitWidth, from, k)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("bw=%d n=%d trunc=%d: [%d] = %d, want %d",
							bitWidth, n, truncate, i, got[i], want[i])
					}
				}
			}
		}
	}
}

// TestRLERunIteratorAgainstLevelEncoder runs the iterator over streams the
// project's own writer produces (definition levels), closing the loop
// between the encoder and the run reader.
func TestRLERunIteratorAgainstLevelEncoder(t *testing.T) {
	for _, nulls := range [][]bool{
		make([]bool, 1000),
		func() []bool {
			b := make([]bool, 1000)
			for i := range b {
				b[i] = true
			}
			return b
		}(),
		func() []bool {
			b := make([]bool, 1000)
			for i := range b {
				b[i] = i%2 == 0
			}
			return b
		}(),
		func() []bool {
			b := make([]bool, 1000)
			for i := range b {
				b[i] = i%97 == 0
			}
			return b
		}(),
	} {
		// encodeDefLevelsRLE returns the bare stream; the 4-byte length
		// prefix is added by the page writer.
		encoded := encodeDefLevelsRLE(nulls, len(nulls))
		if len(encoded) == 0 {
			t.Fatal("level encoder produced no bytes")
		}
		assertRunEquivalence(t, "def-levels", encoded, 1, len(nulls))
	}
}

// FuzzRLERunIterator asserts the equivalence on arbitrary input: whatever
// decodeAllBatch does with a hostile stream, the iterator must do too.
func FuzzRLERunIterator(f *testing.F) {
	f.Add([]byte{0x02, 0x00}, 1, 1)
	f.Add([]byte{0x03, 0x55}, 1, 4)
	f.Add([]byte{}, 0, 0)
	f.Add([]byte{0x02, 0x00}, 0, 1)
	f.Add(rleGroup(nil, 42, 1000, 8), 8, 1000)
	f.Add(rleGroup(nil, 42, 1000, 8), 8, 10)
	f.Add(packGroup(nil, []int32{0, 1, 2, 3, 4, 5, 6, 7}, 3), 3, 8)
	f.Add(append(rleGroup(nil, 1, 50, 3), packGroup(nil, []int32{0, 1, 2, 3, 4, 5, 6, 7}, 3)...), 3, 58)
	f.Add([]byte{0x80}, 8, 10)
	f.Add([]byte{0x08}, 16, 10)

	f.Fuzz(func(t *testing.T, data []byte, bitWidth int, numValues int) {
		if bitWidth < 0 || bitWidth > 32 || numValues < 0 || numValues > 10000 {
			return
		}
		assertRunEquivalence(t, "fuzz", data, bitWidth, numValues)
	})
}

func BenchmarkRLESingleRun(b *testing.B) {
	const n = 1 << 20
	data := rleGroup(nil, 42, n, 8)
	b.Run("decodeAllBatch", func(b *testing.B) {
		dst := make([]int32, n)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			d := NewRLEDecoder(data, 8, n)
			if _, err := d.decodeAllBatch(dst); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("runIterator", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			it := NewRLERunIterator(data, 8, n)
			for {
				if _, _, ok := it.Next(); !ok {
					break
				}
			}
			if it.Err() != nil {
				b.Fatal(it.Err())
			}
		}
	})
}
