package parquet

import "fmt"

// Run-granularity access to the RLE/bit-packing hybrid stream.
//
// decodeAllBatch expands the stream to one int32 per value. For streams
// that are mostly RLE runs that is pure waste: a ClickBench hits row group
// stores EventDate as ONE run covering a million rows, CounterID as three.
// A consumer that only needs to know "these N consecutive values are all
// x" — scan-level predicate evaluation is exactly that consumer — can read
// the runs directly and never touch a per-row array.
//
// This file is strictly additive. decodeAllBatch and every DecodeRLE*
// entry point are unchanged, and RLERunIterator reproduces their parsing
// decisions exactly (same header handling, same clamping, same errors, in
// the same order) so that expanding the runs yields byte-identical output.
// The equivalence is asserted by TestRLERunIteratorEquivalence and
// FuzzRLERunIterator.

// RLERunIterator yields an RLE/bit-packing hybrid stream as (value,
// runLength) pairs without materializing per-value output.
//
// Runs are NOT guaranteed to be maximal: an RLE group yields one run, but
// a bit-packed group is walked through a fixed window and only coalesces
// equal neighbours inside that window, so a long constant span inside a
// bit-packed group can arrive as several runs. Consumers must therefore
// treat runs as "a span of equal values", never as "a distinct value".
//
// Total emitted length is capped at count, exactly as decodeAllBatch caps
// at len(dst): a final run that would overshoot is clamped, and a stream
// that ends early simply emits fewer values (check Emitted).
type RLERunIterator struct {
	dec     RLEDecoder
	count   int
	emitted int

	// Current bit-packed group: its (possibly truncated) byte range, the
	// next value index within it, and how many values it still owes.
	bpData  []byte
	bpFrom  int
	bpTotal int

	// Decode window over the current bit-packed group.
	buf    [64]int32
	bufPos int
	bufLen int

	err  error
	done bool
}

// NewRLERunIterator creates a run iterator over the same input
// DecodeRLEInt32 takes: a bare RLE/bit-packing hybrid stream, its bit
// width, and the number of values to produce.
func NewRLERunIterator(data []byte, bitWidth, count int) *RLERunIterator {
	return &RLERunIterator{
		dec:   RLEDecoder{data: data, bitWidth: bitWidth, count: count},
		count: count,
	}
}

// Next returns the next run. ok is false at end of stream, when count
// values have been emitted, or when the stream is malformed — check Err to
// tell the last case apart. Zero-length runs are never returned.
func (it *RLERunIterator) Next() (val int32, runLen int, ok bool) {
	if it.err != nil || it.done {
		return 0, 0, false
	}
	for {
		// Drain the bit-packed decode window, coalescing equal neighbours.
		if it.bufPos < it.bufLen {
			v := it.buf[it.bufPos]
			it.bufPos++
			n := 1
			for it.bufPos < it.bufLen && it.buf[it.bufPos] == v {
				n++
				it.bufPos++
			}
			it.emitted += n
			return v, n, true
		}
		// Refill the window from the current bit-packed group.
		if it.bpFrom < it.bpTotal {
			k := it.bpTotal - it.bpFrom
			if k > len(it.buf) {
				k = len(it.buf)
			}
			decodeBitPackedRange(it.buf[:k], it.bpData, it.dec.bitWidth, it.bpFrom, k)
			it.bpFrom += k
			it.bufPos, it.bufLen = 0, k
			continue
		}
		// Next group header. Same loop guard decodeAllBatch uses.
		if it.dec.off >= len(it.dec.data) || it.emitted >= it.count {
			it.done = true
			return 0, 0, false
		}
		header, err := it.dec.readVarint()
		if err != nil {
			it.err = fmt.Errorf("rle: reading group header: %w", err)
			return 0, 0, false
		}
		if header&1 == 0 {
			// RLE run: header >> 1 repeats of one value.
			runLen := int(header >> 1)
			v, err := it.dec.readRLEValue()
			if err != nil {
				it.err = err
				return 0, 0, false
			}
			if rem := it.count - it.emitted; runLen > rem {
				runLen = rem
			}
			if runLen <= 0 {
				continue
			}
			it.emitted += runLen
			return v, runLen, true
		}
		// Bit-packed run: header >> 1 groups of 8 values.
		groupCount := int(header >> 1)
		if groupCount <= 0 {
			continue
		}
		numValues := groupCount * 8
		if numValues < 0 || numValues/8 != groupCount {
			it.err = fmt.Errorf("rle: bit-packed group count overflow: %d", groupCount)
			return 0, 0, false
		}
		if rem := it.count - it.emitted; numValues > rem {
			numValues = rem
		}
		// byteCount covers the WHOLE group even when only part of it is
		// wanted — the stream position must advance past all of it.
		byteCount := groupCount * it.dec.bitWidth
		if byteCount < 0 {
			it.err = fmt.Errorf("rle: bit-packed byte count overflow")
			return 0, 0, false
		}
		if it.dec.off+byteCount > len(it.dec.data) {
			byteCount = len(it.dec.data) - it.dec.off
		}
		it.bpData = it.dec.data[it.dec.off : it.dec.off+byteCount]
		it.dec.off += byteCount
		it.bpFrom, it.bpTotal = 0, numValues
		it.bufPos, it.bufLen = 0, 0
	}
}

// Err returns the decode error that stopped iteration, if any. It is the
// same error decodeAllBatch returns for the same input.
func (it *RLERunIterator) Err() error { return it.err }

// Emitted returns the total number of values covered by the runs yielded
// so far — the counterpart of decodeAllBatch's returned length.
func (it *RLERunIterator) Emitted() int { return it.emitted }

// CountRLERuns walks the group headers of an RLE/bit-packing hybrid stream
// and returns an UPPER BOUND on the number of runs RLERunIterator will
// yield for it: one per non-empty RLE group, and one per VALUE in a
// bit-packed group (bit-packed values only coalesce opportunistically, so
// one-run-each is their worst case).
//
// Counting stops as soon as the bound exceeds limit, so a census against a
// small limit costs a varint read per group and nothing more — bit-packed
// groups are skipped by byte length, never decoded. When the return value
// exceeds limit it means only "more than limit".
//
// Errors mirror the decoder's: a caller that treats any error as "use the
// expanding path" gets the identical error reported from there.
func CountRLERuns(data []byte, bitWidth, count, limit int) (int, error) {
	d := RLEDecoder{data: data, bitWidth: bitWidth, count: count}
	runs, emitted := 0, 0
	for d.off < len(d.data) && emitted < count {
		header, err := d.readVarint()
		if err != nil {
			return runs, fmt.Errorf("rle: reading group header: %w", err)
		}
		if header&1 == 0 {
			runLen := int(header >> 1)
			if _, err := d.readRLEValue(); err != nil {
				return runs, err
			}
			if rem := count - emitted; runLen > rem {
				runLen = rem
			}
			if runLen <= 0 {
				continue
			}
			emitted += runLen
			runs++
		} else {
			groupCount := int(header >> 1)
			if groupCount <= 0 {
				continue
			}
			numValues := groupCount * 8
			if numValues < 0 || numValues/8 != groupCount {
				return runs, fmt.Errorf("rle: bit-packed group count overflow: %d", groupCount)
			}
			if rem := count - emitted; numValues > rem {
				numValues = rem
			}
			byteCount := groupCount * bitWidth
			if byteCount < 0 {
				return runs, fmt.Errorf("rle: bit-packed byte count overflow")
			}
			if d.off+byteCount > len(d.data) {
				byteCount = len(d.data) - d.off
			}
			d.off += byteCount
			emitted += numValues
			runs += numValues
		}
		if runs > limit {
			return runs, nil
		}
	}
	return runs, nil
}
