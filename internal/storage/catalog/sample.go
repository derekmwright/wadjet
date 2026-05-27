package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"io"
	"math"
	"sort"
)

// ColumnSample is a fixed-size random sample of a column's values,
// persisted in FileColumnStats so the catalog can build aggregate
// histograms across files at query time. The sample is stored sorted
// so cross-file merge is a sorted-merge of K-way streams; the
// histogram is built once from the merged sample on AggregateColumnStats.
//
// Storage: ~256 values × 8 bytes for numerics = 2 KB per column per
// file. Comparable to the per-file Histogram size but mergeable
// without distribution-assumption hacks.
//
// Type-discriminated wire format mirrors Histogram's encodeValue:
//   - int64: 8 bytes LE per value
//   - float64: 8 bytes LE (math.Float64bits) per value
//   - bytes: uint16 length prefix + raw bytes per value
const (
	sampleVersion = 1

	SampleDefaultSize = 256
)

// ReservoirSampler holds an in-memory sample using Algorithm L
// reservoir sampling: O(N) producer cost, uniformly distributed sample
// of size K from a stream of unknown length. Each Add either replaces
// a random existing entry or is dropped, weighted to keep all input
// values equally probable.
type ReservoirSampler struct {
	capacity int
	values   []any
	seen     int64
	rng      maphash.Hash
	typeCode uint8
	typed    bool
}

// NewReservoirSampler creates a sampler of the given capacity. K ≤ 0
// uses the default (SampleDefaultSize).
func NewReservoirSampler(k int) *ReservoirSampler {
	if k <= 0 {
		k = SampleDefaultSize
	}
	rs := &ReservoirSampler{capacity: k, values: make([]any, 0, k)}
	rs.rng.SetSeed(maphash.MakeSeed())
	return rs
}

// Add inserts a value. The sample is updated in O(1) amortized.
func (rs *ReservoirSampler) Add(v any) {
	if v == nil {
		return
	}
	if !rs.typed {
		if tc, ok := sampleTypeOf(v); ok {
			rs.typeCode = tc
			rs.typed = true
		} else {
			return // unsupported type — skip
		}
	}
	rs.seen++
	if len(rs.values) < rs.capacity {
		rs.values = append(rs.values, v)
		return
	}
	// Algorithm R: replace at index j with probability k/n.
	rs.rng.Reset()
	rs.rng.WriteString(fmt.Sprintf("%d", rs.seen))
	j := int(rs.rng.Sum64() % uint64(rs.seen))
	if j < rs.capacity {
		rs.values[j] = v
	}
}

// Snapshot returns the sampled values along with the total observed
// count. The sample slice is sorted in-place.
func (rs *ReservoirSampler) Snapshot() (sorted []any, totalSeen int64, typeCode uint8) {
	if !rs.typed || len(rs.values) == 0 {
		return nil, 0, 0
	}
	out := append([]any(nil), rs.values...)
	sortSample(out, rs.typeCode)
	return out, rs.seen, rs.typeCode
}

// EncodeSample writes a sorted sample to w in version-1 format.
//
//	[1]   version (1)
//	[1]   type code
//	[2]   value count K (uint16 LE)
//	[8]   total observed count (uint64 LE, before reservoir downsampling)
//	[K*?] K values, type-discriminated encoding
func EncodeSample(w io.Writer, values []any, totalSeen int64, typeCode uint8) error {
	if len(values) == 0 {
		return nil
	}
	hdr := []byte{sampleVersion, typeCode}
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	var cnt [2]byte
	binary.LittleEndian.PutUint16(cnt[:], uint16(len(values)))
	if _, err := w.Write(cnt[:]); err != nil {
		return err
	}
	var t [8]byte
	binary.LittleEndian.PutUint64(t[:], uint64(totalSeen))
	if _, err := w.Write(t[:]); err != nil {
		return err
	}
	for _, v := range values {
		if err := encodeValue(w, v, typeCode); err != nil {
			return err
		}
	}
	return nil
}

// DecodeSample reads a sample written by EncodeSample.
func DecodeSample(r io.Reader) (values []any, totalSeen int64, typeCode uint8, err error) {
	var hdr [4]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, 0, fmt.Errorf("sample: read header: %w", err)
	}
	if hdr[0] != sampleVersion {
		return nil, 0, 0, fmt.Errorf("sample: unsupported version %d", hdr[0])
	}
	typeCode = hdr[1]
	count := int(binary.LittleEndian.Uint16(hdr[2:4]))
	var t [8]byte
	if _, err = io.ReadFull(r, t[:]); err != nil {
		return nil, 0, 0, fmt.Errorf("sample: read total: %w", err)
	}
	totalSeen = int64(binary.LittleEndian.Uint64(t[:]))
	values = make([]any, count)
	for i := 0; i < count; i++ {
		v, derr := decodeValue(r, typeCode)
		if derr != nil {
			return nil, 0, 0, fmt.Errorf("sample: value %d: %w", i, derr)
		}
		values[i] = v
	}
	return values, totalSeen, typeCode, nil
}

// SampleBytes serializes a snapshot for catalog persistence.
func SampleBytes(values []any, totalSeen int64, typeCode uint8) []byte {
	if len(values) == 0 {
		return nil
	}
	var buf bytes.Buffer
	if err := EncodeSample(&buf, values, totalSeen, typeCode); err != nil {
		return nil
	}
	return buf.Bytes()
}

// SampleFromBytes parses a sample blob. Returns nil values on any error.
func SampleFromBytes(b []byte) (values []any, totalSeen int64, typeCode uint8, ok bool) {
	if len(b) < 12 {
		return nil, 0, 0, false
	}
	v, t, tc, err := DecodeSample(bytes.NewReader(b))
	if err != nil {
		return nil, 0, 0, false
	}
	return v, t, tc, true
}

// sampleTypeOf returns the type code for a value if supported.
func sampleTypeOf(v any) (uint8, bool) {
	switch v.(type) {
	case int64, int32, int:
		return histTypeInt64, true
	case float64, float32:
		return histTypeFloat64, true
	case string, []byte:
		return histTypeBytes, true
	}
	return 0, false
}

// MergeSamples combines multiple per-file samples into a single sorted
// sample, weighted by each file's totalSeen so larger files contribute
// more samples to the merged distribution. Returns combined values,
// total observed across all files, and the shared type code.
//
// If samples have mixed type codes (shouldn't happen for a valid
// column), the first sample's type wins; mismatched entries are
// skipped.
func MergeSamples(files [][]byte) (values []any, totalSeen int64, typeCode uint8) {
	type entry struct {
		values []any
		total  int64
		tc     uint8
	}
	var entries []entry
	for _, b := range files {
		v, t, tc, ok := SampleFromBytes(b)
		if !ok {
			continue
		}
		entries = append(entries, entry{v, t, tc})
		totalSeen += t
		if len(entries) == 1 {
			typeCode = tc
		}
	}
	if len(entries) == 0 {
		return nil, 0, 0
	}
	// Weight-proportional draws: each file contributes
	// ceil(SampleDefaultSize * (file.total / totalSeen)) samples
	// to the merged sample.
	merged := make([]any, 0, SampleDefaultSize)
	remaining := SampleDefaultSize
	for i, e := range entries {
		if e.tc != typeCode {
			continue
		}
		// For the last file, take whatever's left to ensure we fill
		// the budget when rounding under-allocated.
		var quota int
		if i == len(entries)-1 {
			quota = remaining
		} else {
			weight := float64(e.total) / float64(totalSeen)
			quota = int(math.Round(weight * float64(SampleDefaultSize)))
			if quota > remaining {
				quota = remaining
			}
			if quota < 1 {
				quota = 1
			}
		}
		// Draw evenly-spaced samples from this file's sorted values.
		n := len(e.values)
		if n == 0 || quota <= 0 {
			continue
		}
		if quota >= n {
			merged = append(merged, e.values...)
			remaining -= n
		} else {
			step := float64(n) / float64(quota)
			for j := 0; j < quota; j++ {
				idx := int(float64(j) * step)
				if idx >= n {
					idx = n - 1
				}
				merged = append(merged, e.values[idx])
			}
			remaining -= quota
		}
	}
	sortSample(merged, typeCode)
	values = merged
	return values, totalSeen, typeCode
}

// HistogramFromMergedSample builds a histogram from the merged sample
// and the actual total row count. The total drives the histogram's
// TotalValues so selectivities are reported as fractions of the real
// table size, not of the sample size.
func HistogramFromMergedSample(values []any, totalRows int64, typeCode uint8, k int) *Histogram {
	if len(values) == 0 || totalRows == 0 {
		return nil
	}
	h := BuildHistogramFromSamples(values, k)
	if h == nil {
		return nil
	}
	// Scale TotalValues from sample size to the actual table size so
	// SelectivityRange returns true-row-count-based fractions.
	h.TotalValues = totalRows
	// Counts stay as sample counts; selectivities use count/TotalValues
	// which already returns the right fraction since both numerator and
	// denominator scale linearly. Counts will be small (sum to sample
	// size), but the FRACTION below v matches the population fraction
	// under the uniform-sampling assumption.
	scale := float64(totalRows) / float64(sumCounts(h.Counts))
	for i := range h.Counts {
		h.Counts[i] = int64(float64(h.Counts[i]) * scale)
	}
	return h
}

func sumCounts(c []int64) int64 {
	var s int64
	for _, v := range c {
		s += v
	}
	return s
}

// pickRandomFloat returns a deterministic-pseudorandom float in [0, 1)
// derived from x. Used to break ties consistently in sample weighting.
// Unused for the current weight-proportional draw but kept for future
// stratified-sampling improvements.
var _ = sort.SearchInts
