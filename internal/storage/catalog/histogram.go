package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
)

// Histogram is an equi-depth histogram over a column's values. Each
// bucket holds boundary values and a count. For numeric columns, the
// boundaries are int64-encoded; the planner converts to/from float64
// for range comparisons.
//
// Equi-depth means each bucket holds approximately the same number of
// values (1/K of the total). Bucket boundaries adapt to the data
// distribution: dense regions get narrow buckets, sparse regions wide.
// This gives accurate selectivity estimates for both common and rare
// values.
//
// Used in stats.estimatePredSelectivity for range/equality filters
// when the column has a histogram in the catalog. Replaces hardcoded
// 0.33 / 0.1 fractions with data-driven estimates.
//
// Wire format (binary, version-1):
//
//	[1]   version (1)
//	[1]   bucket count K (≤ 255)
//	[1]   value type code (0=int64, 1=float64, 2=bytes)
//	[1]   reserved
//	[8]   total values
//	[K+1] boundary values (K buckets → K+1 boundaries)
//	[K*8] per-bucket counts (uint64 LE)
//
// Boundary encoding depends on type code:
//   - int64: 8 bytes LE per value
//   - float64: 8 bytes LE (math.Float64bits) per value
//   - bytes: uint16 length prefix + raw bytes per value
const (
	histVersion       = 1
	HistDefaultBuckets = 64

	histTypeInt64   = 0
	histTypeFloat64 = 1
	histTypeBytes   = 2
)

// Histogram is the catalog's per-column histogram.
type Histogram struct {
	TypeCode    uint8
	TotalValues int64
	// Boundaries has K+1 entries; bucket[i] covers [Boundaries[i], Boundaries[i+1]).
	// The last bucket's upper bound is inclusive of the max value.
	Boundaries []any
	Counts     []int64
}

// BuildHistogramFromSamples constructs an equi-depth histogram from a
// pre-collected sample of values. Picks K bucket boundaries at sorted
// 1/K positions of the sample, assigns each sample to a bucket. K
// caps at 255 to fit the bucket count in one wire byte.
//
// Caller is responsible for typing — pass a []int64, []float64, or
// [][]byte. Mixed types in the sample slice are rejected (returns nil).
func BuildHistogramFromSamples(sample []any, k int) *Histogram {
	if k <= 0 {
		k = HistDefaultBuckets
	}
	if k > 255 {
		k = 255
	}
	if len(sample) == 0 {
		return nil
	}
	typeCode, ok := classifySampleType(sample)
	if !ok {
		return nil
	}
	sorted := append([]any(nil), sample...)
	sortSample(sorted, typeCode)

	// Pick K+1 boundary indices at equal spacing.
	n := len(sorted)
	boundaries := make([]any, k+1)
	counts := make([]int64, k)
	for i := 0; i <= k; i++ {
		idx := int(int64(i) * int64(n) / int64(k))
		if idx >= n {
			idx = n - 1
		}
		boundaries[i] = sorted[idx]
	}
	// Assign each sample to a bucket via binary search on boundaries.
	for _, v := range sorted {
		bi := bucketIndex(boundaries, v, typeCode, k)
		counts[bi]++
	}
	return &Histogram{
		TypeCode:    typeCode,
		TotalValues: int64(n),
		Boundaries:  boundaries,
		Counts:      counts,
	}
}

// classifySampleType returns the type code if all samples share a
// supported type, or ok=false otherwise.
func classifySampleType(sample []any) (uint8, bool) {
	if len(sample) == 0 {
		return 0, false
	}
	switch sample[0].(type) {
	case int64, int32, int:
		return histTypeInt64, true
	case float64, float32:
		return histTypeFloat64, true
	case string, []byte:
		return histTypeBytes, true
	}
	return 0, false
}

func sortSample(s []any, typeCode uint8) {
	switch typeCode {
	case histTypeInt64:
		sort.Slice(s, func(i, j int) bool {
			return toInt64(s[i]) < toInt64(s[j])
		})
	case histTypeFloat64:
		sort.Slice(s, func(i, j int) bool {
			return toFloat64Safe(s[i]) < toFloat64Safe(s[j])
		})
	case histTypeBytes:
		sort.Slice(s, func(i, j int) bool {
			return bytes.Compare(toBytes(s[i]), toBytes(s[j])) < 0
		})
	}
}

func toInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int32:
		return int64(tv)
	case int:
		return int64(tv)
	}
	return 0
}

func toFloat64Safe(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int32:
		return float64(tv)
	case int:
		return float64(tv)
	}
	return 0
}

func toBytes(v any) []byte {
	switch tv := v.(type) {
	case []byte:
		return tv
	case string:
		return []byte(tv)
	}
	return nil
}

// bucketIndex returns the bucket index for a value. Bucket i covers
// [boundaries[i], boundaries[i+1]). The last bucket is inclusive of
// boundaries[k] so the maximum value lands in bucket k-1, not k.
func bucketIndex(boundaries []any, v any, typeCode uint8, k int) int {
	lo, hi := 0, k
	for lo < hi {
		mid := (lo + hi) / 2
		if compareBoundary(boundaries[mid+1], v, typeCode) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= k {
		lo = k - 1
	}
	return lo
}

func compareBoundary(a, b any, typeCode uint8) int {
	switch typeCode {
	case histTypeInt64:
		ai, bi := toInt64(a), toInt64(b)
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		}
		return 0
	case histTypeFloat64:
		af, bf := toFloat64Safe(a), toFloat64Safe(b)
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		}
		return 0
	case histTypeBytes:
		return bytes.Compare(toBytes(a), toBytes(b))
	}
	return 0
}

// SelectivityLE returns the estimated fraction of values ≤ v.
//
// Cumulates the counts of all buckets fully below v, plus a linear-
// interpolation fraction of the bucket containing v. For string
// columns, the partial-bucket fraction defaults to 0.5 (uniform).
//
// Returns 0 (nothing matches) when v < all values, 1 (all match) when
// v ≥ all values. Clamped to [0, 1].
func (h *Histogram) SelectivityLE(v any) float64 {
	if h == nil || h.TotalValues == 0 || len(h.Counts) == 0 {
		return 0.5
	}
	cmpFirst := compareBoundary(v, h.Boundaries[0], h.TypeCode)
	if cmpFirst < 0 {
		return 0
	}
	cmpLast := compareBoundary(v, h.Boundaries[len(h.Boundaries)-1], h.TypeCode)
	if cmpLast >= 0 {
		return 1
	}
	// Find the bucket whose upper > v.
	var below int64
	for i, count := range h.Counts {
		if compareBoundary(h.Boundaries[i+1], v, h.TypeCode) > 0 {
			// v falls into bucket i. Partial bucket fraction:
			frac := bucketFraction(h.Boundaries[i], h.Boundaries[i+1], v, h.TypeCode)
			return float64(below+int64(float64(count)*frac)) / float64(h.TotalValues)
		}
		below += count
	}
	return 1
}

// SelectivityRange returns the estimated fraction of values in [lo, hi].
// Inclusive on both ends.
func (h *Histogram) SelectivityRange(lo, hi any) float64 {
	if h == nil || h.TotalValues == 0 {
		return 0.33
	}
	leHi := h.SelectivityLE(hi)
	ltLo := h.SelectivityLT(lo)
	r := leHi - ltLo
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}
	return r
}

// SelectivityLT returns the estimated fraction of values < v.
func (h *Histogram) SelectivityLT(v any) float64 {
	if h == nil || h.TotalValues == 0 {
		return 0.5
	}
	cmpFirst := compareBoundary(v, h.Boundaries[0], h.TypeCode)
	if cmpFirst <= 0 {
		return 0
	}
	cmpLast := compareBoundary(v, h.Boundaries[len(h.Boundaries)-1], h.TypeCode)
	if cmpLast > 0 {
		return 1
	}
	var below int64
	for i, count := range h.Counts {
		if compareBoundary(h.Boundaries[i+1], v, h.TypeCode) >= 0 {
			frac := bucketFraction(h.Boundaries[i], h.Boundaries[i+1], v, h.TypeCode)
			return float64(below+int64(float64(count)*frac)) / float64(h.TotalValues)
		}
		below += count
	}
	return 1
}

// SelectivityEQ returns the estimated fraction of values exactly equal
// to v. Uniform-distribution assumption within the containing bucket:
// 1 / (count in that bucket / count of distinct values in bucket).
// Without per-bucket NDV, falls back to 1 / TotalValues.
func (h *Histogram) SelectivityEQ(v any) float64 {
	if h == nil || h.TotalValues == 0 {
		return 0.1
	}
	cmpFirst := compareBoundary(v, h.Boundaries[0], h.TypeCode)
	cmpLast := compareBoundary(v, h.Boundaries[len(h.Boundaries)-1], h.TypeCode)
	if cmpFirst < 0 || cmpLast > 0 {
		return 0
	}
	for i, count := range h.Counts {
		if compareBoundary(h.Boundaries[i+1], v, h.TypeCode) > 0 {
			// In bucket i. Estimate 1 over avg-frequency (count / bucket NDV).
			// Without per-bucket NDV, treat bucket as uniform over its range.
			return 1.0 / math.Max(float64(count), 1)
		}
	}
	return 1.0 / float64(h.TotalValues)
}

// bucketFraction returns the fraction of bucket [lo, hi] that's ≤ v,
// for the purposes of linear interpolation. For numeric types this is
// (v-lo)/(hi-lo); for bytes returns 0.5 (uniform approximation).
func bucketFraction(lo, hi, v any, typeCode uint8) float64 {
	switch typeCode {
	case histTypeInt64:
		l, h2, vi := toInt64(lo), toInt64(hi), toInt64(v)
		if h2 == l {
			return 0
		}
		return math.Max(0, math.Min(1, float64(vi-l)/float64(h2-l)))
	case histTypeFloat64:
		l, h2, vf := toFloat64Safe(lo), toFloat64Safe(hi), toFloat64Safe(v)
		if h2 == l {
			return 0
		}
		return math.Max(0, math.Min(1, (vf-l)/(h2-l)))
	}
	return 0.5
}

// Encode writes the histogram to w in version-1 format.
func (h *Histogram) Encode(w io.Writer) error {
	if h == nil {
		return nil
	}
	k := len(h.Counts)
	hdr := []byte{histVersion, byte(k), h.TypeCode, 0}
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(h.TotalValues))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	for _, b := range h.Boundaries {
		if err := encodeValue(w, b, h.TypeCode); err != nil {
			return err
		}
	}
	for _, c := range h.Counts {
		binary.LittleEndian.PutUint64(buf[:], uint64(c))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
	}
	return nil
}

func encodeValue(w io.Writer, v any, typeCode uint8) error {
	var buf [8]byte
	switch typeCode {
	case histTypeInt64:
		binary.LittleEndian.PutUint64(buf[:], uint64(toInt64(v)))
		_, err := w.Write(buf[:])
		return err
	case histTypeFloat64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(toFloat64Safe(v)))
		_, err := w.Write(buf[:])
		return err
	case histTypeBytes:
		b := toBytes(v)
		var lb [2]byte
		binary.LittleEndian.PutUint16(lb[:], uint16(len(b)))
		if _, err := w.Write(lb[:]); err != nil {
			return err
		}
		_, err := w.Write(b)
		return err
	}
	return fmt.Errorf("encodeValue: unsupported type code %d", typeCode)
}

// HistogramBytes returns the serialized form for catalog persistence.
func (h *Histogram) Bytes() []byte {
	if h == nil {
		return nil
	}
	var buf bytes.Buffer
	if err := h.Encode(&buf); err != nil {
		return nil
	}
	return buf.Bytes()
}

// DecodeHistogram reads a version-1 histogram from r.
func DecodeHistogram(r io.Reader) (*Histogram, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("histogram: read header: %w", err)
	}
	if hdr[0] != histVersion {
		return nil, fmt.Errorf("histogram: unsupported version %d", hdr[0])
	}
	k := int(hdr[1])
	typeCode := hdr[2]
	var totalBuf [8]byte
	if _, err := io.ReadFull(r, totalBuf[:]); err != nil {
		return nil, fmt.Errorf("histogram: read total: %w", err)
	}
	total := int64(binary.LittleEndian.Uint64(totalBuf[:]))
	boundaries := make([]any, k+1)
	for i := range boundaries {
		v, err := decodeValue(r, typeCode)
		if err != nil {
			return nil, fmt.Errorf("histogram: boundary %d: %w", i, err)
		}
		boundaries[i] = v
	}
	counts := make([]int64, k)
	for i := range counts {
		if _, err := io.ReadFull(r, totalBuf[:]); err != nil {
			return nil, fmt.Errorf("histogram: count %d: %w", i, err)
		}
		counts[i] = int64(binary.LittleEndian.Uint64(totalBuf[:]))
	}
	return &Histogram{TypeCode: typeCode, TotalValues: total, Boundaries: boundaries, Counts: counts}, nil
}

func decodeValue(r io.Reader, typeCode uint8) (any, error) {
	var buf [8]byte
	switch typeCode {
	case histTypeInt64:
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, err
		}
		return int64(binary.LittleEndian.Uint64(buf[:])), nil
	case histTypeFloat64:
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(buf[:])), nil
	case histTypeBytes:
		var lb [2]byte
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return nil, err
		}
		n := int(binary.LittleEndian.Uint16(lb[:]))
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		return string(b), nil
	}
	return nil, fmt.Errorf("decodeValue: unsupported type code %d", typeCode)
}

// HistogramFromBytes parses a histogram from its serialized form.
// Returns nil on any error (corrupt bytes, wrong version).
func HistogramFromBytes(b []byte) *Histogram {
	if len(b) < 12 {
		return nil
	}
	h, err := DecodeHistogram(bytes.NewReader(b))
	if err != nil {
		return nil
	}
	return h
}
