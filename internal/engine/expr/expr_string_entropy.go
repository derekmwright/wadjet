// This file holds expr string entropy; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"math"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- String entropy ---

// fnEntropy computes the Shannon entropy of a string in bits per character.
// High entropy (>4.5) suggests encoded, encrypted, or random data.
// Low entropy (<3.0) suggests natural language or repetitive patterns.
// entropy('aaaa') → 0.0
// entropy('hello world') → ~2.85
// entropy('a3f8b2c9e1d7') → ~3.58
func fnEntropy(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if len(s) == 0 {
		return float64(0)
	}
	freq := make(map[rune]int)
	total := 0
	for _, r := range s {
		freq[r]++
		total++
	}
	entropy := 0.0
	ft := float64(total)
	for _, count := range freq {
		p := float64(count) / ft
		entropy -= p * math.Log2(p)
	}
	return entropy
}

// columnInstant is the shared UTC-instant resolver for vectorized date parts and
// scalar FuncCall.resolveTemporalArgs; both paths must read the same units (#319).
// TypeDate reads Int32Data as epoch DAYS; TypeTimestamp reads Int64Data as epoch
// MILLISECONDS, matching TimestampMillis writes and parseTemporalInt64OK comparisons.
// String/Bytes parse text; other Int64Data values read as seconds, as parseTime(int64).
// Report whether resolution succeeded; never read a DATE from the absent Int64Data.
// See docs/internals/column-instant-storage-units.md for the design.
func columnInstant(src *batch.Vector, i int) (time.Time, bool) {
	switch src.Type {
	case batch.TypeString, batch.TypeBytes:
		t := parseTime(src.BytesData.StringValue(i))
		if t.IsZero() {
			return time.Time{}, false
		}
		return t, true
	case batch.TypeDate:
		if i < len(src.Int32Data) {
			// Days since the Unix epoch.
			return time.Unix(int64(src.Int32Data[i])*86400, 0).UTC(), true
		}
	case batch.TypeTimestamp:
		if i < len(src.Int64Data) {
			// Milliseconds since the Unix epoch.
			return time.UnixMilli(src.Int64Data[i]).UTC(), true
		}
	default:
		if i < len(src.Int64Data) {
			return time.Unix(src.Int64Data[i], 0).UTC(), true
		}
	}
	return time.Time{}, false
}
