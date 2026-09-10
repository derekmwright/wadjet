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

// columnInstant resolves row i of a vector to the UTC instant it denotes, and
// reports whether it could. It is THE definition of "what time is stored in
// this column", and both evaluation paths go through it: the vectorized
// date-part kernels below call it directly, and the scalar path reaches it
// through (*FuncCall).resolveTemporalArgs. Divergence between the two paths is
// what let this defect survive — one shared resolver makes agreement
// structural rather than something a test has to keep rediscovering.
//
// A raw stored number carries no unit, and each type stores a different one:
//
//	TypeDate      Int32Data, days since the epoch
//	TypeTimestamp Int64Data, MILLISECONDS since the epoch (what the parquet
//	              writer emits — file_writer.go encodes TimestampMillis — and
//	              what the comparison path assumes, parseTemporalInt64OK)
//	String/Bytes  text, parsed
//	anything else Int64Data read as seconds, the only defensible reading of
//	              an untyped integer and what parseTime(int64) has always done
//
// Reading Int64Data unconditionally was right only for a timestamp-in-seconds
// column: a DATE column has nothing in Int64Data at all, so every row came
// back as 1970 — silently, with no error and no null, collapsing a decade of
// `GROUP BY EXTRACT(YEAR FROM d)` into one bogus bucket (issue #319).
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
