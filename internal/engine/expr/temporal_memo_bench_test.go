package expr

import (
	"fmt"
	"testing"
)

// The A/B that decides whether the temporal memo earns its keep.
//
// ADR-0027's position on this arc's #619 was "measure whether removing the
// two memos is neutral; if neutral, delete them". The first draft of that
// work answered with an ARGUMENT (the memo's stated rationale describes a
// population that never reaches it) rather than a measurement, which is the
// right observation and the wrong method — the decision procedure named a
// number. This is the number, kept in the tree so the next person deciding
// the memo's fate re-runs it instead of re-arguing it.
//
//	go test -run XXX -bench BenchmarkTemporalMemo -benchtime 200000x \
//	    -benchmem ./internal/engine/expr/
//
// Read ns/op and allocs/op, never wall: the two arms interleave on a shared
// box. The Repeated arms model what the memo is FOR (one scan re-parsing
// the same handful of texts); the WorkingSet arms model what it actually
// sees on #826's path (a column's values, many distinct strings).
func BenchmarkTemporalMemoRepeatedCached(b *testing.B) {
	resetTemporalMemos()
	for i := 0; i < b.N; i++ {
		parseTimestampToEpochMsCachedOK("1998-09-02T00:00:00Z")
	}
}

func BenchmarkTemporalMemoRepeatedUncached(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseTimestampToEpochMsOK("1998-09-02T00:00:00Z")
	}
}

func BenchmarkTemporalMemoWorkingSetCached(b *testing.B) {
	resetTemporalMemos()
	texts := temporalMemoBenchTexts(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseTimestampToEpochMsCachedOK(texts[i%len(texts)])
	}
}

func BenchmarkTemporalMemoWorkingSetUncached(b *testing.B) {
	texts := temporalMemoBenchTexts(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseTimestampToEpochMsOK(texts[i%len(texts)])
	}
}

func temporalMemoBenchTexts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("2024-01-01T%02d:%02d:00Z", i/60%24, i%60)
	}
	return out
}
