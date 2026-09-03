package expr

import (
	"fmt"
	"testing"
)

// The temporal parse memos are BOUNDED, and the bound is asserted by
// counting entries — a correctness assertion cannot see a leak (#619).
//
// The two sync.Maps were justified by this argument, quoted from their own
// comment: "SQL queries have a fixed, tiny set of date literals … so a
// memoization cache stays trivially small and never grows unbounded."
//
// The shape that argument describes NEVER REACHES THEM. compileCmp
// specializes a bare column against a string literal into CmpTemporalLit,
// which pre-parses the literal once at compile time through the UNCACHED
// parser; `ts <= '1998-09-02'` adds zero entries here, which the last case
// below pins. What does reach the cached fallback is by construction the
// shape the specialization declined — a column against another COLUMN's
// value — and those strings are DATA. The population is unbounded, and the
// map is process-global with no eviction: a query over a text column of
// timestamps adds one entry per distinct value, forever.
//
// So the cache pays unbounded memory for exactly the population its
// rationale excluded. The bound below is the smallest thing that makes the
// structure honest; it is a cap, not a policy, and the four assertions are
// what distinguishes a working set from a leak.
func TestTemporalParseMemosAreBounded(t *testing.T) {
	resetTemporalMemos()

	// (i) A large distinct population must not grow past the cap. Errorf,
	// not Fatalf: three assertions behind one fatal means the later two
	// never run when the first fails, which is how (ii) below stayed vacuous
	// unnoticed.
	for i := 0; i < 100_000; i++ {
		parseTimestampToEpochMsCachedOK(fmt.Sprintf("2024-01-01T00:00:%02d.%03dZ", i%60, i%1000))
		parseDateToEpochDaysCachedOK(fmt.Sprintf("%04d-01-01", 1000+i%9000))
	}
	if n := temporalMemoEntries(); n > temporalMemoCap*2 {
		t.Errorf("100,000 distinct strings left %d memo entries; the cap is %d per map. "+
			"An unbounded process-global memo over DATA is a leak, not a working set (#619).",
			n, temporalMemoCap)
	}

	// (ii) No growth across queries over DISJOINT string sets. This is the
	// property that distinguishes a leak from a working set: a cache that
	// keeps everything grows by its whole population every time.
	//
	// Each query has to contribute MORE distinct strings than the cap on its
	// own, or the assertion cannot fail on its own evidence. The first draft
	// wrote `i%20` — twenty distinct strings per query, eighty in total,
	// against a threshold of 8192 — so it could only ever trip on the
	// residue (i) left behind, three orders of magnitude away from testing
	// anything. The unit of this assertion is ONE query's own population.
	resetTemporalMemos()
	const perQuery = 20_000
	if perQuery <= temporalMemoCap {
		t.Fatalf("this assertion is vacuous by construction: %d distinct strings per "+
			"query is not more than the cap of %d", perQuery, temporalMemoCap)
	}
	after := make([]int, 0, 4)
	for q := 0; q < 4; q++ {
		for i := 0; i < perQuery; i++ {
			// A namespace per query, so no string is shared between them.
			parseTimestampToEpochMsCachedOK(fmt.Sprintf("2024-%02d-01T%02d:%02d:%02d.%03dZ",
				1+q, i/3600%24, i/60%60, i%60, i%1000))
		}
		after = append(after, temporalMemoEntries())
	}
	for i := range after {
		if after[i] > temporalMemoCap*2 {
			t.Errorf("after query %d over a DISJOINT population of %d the memos hold %d "+
				"entries (sequence %v); the cap is %d per map",
				i, perQuery, after[i], after, temporalMemoCap)
		}
	}
	if after[len(after)-1] > after[0]+temporalMemoCap {
		t.Errorf("the memos GREW across four disjoint query populations: %v. A working "+
			"set turns over; a leak accumulates (#619).", after)
	}

	// (iii) Strings that never parse must not be memoized without bound
	// either — a refusal is as cacheable as a success and just as unbounded.
	resetTemporalMemos()
	for i := 0; i < 20_000; i++ {
		parseTimestampToEpochMsCachedOK(fmt.Sprintf("not-a-timestamp-%d", i))
	}
	if n := temporalMemoEntries(); n > temporalMemoCap*2 {
		t.Errorf("20,000 never-parsing strings left %d memo entries; the cap is %d per map",
			n, temporalMemoCap)
	}
}

// TestTheDominantTemporalShapeBypassesTheMemo pins the finding that makes
// the bound above necessary: the literal comparison the memo's own comment
// describes is specialized at compile time and adds ZERO entries, so the
// memo's whole population is data. If this ever stops being true the bound
// still holds, but the reasoning above needs rewriting.
func TestTheDominantTemporalShapeBypassesTheMemo(t *testing.T) {
	resetTemporalMemos()
	// compileCmp's temporal-literal specialization pre-parses through the
	// UNCACHED entry points; this is that call, verbatim.
	if _, ok := parseDateToEpochDaysOK("1998-09-02"); !ok {
		t.Fatal("the fixture literal does not parse")
	}
	if _, ok := parseTimestampToEpochMsOK("1998-09-02T00:00:00Z"); !ok {
		t.Fatal("the fixture literal does not parse")
	}
	if n := temporalMemoEntries(); n != 0 {
		t.Fatalf("the compile-time literal parse added %d memo entries, want 0 — "+
			"the memo's stated rationale is the literal shape, which does not reach it", n)
	}
}

// resetTemporalMemos and temporalMemoEntries are the gate's observation
// surface. Entry COUNTS, not answers: a correctness assertion cannot see a
// leak, which is why this file exists beside temporal_parse_cache_test.go.
func resetTemporalMemos() {
	dateEpochDaysCache.reset()
	timestampEpochMsCache.reset()
}

func temporalMemoEntries() int {
	return dateEpochDaysCache.entries() + timestampEpochMsCache.entries()
}
