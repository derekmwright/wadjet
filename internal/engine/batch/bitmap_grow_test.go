package batch

import "testing"

// TestBitmapGrowPreservesExcessBitsBug is a regression test for the
// distributed Q17 row-loss bug. NewBitmap(670) clears the high 34 bits
// of word 10 as excess; Grow(1340) should bring those bits back into
// scope and mark them VALID (bit=1), since the old "len" of 670 was
// bookkeeping, not a claim that those rows were null.
//
// Without the fix, Grow's "allocate new" branch (taken when newWords >
// len(b.data)) copies the old data verbatim and sets only the FRESH
// words to all-1s — leaving the previously-excess bits in the old last
// word as 0 (null). When this bitmap is later read by HashAggregate,
// rows 670..703 are treated as null GROUP BY keys and silently
// rerouted to strGroupStates, which the int-group output path then
// skips entirely.
func TestBitmapGrowPreservesExcessBitsBug(t *testing.T) {
	b := NewBitmap(670)
	// All 670 valid; bits 670..703 (high 34 bits of word 10) are excess
	// and zeroed by NewBitmap as padding.
	for i := 0; i < 670; i++ {
		if b.IsNull(i) {
			t.Fatalf("NewBitmap(670): bit %d should be valid", i)
		}
	}

	// Grow past the word boundary. Old data has 11 words; newLen=1340 needs 22.
	// Takes the allocate-new branch.
	b = b.Grow(1340)
	if b.Len() != 1340 {
		t.Fatalf("Grow(1340): Len=%d, want 1340", b.Len())
	}

	// All 1340 bits should be valid (no SetNull was called between).
	missing := 0
	for i := 0; i < 1340; i++ {
		if b.IsNull(i) {
			missing++
			if missing < 6 {
				t.Logf("bit %d unexpectedly null", i)
			}
		}
	}
	if missing > 0 {
		t.Fatalf("Grow(1340): %d bits incorrectly marked null (e.g., the 30..63 bits of word 10 = positions 670..703)", missing)
	}
}
