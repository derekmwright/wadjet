package main

import (
	"fmt"
	"strings"
	"testing"
)

// TestCacheBytesHelpStringMatchesComputation pins --cache-bytes' help text
// to the SAME constants its auto-detect default is computed from
// (PersistentPreRunE: goMemLimit = detected memory *
// memoryEnvelopeNumerator / memoryEnvelopeDenominator; cacheBytes =
// goMemLimit / cacheBytesAutoDivisor when --memory-budget is left at its 0
// default). This is the regression test for a figure that drifted twice
// without one: the help string first said the auto-detect share was 20%
// (goMemLimit / 5 — an unrelated cap used only in a different branch),
// then, after that was fixed, "10% of memory" with no envelope/detected
// distinction — still wrong, since goMemLimit is itself only
// memoryEnvelopeNumerator/memoryEnvelopeDenominator of detected memory.
// Recomputing both expected percentages from the constants (rather than
// hard-coding "10%"/"7.5%" here) means this test fails if EITHER the
// constants or the string move without the other.
func TestCacheBytesHelpStringMatchesComputation(t *testing.T) {
	envelopeSharePct := 100 / cacheBytesAutoDivisor // % of goMemLimit that --cache-bytes auto-sizes to
	// % of detected memory, expressed in tenths of a percent to stay in
	// integer arithmetic (matches the real computation's operator order:
	// goMemLimit = detected * numerator / denominator).
	detectedShareTenths := 1000 * memoryEnvelopeNumerator / memoryEnvelopeDenominator / cacheBytesAutoDivisor

	flag := newRootCmd().PersistentFlags().Lookup("cache-bytes")
	if flag == nil {
		t.Fatal("--cache-bytes flag not registered")
	}

	wantEnvelope := fmt.Sprintf("%d%%", envelopeSharePct)
	if !strings.Contains(flag.Usage, wantEnvelope) {
		t.Errorf("--cache-bytes usage %q does not state the envelope share %q (goMemLimit / cacheBytesAutoDivisor=%d)",
			flag.Usage, wantEnvelope, cacheBytesAutoDivisor)
	}

	wantDetected := fmt.Sprintf("%d.%d%%", detectedShareTenths/10, detectedShareTenths%10)
	if !strings.Contains(flag.Usage, wantDetected) {
		t.Errorf("--cache-bytes usage %q does not state the detected-memory share %q (memoryEnvelopeNumerator=%d, memoryEnvelopeDenominator=%d, cacheBytesAutoDivisor=%d)",
			flag.Usage, wantDetected, memoryEnvelopeNumerator, memoryEnvelopeDenominator, cacheBytesAutoDivisor)
	}
}
