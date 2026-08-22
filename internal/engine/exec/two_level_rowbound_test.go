package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Construction-time group-index layout for UNBOUNDED sinks that know their
// INPUT SIZE — the second bound in two_level_hash.go
// (twoLevelAmortizeMultiple / twoLevelMinAmortizeRows).
//
// A flat→bucketed conversion is paid once, in full, at the moment it fires,
// and is repaid only by the rows that pass through the index afterwards. The
// runtime gate tests WHEN to convert but never whether anything is left to
// repay it. A DAG aggregate task does know: the coordinator sums the upstream
// partitions' reported row counts and declares the bound before Init. SF100
// Q18's `final_aggregate-7` reads ~6.25 M rows per task and holds ~6.25 M
// near-unique groups (merge mode ⇒ rows ≈ groups); it measured 4.14 s flat
// against 5.16–6.52 s bucketed, and it is the query's whole residual.

// runRowBoundFill consumes batches through one HashAggregate carrying the
// given input-row bound and reports what its group index did.
func runRowBoundFill(t *testing.T, bound int64, batches []*batch.RecordBatch) (conversions int64, bucketed, bornFlat bool, why indexFlatReason) {
	t.Helper()
	ctx := context.Background()
	h := NewHashAggregate([]string{"k"}, boundedTestAggs())
	h.SetInputRowBound(bound)
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	before := TwoLevelConversions.Load()
	for _, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	return TwoLevelConversions.Load() - before, h.intTwoLevel != nil, h.IndexBornFlat(), h.indexFlatWhy
}

// TestRowBoundedSinkIsBornFlat is the regression for the Q18
// `final_aggregate-7` shape: an UNBOUNDED sink (no epoch cap) whose declared
// input-row bound is under R* takes the flat layout and never converts, while
// the same fill with a bound at or above R* keeps the adaptive path.
func TestRowBoundedSinkIsBornFlat(t *testing.T) {
	// 24 batches × 2048 = 49 152 near-unique rows. Under the 4096-row
	// conversion threshold below, R* = 8 × 4096 = 32 768 rows, so the fill
	// straddles the rule: a bound of 49 152 is above R*, one of 20 000 below.
	batches := boundedTestBatches(24)
	const (
		convertAt   = 4096
		aboveStar   = int64(49152) // = the whole fill, > 8 × convertAt
		belowStar   = int64(20000) // < 8 × convertAt
		exactlyStar = int64(convertAt * twoLevelAmortizeMultiple)
	)

	cases := []struct {
		name        string
		bound       int64
		switchOff   bool
		wantFlat    bool
		wantConvert int64
		wantReason  indexFlatReason
	}{
		{
			name:        "no bound keeps the adaptive path",
			bound:       0,
			wantFlat:    false,
			wantConvert: 1,
		},
		{
			// The Q18 final-aggregate shape: rows ≈ groups, and too few of
			// either to repay the rehash.
			name:        "bound below R* is born flat",
			bound:       belowStar,
			wantFlat:    true,
			wantConvert: 0,
			wantReason:  flatReasonRowBound,
		},
		{
			// R* is a floor, not a window: at exactly R* the sink still has
			// R* − convertAt rows after the earliest conversion point.
			name:        "bound exactly at R* still converts",
			bound:       exactlyStar,
			wantFlat:    false,
			wantConvert: 1,
		},
		{
			// The ClickBench-class shape: many rows still to come after the
			// conversion, which is what pays for it.
			name:        "bound above R* still converts",
			bound:       aboveStar,
			wantFlat:    false,
			wantConvert: 1,
		},
		{
			name:        "kill switch restores runtime conversion",
			bound:       belowStar,
			switchOff:   true,
			wantFlat:    false,
			wantConvert: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.switchOff {
				prev := rowBoundToggle.Set(false)
				defer rowBoundToggle.Set(prev)
			}
			withTwoLevelStrict(t, true, convertAt, func() {
				conv, bucketed, bornFlat, reason := runRowBoundFill(t, tc.bound, batches)
				if bornFlat != tc.wantFlat {
					t.Errorf("bornFlat = %v, want %v (bound=%d, R*=%d)",
						bornFlat, tc.wantFlat, tc.bound, twoLevelMinAmortizeRows())
				}
				if conv != tc.wantConvert {
					t.Errorf("conversions = %d, want %d", conv, tc.wantConvert)
				}
				if bucketed != (tc.wantConvert > 0) {
					t.Errorf("index bucketed = %v, want %v", bucketed, tc.wantConvert > 0)
				}
				if reason != tc.wantReason {
					t.Errorf("flat reason = %q, want %q", reason, tc.wantReason)
				}
			})
		})
	}
}

// TestRowBoundIsMonotone pins the safety property the rule rests on: declaring
// a bound can only ever REMOVE a conversion, never add one. For every bound
// and every conversion threshold, the switch-on arm must pay no more
// conversions than the switch-off arm — so no shape can become bucketed that
// was not bucketed before.
func TestRowBoundIsMonotone(t *testing.T) {
	batches := boundedTestBatches(24)
	for _, convertAt := range []int{2048, 4096, 16384, 1 << 30} {
		for _, bound := range []int64{0, 1, 1024, 20000, 49152, 1 << 40} {
			withTwoLevelStrict(t, true, convertAt, func() {
				prev := rowBoundToggle.Set(false)
				off, offBucketed, _, _ := runRowBoundFill(t, bound, batches)
				rowBoundToggle.Set(true)
				on, onBucketed, _, _ := runRowBoundFill(t, bound, batches)
				rowBoundToggle.Set(prev)
				if on > off || (onBucketed && !offBucketed) {
					t.Fatalf("convertAt=%d bound=%d: on=%d/%v off=%d/%v — the row "+
						"bound must never CAUSE a conversion",
						convertAt, bound, on, onBucketed, off, offBucketed)
				}
			})
		}
	}
}

// TestRowBoundAndEpochCapCompose: the two construction-time bounds are
// independent, and either one alone pins the layout. The epoch cap is
// reported first because its ceiling is what the worker logs.
func TestRowBoundAndEpochCapCompose(t *testing.T) {
	batches := boundedTestBatches(24)
	ctx := context.Background()
	cases := []struct {
		name       string
		cap        int64
		bound      int64
		wantFlat   bool
		wantReason indexFlatReason
	}{
		{"neither", 0, 0, false, flatNotPinned},
		{"epoch cap only", defaultBenchPartialAggCap, 0, true, flatReasonEpochCap},
		{"row bound only", 0, 20000, true, flatReasonRowBound},
		{"both", defaultBenchPartialAggCap, 20000, true, flatReasonEpochCap},
		// A cap whose ceiling clears G* leaves the decision to the row
		// bound, which is the composition that matters: two bounds, one
		// verdict, taken once.
		{"cap above G*, row bound below R*", twoLevelBoundedMinGroups * 64, 20000, true, flatReasonRowBound},
		{"cap above G*, row bound above R*", twoLevelBoundedMinGroups * 64, 49152, false, flatNotPinned},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTwoLevelStrict(t, true, 4096, func() {
				h := NewHashAggregate([]string{"k"}, boundedTestAggs())
				h.SetEpochByteCap(tc.cap)
				h.SetInputRowBound(tc.bound)
				if err := h.Init(ctx); err != nil {
					t.Fatal(err)
				}
				defer h.Close()
				if err := h.Consume(ctx, batches[0]); err != nil {
					t.Fatal(err)
				}
				if got := h.IndexBornFlat(); got != tc.wantFlat {
					t.Errorf("bornFlat = %v, want %v", got, tc.wantFlat)
				}
				// Through the exported accessor: it is what the task logs
				// and any future A/B arm reads.
				if got := h.IndexFlatReason(); got != tc.wantReason.String() {
					t.Errorf("flat reason = %q, want %q", got, tc.wantReason)
				}
			})
		})
	}
}

// TestCloneInheritsRowBound: morsel clones consume a SUBSET of the parent's
// input, so the parent's bound bounds them too and must ride the clone. It is
// deliberately not divided by the clone count — clones take dynamic row
// slices, and a bound that over-states only ever keeps the adaptive path.
func TestCloneInheritsRowBound(t *testing.T) {
	h := NewHashAggregate([]string{"k"}, boundedTestAggs())
	h.SetInputRowBound(20000)
	clone := h.CloneSink().(*HashAggregate)
	if got := clone.InputRowBound(); got != 20000 {
		t.Fatalf("clone input row bound = %d, want 20000", got)
	}
}

// TestRStarBracketsTheMeasurements pins the derivation in
// twoLevelAmortizeMultiple against the two measurements that bracket it, so
// moving the multiple has to argue with them.
//
// A third measurement — BenchmarkAggIntCardinalitySweep's 4.19 M-group arm,
// which is 16.78 M rows (rows is fixed at 16 << 20 for every arm of that
// sweep; only groups varies) over 4.19 M groups, +25/+31 % bucketed — is
// deliberately NOT asserted here. Its 16.78 M rows already exceed R*, so the
// pure-row rule classifies it adaptive despite the measured loss: a known
// gap the rule does not cover (see the twoLevelAmortizeMultiple derivation
// comment), not something a row-only bracket can express.
//
// Skips under WADJET_TWO_LEVEL_AT: the row constants below are anchored to
// production's default twoLevelConvertAt (1 M ⇒ R* = 8 M) and do not hold at
// another threshold.
func TestRStarBracketsTheMeasurements(t *testing.T) {
	if twoLevelConvertAt != 1_000_000 {
		t.Skipf("twoLevelConvertAt = %d (WADJET_TWO_LEVEL_AT override?) — "+
			"the bracket below is anchored to the production default of 1,000,000",
			twoLevelConvertAt)
	}
	const (
		q18FinalRowsPerTask = 6_250_000 // SF100 Q18 final_aggregate-7: ~6.25M rows ≈ groups per task, a measured LOSS bucketed
		sweepNearUniqueRows = 16 << 20  // BenchmarkAggIntCardinalitySweep's near-unique arm: groups == rows == 16.78M, a measured WIN bucketed (-4.1/-11%)
	)
	rstar := twoLevelMinAmortizeRows()
	if rstar <= q18FinalRowsPerTask {
		t.Errorf("R* = %d must exceed the Q18 final aggregate's %d rows per task — "+
			"that shape measures as a loss bucketed", rstar, q18FinalRowsPerTask)
	}
	if rstar > sweepNearUniqueRows {
		t.Errorf("R* = %d must not exceed the sweep's %d-row near-unique win arm — "+
			"above it the bucketed layout is measured to pay", rstar, sweepNearUniqueRows)
	}
}
