package memory

import (
	"testing"
)

// TestSpillManager_TrackingOnlyView verifies the morsel-clone accounting
// contract: the view charges the SAME tracker as the real manager but never
// asks its operators to spill, regardless of tracker pressure or the
// heap-pressure backstop.
func TestSpillManager_TrackingOnlyView(t *testing.T) {
	tracker := NewTracker("test", 1000)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatalf("NewSpillManager: %v", err)
	}
	view := sm.TrackingOnlyView()

	// Charges through the view are visible to the real manager's tracker.
	view.TrackBatch(900)
	if got := tracker.Used(); got != 900 {
		t.Fatalf("tracker.Used = %d, want 900 (view charge must hit the shared tracker)", got)
	}

	// 900/1000 is past every static threshold: the REAL manager wants spill,
	// the view never does.
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Error("real manager at 90%% of budget should want SpillCheap")
	}
	if view.ShouldSpillFor(SpillCheap) || view.ShouldSpillFor(SpillExpensive) {
		t.Error("tracking-only view must never request spill")
	}
	if view.ShouldSpill() {
		t.Error("tracking-only view ShouldSpill must be false")
	}

	view.ReleaseTracking(900)
	if got := tracker.Used(); got != 0 {
		t.Fatalf("tracker.Used after release = %d, want 0", got)
	}
}
