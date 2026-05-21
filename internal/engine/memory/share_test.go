package memory

import "testing"

func TestTrackerShareGetterSetter(t *testing.T) {
	tr := NewTracker("t", 1000)
	if got := tr.Share(); got != 0 {
		t.Errorf("Share() default = %d, want 0", got)
	}
	tr.SetShare(300)
	if got := tr.Share(); got != 300 {
		t.Errorf("Share() after SetShare(300) = %d, want 300", got)
	}
	// Share is independent of Budget.
	if got := tr.Budget(); got != 1000 {
		t.Errorf("Budget() = %d, want 1000 (SetShare must not touch Budget)", got)
	}
}

func TestShouldSpillForTaskShare_FiresOnPerTaskThreshold(t *testing.T) {
	// Shared tracker with one heavy task that doesn't push cumulative
	// over the shared threshold but exceeds its own fair share. The
	// per-task check should fire, the shared-only check should not.
	shared := NewTracker("shared", 1000)
	sm, err := NewSpillManager(t.TempDir(), shared)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()
	// Per-task tracker representing one of three concurrent tasks.
	task := shared.Child("task-1")
	task.SetShare(1000 / 3) // ~333 bytes

	// Reserve 200 bytes on the task — over 40% of share (133) but well
	// under 40% of shared budget (400).
	if err := task.Reserve(200); err != nil {
		t.Fatal(err)
	}
	if sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("shared-only check fired at cumulative=200/1000 (20%%); expected false")
	}
	if !sm.ShouldSpillForTaskShare(SpillCheap, task) {
		t.Errorf("per-task check did not fire at task=200/share=333 (60%%); expected true")
	}
}

func TestShouldSpillForTaskShare_FallsBackToShared(t *testing.T) {
	// When the per-task tracker has no Share set, the check falls
	// through to the shared-pool threshold. Confirms backward-compat
	// for callers that don't yet configure per-task share.
	shared := NewTracker("shared", 1000)
	sm, err := NewSpillManager(t.TempDir(), shared)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()
	task := shared.Child("task-1") // no SetShare

	// Reserve 500 bytes — over the 40% shared threshold (400).
	if err := task.Reserve(500); err != nil {
		t.Fatal(err)
	}
	if !sm.ShouldSpillForTaskShare(SpillCheap, task) {
		t.Errorf("expected fallback shared-pool check to fire at 500/1000 (50%%)")
	}
}

func TestShouldSpillForTaskShare_NilTrackerSafe(t *testing.T) {
	// Operators that aren't wired with a tracker must not panic when
	// they pass nil. ShouldSpillForTaskShare(nil) should fall back to
	// the shared-pool check as if called without a tracker argument.
	shared := NewTracker("shared", 1000)
	sm, err := NewSpillManager(t.TempDir(), shared)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Below shared threshold → false.
	_ = shared.Reserve(100)
	if sm.ShouldSpillForTaskShare(SpillCheap, nil) {
		t.Errorf("expected false at shared=100/1000 (10%%)")
	}
	// Above shared threshold → true via fallback.
	_ = shared.Reserve(500)
	if !sm.ShouldSpillForTaskShare(SpillCheap, nil) {
		t.Errorf("expected true via fallback at shared=600/1000 (60%%)")
	}
}

func TestShouldSpillForTaskShare_ExpensiveUrgency(t *testing.T) {
	// SpillExpensive must use the 90% threshold against share, not 40%.
	shared := NewTracker("shared", 1000)
	sm, err := NewSpillManager(t.TempDir(), shared)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()
	task := shared.Child("task-1")
	task.SetShare(300)

	// 180 / 300 = 60%: under SpillExpensive (90%), over SpillCheap (40%).
	if err := task.Reserve(180); err != nil {
		t.Fatal(err)
	}
	if !sm.ShouldSpillForTaskShare(SpillCheap, task) {
		t.Errorf("SpillCheap should fire at task=180/share=300 (60%%)")
	}
	if sm.ShouldSpillForTaskShare(SpillExpensive, task) {
		t.Errorf("SpillExpensive should not fire at task=180/share=300 (60%%)")
	}
	// Push to 280/300 = 93% — over SpillExpensive threshold (90%).
	if err := task.Reserve(100); err != nil {
		t.Fatal(err)
	}
	if !sm.ShouldSpillForTaskShare(SpillExpensive, task) {
		t.Errorf("SpillExpensive should fire at task=280/share=300 (93%%)")
	}
}

func TestChildTrackerUsedIsPerTask(t *testing.T) {
	// Child.Used() must track only THIS task's reservations, not the
	// parent's cumulative. This is what makes the per-task share check
	// meaningful — without it, all child trackers see the same Used()
	// and the share threshold can't differentiate heavy vs light tasks.
	parent := NewTracker("parent", 10000)
	a := parent.Child("a")
	b := parent.Child("b")

	if err := a.Reserve(500); err != nil {
		t.Fatal(err)
	}
	if err := b.Reserve(100); err != nil {
		t.Fatal(err)
	}

	if got := a.Used(); got != 500 {
		t.Errorf("a.Used() = %d, want 500", got)
	}
	if got := b.Used(); got != 100 {
		t.Errorf("b.Used() = %d, want 100", got)
	}
	if got := parent.Used(); got != 600 {
		t.Errorf("parent.Used() = %d, want 600 (cumulative)", got)
	}
}
