package memory

import (
	"errors"
	"sync"
	"testing"
)

func TestTrackerName(t *testing.T) {
	tr := NewTracker("my-tracker", 1024)
	if tr.Name() != "my-tracker" {
		t.Fatalf("expected 'my-tracker', got %q", tr.Name())
	}
}

func TestTrackerBudget(t *testing.T) {
	tr := NewTracker("test", 4096)
	if tr.Budget() != 4096 {
		t.Fatalf("expected budget=4096, got %d", tr.Budget())
	}
}

func TestTrackerReset(t *testing.T) {
	tr := NewTracker("test", 1024)
	tr.Reserve(512)
	if tr.Used() != 512 {
		t.Fatalf("expected used=512, got %d", tr.Used())
	}
	tr.Reset()
	if tr.Used() != 0 {
		t.Fatalf("expected used=0 after reset, got %d", tr.Used())
	}
}

func TestTrackerReserveExactBudget(t *testing.T) {
	tr := NewTracker("test", 1024)
	if err := tr.Reserve(1024); err != nil {
		t.Fatalf("exact budget reservation should succeed: %v", err)
	}
	if tr.Used() != 1024 {
		t.Fatalf("expected used=1024, got %d", tr.Used())
	}
	// One more byte should fail
	if err := tr.Reserve(1); !errors.Is(err, ErrMemoryExceeded) {
		t.Fatal("reservation beyond budget should fail")
	}
}

func TestTrackerReserveZero(t *testing.T) {
	tr := NewTracker("test", 1024)
	if err := tr.Reserve(0); err != nil {
		t.Fatalf("zero reservation should succeed: %v", err)
	}
	if tr.Used() != 0 {
		t.Fatalf("expected used=0, got %d", tr.Used())
	}
}

func TestTrackerChildName(t *testing.T) {
	parent := NewTracker("parent", 1024)
	child := parent.Child("child-op")
	if child.Name() != "child-op" {
		t.Fatalf("expected child name 'child-op', got %q", child.Name())
	}
}

func TestTrackerChildBudgetShared(t *testing.T) {
	parent := NewTracker("query", 1024)
	child1 := parent.Child("op1")
	child2 := parent.Child("op2")

	if err := child1.Reserve(600); err != nil {
		t.Fatal(err)
	}
	// child2 trying to reserve 600 should fail (parent has 600 used, budget 1024)
	if err := child2.Reserve(600); !errors.Is(err, ErrMemoryExceeded) {
		t.Fatal("expected ErrMemoryExceeded for shared budget")
	}
	// After child2 fails, child2 usage should be 0
	if child2.Used() != 0 {
		t.Fatalf("child2 used should be 0 after failed reserve, got %d", child2.Used())
	}
	// Parent should still show 600
	if parent.Used() != 600 {
		t.Fatalf("parent used should be 600, got %d", parent.Used())
	}
}

func TestTrackerChildReleasePropagation(t *testing.T) {
	parent := NewTracker("query", 2048)
	child := parent.Child("sort")

	child.Reserve(1000)
	if parent.Used() != 1000 {
		t.Fatalf("parent should reflect child usage")
	}

	child.Release(500)
	if parent.Used() != 500 {
		t.Fatalf("parent should reflect child release, got %d", parent.Used())
	}
	if child.Used() != 500 {
		t.Fatalf("child should show 500, got %d", child.Used())
	}
}

func TestTrackerGrandchild(t *testing.T) {
	root := NewTracker("root", 1024)
	child := root.Child("query")
	grandchild := child.Child("operator")

	if err := grandchild.Reserve(512); err != nil {
		t.Fatal(err)
	}
	if root.Used() != 512 {
		t.Fatal("root should reflect grandchild usage")
	}
	if child.Used() != 512 {
		t.Fatal("child should reflect grandchild usage")
	}

	grandchild.Release(512)
	if root.Used() != 0 {
		t.Fatal("root should be 0 after release")
	}
}

func TestTrackerConcurrentReserveRelease(t *testing.T) {
	tr := NewTracker("concurrent", 1_000_000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				tr.Reserve(100)
				tr.Release(100)
			}
		}()
	}
	wg.Wait()
	if tr.Used() != 0 {
		t.Fatalf("expected 0 after balanced reserve/release, got %d", tr.Used())
	}
}

func TestTrackerNegativeBudget(t *testing.T) {
	// Negative budget should behave like unlimited (budget <= 0)
	tr := NewTracker("test", -1)
	if err := tr.Reserve(1 << 30); err != nil {
		t.Fatalf("negative budget should act unlimited: %v", err)
	}
}

func TestTrackerErrorMessage(t *testing.T) {
	tr := NewTracker("my-query", 100)
	tr.Reserve(50)
	err := tr.Reserve(200)
	if err == nil {
		t.Fatal("expected error")
	}
	errMsg := err.Error()
	if !errors.Is(err, ErrMemoryExceeded) {
		t.Fatal("expected ErrMemoryExceeded")
	}
	// Check that error message contains useful info
	if !containsStr(errMsg, "my-query") {
		t.Error("error should contain tracker name")
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && findSubstr(s, substr)
}

func findSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
