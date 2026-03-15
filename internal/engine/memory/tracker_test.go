package memory

import (
	"errors"
	"testing"
)

func TestTracker_BasicReserveRelease(t *testing.T) {
	tr := NewTracker("test", 1024)

	if err := tr.Reserve(512); err != nil {
		t.Fatal(err)
	}
	if tr.Used() != 512 {
		t.Fatalf("expected used=512, got %d", tr.Used())
	}

	tr.Release(256)
	if tr.Used() != 256 {
		t.Fatalf("expected used=256, got %d", tr.Used())
	}
}

func TestTracker_BudgetExceeded(t *testing.T) {
	tr := NewTracker("test", 1024)

	err := tr.Reserve(2048)
	if !errors.Is(err, ErrMemoryExceeded) {
		t.Fatalf("expected ErrMemoryExceeded, got %v", err)
	}
	if tr.Used() != 0 {
		t.Fatalf("expected used=0 after failed reserve, got %d", tr.Used())
	}
}

func TestTracker_ChildHierarchy(t *testing.T) {
	parent := NewTracker("query", 1024)
	child := parent.Child("operator")

	if err := child.Reserve(512); err != nil {
		t.Fatal(err)
	}
	if parent.Used() != 512 {
		t.Fatalf("expected parent used=512, got %d", parent.Used())
	}
	if child.Used() != 512 {
		t.Fatalf("expected child used=512, got %d", child.Used())
	}

	// Child reservation exceeding parent budget should fail
	err := child.Reserve(600)
	if !errors.Is(err, ErrMemoryExceeded) {
		t.Fatalf("expected ErrMemoryExceeded, got %v", err)
	}

	child.Release(512)
	if parent.Used() != 0 {
		t.Fatalf("expected parent used=0, got %d", parent.Used())
	}
}

func TestTracker_UnlimitedBudget(t *testing.T) {
	tr := NewTracker("unlimited", 0)

	if err := tr.Reserve(1 << 30); err != nil {
		t.Fatalf("unlimited tracker should accept any reservation: %v", err)
	}
}
