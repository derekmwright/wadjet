package exec

import (
	"context"
	"strings"
	"testing"
)

// TestSortMissingKeyColumnErrors pins the loud contract adopted for #386: a
// sort key that resolves to no input column is a planner bug, and the old
// behavior — silently skipping the key and emitting rows in arbitrary order —
// is how that bug class (#313/#314/#316/#386) stayed invisible. The first
// Consume must fail instead.
func TestSortMissingKeyColumnErrors(t *testing.T) {
	s := NewSort([]SortKey{{Column: "no_such_column"}})
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := s.Consume(context.Background(), nullPlacementBatch(t, int64(1), int64(2)))
	if err == nil {
		t.Fatal("Consume accepted a sort key that matches no input column; want an error")
	}
	if !strings.Contains(err.Error(), "no_such_column") {
		t.Fatalf("error should name the missing key column, got: %v", err)
	}
}

// TestSortQualifiedKeyResolvesBare guards the resolution the check relies
// on: a planner-qualified key ("t.v") must still bind a bare column via
// columnIndexFallback (#314), not trip the missing-key error.
func TestSortQualifiedKeyResolvesBare(t *testing.T) {
	s := NewSort([]SortKey{{Column: "t.v", Order: Descending}})
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Consume(context.Background(), nullPlacementBatch(t, int64(1), int64(3), int64(2))); err != nil {
		t.Fatalf("qualified key over bare column must resolve, got: %v", err)
	}
	got := drainNullable(t, s)
	want := []any{int64(3), int64(2), int64(1)}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted = %v, want %v", got, want)
		}
	}
}
