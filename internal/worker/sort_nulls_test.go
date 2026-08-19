package worker

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// TestSortKeyNullPlacement pins #330 at the seam where it was lost: the wire
// spec carried Column and Desc and nothing about NULLs, so every distributed
// sort used Go's zero value — NULLs first — no matter what the SQL asked.
// Ascending order was wrong (SQL means NULLS LAST), an explicit NULLS LAST
// was dropped, and descending was correct by accident.
func TestSortKeyNullPlacement(t *testing.T) {
	last, first := true, false
	cases := []struct {
		name string
		spec distributed.SortKeySpec
		want bool // NullsLast
	}{
		// No explicit placement: the engine default is PostgreSQL's rule —
		// NULLS LAST for ASC, NULLS FIRST for DESC (see
		// SortKeySpec.PlaceNullsLast). This is also the only safe reading of
		// a spec written before the field existed, since that default is
		// what the SQL asked for.
		{"ascending default", distributed.SortKeySpec{Column: "k"}, true},
		{"descending default", distributed.SortKeySpec{Column: "k", Desc: true}, false},
		// Explicit placement wins in both directions. The DESC pair is
		// #343's: both spellings reached the executor intact and were then
		// inverted by the comparator's direction negation.
		{"ascending NULLS FIRST", distributed.SortKeySpec{Column: "k", NullsLast: &first}, false},
		{"ascending NULLS LAST", distributed.SortKeySpec{Column: "k", NullsLast: &last}, true},
		{"descending NULLS FIRST", distributed.SortKeySpec{Column: "k", Desc: true, NullsLast: &first}, false},
		{"descending NULLS LAST", distributed.SortKeySpec{Column: "k", Desc: true, NullsLast: &last}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.spec.PlaceNullsLast(); got != tc.want {
				t.Errorf("PlaceNullsLast() = %v, want %v", got, tc.want)
			}
			e := &Executor{}
			sorter, err := e.buildFragmentSort(distributed.OpSpec{
				Type:         distributed.OpSort,
				SortKeySpecs: []distributed.SortKeySpec{tc.spec},
			})
			if err != nil {
				t.Fatalf("buildFragmentSort: %v", err)
			}
			key := sorter.Keys[0]
			if key.NullsLast != tc.want {
				t.Errorf("exec.SortKey.NullsLast = %v, want %v", key.NullsLast, tc.want)
			}
			wantOrder := exec.Ascending
			if tc.spec.Desc {
				wantOrder = exec.Descending
			}
			if key.Order != wantOrder {
				t.Errorf("exec.SortKey.Order = %v, want %v", key.Order, wantOrder)
			}
		})
	}
}
