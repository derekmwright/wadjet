package scan

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestCanPruneRowGroup(t *testing.T) {
	stats := pqt.RowGroupStats{
		NumRows: 1000,
		Columns: map[string]pqt.ColumnStats{
			"age": {
				HasStats:  true,
				MinValue:  int64(18),
				MaxValue:  int64(65),
				NullCount: 0,
			},
			"name": {
				HasStats:  true,
				MinValue:  "alice",
				MaxValue:  "zara",
				NullCount: 10,
			},
			"empty": {
				HasStats:  true,
				MinValue:  nil,
				MaxValue:  nil,
				NullCount: 1000,
			},
		},
	}

	tests := []struct {
		name   string
		pred   StatsPredicate
		prune  bool
	}{
		// age range [18, 65]
		{"age > 100 (above max)", StatsPredicate{"age", exec.OpGt, int64(100)}, true},
		{"age > 50 (within range)", StatsPredicate{"age", exec.OpGt, int64(50)}, false},
		{"age < 10 (below min)", StatsPredicate{"age", exec.OpLt, int64(10)}, true},
		{"age < 30 (within range)", StatsPredicate{"age", exec.OpLt, int64(30)}, false},
		{"age = 70 (above max)", StatsPredicate{"age", exec.OpEq, int64(70)}, true},
		{"age = 5 (below min)", StatsPredicate{"age", exec.OpEq, int64(5)}, true},
		{"age = 30 (in range)", StatsPredicate{"age", exec.OpEq, int64(30)}, false},
		{"age >= 66 (above max)", StatsPredicate{"age", exec.OpGe, int64(66)}, true},
		{"age >= 65 (at max)", StatsPredicate{"age", exec.OpGe, int64(65)}, false},
		{"age <= 17 (below min)", StatsPredicate{"age", exec.OpLe, int64(17)}, true},
		{"age <= 18 (at min)", StatsPredicate{"age", exec.OpLe, int64(18)}, false},

		// string column
		{"name = 'aaa' (below min)", StatsPredicate{"name", exec.OpEq, "aaa"}, true},
		{"name = 'zzz' (above max)", StatsPredicate{"name", exec.OpEq, "zzz"}, true},
		{"name = 'bob' (in range)", StatsPredicate{"name", exec.OpEq, "bob"}, false},

		// all-null column
		{"empty is null (all null)", StatsPredicate{"empty", exec.OpIsNull, nil}, false},
		{"empty is not null (all null)", StatsPredicate{"empty", exec.OpIsNotNull, nil}, true},
		{"empty = 5 (all null)", StatsPredicate{"empty", exec.OpEq, int64(5)}, true},

		// missing column — can't prune
		{"missing col", StatsPredicate{"nonexistent", exec.OpEq, int64(1)}, false},

		// no stats
		{"no stats col", StatsPredicate{"nostats", exec.OpEq, int64(1)}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanPruneRowGroup(tt.pred, stats)
			if got != tt.prune {
				t.Fatalf("expected prune=%v, got %v", tt.prune, got)
			}
		})
	}
}
