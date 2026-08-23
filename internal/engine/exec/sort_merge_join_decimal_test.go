package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SortMergeJoin decides key EQUALITY with kernel.ResolveSortCompare — the
// same comparator ORDER BY uses (sort_merge_join.go, resolveCompareKernels).
// A DECIMAL key therefore inherits whatever that comparator says, and #394
// left two ways for it to be wrong: before the kernel arm existed every
// DECIMAL pair compared EQUAL, so the join emitted the full cross product;
// after it, unequal scales were rescaled through float64, so two keys one
// unscaled unit apart above 2^53 compared equal and the join emitted a row
// for a pair that does not match.
//
// The suite had no sort-merge join over a DECIMAL key at all, which is why
// neither showed up here.

func TestSortMergeJoin_DecimalKeysSameScale(t *testing.T) {
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
		{Name: "lv", Type: parquet.TypeString},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
		{Name: "rv", Type: parquet.TypeString},
	}
	// Values whose NUMERIC order differs from their lexicographic order, so a
	// comparator that fell back to formatted strings would mis-group them.
	leftRows := []map[string]any{
		{"k": "2.0002", "lv": "l-2.0002-a"},
		{"k": "10.001", "lv": "l-10.001"},
		{"k": "2.0002", "lv": "l-2.0002-b"},
		{"k": "-3.5", "lv": "l--3.5"},
		{"k": "0", "lv": "l-0"},
		{"k": "7.25", "lv": "l-7.25-unmatched"},
	}
	rightRows := []map[string]any{
		{"rk": "10.001", "rv": "r-10.001"},
		{"rk": "2.0002", "rv": "r-2.0002-a"},
		{"rk": "2.0002", "rv": "r-2.0002-b"},
		{"rk": "-3.5", "rv": "r--3.5"},
		{"rk": "0", "rv": "r-0"},
		{"rk": "9.99", "rv": "r-9.99-unmatched"},
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 2),
		chunkBatches(t, leftSchema, leftRows, 2))
	defer j.Close()

	// 2.0002: 2 left × 2 right, 10.001: 1×1, -3.5: 1×1, 0: 1×1 = 7.
	if len(got) != 7 {
		t.Fatalf("row count: got %d want 7 (a comparator that reports every DECIMAL "+
			"equal emits 36, one that mis-groups emits fewer)", len(got))
	}
	want := bruteInnerJoin(leftRows, rightRows, []string{"k"}, []string{"rk"})
	// Compare on the VALUE columns: the oracle holds the input spelling
	// ("0") while the join emits the DECIMAL rendering ("0.0"), and the
	// property under test is which rows paired with which.
	assertSameRows(t, got, want, []string{"lv", "rv"})
}

func TestSortMergeJoin_DecimalKeysDifferentScales(t *testing.T) {
	// Two separately declared DECIMAL columns: the join key types match
	// (resolveCompareKernels requires that) but the SCALES do not, which is
	// the only way the cross-scale comparator arm is reached in a join.
	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeDecimal, Precision: 38, Scale: 0},
		{Name: "lv", Type: parquet.TypeString},
	}
	rightSchema := []parquet.Column{
		{Name: "rk", Type: parquet.TypeDecimal, Precision: 38, Scale: 1},
		{Name: "rv", Type: parquet.TypeString},
	}
	// 2^53 and its neighbours: every one of these is a distinct number, and
	// every one of them collides with its neighbour in float64.
	leftRows := []map[string]any{
		{"k": "9007199254740993", "lv": "l-93"},
		{"k": "9007199254740992", "lv": "l-92"},
		{"k": "1", "lv": "l-1"},
	}
	rightRows := []map[string]any{
		{"rk": "9007199254740993.0", "rv": "r-93"},
		{"rk": "9007199254740992.0", "rv": "r-92"},
		{"rk": "1.0", "rv": "r-1"},
		{"rk": "0.5", "rv": "r-half"},
	}

	j := NewSortMergeJoin([]string{"k"}, []string{"rk"})
	got := runSMJ(t, j,
		chunkBatches(t, rightSchema, rightRows, 2),
		chunkBatches(t, leftSchema, leftRows, 2))
	defer j.Close()

	// Exactly the three pairs that are the same number. Under the float64
	// rescale, l-93/l-92 each matched BOTH r-93 and r-92: five rows.
	want := map[string]string{"l-93": "r-93", "l-92": "r-92", "l-1": "r-1"}
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d — %v", len(got), len(want), got)
	}
	for _, row := range got {
		lv, _ := row["lv"].(string)
		rv, _ := row["rv"].(string)
		if want[lv] != rv {
			t.Errorf("left %q joined right %q, want %q", lv, rv, want[lv])
		}
	}
}
