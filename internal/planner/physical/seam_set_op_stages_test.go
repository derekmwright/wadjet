// SPDX-License-Identifier: MIT

package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestAlignSetOpRows covers the single-process half: set-operation arms
// correspond by POSITION, but the pipeline's rows are name-keyed maps. Before
// the alignment, `SELECT a FROM t UNION SELECT b FROM u` deduped nothing (no
// two maps shared a key) and the batch built from the first arm's schema read
// the second arm's values under names it does not carry, writing NULLs.
func TestAlignSetOpRows(t *testing.T) {
	left := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString}}
	right := []parquet.Column{{Name: "x", Type: parquet.TypeInt64}, {Name: "y", Type: parquet.TypeString}}
	rows := []map[string]any{{"x": int64(1), "y": "one"}, {"x": int64(2), "y": "two"}}

	got := alignSetOpRows(left, right, rows)
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	if got[0]["a"] != int64(1) || got[0]["b"] != "one" {
		t.Errorf("row 0 = %v, want the right arm's values under the left arm's names", got[0])
	}
	if _, stale := got[1]["x"]; stale {
		t.Errorf("row 1 = %v still carries the right arm's own column names", got[1])
	}

	// Identical schemas are returned untouched (same backing slice).
	same := alignSetOpRows(left, left, rows)
	if &same[0] != &rows[0] {
		t.Error("matching schemas were needlessly re-keyed")
	}
	// A width mismatch is malformed, not something to paper over.
	if got := alignSetOpRows(left, right[:1], rows); &got[0] != &rows[0] {
		t.Error("mismatched widths were re-keyed")
	}
}
