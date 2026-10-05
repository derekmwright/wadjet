// SPDX-License-Identifier: MIT

package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A spill run keeps the mark (ADR-0024 §10): the columnar run format a grace
// join, a CTE collector and the external sort write and read back carries it
// beside the nullable flag, so a column created from an unconstrained numeric
// prints its own text after a spill as before one.
func TestSpillRunKeepsTheUnconstrainedMark(t *testing.T) {
	schema := []parquet.Column{
		{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true, Unconstrained: true},
		{Name: "n", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
	}
	b := batch.FromRows(schema, []map[string]any{{"v": "7", "n": "1.50"}})
	path, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{b})
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSpillBatches(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Schema[0].Unconstrained || got[0].Schema[1].Unconstrained || !got[0].Schema[0].Nullable {
		t.Fatalf("read back %+v: want v marked and nullable, n unmarked", got[0].Schema)
	}
	if v := got[0].Columns[0].GetValueOf(0, got[0].Schema[0].Unconstrained); v != "7" {
		t.Fatalf("v prints %v, want 7", v)
	}
}
