// SPDX-License-Identifier: MIT

package exec

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The names an exchange producer reports, and the stamp an exchange reader
// applies (ADR-0024 §10): a DECIMAL column marked in the written schema is
// reported; a name two columns share is reported only when both are marked;
// the stamp marks exactly the named DECIMAL columns, copies the shared schema
// rather than writing it, and reuses the copy for a batch of the same schema.
func TestUnconstrainedExchangeNamesAndStamp(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "V", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Unconstrained: true},
		{Name: "n", Type: parquet.TypeDecimal, Precision: 10, Scale: 2},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Unconstrained: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10},
	}
	if got, want := UnconstrainedNames(schema), []string{"v"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("UnconstrainedNames = %v, want %v", got, want)
	}

	decoded := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 10},
		{Name: "n", Type: parquet.TypeDecimal, Precision: 10, Scale: 2},
		{Name: "s", Type: parquet.TypeString},
	}
	st := NewUnconstrainedStamp([]string{"V", "s"})
	b1 := &batch.RecordBatch{Schema: decoded}
	st.Apply(b1)
	if !b1.Schema[1].Unconstrained || b1.Schema[2].Unconstrained || b1.Schema[3].Unconstrained {
		t.Fatalf("stamped schema %+v: want only v marked", b1.Schema)
	}
	if decoded[1].Unconstrained {
		t.Fatal("the decoded schema was written; the stamp must copy it")
	}
	b2 := &batch.RecordBatch{Schema: decoded}
	st.Apply(b2)
	if &b2.Schema[0] != &b1.Schema[0] {
		t.Fatal("a batch of the same decoded schema was stamped again rather than reusing the copy")
	}
	var none *UnconstrainedStamp
	b3 := &batch.RecordBatch{Schema: decoded}
	none.Apply(b3)
	if &b3.Schema[0] != &decoded[0] {
		t.Fatal("a nil stamp changed the schema")
	}
}

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
