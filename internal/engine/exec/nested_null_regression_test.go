package exec

import (
	"context"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression tests for the advance-on-NULL / nested-copy sweep (2026-06-12).

func nestedRegRowSchema() parquet.Column {
	return parquet.Column{Name: "rec", Type: parquet.TypeRow, Fields: []parquet.Column{
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}
}

// TestSetVectorNull_NestedAdvance: join.go's setVectorNull (unmatched outer
// rows) interleaved with valid writes must keep ROW string children aligned.
// Live sweep repro: matched/unmatched/matched read back "aaabbb" not "bbb".
func TestSetVectorNull_NestedAdvance(t *testing.T) {
	dst := batch.NewRecordBatch([]parquet.Column{nestedRegRowSchema()}, 3)
	src := batch.NewRecordBatch([]parquet.Column{nestedRegRowSchema()}, 2)
	src.Columns[0].SetValue(0, map[string]any{"name": "aaa"})
	src.Columns[0].SetValue(1, map[string]any{"name": "bbb"})

	dst.Columns[0].CopyValueFrom(0, src.Columns[0], 0) // matched
	setVectorNull(dst.Columns[0], 1)                   // unmatched outer row
	dst.Columns[0].CopyValueFrom(2, src.Columns[0], 1) // matched

	if got := dst.Columns[0].GetValue(2); !reflect.DeepEqual(got, map[string]any{"name": "bbb"}) {
		t.Fatalf("row after setVectorNull: got %#v want {name:bbb}", got)
	}
	if dst.Columns[0].GetValue(1) != nil {
		t.Fatalf("unmatched row: got %#v want nil", dst.Columns[0].GetValue(1))
	}
}

// TestGatherVector_NestedDefault: gatherVector had NO case (and no default)
// for nested types — it returned having written nothing, leaving whatever
// the pooled destination batch held, rows still marked valid.
func TestGatherVector_NestedDefault(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	arrCol := parquet.Column{Name: "arr", Type: parquet.TypeArray, ElementType: &elem}

	src := batch.NewRecordBatch([]parquet.Column{arrCol}, 3)
	src.Columns[0].SetValue(0, []any{int64(1), int64(2)})
	src.Columns[0].SetValue(1, nil)
	src.Columns[0].SetValue(2, []any{int64(3)})

	dst := batch.NewRecordBatch([]parquet.Column{arrCol}, 3)
	gatherVector(dst.Columns[0], src.Columns[0], []int{2, 1, 0})

	if got := dst.Columns[0].GetValue(0); !reflect.DeepEqual(got, []any{int64(3)}) {
		t.Fatalf("gathered row 0: got %#v want [3]", got)
	}
	if dst.Columns[0].GetValue(1) != nil {
		t.Fatalf("gathered null row: got %#v want nil", dst.Columns[0].GetValue(1))
	}
	if got := dst.Columns[0].GetValue(2); !reflect.DeepEqual(got, []any{int64(1), int64(2)}) {
		t.Fatalf("gathered row 2: got %#v want [1 2]", got)
	}

	// Pooled-reuse half: a second gather into the SAME batch after Reset
	// must not see the first cycle's child data.
	dst.Reset(3)
	gatherVector(dst.Columns[0], src.Columns[0], []int{0, 0, 0})
	if got := dst.Columns[0].GetValue(0); !reflect.DeepEqual(got, []any{int64(1), int64(2)}) {
		t.Fatalf("pooled-reuse row 0: got %#v want [1 2]", got)
	}
	if dst.Columns[0].Child.Len != 6 {
		t.Fatalf("pooled-reuse child len = %d, want 6 (stale elements retained)", dst.Columns[0].Child.Len)
	}
}

// TestAppendRows_NestedNullAdvance: join_spill's appendRows (build-side
// freeze) used boxed SetValue(GetValue) — a null ROW row corrupted the
// frozen batch and a trailing-null ARRAY left Offsets[n] stale (spill
// truncation).
func TestAppendRows_NestedNullAdvance(t *testing.T) {
	elem := parquet.Column{Name: "element", Type: parquet.TypeInt64}
	schema := []parquet.Column{
		nestedRegRowSchema(),
		{Name: "arr", Type: parquet.TypeArray, ElementType: &elem, Nullable: true},
	}
	src := batch.NewRecordBatch(schema, 3)
	src.Columns[0].SetValue(0, map[string]any{"name": "aaa"})
	src.Columns[0].SetValue(1, nil)
	src.Columns[0].SetValue(2, map[string]any{"name": "bbb"})
	src.Columns[1].SetValue(0, []any{int64(7)})
	src.Columns[1].SetValue(1, []any{int64(8)})
	src.Columns[1].SetValue(2, nil) // trailing null in dst order

	dst := batch.NewRecordBatch(schema, 3)
	appendRows(dst, src, []int{0, 1, 2}, 0)

	if got := dst.Columns[0].GetValue(2); !reflect.DeepEqual(got, map[string]any{"name": "bbb"}) {
		t.Fatalf("frozen row after null: got %#v want {name:bbb}", got)
	}
	arr := dst.Columns[1]
	if got := arr.Offsets[3]; got != int32(arr.Child.Len) {
		t.Fatalf("trailing-null ARRAY Offsets[n] = %d, want child len %d (spill would truncate)", got, arr.Child.Len)
	}
}

// TestProject_PooledNestedDirectCopy: Project's cachedSchema dropped
// Fields/ElementType, so pooled destination vectors had nil Children/Child
// and every nested value was silently dropped (rows still valid).
func TestProject_PooledNestedDirectCopy(t *testing.T) {
	schema := []parquet.Column{nestedRegRowSchema()}
	p := NewProject([]ProjectColumn{{Name: "rec", Type: parquet.TypeRow, DirectCopy: "rec"}})
	if err := p.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	mk := func(vals ...any) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, len(vals))
		for i, v := range vals {
			b.Columns[0].SetValue(i, v)
		}
		return b
	}
	// Two Execute calls so the second goes through cachedSchema + pool.
	for round := 0; round < 2; round++ {
		out, err := p.Execute(context.Background(), mk(
			map[string]any{"name": "alpha"},
			nil,
			map[string]any{"name": "beta"},
		))
		if err != nil {
			t.Fatal(err)
		}
		if got := out.Columns[0].GetValue(0); !reflect.DeepEqual(got, map[string]any{"name": "alpha"}) {
			t.Fatalf("round %d row 0: got %#v want {name:alpha}", round, got)
		}
		if got := out.Columns[0].GetValue(2); !reflect.DeepEqual(got, map[string]any{"name": "beta"}) {
			t.Fatalf("round %d row 2: got %#v want {name:beta}", round, got)
		}
		out.Release()
	}
}
