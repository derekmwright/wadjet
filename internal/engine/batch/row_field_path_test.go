package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ONE place the engine asks "is this dotted reference a ROW field path?",
// asserted from BOTH sides of its boundary (ADR-0022, #769).
//
// Four resolvers used to ask it for themselves — the single-process evaluator,
// the stage DAG's projection, the declaration half that types it, and the
// vectorized filters' ROW delegation — and each stripped the qualifier BEFORE
// asking, so a join arm publishing a column of the FIELD's name took the
// reference: `c_row.b` beside `decpair.b` answered the arm's DECIMAL where the
// field is an INT64, on every arm and in silence.
//
// The boundary is a CLAIM and is attempted from both sides: the container must
// DECLARE the field, so an ordinary qualified reference whose qualifier
// happens to name a ROW column is still read as a relation reference, and a
// field path naming no field still resolves to nothing here (which is what
// keeps #604's disposition unchanged).
func TestRowFieldPathIsAskedBeforeTheQualifierIsStripped(t *testing.T) {
	row := parquet.Column{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
		{Name: "a", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
	mk := func(cols ...parquet.Column) *RecordBatch {
		b := NewRecordBatch(cols, 1)
		b.Len = 1
		return b
	}
	flatB := parquet.Column{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}
	id := parquet.Column{Name: "id", Type: parquet.TypeInt64}

	for _, tc := range []struct {
		name   string
		batch  *RecordBatch
		ref    string
		parent int
		field  int
		ok     bool
	}{
		{
			// The defect's own shape: a ROW column and a scalar column of the
			// FIELD's name in one stream.
			name:  "a container beside a column of the field's name",
			batch: mk(id, row, flatB), ref: "c_row.b",
			parent: 1, field: 1, ok: true,
		},
		{
			// The join QUALIFIED the container, which is what a colliding
			// build column gets — the reference still has to find it.
			name:  "a container the join qualified",
			batch: mk(id, parquet.Column{Name: "x.c_row", Type: row.Type, Fields: row.Fields}, flatB),
			ref:   "c_row.b", parent: 1, field: 1, ok: true,
		},
		{
			// TWO arms spell the container: ambiguous, and declining keeps
			// that loud rather than picking an arm.
			name: "two containers spelled alike decline",
			batch: mk(parquet.Column{Name: "x.c_row", Type: row.Type, Fields: row.Fields},
				parquet.Column{Name: "y.c_row", Type: row.Type, Fields: row.Fields}),
			ref: "c_row.b", ok: false,
		},
		{
			// The BOUNDARY, first side: the container does not declare the
			// field, so this is not a field path and the ordinary resolution
			// answers for it.
			name: "a field the container does not declare", batch: mk(id, row, flatB),
			ref: "c_row.nosuch", ok: false,
		},
		{
			// The BOUNDARY, second side: an ordinary qualified reference
			// whose qualifier names a relation and not a container.
			name: "a qualified reference to a relation", batch: mk(id, row, flatB),
			ref: "d.b", ok: false,
		},
		{
			// A flat column literally called `a.b` — a Zeek `id.orig_h` — is
			// that column and not a path into anything.
			name:  "a flat column whose own name has a dot",
			batch: mk(id, row, parquet.Column{Name: "c_row.b", Type: parquet.TypeString}),
			ref:   "c_row.b", ok: false,
		},
		{
			// The FOLD applies here as it does everywhere else (#731): an
			// unquoted reference arrives lower-cased and the container
			// carries the parquet schema's own spelling.
			name: "the field name folds",
			batch: mk(id, parquet.Column{Name: "c_row", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "MixedField", Type: parquet.TypeInt64},
			}}),
			ref: "c_row.mixedfield", parent: 1, field: 0, ok: true,
		},
		{"a bare name is not a path", mk(id, row), "id", 0, 0, false},
		{"a trailing dot is not a path", mk(id, row), "c_row.", 0, 0, false},
		{"a leading dot is not a path", mk(id, row), ".b", 0, 0, false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			parent, field, ok := tc.batch.RowFieldPath(tc.ref)
			if ok != tc.ok {
				t.Fatalf("RowFieldPath(%q) ok = %v, want %v", tc.ref, ok, tc.ok)
			}
			if !ok {
				return
			}
			if parent != tc.parent || field != tc.field {
				t.Fatalf("RowFieldPath(%q) = (%d, %d), want (%d, %d)",
					tc.ref, parent, field, tc.parent, tc.field)
			}
		})
	}
}
