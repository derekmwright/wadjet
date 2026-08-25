package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestColumnCompareLitUUIDGarbageRaises is the regression for
// ColumnCompareLit's row-at-a-time UUID arm discarding
// kernel.UUIDLiteralToRaw's ok: `uuidVal, _ := kernel.UUIDLiteralToRaw(strVal)`
// silently compared against the empty string for a literal that names no
// UUID at all — matching no row for `=` and EVERY row for `<>`, the same
// wrong-in-two-directions answer networkConstError already documents for the
// vectorized kernel path (#519's IPv4/MAC/UUID close). ADR-0012 item 10's
// rule applies here too: a literal that names no address (or, for UUID, no
// value at all) is a query ERROR, never a match-nothing or
// match-everything predicate — and this row-at-a-time path (reached from
// physical/plan.go and worker/executor_fragment.go, not only the vectorized
// kernel) has no error return of its own, so it must raise through the
// pipeline's FatalEvalPanic shape instead.
func TestColumnCompareLitUUIDGarbageRaises(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "u", Type: parquet.TypeUUID},
	}, 1)
	b.Columns[0].BytesData.Set(0, make([]byte, 16))

	for _, tc := range []struct {
		name string
		op   CompareOp
	}{
		{"eq", OpEq},
		{"ne", OpNe},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pred := ColumnCompareLit("u", tc.op, "not-a-uuid", "")
			raised := func() (r any) {
				defer func() { r = recover() }()
				pred(b, 0)
				return nil
			}()
			if raised == nil {
				t.Fatalf("comparing a UUID column against a literal that names no UUID " +
					"answered instead of raising")
			}
			fe, ok := raised.(interface{ FatalEvalError() error })
			if !ok {
				t.Fatalf("panic value %#v is not the pipeline's FatalEvalPanic shape", raised)
			}
			err := fe.FatalEvalError()
			if err == nil || sqlerr.StateOf(err) != "22P02" {
				t.Fatalf("panic error = %v (SQLSTATE %q), want 22P02 (invalid_text_representation)",
					err, sqlerr.StateOf(err))
			}
		})
	}
}

// TestColumnCompareLitUUIDValidLiteralStillMatches pins that a WELL-FORMED
// UUID literal is unaffected by the ok-honoring fix above — only a literal
// with no reading at all is refused.
func TestColumnCompareLitUUIDValidLiteralStillMatches(t *testing.T) {
	const want = "00000000-0000-4000-8000-000000000001"
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "u", Type: parquet.TypeUUID},
	}, 2)
	raw, ok := kernel.UUIDLiteralToRaw(want)
	if !ok {
		t.Fatalf("test fixture literal %q does not parse as a UUID", want)
	}
	b.Columns[0].BytesData.Set(0, []byte(raw))
	b.Columns[0].BytesData.Set(1, make([]byte, 16))

	pred := ColumnCompareLit("u", OpEq, want, "")
	if !pred(b, 0) {
		t.Errorf("row 0 (%s) should match its own literal", want)
	}
	if pred(b, 1) {
		t.Errorf("row 1 (the zero UUID) should not match %s", want)
	}
}
