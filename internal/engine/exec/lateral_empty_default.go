package exec

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// LateralEmptyDefault carries an ungrouped aggregate's EMPTY-INPUT VALUE on
// the lateral's own output column, above the join that manufactured the row.
//
// PostgreSQL evaluates a LATERAL subquery once per outer row, and an UNGROUPED
// aggregate over an empty input still yields a row — `COUNT(*)` is 0 there,
// not NULL. This engine decorrelates the subquery into a join, so an outer row
// the lateral matches nothing for survives only as a LEFT-JOIN pad, and a pad
// writes NULL.
//
// The planner used to repair that by rewriting the ENCLOSING query's
// references to `COALESCE(<ref>, 0)`. That reaches a named reference and
// nothing else: a `SELECT *` has no reference to rewrite — it expands in a
// later pass, and a star over a JOIN is never expanded at all — so the column
// read NULL where PostgreSQL reads 0, for exactly the rows the pad
// manufactures. A refusal was tried in its place and refused queries whose
// outer rows all match, which is a right answer taken away (#977).
//
// The default belongs on the COLUMN, not on the references to it. This
// operator sits directly above the join and fills the column's own position,
// so a star, a derived table's star, a CTE's star and the wire all see 0
// without anybody rewriting anything.
//
// Only the COUNT family is defaulted, and only for an ungrouped aggregate:
// COUNT never yields NULL over a non-empty input, so a NULL in that column IS
// the pad. MAX over an empty input IS NULL, so those columns are not in the
// list and their stars were always right.
type LateralEmptyDefault struct {
	// Cols are the output columns whose empty-input value is 0, folded.
	Cols []string

	resolved bool
	idx      []int
}

// NewLateralEmptyDefault returns the operator for cols, or nil when there is
// nothing to default.
func NewLateralEmptyDefault(cols []string) *LateralEmptyDefault {
	if len(cols) == 0 {
		return nil
	}
	return &LateralEmptyDefault{Cols: append([]string(nil), cols...)}
}

func (o *LateralEmptyDefault) Init(_ context.Context) error { return nil }

func (o *LateralEmptyDefault) Close() error { return nil }

func (o *LateralEmptyDefault) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || in.Len == 0 {
		return in, nil
	}
	if !o.resolved {
		o.idx = o.idx[:0]
		want := make(map[string]bool, len(o.Cols))
		for _, c := range o.Cols {
			want[batch.FoldIdent(strings.TrimSpace(c))] = true
		}
		for i, col := range in.Schema {
			name := col.Name
			// A build column that collided with a probe column is emitted
			// qualified ("s.n"); it is the same column either way.
			if dot := strings.IndexByte(name, '.'); dot > 0 && dot < len(name)-1 {
				name = name[dot+1:]
			}
			if want[batch.FoldIdent(name)] {
				o.idx = append(o.idx, i)
			}
		}
		o.resolved = true
	}
	for _, ci := range o.idx {
		if ci >= len(in.Columns) {
			continue
		}
		v := in.Columns[ci]
		if v == nil || !v.Nulls.HasNulls() {
			continue
		}
		// A VIEW shares its base vector's storage with other consumers, so
		// writing through it would edit rows this batch does not own. The
		// deferred gather materializes it first.
		if v.IsView() {
			in.FlattenColumn(ci)
			v = in.Columns[ci]
			if v == nil || !v.Nulls.HasNulls() {
				continue
			}
		}
		for r := 0; r < in.Len; r++ {
			if !v.Nulls.IsNull(r) {
				continue
			}
			switch v.Type {
			case batch.TypeInt64:
				v.Int64Data[r] = 0
			case batch.TypeInt32:
				v.Int32Data[r] = 0
			case batch.TypeFloat64:
				v.Float64Data[r] = 0
			case batch.TypeFloat32:
				v.Float32Data[r] = 0
			default:
				// A type this cannot write is left NULL rather than filled
				// with a value invented for it — an aggregate whose empty
				// value is 0 arrives as one of the four above, and anything
				// else means the plan and the runtime disagree about which
				// column this is.
				continue
			}
			v.Nulls.SetValid(r)
		}
	}
	return in, nil
}
