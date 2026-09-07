package exec

import (
	"context"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// LateralDefault is one output column's EMPTY-INPUT value: the constant the
// lateral's SELECT item evaluates to when the subquery sees no rows, rendered
// as text so one spelling crosses the wire and both paths parse it at the
// vector's own type. See logical.LateralEmptyDefault.
type LateralDefault struct {
	Column string
	Text   string
}

// LateralEmptyDefault carries an ungrouped aggregate's EMPTY-INPUT VALUES on
// the lateral's own output columns, above the join that manufactured the row.
//
// PostgreSQL evaluates a LATERAL once per outer row, and an UNGROUPED
// aggregate over an empty input still yields a row: `COUNT(*)` is 0 there,
// `SUM(x)` is NULL, and an item BUILT from them is that item over those
// values. This engine decorrelates the subquery into a join, so an outer row
// the lateral matches nothing for survives only as a LEFT pad — and a pad
// writes NULL into every column of that side.
//
// TWO RULES, and the first cut had neither.
//
//  1. WHICH ROWS. A pad is not "a row whose value is NULL": a matched row may
//     hold a NULL of its own. `NULLIF(COUNT(*), 2)` is NULL for a matched row
//     that counted 2, and stamping the column's own nulls turned two RIGHT
//     rows into wrong ones. The pad is marked by the correlation key: the join
//     keys on it, a NULL key matches nothing, so the KEY column is NULL
//     exactly on the rows the pad manufactured. This operator reads that
//     column — the hidden slot where the lowering minted one, the published
//     key otherwise — and writes only where IT is null.
//
//  2. WHAT VALUE. Not a literal 0 on a COUNT column: the constant is the
//     ITEM's own value over the empty input, folded at plan time
//     (logical.lateralEmptyDefaults). `COUNT(*)+1` is 1, `COUNT(*)=0` is
//     true, `COALESCE(SUM(x),0)` is 0, `CASE WHEN COUNT(*)>5 THEN 1 END` is
//     NULL — and a NULL default is not carried at all, because the pad
//     already wrote it.
//
// The marker is dropped here rather than by the join, because the join's drop
// runs BELOW this operator and the marker is what this operator reads.
type LateralEmptyDefault struct {
	// Marker is the column whose NULL marks a padded row.
	Marker string
	// Cols are the defaults to write, by output column name.
	Cols []LateralDefault
	// DropMarker removes the marker column from the output — the join left it
	// in place for this operator, and nothing above may see it.
	DropMarker bool

	resolved  bool
	markerIdx int
	targets   []int
	texts     []string
	keep      []int
}

// NewLateralEmptyDefault returns the operator, or nil when this join has
// nothing to default and nothing to hide.
func NewLateralEmptyDefault(marker string, cols []LateralDefault, dropMarker bool) *LateralEmptyDefault {
	if marker == "" || (len(cols) == 0 && !dropMarker) {
		return nil
	}
	return &LateralEmptyDefault{
		Marker:     marker,
		Cols:       append([]LateralDefault(nil), cols...),
		DropMarker: dropMarker,
	}
}

func (o *LateralEmptyDefault) Init(_ context.Context) error { return nil }

func (o *LateralEmptyDefault) Close() error { return nil }

func (o *LateralEmptyDefault) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil {
		return in, nil
	}
	if !o.resolved {
		o.resolve(in)
	}
	if o.markerIdx >= 0 && o.markerIdx < len(in.Columns) && in.Len > 0 {
		marker := in.Columns[o.markerIdx]
		if marker != nil && marker.Nulls.HasNulls() {
			for _, ci := range o.targets {
				if ci < 0 || ci >= len(in.Columns) {
					continue
				}
				o.fill(in, ci, marker)
			}
		}
	}
	if !o.DropMarker || o.markerIdx < 0 {
		return in, nil
	}
	return o.narrow(in), nil
}

// resolve locates the marker and each defaulted column, once.
func (o *LateralEmptyDefault) resolve(in *batch.RecordBatch) {
	o.resolved = true
	o.markerIdx = -1
	bare := func(name string) string {
		if dot := strings.IndexByte(name, '.'); dot > 0 && dot < len(name)-1 {
			return name[dot+1:]
		}
		return name
	}
	want := make(map[string]string, len(o.Cols))
	for _, c := range o.Cols {
		if c.Text == "" {
			continue
		}
		want[batch.FoldIdent(bare(c.Column))] = c.Text
	}
	// THE MARKER IS MATCHED QUALIFIED FIRST. A build column that collided
	// with a probe column is emitted qualified ("s.__key_0") and the PROBE's
	// column of that name is a different one — a user's stored `__key_0`
	// (ADR-0012). Taking the first bare match dropped the user's column and
	// published the slot. Exact first, then a bare name only when exactly ONE
	// column carries it; two candidates and nothing is done at all, which
	// leaves an extra column rather than the wrong one.
	markerExact := batch.FoldIdent(strings.TrimSpace(o.Marker))
	markerFold := batch.FoldIdent(bare(o.Marker))
	var bareHits []int
	for i, col := range in.Schema {
		if batch.FoldIdent(strings.TrimSpace(col.Name)) == markerExact {
			o.markerIdx = i
			break
		}
		if batch.FoldIdent(bare(col.Name)) == markerFold {
			bareHits = append(bareHits, i)
		}
	}
	if o.markerIdx < 0 && len(bareHits) == 1 {
		o.markerIdx = bareHits[0]
	}
	o.texts = o.texts[:0]
	for i, col := range in.Schema {
		if i == o.markerIdx {
			continue
		}
		if text, ok := want[batch.FoldIdent(bare(col.Name))]; ok {
			o.targets = append(o.targets, i)
			o.texts = append(o.texts, text)
		}
	}
	for i := range in.Schema {
		if i == o.markerIdx {
			continue
		}
		o.keep = append(o.keep, i)
	}
}

// fill writes one column's constant on every padded row.
func (o *LateralEmptyDefault) fill(in *batch.RecordBatch, ci int, marker *batch.Vector) {
	text := ""
	for i, t := range o.targets {
		if t == ci {
			text = o.texts[i]
			break
		}
	}
	if text == "" {
		return
	}
	v := in.Columns[ci]
	if v == nil {
		return
	}
	// A VIEW shares its base vector's storage with other consumers, so writing
	// through it would edit rows this batch does not own.
	if v.IsView() {
		in.FlattenColumn(ci)
		v = in.Columns[ci]
		if v == nil {
			return
		}
	}
	val, ok := constantAt(v.Type, text)
	if !ok {
		return
	}
	for r := 0; r < in.Len; r++ {
		if !marker.Nulls.IsNull(r) {
			continue
		}
		v.SetValue(r, val)
		v.Nulls.SetValid(r)
	}
}

// narrow returns the batch without the marker column.
func (o *LateralEmptyDefault) narrow(in *batch.RecordBatch) *batch.RecordBatch {
	cols := make([]*batch.Vector, 0, len(o.keep))
	schema := make([]parquet.Column, 0, len(o.keep))
	for _, i := range o.keep {
		if i >= len(in.Columns) || i >= len(in.Schema) {
			continue
		}
		cols = append(cols, in.Columns[i])
		schema = append(schema, in.Schema[i])
	}
	out := &batch.RecordBatch{Columns: cols, Schema: schema, Len: in.Len, Sel: in.Sel}
	return out
}

// constantAt parses the rendered constant at the column's own type. A type it
// cannot write leaves the row as the pad wrote it, which is NULL — never a
// value invented for it.
func constantAt(t batch.TypeID, text string) (any, bool) {
	switch t {
	case batch.TypeInt64, batch.TypeInt32:
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(text, 64)
			if ferr != nil {
				return nil, false
			}
			n = int64(f)
		}
		if t == batch.TypeInt32 {
			return int32(n), true
		}
		return n, true
	case batch.TypeFloat64, batch.TypeFloat32:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, false
		}
		if t == batch.TypeFloat32 {
			return float32(f), true
		}
		return f, true
	case batch.TypeBool:
		b, err := strconv.ParseBool(text)
		if err != nil {
			return nil, false
		}
		return b, true
	case batch.TypeString, batch.TypeBytes, batch.TypeDecimal:
		return text, true
	}
	return nil, false
}
