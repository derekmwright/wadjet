package exec

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DecimalCoerceColumn names one column and the DECIMAL(Precision, Scale) it
// must carry from here on.
type DecimalCoerceColumn struct {
	Name      string
	Precision int
	Scale     int
}

// DecimalCoerce puts named columns into ONE declared DECIMAL(p,s), rewriting
// the unscaled carrier as it goes.
//
// It exists for a set operation's arms. A DECIMAL value is an UNSCALED
// integer plus the column's declared SCALE (ADR-0018 §4), and the two travel
// apart on the stage DAG: each arm's task writes its own .wshf file carrying
// its own scale in the header, and a downstream task that reads several such
// files writes ONE file under the schema of the first batch it saw. Two arms
// at different scales therefore hand the same unscaled integer to a reader
// that believes a different scale — 12.7501 from a DECIMAL(18,4) arm came
// back as 1275.01 under a DECIMAL(9,2) first arm, 100x too large, with
// nothing anywhere reporting a problem (#533).
//
// The fix is to make the arms AGREE before they meet, which means moving the
// values: rescaling to the set operation's output scale is a multiplication
// by a power of ten, not a reinterpretation. INT32/INT64 arms are coerced the
// same way, because `numeric UNION ALL bigint` is `numeric` in PostgreSQL and
// an integer box is a value at scale 0.
//
// Only UPWARD moves are accepted. The output scale is the max over the arms,
// so no arm is ever asked to drop digits; a request to scale DOWN is a
// planner defect and is refused rather than silently truncating.
//
// A value with no Int128 at the output scale is an ERROR naming the column,
// not a wrapped one — the same rule, and the same reason, as SUM's overflow
// (ADR-0012 item 9): a wrapped number is a different number wearing the right
// type, and nobody downstream can tell.
type DecimalCoerce struct {
	cols []DecimalCoerceColumn

	// Resolved against the input schema on the first batch and re-verified
	// on every batch after it: a source column whose own type or scale
	// CHANGES mid-stream is the very defect this operator exists for, so it
	// is reported rather than absorbed.
	resolved bool
	plan     []decimalCoercion
	schema   []parquet.Column
}

// decimalCoercion is one column's resolved work: where it is, what it is now,
// and what it must become. rewrite is false when only the DECLARED precision
// differs — the carrier is already right, so the vectors pass through
// untouched and only the schema is restated.
type decimalCoercion struct {
	idx       int
	srcType   parquet.TypeID
	srcScale  int
	dstScale  int
	dstPrec   int
	name      string
	rewrite   bool
	shiftPow1 int // 10^shiftPow1 multiplies the unscaled carrier
	// limit is 10^dstPrec, the exclusive bound on the coerced magnitude,
	// resolved once per column rather than per row. Zero means the
	// declaration named no precision this carrier can express.
	limit batch.Int128
}

// NewDecimalCoerce returns an operator that coerces the named columns. An
// empty list is a pass-through.
func NewDecimalCoerce(cols []DecimalCoerceColumn) *DecimalCoerce {
	return &DecimalCoerce{cols: append([]DecimalCoerceColumn(nil), cols...)}
}

func (d *DecimalCoerce) Init(_ context.Context) error { return nil }

func (d *DecimalCoerce) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil || len(d.cols) == 0 {
		return in, nil
	}
	if err := d.resolve(in); err != nil {
		return nil, err
	}
	if len(d.plan) == 0 {
		return in, nil
	}
	// A NEW shell over the input's vectors, replacing only the columns whose
	// carrier moved: the input batch is often pooled by its producer, and
	// writing into its slots (or over its vectors) would corrupt storage a
	// consumer upstream may still be reading. Same shape as ColumnPrune.
	out := &batch.RecordBatch{
		Schema:  d.schema,
		Columns: make([]*batch.Vector, len(in.Columns)),
		Len:     in.Len,
		Sel:     in.Sel,
	}
	copy(out.Columns, in.Columns)
	for i := range d.plan {
		c := &d.plan[i]
		if !c.rewrite {
			continue
		}
		v, err := coerceDecimalVector(in.Columns[c.idx], c)
		if err != nil {
			return nil, err
		}
		out.Columns[c.idx] = v
	}
	return out, nil
}

func (d *DecimalCoerce) Close() error { return nil }

// Clone satisfies Cloneable so a fragment carrying this operator can still
// run its morsel workers in parallel. The clone re-resolves against its own
// first batch; nothing here is shared state.
func (d *DecimalCoerce) Clone() UnaryOperator { return NewDecimalCoerce(d.cols) }

// resolve binds the coercion list to the input schema, and on every later
// batch re-checks that the binding still describes what arrived.
func (d *DecimalCoerce) resolve(in *batch.RecordBatch) error {
	if d.resolved {
		for i := range d.plan {
			c := &d.plan[i]
			if c.idx >= len(in.Schema) {
				return fmt.Errorf("decimal coerce: column %q left the input schema mid-stream", c.name)
			}
			got := in.Schema[c.idx]
			if got.Name != c.name || got.Type != c.srcType || sourceDecimalScale(in.Columns[c.idx], got) != c.srcScale {
				return fmt.Errorf(
					"decimal coerce: column %q changed type mid-stream (was %s scale %d, now %s scale %d) — "+
						"a set operation's arms must reach this operator already agreed",
					c.name, c.srcType, c.srcScale, got.Type, sourceDecimalScale(in.Columns[c.idx], got))
			}
		}
		return nil
	}
	d.resolved = true
	schema := append([]parquet.Column(nil), in.Schema...)
	for _, want := range d.cols {
		idx := in.ColumnIndex(want.Name)
		if idx < 0 {
			return fmt.Errorf("decimal coerce: column %q is not in the input schema", want.Name)
		}
		src := in.Schema[idx]
		srcScale := sourceDecimalScale(in.Columns[idx], src)
		c := decimalCoercion{
			idx: idx, srcType: src.Type, srcScale: srcScale,
			dstScale: want.Scale, dstPrec: want.Precision, name: want.Name,
		}
		c.limit, _ = batch.DecimalPrecisionLimit(want.Precision)
		switch src.Type {
		case parquet.TypeDecimal:
			if srcScale > want.Scale {
				return fmt.Errorf(
					"decimal coerce: column %q would drop digits (scale %d to %d); a set operation's "+
						"output scale is the maximum over its arms, so no arm is ever scaled down",
					want.Name, srcScale, want.Scale)
			}
			c.rewrite = srcScale != want.Scale
			c.shiftPow1 = want.Scale - srcScale
		case parquet.TypeInt32, parquet.TypeInt64:
			// An integer is a value at scale 0; the whole output scale is
			// the shift.
			c.rewrite = true
			c.shiftPow1 = want.Scale
		default:
			return fmt.Errorf("decimal coerce: column %q is %s, which has no exact DECIMAL carrier", want.Name, src.Type)
		}
		schema[idx].Type = parquet.TypeDecimal
		schema[idx].Scale = want.Scale
		schema[idx].Precision = want.Precision
		schema[idx].Nullable = true
		d.plan = append(d.plan, c)
	}
	d.schema = schema
	return nil
}

// sourceDecimalScale is the scale the VECTOR actually carries, which is the
// authority over the schema's copy of it — a view vector holds none of its
// own and defers to the base it reads through.
func sourceDecimalScale(v *batch.Vector, col parquet.Column) int {
	if col.Type != parquet.TypeDecimal {
		return 0
	}
	if v == nil {
		return col.Scale
	}
	if v.Base != nil {
		return v.Base.DecimalData.Scale
	}
	return v.DecimalData.Scale
}

// coerceDecimalVector builds the rescaled column. Every row is converted,
// including rows a selection vector excludes, so the caller's Sel stays valid
// against the new vector without being rewritten.
//
// The bound checked is the DECLARED PRECISION, not the Int128 carrier's. They
// are different bounds and only one of them is the type: a DECIMAL(38,2) whose
// unscaled value lands in [10^38, 2^127-1] fits the carrier and does NOT fit
// the declaration, and DECIMAL(11,4) is 10^11 short of the carrier entirely.
// Admitting such a value writes a number the declared type cannot hold into a
// column the parquet writer will size from that precision (ADR-0018 §4's
// encoding rule) and that every consumer will read back as in-range.
func coerceDecimalVector(src *batch.Vector, c *decimalCoercion) (*batch.Vector, error) {
	n := src.Len
	out := batch.NewVectorWithScale(batch.TypeDecimal, n, c.dstScale)
	for i := 0; i < n; i++ {
		unscaled, ok := decimalSourceCell(src, c.srcType, i)
		if !ok {
			out.Nulls.SetNull(i)
			continue
		}
		shifted, ok := unscaled.MulPow10(c.shiftPow1)
		if ok {
			ok = batch.DecimalFitsPrecision(shifted, c.limit)
		}
		if !ok {
			// 22003 numeric_value_out_of_range: PostgreSQL's SQLSTATE for the
			// same condition, and the one ADR-0024 item 4 makes mandatory at
			// every value-producing site. A bare fmt.Errorf reached clients as
			// the internal-error class, so a client branching on SQLSTATE
			// could not tell a numeric overflow from a server fault.
			return nil, sqlerr.New("22003",
				"numeric field overflow: %s does not fit DECIMAL(%d,%d), the type this set operation's "+
					"arms agree on for column %q — a field with precision %d, scale %d holds an "+
					"absolute value below 10^%d",
				unscaled.FormatDecimal(c.srcScale), c.dstPrec, c.dstScale, c.name,
				c.dstPrec, c.dstScale, c.dstPrec-c.dstScale)
		}
		out.DecimalData.Data[i] = shifted
	}
	return out, nil
}

// decimalSourceCell reads row i's unscaled carrier, through a view when the
// vector is one. ok=false is a NULL.
func decimalSourceCell(v *batch.Vector, typ parquet.TypeID, i int) (batch.Int128, bool) {
	if v.Nulls.IsNull(i) {
		return batch.Int128{}, false
	}
	src, row := v, i
	if v.Base != nil {
		row = int(v.Indices[i])
		src = v.Base
		if src.Nulls.IsNull(row) {
			return batch.Int128{}, false
		}
	}
	switch typ {
	case parquet.TypeDecimal:
		return src.DecimalData.Data[row], true
	case parquet.TypeInt32:
		return batch.Int128From(int64(src.Int32Data[row])), true
	default:
		return batch.Int128From(src.Int64Data[row]), true
	}
}
