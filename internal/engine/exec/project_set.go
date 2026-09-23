// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SetColumn is one set-returning SELECT item in a ProjectSet: the column at
// Index holds, per row, the ARRAY the function expands.
type SetColumn struct {
	Index int
	// Subscripts makes the column generate_subscripts(array, Dim): the
	// subscripts 1..n of a one-dimensional array's elements (no rows for a
	// Dim other than 1). Otherwise it is unnest(array): the elements.
	Subscripts bool
	Dim        int64
	// Expand makes the column information_schema._pg_expandarray(array):
	// ROW(x element, n subscript) per element.
	Expand bool
	// Out is the column the item publishes: the element's declaration for
	// unnest, int4 for generate_subscripts.
	Out parquet.Column
	// OutKnown is the planner's own bookkeeping: whether Out was decided.
	OutKnown bool
}

// ProjectSet evaluates set-returning functions in a SELECT list the way
// PostgreSQL 10+ does (its ProjectSet node): each input row produces as
// many output rows as its LONGEST set; a shorter set is padded with NULL,
// every other column repeats the row's value, and a row whose sets are all
// empty (or NULL) produces nothing. It runs directly above the Project that
// computed the set arguments, so every item of the list is in its input.
type ProjectSet struct {
	Cols []SetColumn
}

func (p *ProjectSet) Init(context.Context) error { return nil }
func (p *ProjectSet) Close() error               { return nil }

// Clone: ProjectSet holds no per-execution state.
func (p *ProjectSet) Clone() UnaryOperator { return p }

func (p *ProjectSet) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil {
		return nil, nil
	}
	setAt := make(map[int]SetColumn, len(p.Cols))
	for _, c := range p.Cols {
		if c.Index < 0 || c.Index >= len(in.Columns) {
			return nil, fmt.Errorf("set-returning column %d is outside the %d-column input", c.Index, len(in.Columns))
		}
		setAt[c.Index] = c
	}
	rows := in.Sel
	if rows == nil {
		rows = make([]uint32, in.Len)
		for i := range rows {
			rows[i] = uint32(i)
		}
	}

	// Each set column's per-row (element vector, first element, count).
	type span struct {
		child *batch.Vector
		off   int
		n     int
	}
	spans := make(map[int][]span, len(p.Cols))
	var srcRow, ord []int32
	for ci, c := range setAt {
		arr := in.Columns[ci]
		if arr.Type != batch.TypeArray {
			return nil, fmt.Errorf("set-returning column %q is %v, not an array", in.Schema[ci].Name, arr.Type)
		}
		sp := make([]span, len(rows))
		for k, r := range rows {
			v, ri := arr, int(r)
			if v.Base != nil {
				if v.Nulls.IsNullFast(ri) {
					continue
				}
				v, ri = v.Base, int(v.Indices[ri])
			}
			if v.Nulls.IsNull(ri) {
				continue
			}
			off, end := int(v.Offsets[ri]), int(v.Offsets[ri+1])
			n := end - off
			if c.Subscripts && c.Dim != 1 {
				n = 0
			}
			sp[k] = span{child: v.Child, off: off, n: n}
		}
		spans[ci] = sp
	}
	for k := range rows {
		most := 0
		for ci := range setAt {
			if n := spans[ci][k].n; n > most {
				most = n
			}
		}
		for j := 0; j < most; j++ {
			srcRow = append(srcRow, int32(rows[k]))
			ord = append(ord, int32(j))
		}
	}
	if len(srcRow) == 0 {
		return nil, nil
	}
	// The row index within `rows` of each output row, for the span lookup.
	rowPos := make(map[uint32]int, len(rows))
	for k, r := range rows {
		rowPos[r] = k
	}

	schema := make([]parquet.Column, len(in.Schema))
	copy(schema, in.Schema)
	cols := make([]*batch.Vector, len(in.Columns))
	for ci, src := range in.Columns {
		c, isSet := setAt[ci]
		if !isSet {
			dst := batch.NewVectorLike(src)
			for _, r := range srcRow {
				dst.AppendFrom(src, int(r))
			}
			cols[ci] = dst
			continue
		}
		schema[ci] = c.Out
		schema[ci].Name = in.Schema[ci].Name
		if c.Subscripts {
			dst := batch.NewVector(batch.TypeInt32, len(srcRow))
			for i, r := range srcRow {
				sp := spans[ci][rowPos[uint32(r)]]
				if int(ord[i]) >= sp.n {
					dst.Nulls.SetNull(i)
					continue
				}
				dst.Int32Data[i] = ord[i] + 1
			}
			cols[ci] = dst
			continue
		}
		if c.Expand {
			dst := batch.NewColumnVector(c.Out, 0)
			for i, r := range srcRow {
				sp := spans[ci][rowPos[uint32(r)]]
				if int(ord[i]) >= sp.n || sp.child == nil {
					dst.AppendNull()
					continue
				}
				dst.AppendValue(map[string]any{
					"x": sp.child.GetValue(sp.off + int(ord[i])),
					"n": ord[i] + 1,
				})
			}
			cols[ci] = dst
			continue
		}
		var dst *batch.Vector
		if src.Base != nil && src.Base.Child != nil {
			dst = batch.NewVectorLike(src.Base.Child)
		} else if src.Child != nil {
			dst = batch.NewVectorLike(src.Child)
		} else {
			dst = batch.NewColumnVector(c.Out, 0)
		}
		for i, r := range srcRow {
			sp := spans[ci][rowPos[uint32(r)]]
			if int(ord[i]) >= sp.n || sp.child == nil {
				dst.AppendNull()
				continue
			}
			dst.AppendFrom(sp.child, sp.off+int(ord[i]))
		}
		cols[ci] = dst
	}
	return &batch.RecordBatch{Columns: cols, Schema: schema, Len: len(srcRow)}, nil
}
