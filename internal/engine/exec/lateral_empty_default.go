package exec

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// LateralDefault is one output column's empty-input rule, COMPILED: the
// expression `CASE WHEN <marker> IS NULL THEN <the item over an empty input>
// ELSE <the column> END`, built as SQL at plan time and compiled by whichever
// side is running — the planner on the single-process path, the worker on the
// distributed one.
type LateralDefault struct {
	Column string
	Expr   Expression
}

// LateralEmptyDefault restores ungrouped aggregate empty-input item values above
// a decorrelated LATERAL join: COUNT(*) is 0, SUM is NULL, composed items use those values.
// Default only padded rows, identified by the NULL marker/correlation key, never
// by an output value's NULL bit: a matched row may legitimately contain NULL.
// Use one compiled expression for pad detection and the item's empty-input value,
// with existing typed kernels and a new vector for all types, including containers/varlen.
// Do not stamp text defaults in place. Drop the marker here after evaluation,
// not in the join below, because the expressions still need to read it.
// See docs/internals/lateral-empty-aggregate-default-projection.md for the design.
type LateralEmptyDefault struct {
	// Marker is the column whose NULL marks a padded row.
	Marker string
	// Cols are the compiled per-column rules.
	Cols []LateralDefault
	// DropMarker removes the marker column from the output — the join left it
	// in place for this operator, and nothing above may see it.
	DropMarker bool

	resolved  bool
	markerIdx int
	inner     *Project
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

func (o *LateralEmptyDefault) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil {
		return in, nil
	}
	if !o.resolved {
		o.resolve(in)
	}
	if o.inner == nil {
		return in, nil
	}
	return o.inner.Execute(ctx, in)
}

// resolve locates the marker and builds the projection this operator IS: one
// output column per input column, the defaulted ones computed by their own
// compiled expression and every other one copied by POSITION.
//
// It is built from the first batch because that is where the column list is: a
// `SELECT *` over a join has none at plan time, which is the whole reason this
// arc exists.
func (o *LateralEmptyDefault) resolve(in *batch.RecordBatch) {
	o.resolved = true
	o.markerIdx = -1
	bare := func(name string) string {
		if dot := strings.IndexByte(name, '.'); dot > 0 && dot < len(name)-1 {
			return name[dot+1:]
		}
		return name
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
	if o.markerIdx < 0 {
		return // the plan and the runtime disagree; leave the batch alone
	}

	byName := make(map[string]Expression, len(o.Cols))
	for _, c := range o.Cols {
		if c.Expr == nil {
			continue
		}
		byName[batch.FoldIdent(bare(c.Column))] = c.Expr
	}
	projections := make([]ProjectColumn, 0, len(in.Schema))
	for i, col := range in.Schema {
		if o.DropMarker && i == o.markerIdx {
			continue
		}
		p := ProjectColumn{
			Name:         col.Name,
			Type:         col.Type,
			SourceCol:    col.Name,
			DirectCopy:   col.Name,
			SourceIdx:    i,
			SourceIdxSet: true,
		}
		if e, ok := byName[batch.FoldIdent(bare(col.Name))]; ok {
			p = ProjectColumn{Name: col.Name, Type: col.Type, Expr: e, SourceCol: col.Name}
		}
		projections = append(projections, p)
	}
	o.inner = &Project{Projections: projections}
}
