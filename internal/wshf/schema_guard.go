package wshf

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SchemaGuard holds ALL files of one stage input to one relation declaration
// (ADR-0010, #685). DECIMAL headers supply scale for unscaled chunk carriers.
// The writer's chunk/header check covers one writer only; individually valid
// files can still disagree when concatenated. Every worker/coordinator reader,
// including inline, gather replay and scalar extraction, must share this guard.
// It cannot repair ambiguous carriers: fail the read by name rather than
// reinterpret later files using the first schema.
// Zero value is ready; one reader owns each guard, with no concurrent use.
// See docs/internals/wshf-stage-input-schema-guard.md for the design.
type SchemaGuard struct {
	cols []guardCol
	seen bool
	// last is the schema slice of the previously checked batch. A chunk reader
	// hands every batch of one file the SAME slice (decodeChunkAt ->
	// batch.NewRecordBatch shares it), so comparing slice identity makes the
	// check run once per FILE rather than once per batch on the read hot path.
	last []parquet.Column
}

type guardCol struct {
	name      string
	typ       parquet.TypeID
	precision int
	scale     int
	isDec     bool
}

// CheckBatch holds b's schema against the first one this guard saw. what names
// the source of b — a file key, an object key, a worker id — and appears in the
// refusal.
func (g *SchemaGuard) CheckBatch(what string, b *batch.RecordBatch) error {
	if b == nil {
		return nil
	}
	return g.check(what, b.Schema, b.Schema)
}

// CheckBatches is CheckBatch over a decoded payload: every batch of one file
// shares its header, so this costs one comparison however many chunks it holds.
func (g *SchemaGuard) CheckBatches(what string, bs []*batch.RecordBatch) error {
	for _, b := range bs {
		if err := g.CheckBatch(what, b); err != nil {
			return err
		}
	}
	return nil
}

// Check holds a decoded header against the first one this guard saw.
func (g *SchemaGuard) Check(what string, schema []parquet.Column) error {
	return g.check(what, schema, nil)
}

// check compares schema; ident, when non-nil, is the slice whose IDENTITY
// short-circuits a repeat of the same file.
func (g *SchemaGuard) check(what string, schema []parquet.Column, ident []parquet.Column) error {
	if len(schema) == 0 {
		return nil
	}
	if ident != nil && len(g.last) == len(ident) && len(ident) > 0 && &g.last[0] == &ident[0] {
		return nil
	}
	if ident != nil {
		g.last = ident
	}
	got := make([]guardCol, len(schema))
	for i, c := range schema {
		got[i] = guardCol{name: c.Name, typ: c.Type,
			precision: c.Precision, scale: c.Scale, isDec: c.Type == parquet.TypeDecimal}
	}
	if !g.seen {
		g.cols, g.seen = got, true
		return nil
	}
	if len(got) != len(g.cols) {
		return fmt.Errorf("shuffle read: %s declares %d columns where an earlier file of the same "+
			"stage input declared %d — one stage's files describe one relation (ADR-0010)",
			what, len(got), len(g.cols))
	}
	for i, want := range g.cols {
		if got[i].name != want.name {
			return fmt.Errorf("shuffle read: %s names column %d %q where an earlier file of the same "+
				"stage input named it %q (ADR-0010)", what, i, got[i].name, want.name)
		}
		if got[i].typ != want.typ {
			// The TYPE half. An aggregate's identity row used to declare
			// FLOAT64 for the SUM leg of an AVG over a DECIMAL while every
			// partial that saw a row declared DECIMAL, and a merge that
			// resolved against the float batch first read the DECIMAL vectors
			// through a float kernel (#685). Fixed at the producer; refused
			// here so the next such producer is found by a query, not a user.
			return fmt.Errorf("shuffle read: column %q is %v in %s but %v in an earlier file of the "+
				"same stage input — the consumer types itself from whichever it reads first, so the "+
				"other file's values are decoded as a type they are not (ADR-0010)",
				want.name, got[i].typ, what, want.typ)
		}
		if want.isDec && (got[i].precision != want.precision || got[i].scale != want.scale) {
			return fmt.Errorf("shuffle read: column %q is DECIMAL(%d,%d) in %s but DECIMAL(%d,%d) in "+
				"an earlier file of the same stage input — the chunks carry unscaled integers and the "+
				"header carries the scale, so reading both under one declaration means a different "+
				"number silently (ADR-0010)",
				want.name, got[i].precision, got[i].scale, what, want.precision, want.scale)
		}
	}
	return nil
}
