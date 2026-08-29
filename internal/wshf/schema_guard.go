package wshf

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SchemaGuard holds the several .wshf files of ONE stage input to ONE
// description of the relation they carry.
//
// ADR-0010: a `.wshf` header declares its schema once and every chunk in the
// file is read under it, so for a DECIMAL the header holds half of every value
// — the chunk carries the unscaled integer and the header carries the scale.
// `shuffleWriter.writeChunk` already refuses a CHUNK that disagrees with its
// own header, and the ADR says in as many words that this covers the
// SINGLE-WRITER shape and only that shape: it fires where one task is handed
// batches at two scales, and cannot fire where each producer writes its own
// internally-consistent file and a downstream reader concatenates several of
// them. There is no writer at the point of reinterpretation — the consumer
// resolves against the first batch it sees and reads every later file under
// that.
//
// This is the missing half, and it lives HERE rather than in any one reader
// because there are six of them: the worker's stage source and its inline
// result decode, and the coordinator's inline-result, stage-result, gather
// receiver, gather replay and scalar-extract reads. #685 was found through one
// of them; a guard in that one would have left the other five open, which is
// the shape ADR-0010 already refuses for the DECODER itself ("one reader,
// fuzzed" — the coordinator and the worker each having their own copy is how
// they drifted).
//
// It cannot repair anything: by the time a batch is in hand the integers are
// already ambiguous. It does the one thing that is better than a silent wrong
// answer — fails the read by name.
//
// The zero value is ready to use. Not safe for concurrent use; each guard
// belongs to one reader.
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
