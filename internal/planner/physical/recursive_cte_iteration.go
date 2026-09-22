// SPDX-License-Identifier: MIT

// This file holds the recursive CTE fixed point for the physical planner,
// governed by ADR-0021 §1o and ADR-0012.
package physical

import (
	"context"
	"fmt"
	"math"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// recursiveIterationLimit is the LOUD bound on a recursive CTE's fixed point.
//
// PostgreSQL has no such limit: it iterates until the recursive term yields no
// rows, and a recursion that never stops runs until statement_timeout or
// temp_file_limit ends it (measured on 17.11: `WITH RECURSIVE r(n) AS (SELECT 1
// UNION ALL SELECT n+1 FROM r) SELECT count(*) FROM r` is 53400 after 4.9 s
// under a 256 MB temp_file_limit). This engine iterates to the fixed point the
// same way, and three things end a recursion that has none, each with an
// error and never with the rows produced so far (#1246):
//
//   - a cancelled statement (statement_timeout, CancelRequest) between
//     iterations;
//   - one iteration's rows exceeding the memory budget (53200) — the working
//     table is held in memory, so a recursion whose rows GROW is stopped by
//     the budget;
//   - this many iterations (54000). The closure spills past the budget like any
//     other materialization, so a recursion whose rows do NOT grow — `SELECT
//     n+1 FROM r` with no WHERE — is bounded by nothing else, and without a
//     bound it would run until the disk filled.
//
// The number is measured, not chosen for comfort: an iteration costs tens of
// microseconds, so this is the order of seconds of work before the refusal, and
// it is two orders of magnitude above any date series or hierarchy depth an
// application writes. The old limit was 1000 and it was SILENT: it returned
// the partial closure as if it were the answer.
const recursiveIterationLimit = 1_000_000

// iterateRecursiveCTE runs `anchor UNION ALL recursive-term` to its fixed
// point, COLUMNAR, under the types the anchor's PLAN declares.
//
// The previous loop boxed every row into a map and re-derived the CTE's schema
// from the Go values of the anchor's first row, which is a guess and was wrong
// in both directions: a DATE arrived as text, so a date series added 1 to a
// string; a text '1' became a bigint; a zero-row or NULL seed declared text.
// PostgreSQL's rule is that the non-recursive term DECIDES the column types,
// and the plan knows them whether or not a row ever arrives.
func (p *Planner) iterateRecursiveCTE(ctx context.Context, cte plansql.CTEDef, anchorSQL, recursiveSQL string) error {
	anchorBatches, schema, err := p.runRecursiveArm(ctx, anchorSQL)
	if err != nil {
		return err
	}
	if len(schema) == 0 {
		return errNoCTESchema(cte.Name)
	}
	// The CTE's names: its column list over the anchor's published names,
	// positionally — the list a reference to it was built with.
	if len(cte.Columns) > len(schema) {
		return sqlerr.New("42P10",
			"WITH query %q has %d columns available but %d columns specified",
			cte.Name, len(schema), len(cte.Columns))
	}
	for i, name := range cte.Columns {
		schema[i].Name = name
	}
	// Every column may hold a NULL the anchor never did: the recursive term
	// produces rows of its own.
	for i := range schema {
		schema[i].Nullable = true
	}

	closure := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	if sm := closure.Spill; sm != nil {
		// The closure is replayed in order and never merged, so it drains in
		// runs of a quarter of the budget rather than the sort floor: held in
		// forced tracking up to the floor, it refused the join build of the
		// very term it was iterating once it passed a small budget.
		closure.RunBytes = sm.SpillBudget() / 4
	}
	writer := &closureWriter{coll: closure, schema: schema}
	fail := func(err error) error {
		closure.Release()
		delete(p.cteCache, cte.Name)
		return err
	}
	// The working table: the rows the LAST iteration produced, which is what
	// the recursive term's self-reference reads. It is held in memory — it is
	// one iteration's delta — and charged against the budget below.
	work, err := p.recursiveWorkTable(ctx, cte, schema, anchorBatches)
	if err != nil {
		return fail(err)
	}
	for iter := 0; work.Rows() > 0; iter++ {
		if err := writer.add(ctx, work); err != nil {
			work.Release()
			return fail(err)
		}
		if err := ctx.Err(); err != nil {
			work.Release()
			return fail(err)
		}
		if iter >= recursiveIterationLimit {
			work.Release()
			return fail(sqlerr.New("54000",
				"recursive query %q did not reach a fixed point within %d iterations: "+
					"its recursive term still produced rows. Stop the recursion in the "+
					"recursive term's WHERE clause",
				cte.Name, recursiveIterationLimit))
		}
		// Seed the self-reference with the working table.
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: work}
		// An error is the STATEMENT's error (#1041): a term that fails on
		// iteration k does not make iterations 1..k-1 the answer.
		termBatches, _, err := p.runRecursiveArm(ctx, recursiveSQL)
		if err != nil {
			work.Release()
			return fail(err)
		}
		termBatches, err = coerceRecursiveTerm(cte.Name, schema, termBatches)
		if err != nil {
			work.Release()
			return fail(err)
		}
		next, err := p.recursiveWorkTable(ctx, cte, schema, termBatches)
		work.Release()
		if err != nil {
			return fail(err)
		}
		work = next
	}
	work.Release()
	if err := writer.flush(ctx); err != nil {
		return fail(err)
	}
	p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: closure}
	return nil
}

// runRecursiveArm plans one arm of a recursive CTE as a statement and runs it,
// returning its batches and its schema: the batches' own when a row arrived —
// the runtime saw the vectors — and the PLAN's declaration when none did,
// because a zero-row arm still has column types.
func (p *Planner) runRecursiveArm(ctx context.Context, sql string) ([]*batch.RecordBatch, []parquet.Column, error) {
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("subquery parse error: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil, nil, fmt.Errorf("subquery extract error: %w", err)
	}
	// What this ONE run builds is released when it ends. The plan's Cleanup
	// is the owner of a join's build reservation and of an IN-set's charge,
	// and it runs once, at the end of the statement — so a term that joins
	// held one iteration's build per iteration until then, and 20 000
	// iterations of `… FROM r JOIN t ON …` exhausted a 512 KiB budget with
	// builds nothing would read again.
	joinsBefore := len(p.builtJoins)
	res := p.resources()
	inSetsBefore := res.subqueryChargeCount()
	defer func() {
		for _, hj := range p.builtJoins[joinsBefore:] {
			hj.Close()
		}
		p.builtJoins = p.builtJoins[:joinsBefore]
		res.releaseSubqueryChargesSince(inSetsBefore)
	}()
	source, ops, sink, plan, err := p.buildSubqueryPipelineForPlan(ctx, info)
	if err != nil {
		return nil, nil, err
	}
	cs, ok := sink.(*exec.CollectSink)
	if !ok {
		cs = &exec.CollectSink{}
	}
	cs.SkipFinalizeToRows = true
	cs.SchemaHint = declaredOutputSchema(plan, p.SubqueryOutputColumn)
	cs.OutputNames = publishedNamesOfProjection(publishedOutputProjectionNode(plan))
	if err := (&exec.Pipeline{Source: source, Ops: ops, Sink: cs}).Run(ctx); err != nil {
		return nil, nil, fmt.Errorf("subquery execution error: %w", err)
	}
	schema := append([]parquet.Column(nil), cs.Schema()...)
	return cs.Batches(), schema, nil
}

// recursiveWorkTable holds one iteration's rows under the CTE's schema — its
// names and the anchor's types — and refuses a delta the memory budget cannot
// hold, which is what ends a recursion whose rows grow (53200).
func (p *Planner) recursiveWorkTable(ctx context.Context, cte plansql.CTEDef,
	schema []parquet.Column, batches []*batch.RecordBatch) (*exec.SpillableBatchCollector, error) {
	work := &exec.SpillableBatchCollector{}
	var bytes int64
	for _, b := range batches {
		if b == nil || b.ActiveLen() == 0 {
			continue
		}
		if len(b.Columns) != len(schema) {
			work.Release()
			return nil, sqlerr.New("42601",
				"each UNION query in recursive query %q must have the same number of columns", cte.Name)
		}
		named := &batch.RecordBatch{Schema: schema, Columns: b.Columns, Len: b.Len, Sel: b.Sel}
		bytes += named.MemBytes()
		if err := work.Consume(ctx, named); err != nil {
			work.Release()
			return nil, err
		}
	}
	if sm := p.getSpillManager(); sm != nil {
		if budget := sm.SpillBudget(); budget > 0 && bytes > budget {
			work.Release()
			return nil, sqlerr.Wrap("53200", fmt.Errorf(
				"recursive query %q: one iteration produced %d bytes, more than the whole memory budget "+
					"(%d bytes) — a recursion whose rows grow at every step does not terminate: %w",
				cte.Name, bytes, budget, memory.ErrMemoryExceeded))
		}
	}
	return work, nil
}

// closureWriter appends working tables to a recursive CTE's accumulated
// result, COALESCING small ones. A recursion that adds one row per iteration —
// every counter and date series — handed the collector a one-row batch per
// iteration, and a batch costs about a kilobyte of heap the tracker never
// sees: 100 000 iterations held ~100 MB that the budget read as a few hundred
// kilobytes, so the closure never spilled.
type closureWriter struct {
	coll    *exec.SpillableBatchCollector
	schema  []parquet.Column
	pending []*batch.RecordBatch
	rows    int
}

func (w *closureWriter) add(ctx context.Context, work *exec.SpillableBatchCollector) error {
	return work.Iterate(func(b *batch.RecordBatch) error {
		n := b.ActiveLen()
		if n == 0 {
			return nil
		}
		if n >= batch.DefaultBatchSize/2 {
			if err := w.flush(ctx); err != nil {
				return err
			}
			return w.coll.Consume(ctx, &batch.RecordBatch{Schema: b.Schema, Columns: b.Columns, Len: b.Len, Sel: b.Sel})
		}
		w.pending = append(w.pending, b)
		w.rows += n
		if w.rows >= batch.DefaultBatchSize {
			return w.flush(ctx)
		}
		return nil
	})
}

func (w *closureWriter) flush(ctx context.Context) error {
	if w.rows == 0 {
		w.pending = w.pending[:0]
		return nil
	}
	out := batch.NewRecordBatch(w.schema, w.rows)
	di := 0
	for _, b := range w.pending {
		for j := range w.schema {
			dst, src := out.Columns[j], b.Columns[j]
			if b.Sel != nil {
				for k, si := range b.Sel {
					dst.CopyValueFrom(di+k, src, int(si))
				}
			} else {
				for si := 0; si < b.Len; si++ {
					dst.CopyValueFrom(di+si, src, si)
				}
			}
		}
		di += b.ActiveLen()
	}
	w.pending, w.rows = w.pending[:0], 0
	return w.coll.Consume(ctx, out)
}

// coerceRecursiveTerm restates the recursive term's batches under the anchor's
// types, which is PostgreSQL's rule: the NON-RECURSIVE term decides a
// recursive CTE's column types, and the recursive term's must resolve to them.
// `SELECT 1 UNION ALL SELECT n + 0.5 FROM r` is 42804 there ("column 1 has
// type integer in non-recursive term but type numeric overall"); storing the
// term's values under the anchor's type instead — what the boxed loop did —
// truncated 1.5 to 1 and recursed without end.
//
// Two conversions are the union's own and are applied, never refused:
//
//   - an INTEGER term into an INTEGER anchor. This engine declares `n + 1`
//     over an integer column bigint where PostgreSQL declares integer, so
//     refusing the width difference would refuse the most common recursive
//     CTE there is. The value is range-checked into the anchor's width, which
//     is exactly PostgreSQL's integer arithmetic: 22003 when it does not fit.
//   - an integer or real term into a DOUBLE PRECISION anchor, which is what
//     PostgreSQL's union resolves to the anchor's type.
//
// Everything else must be the anchor's carrier exactly, or it is 42804.
//
// It reads the types the term's BATCHES carry — what the runtime produced —
// and not the term's plan-time declaration. A term that produces no row at all
// is therefore not checked: there is no value to read under the wrong type,
// where PostgreSQL refuses it at parse time. That is a refusal this engine does
// not make, never a value it gets wrong.
func coerceRecursiveTerm(name string, anchor []parquet.Column, batches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	out := make([]*batch.RecordBatch, 0, len(batches))
	for _, b := range batches {
		if b == nil || b.ActiveLen() == 0 {
			continue
		}
		if len(b.Columns) != len(anchor) || len(b.Schema) != len(anchor) {
			return nil, sqlerr.New("42601",
				"each UNION query in recursive query %q must have the same number of columns", name)
		}
		converted := false
		for i := range anchor {
			if sameRecursiveCarrier(anchor[i], b.Schema[i]) {
				continue
			}
			if !recursiveTermConvertible(anchor[i].Type, b.Schema[i].Type) {
				return nil, sqlerr.New("42804",
					"recursive query %q column %d has type %s in non-recursive term but type %s overall",
					name, i+1, pgTypeName(anchor[i].Type), pgTypeName(b.Schema[i].Type))
			}
			if !converted {
				if b.HasViews() {
					b.FlattenViews()
				}
				b = b.Compact()
				b = &batch.RecordBatch{Schema: b.Schema, Columns: append([]*batch.Vector(nil), b.Columns...), Len: b.Len}
				converted = true
			}
			v, err := convertRecursiveColumn(b.Columns[i], anchor[i].Type, b.Len)
			if err != nil {
				return nil, err
			}
			b.Columns[i] = v
		}
		out = append(out, b)
	}
	return out, nil
}

func isIntegerCarrier(t parquet.TypeID) bool {
	return t == parquet.TypeInt32 || t == parquet.TypeInt64
}

// recursiveTermConvertible names the two conversions coerceRecursiveTerm
// applies; see there.
func recursiveTermConvertible(anchor, term parquet.TypeID) bool {
	switch {
	case isIntegerCarrier(anchor) && isIntegerCarrier(term):
		return true
	case anchor == parquet.TypeFloat64 &&
		(isIntegerCarrier(term) || term == parquet.TypeFloat32):
		return true
	}
	return false
}

// convertRecursiveColumn copies the first n rows of v into a vector of type
// to. A NULL stays NULL; an integer outside the target width is PostgreSQL's
// 22003.
func convertRecursiveColumn(v *batch.Vector, to parquet.TypeID, n int) (*batch.Vector, error) {
	out := batch.NewVector(to, n)
	out.Nulls.CopyFrom(&v.Nulls, n)
	read := func(i int) (int64, float64) {
		switch v.Type {
		case parquet.TypeInt32:
			return int64(v.Int32Data[i]), float64(v.Int32Data[i])
		case parquet.TypeInt64:
			return v.Int64Data[i], float64(v.Int64Data[i])
		case parquet.TypeFloat32:
			return 0, float64(v.Float32Data[i])
		}
		return 0, v.Float64Data[i]
	}
	for i := 0; i < n; i++ {
		if v.Nulls.IsNull(i) {
			continue
		}
		iv, fv := read(i)
		switch to {
		case parquet.TypeInt32:
			if iv < math.MinInt32 || iv > math.MaxInt32 {
				return nil, sqlerr.New("22003", "integer out of range")
			}
			out.Int32Data[i] = int32(iv)
		case parquet.TypeInt64:
			out.Int64Data[i] = iv
		case parquet.TypeFloat64:
			out.Float64Data[i] = fv
		}
	}
	return out, nil
}

// sameRecursiveCarrier: the term's column holds values of exactly the anchor's
// declared type, so its vectors can be read under the anchor's schema.
func sameRecursiveCarrier(a, b parquet.Column) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case parquet.TypeDecimal:
		return a.Scale == b.Scale && a.Precision == b.Precision
	case parquet.TypeVector:
		return a.Dimension == b.Dimension
	case parquet.TypeArray, parquet.TypeMap:
		if a.ElementType == nil || b.ElementType == nil {
			return a.ElementType == b.ElementType
		}
		return sameRecursiveCarrier(*a.ElementType, *b.ElementType)
	case parquet.TypeRow:
		if len(a.Fields) != len(b.Fields) {
			return false
		}
		for i := range a.Fields {
			if !sameRecursiveCarrier(a.Fields[i], b.Fields[i]) {
				return false
			}
		}
	}
	return true
}
