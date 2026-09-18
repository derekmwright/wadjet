// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// residualColPrefix names the columns of the COMBINED row a residual is
// evaluated over. The reference the query wrote is resolved to a SIDE and a
// column here, at plan time; the compiled expression then reads position i of
// the combined row under this name and never resolves a user name itself.
// residualSyntheticNames widens the prefix if a query happens to spell one.
const residualColPrefix = "_wj_on_"

// buildJoinResidualFilter compiles an outer join's ON-clause residual — every
// conjunct that is not an equi-join key pair — into a FACTORY of predicates
// over the COMBINED row: the probe row plus one candidate build row (#358).
//
// An outer join's ON runs BEFORE the NULL-padding, so the residual is
// evaluated AT the join and cannot be lifted above it or pushed into a
// preserved side's scan (ADR-0006's 2026-09-18 amendment). The expression is
// the ENGINE'S OWN: each distinct reference binds to a side and a column here,
// the AST is rewritten to read that binding by position, and expr.Compile
// compiles the rest — the same compiler the inner join's lifted filter runs
// above the join, so a predicate means the same thing on both sides (#1153).
//
// A FACTORY because an evaluator owns the combined-row scratch it rewrites per
// candidate: exec.HashJoin.Probe mints one per clone. The compiled tree is
// shared, as every predicate closure in this engine is.
//
// The error is the plan's refusal and NAMES the construct — a subquery in ON,
// a window function, a function or type the compiler refuses. The caller raises
// it rather than dropping the conjunct (#351).
// See docs/internals/outer-join-residual-evaluation.md for the design.
func buildJoinResidualFilter(filter, buildAlias string) (func() exec.JoinResidual, error) {
	node := parseJoinCondExpr(filter)
	if node == nil {
		return nil, fmt.Errorf("%q does not parse as an expression", filter)
	}
	refs, err := plansql.ColumnRefs(node)
	if err != nil {
		return nil, err
	}
	binds, prefix := residualBindings(refs)
	// The compiled tree is SHARED by every evaluator this factory mints: it is
	// a predicate closure, which this engine already requires to be stateless
	// across parallel workers (exec.Filter.Clone shares Pred for the same
	// reason). Only the combined-row scratch is per evaluator.
	compiled, err := expr.Compile(node)
	if err != nil {
		return nil, err
	}
	alias := strings.ToLower(buildAlias)
	return func() exec.JoinResidual {
		e := &residualEval{filter: filter, buildAlias: alias, compiled: compiled, prefix: prefix}
		e.binds = make([]residualBind, len(binds))
		copy(e.binds, binds)
		return e.eval
	}, nil
}

// residualBind is one distinct column reference of the residual: the spelling
// the query wrote, and — once the first candidate pair has been seen — the
// side, column index and ROW-field index it binds to, with the scratch vector
// its value is written into for the compiled expression to read.
type residualBind struct {
	table   string
	column  string
	spelled string // as written, for the "resolves on neither side" warning

	fromBuild bool
	idx       int
	// field is the CHILD index when idx names a ROW CONTAINER and the
	// reference is a field path into it, or -1 for a plain column.
	field int
	// dst is this binding's slot in the combined row. Reused across
	// candidates for every type whose storage can be reset in place; the
	// append-built nested types (ARRAY, MAP, ROW) are minted per refresh,
	// which is what batch.Vector.ResetForWrite refuses to do.
	dst      *batch.Vector
	declared parquet.Column
	nested   bool
}

// residualBindings assigns each DISTINCT reference of the residual a position
// in the combined row and rewrites the AST to read that position. The rewrite
// is why the compiled expression never resolves a user name: two arms of a
// self-join publish the same bare names, and a compiler asked to choose
// between them would be a THIRD resolution rule beside this file's and
// exec.ColRef's. Deduplication is by the spelling as written, folded the way
// the resolver folds it, so `b.y` twice is one slot and `b.y`/`y` are two.
func residualBindings(refs []*plansql.ColRef) ([]residualBind, string) {
	prefix := residualSyntheticNames(refs)
	var binds []residualBind
	seen := map[string]int{}
	for _, r := range refs {
		spelled := r.Column
		if r.Table != "" {
			spelled = r.Table + "." + r.Column
		}
		key := strings.ToLower(spelled)
		i, ok := seen[key]
		if !ok {
			i = len(binds)
			seen[key] = i
			binds = append(binds, residualBind{
				table: r.Table, column: r.Column, spelled: spelled, field: -1,
			})
		}
		r.Table = ""
		r.Column = fmt.Sprintf("%s%d", prefix, i)
	}
	// A residual over no columns at all (`ON true`) is still a predicate; it
	// simply reads nothing from the combined row.
	return binds, prefix
}

// residualSyntheticNames returns a combined-row column prefix that no
// reference in the residual spells, so a table whose own column is called
// `_wj_on_0` cannot be shadowed by the slot that carries it.
func residualSyntheticNames(refs []*plansql.ColRef) string {
	prefix := residualColPrefix
	for {
		clash := false
		for _, r := range refs {
			if strings.HasPrefix(strings.ToLower(r.Column), prefix) {
				clash = true
				break
			}
		}
		if !clash {
			return prefix
		}
		prefix = "_" + prefix
	}
}

// residualEval is ONE evaluator's state: the bindings with their scratch
// vectors and the combined-row batch built over them. Every parallel probe
// mints its own (exec.HashJoin.Probe), because the combined row is rewritten
// per candidate.
type residualEval struct {
	filter     string
	buildAlias string
	compiled   expr.Expr
	prefix     string
	binds      []residualBind

	row      *batch.RecordBatch
	resolved bool
}

func (e *residualEval) eval(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	if !e.resolved {
		e.resolve(probe, build)
		e.resolved = true
	}
	// BOTH halves, every candidate. Caching the probe half on the batch
	// pointer would be wrong rather than merely stale: probe batches are
	// POOLED, so the same pointer carrying different rows is the ordinary
	// case and pointer identity is not freshness.
	e.refresh(false, probe, probeRow)
	e.refresh(true, build, buildRow)

	// SQL ON semantics: a residual that is FALSE or UNKNOWN rejects. Both
	// boolean protocols collapse UNKNOWN to false; the boxed fall-through is
	// for an expression with no native boolean form (a function call), where
	// nil is the UNKNOWN.
	switch c := e.compiled.(type) {
	case expr.BoolNullExpr:
		val, null := c.EvalBoolNull(e.row, 0)
		return val && !null
	case expr.BoolExpr:
		return c.EvalBool(e.row, 0)
	}
	v, ok := e.compiled.Eval(e.row, 0).(bool)
	return ok && v
}

// refresh writes one side's half of the combined row.
func (e *residualEval) refresh(fromBuild bool, src *batch.RecordBatch, row int) {
	for i := range e.binds {
		b := &e.binds[i]
		if b.fromBuild != fromBuild || b.idx < 0 {
			continue
		}
		v := src.Columns[b.idx]
		if b.field >= 0 {
			fv, frow, ok := residualFieldVector(v, b.field, row)
			if !ok {
				// A NULL container has no field, and a field the container
				// does not declare has no value: the combined row carries
				// NULL, which rejects the candidate.
				e.writeNull(i, b)
				continue
			}
			v, row = fv, frow
		}
		if b.nested {
			// ARRAY/MAP/ROW element storage is append-built, so the slot is
			// minted fresh rather than reset (batch.Vector.ResetForWrite
			// panics on these).
			dst := batch.NewVectorLike(v)
			dst.AppendFrom(v, row)
			b.dst = dst
			e.row.SetColumn(i, dst)
			continue
		}
		b.dst.ResetForWrite(1)
		b.dst.CopyValueFrom(0, v, row)
	}
}

// writeNull puts SQL NULL in one slot of the combined row.
func (e *residualEval) writeNull(i int, b *residualBind) {
	if b.nested {
		b.dst = batch.NewColumnVector(b.declared, 1)
		b.dst.Nulls.SetNull(0)
		e.row.SetColumn(i, b.dst)
		return
	}
	b.dst.ResetForWrite(1)
	b.dst.Nulls.SetNull(0)
}

// resolve binds every reference to a side and a column, using the first
// candidate pair's schemas, and builds the combined row over them.
func (e *residualEval) resolve(probe, build *batch.RecordBatch) {
	schema := make([]parquet.Column, len(e.binds))
	for i := range e.binds {
		b := &e.binds[i]
		b.idx, b.field = -1, -1
		col := strings.ToLower(b.column)
		if b.table != "" {
			qual := strings.ToLower(b.table) + "." + col
			// ADR-0022 rule 1, at this resolver: ask whether the dotted
			// reference is a ROW FIELD PATH *before* the qualifier is
			// stripped. Stripping first bound `c_row.b` to whatever OTHER
			// side published a column of the FIELD's name — under
			// `LEFT JOIN decpair d ON c_row.b = d.b` the build's own `b`
			// answered for the field, so the residual read `d.b = d.b`, was
			// TRUE for every candidate, and the join returned the full cross
			// product on all four arms where PostgreSQL returns 12 rows.
			// That is #769's silent-wrong-value one operator over, arriving
			// as a silent wrong ROW SET.
			switch {
			case bindRowField(b, probe, qual, false):
			case bindRowField(b, build, qual, true):
			case bindColumn(b, probe, qual, false):
			case bindColumn(b, build, qual, true):
			case strings.ToLower(b.table) == e.buildAlias:
				b.fromBuild, b.idx = true, build.ResolveColumnIndex(col)
			}
		}
		if b.idx < 0 && !bindColumn(b, probe, col, false) {
			b.fromBuild, b.idx = true, build.ResolveColumnIndex(col)
		}
		src := probe
		if b.fromBuild {
			src = build
		}
		if b.idx < 0 {
			slog.Warn("join residual column resolves on neither side — every candidate will be rejected",
				"column", b.spelled, "filter", e.filter)
		}
		schema[i], b.nested = residualSlot(src, b, fmt.Sprintf("%s%d", e.prefix, i))
		b.declared = schema[i]
	}
	e.row = batch.NewRecordBatch(schema, 1)
	for i := range e.binds {
		e.binds[i].dst = e.row.Columns[i]
		if e.binds[i].idx < 0 {
			// AN UNBOUND REFERENCE IS SQL NULL, and it has to be written as
			// one HERE: refresh never touches this slot again, and a freshly
			// minted vector's null bitmap is all NON-NULL (batch.NewBitmap),
			// so the slot would otherwise read a zero value — an empty string
			// — and the residual would compare against it instead of being
			// UNKNOWN. That inverts the documented failure mode from
			// "rejects every candidate and NULL-pads each preserved row" to
			// "accepts every candidate", which is a join's whole cross
			// product. Caught by arc L1's own pins: `LEFT JOIN LATERAL (…) s
			// ON true` under an enclosing star DECLINES the lifted
			// predicate's materialization, so the residual names a column the
			// join does not publish, and five pinned cells went from three
			// NULL-padded rows to twelve.
			e.binds[i].dst.Nulls.SetNull(0)
		}
	}
}

func bindRowField(b *residualBind, src *batch.RecordBatch, qual string, fromBuild bool) bool {
	pi, fj, ok := src.RowFieldPath(qual)
	if !ok {
		return false
	}
	b.fromBuild, b.idx, b.field = fromBuild, pi, fj
	return true
}

func bindColumn(b *residualBind, src *batch.RecordBatch, name string, fromBuild bool) bool {
	idx := src.ResolveColumnIndex(name)
	if idx < 0 {
		return false
	}
	b.fromBuild, b.idx = fromBuild, idx
	return true
}

// residualSlot declares the combined row's column i: the DECLARATION of what
// the reference yields, which for a ROW field path is the FIELD's and not the
// container's (#568). An unresolvable reference gets a STRING slot that is
// never written, so every evaluation reads NULL.
func residualSlot(src *batch.RecordBatch, b *residualBind, name string) (parquet.Column, bool) {
	if b.idx < 0 || b.idx >= len(src.Columns) {
		return parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true}, false
	}
	col := parquet.Column{Name: name, Type: src.Columns[b.idx].Type, Nullable: true}
	if b.idx < len(src.Schema) {
		col = src.Schema[b.idx].Clone()
		col.Name, col.Nullable = name, true
	}
	if b.field >= 0 {
		col = residualFieldSlot(src.Columns[b.idx], col, b.field, name)
	}
	switch col.Type {
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow:
		return col, true
	}
	return col, false
}

// residualFieldSlot is the declaration of one ROW field: the parquet
// declaration when the container carries one, and the child VECTOR's own type
// otherwise (a container assembled at run time may out-declare its schema).
func residualFieldSlot(v *batch.Vector, container parquet.Column, field int, name string) parquet.Column {
	if field < len(container.Fields) {
		col := container.Fields[field].Clone()
		col.Name, col.Nullable = name, true
		return col
	}
	col := parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true}
	if field < len(v.Children) && v.Children[field] != nil {
		col.Type = v.Children[field].Type
		col.Scale = v.Children[field].DecimalData.Scale
		col.Dimension = v.Children[field].VectorDim
	}
	return col
}

// residualFieldVector follows a container vector's views down to its base and
// returns the child vector for field j together with the row index THAT base
// is addressed by, so the combined row carries a field exactly as it carries a
// column.
//
// A view's children are not addressable by the view's own row index, which is
// the walk exec.rowFieldValue makes for the same reason. A NULL container has
// no field, so it answers "not ok" and the residual sees NULL — which rejects
// the candidate, as SQL's ON semantics require.
func residualFieldVector(v *batch.Vector, field, row int) (*batch.Vector, int, bool) {
	for {
		if v == nil || v.Nulls.IsNullFast(row) {
			return nil, 0, false
		}
		if v.Base == nil {
			break
		}
		row = int(v.Indices[row])
		v = v.Base
	}
	if field < 0 || field >= len(v.Children) || v.Children[field] == nil {
		return nil, 0, false
	}
	return v.Children[field], row, true
}
