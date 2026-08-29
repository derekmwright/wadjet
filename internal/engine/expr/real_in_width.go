package expr

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The row-at-a-time half of the FLOAT32 IN-list width rule (#633, the
// distributed sibling of #549).
//
// PostgreSQL decides a `real IN (...)`'s comparison WIDTH from the list's
// SYNTACTIC arity, and the two arities disagree (EXPLAIN VERBOSE, postgres:17):
//
//	more than one element  ->  real = ANY('{...}'::real[])   -- NARROW to real
//	single element         ->  real = 'x'::double precision  -- WIDEN to double
//
// #549 taught the VECTORIZED kernel that rule
// (kernel.ResolveInFilterKernelArity's TypeFloat32 arm). It did not reach this
// path, and this path is the one the stage DAG uses: a worker compiles every
// scan-pushed filter straight to the row evaluator (worker.compileFilterExprs
// -> expr.FilterPredicate), where `In` compares BOXED values and a FLOAT32
// column boxes as float64 (ColRef.Eval). Every member was therefore compared
// at DOUBLE width, so `real IN (3.1, 7.1)` matched NOTHING on the DAG while
// the single-process kernel matched both rows — a silent wrong answer on the
// distributed path only, which the pg-oracle corpus cannot see because it runs
// at SF0.01, where the coordinator takes the in-process fast path.
//
// The binding below narrows for exactly the lists the kernel narrows for, so
// the two paths answer one predicate:
//
//   - the probed operand is a bare FLOAT32 column (resolved from the batch,
//     since the compiler has no schema);
//   - the list holds MORE THAN ONE member (the syntactic count, NULL members
//     included — PostgreSQL casts `{3.1,NULL}` to real[] as readily as
//     `{3.1,7.1}`);
//   - every member is a literal boxed as a NUMBER.
//
// A member that is not a constant takes the binding away entirely, because
// PostgreSQL stops building an array at all: `real IN (3.1, other_col)` plans
// as `(r_val = '3.1'::double precision) OR (r_val = other_col)` — the widened
// scalar rule, twice — so the fallthrough below is that answer, unchanged.
//
// A QUOTED literal is also excluded, and deliberately: PostgreSQL narrows
// `real IN ('3.1','7.1')` to real[] the same way, but the kernel's
// float32InSet reads a text constant through kernel.toFloat64, which answers
// ZERO for any string. Narrowing here alone would make the two paths disagree
// about a shape they agree (wrongly) about today. The text-constant-as-zero
// defect is one bug for every float arm and is tracked on its own.

// realLitSet binds a FLOAT32 column against an all-numeric-literal IN list of
// arity two or more, and answers membership at REAL width.
type realLitSet struct {
	col *ColRef
	// set holds the literals already narrowed to float32 — the same set
	// kernel.float32InSet builds, so both paths probe identical keys. Go map
	// keys compare with `==`, which folds -0.0 and +0.0 into one key (the
	// answer PostgreSQL gives) and leaves a NaN key unreachable, which is why
	// hasNaN is carried separately.
	set    map[float32]struct{}
	hasNaN bool
	// sawNull records a NULL member. It is stripped from the set for
	// three-valued logic but still counts toward the ARITY above, and a miss
	// with a NULL in the list is UNKNOWN rather than FALSE.
	sawNull bool
	// overflow is the source text of a FINITE literal whose magnitude exceeds
	// real's range. Narrowing it yields ±Inf, which would MATCH a genuine
	// infinite row; PostgreSQL raises 22003 for the whole predicate when it
	// casts the array to real[], and so does the kernel (exec.floatConstError).
	overflow    string
	hasOverflow bool
	// notReal caches "the probed operand is not a FLOAT32 column", which is a
	// pure function of a declaration and so is settled by the first batch that
	// resolves the name. Same publish decimalLitCmp.notDecimal uses, for the
	// same reason: these nodes are shared across parallel pipeline workers and
	// concurrent writers can only ever agree.
	notReal atomic.Bool
}

// bindRealLitList binds `col IN (lit, lit, ...)` for the FLOAT32 narrowing, or
// returns nil when any of the rule's conditions above fails. The column's TYPE
// is not among them — no schema is available at compile time — so a binding is
// provisional until applies() sees a batch.
func bindRealLitList(col Expr, values []Expr) *realLitSet {
	c, ok := bareCol(col)
	if !ok || len(values) < 2 {
		// Arity 1 (and the degenerate empty list) WIDENS in PostgreSQL, which
		// is what the unbound path already does.
		return nil
	}
	r := &realLitSet{col: c, set: make(map[float32]struct{}, len(values))}
	for _, v := range values {
		lit, ok := v.(*Lit)
		if !ok {
			return nil // a non-constant member: PostgreSQL builds no array
		}
		if lit.Val == nil {
			r.sawNull = true
			continue
		}
		f64, ok := literalFloat64(lit.Val)
		if !ok {
			return nil // a quoted or non-numeric literal: see the file comment
		}
		if kernel.Float32LitOverflow(f64) {
			r.overflow, r.hasOverflow = litOverflowText(lit), true
			continue
		}
		f := float32(f64)
		if f != f {
			r.hasNaN = true
			continue
		}
		r.set[f] = struct{}{}
	}
	return r
}

// literalFloat64 reads a literal's box as a number, refusing the string box on
// purpose (see the file comment).
func literalFloat64(v any) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case float32:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case int32:
		return float64(tv), true
	case int:
		return float64(tv), true
	}
	return 0, false
}

// litOverflowText is the literal's source text for the 22003 message, falling
// back to the box when the compiler kept no text.
func litOverflowText(lit *Lit) string {
	if lit.Text != "" {
		return lit.Text
	}
	return toString(lit.Val)
}

// applies reports whether this batch's probed operand really is a FLOAT32
// column, and raises 22003 for an over-range literal once it is — the error
// belongs to the predicate, not to a row, and PostgreSQL raises it whether or
// not any row would have matched.
func (r *realLitSet) applies(b *batch.RecordBatch) bool {
	if r == nil || r.notReal.Load() {
		return false
	}
	r.col.resolve(b)
	if r.col.idx < 0 || r.col.idx >= len(b.Columns) || r.col.typ != batch.TypeFloat32 {
		if r.col.idx >= 0 && r.col.idx < len(b.Columns) {
			// The name resolved to a column of another type: no batch will
			// change that answer.
			r.notReal.Store(true)
		}
		return false
	}
	if r.hasOverflow {
		raiseNumericOutOfRange("real", r.overflow)
	}
	return true
}

// contains probes the narrowed set with the column's own float32 value.
//
// The caller hands it the BOXED value, which for a FLOAT32 column is
// float64(float32) — an exact, order-preserving widening, so narrowing it back
// recovers the stored value bit for bit. Reading Float32Data directly would be
// no more exact and would have to decline a VIEW vector, whose values live in
// a base vector through an index.
func (r *realLitSet) contains(f float32) bool {
	if f != f {
		return r.hasNaN
	}
	_, ok := r.set[f]
	return ok
}

// raiseNumericOutOfRange aborts the query with SQLSTATE 22003, PostgreSQL's
// numeric_value_out_of_range, for a literal that cannot be represented in the
// type the comparison casts it to. It is the row path's spelling of the
// refusal exec.floatConstError makes for the vectorized kernel (#549).
func raiseNumericOutOfRange(destType, input string) {
	panic(fatalEval{sqlerr.New("22003", "%q is out of range for type %s", input, destType)})
}
