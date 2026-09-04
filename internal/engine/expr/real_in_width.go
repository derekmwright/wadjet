package expr

import (
	"strings"
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
//   - the probed operand is REAL-TYPED (resolved from the batch, since the
//     compiler has no schema) — a real column, an explicit cast to real, or
//     unary ± over either, which is what PostgreSQL calls real;
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
// A QUOTED literal narrows too (#646). PostgreSQL casts `real IN
// ('3.1','7.1')` to real[] exactly as it casts the unquoted spelling, and a
// MIXED list `real IN ('3.1', 7.1)` likewise — the array's element type is
// resolved once for the whole list. It was excluded while the kernel's
// float32InSet read a text constant through kernel.toFloat64 (zero for any
// string): narrowing here alone would have split the two paths on a shape they
// agreed — wrongly — about. Both read the float input grammar now.
//
// The refusal follows the member's SPELLING, because PostgreSQL's message
// does: a quoted literal names its own TEXT verbatim ("1e40" is out of range
// for type real) and a numeric one names its DIGITS, since the cast that fails
// there is numeric->real. Verified live for both.

// realLitSet binds a FLOAT32 column against an all-numeric-literal IN list of
// arity two or more, and answers membership at REAL width.
type realLitSet struct {
	// probe is the operand the list is tested against. It is an Expr, not a
	// *ColRef: PostgreSQL decides the array's width from the operand's own
	// TYPE, and `-r_val` and `CAST(d_val AS REAL)` are real-typed without
	// being columns (realTypedOperand).
	probe Expr
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
	// overflow is the source text of a literal a real cannot carry, in either
	// direction: one past real's range narrows to ±Inf and would MATCH a
	// genuine infinite row, one below its smallest denormal narrows to 0.0 and
	// would match every zero row. PostgreSQL raises 22003 for the whole
	// predicate when it casts the array to real[], and so does the kernel
	// (exec.floatConstError).
	overflow    string
	hasOverflow bool
	// badQuoted is the same for a QUOTED member the real input function
	// refuses — 22P02 for text that names no real, 22003 for one out of range
	// — kept apart from overflow because PostgreSQL names a quoted literal's
	// TEXT verbatim where it expands a numeric literal's digits (#646).
	badQuoted    string
	badStatus    kernel.NumConstStatus
	hasBadQuoted bool
	// notReal caches "the probed operand is not REAL-typed", which is a pure
	// function of declarations and so is settled by the first batch that
	// resolves them. Same publish decimalLitCmp.notDecimal uses, for the same
	// reason: these nodes are shared across parallel pipeline workers and
	// concurrent writers can only ever agree.
	notReal atomic.Bool
}

// bindRealLitList binds `col IN (lit, lit, ...)` for the FLOAT32 narrowing, or
// returns nil when any of the rule's conditions above fails. The column's TYPE
// is not among them — no schema is available at compile time — so a binding is
// provisional until applies() sees a batch.
func bindRealLitList(probe Expr, values []Expr) *realLitSet {
	if probe == nil || len(values) < 2 {
		// Arity 1 (and the degenerate empty list) WIDENS in PostgreSQL, which
		// is what the unbound path already does.
		return nil
	}
	r := &realLitSet{probe: probe, set: make(map[float32]struct{}, len(values))}
	for _, v := range values {
		lit, ok := realListMember(v)
		if !ok {
			return nil // see realListMember for the three ways a member declines
		}
		if lit.Val == nil {
			r.sawNull = true
			continue
		}
		if text, quoted := quotedLitText(lit); quoted {
			// The COLUMN's own input function, at real width: `r IN ('3.1')` is
			// `r = '3.1'::real` and `r IN ('abc', …)` is 22P02 (#646).
			f32, st := kernel.FloatLitText(text, 32)
			if st != kernel.NumConstOK {
				r.badQuoted, r.badStatus, r.hasBadQuoted = text, st, true
				continue
			}
			f := float32(f32)
			if f != f {
				r.hasNaN = true
				continue
			}
			r.set[f] = struct{}{}
			continue
		}
		f64, ok := literalFloat64(lit.Val)
		if !ok {
			return nil // not a numeric box at all: a bool, a container
		}
		if kernel.Float32FitOf(f64) != kernel.Float32Fits {
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

// realListMember unwraps one IN-list member to the literal whose value the
// narrowed set holds, or declines.
//
// PostgreSQL picks the array's element type by resolving it over the members
// and the probed column, and float8 is the PREFERRED type of the numeric
// category — so what a member is TYPED as decides the whole list's width
// (EXPLAIN VERBOSE, postgres:17):
//
//	r IN (3.1, 7.1)                    -> '{3.1,7.1}'::real[]              NARROW
//	r IN (CAST(3.1 AS REAL), 7.1)      -> '{3.1,7.1}'::real[]              NARROW
//	r IN (CAST(3.1 AS DOUBLE PRECISION), 7.1)
//	                                   -> '{3.1,7.1}'::double precision[]  WIDEN
//	r IN (3.1, other_col)              -> (r = 3.1::float8) OR (r = other_col)
//
// So an unknown-typed numeric literal and an explicit CAST TO REAL both keep
// the list at real width, a cast to any wider type takes it to double, and a
// non-constant member removes the array entirely. Declining is the WIDEN
// answer, which is what the unbound path already gives — so the three
// declining shapes above all land where PostgreSQL puts them.
func realListMember(e Expr) (*Lit, bool) {
	if lit, ok := e.(*Lit); ok {
		return lit, true
	}
	// Unary MINUS over a literal never reaches here — compileWithCtx folds it
	// into the literal, negating Val and Text together (#369, #452) — but
	// unary PLUS is not folded, and `+3.1` is the same constant to PostgreSQL.
	if u, ok := e.(*UnaryOp); ok && u.Op == "+" {
		return realListMember(u.Operand)
	}
	c, ok := e.(*Cast)
	if !ok {
		return nil, false
	}
	switch strings.ToLower(strings.TrimSpace(c.DestType)) {
	case "real", "float4":
	default:
		return nil, false
	}
	return realListMember(c.Operand)
}

// realBox narrows a REAL-typed operand's boxed value back to the float32 it
// came from.
//
// Two boxes reach it and both are exact. A FLOAT32 column boxes as
// float64(float32) (ColRef.Eval), and widening then narrowing round-trips bit
// for bit; a CAST to REAL boxes a float32 outright. Unary ± over either keeps
// the box it was handed for the float32 column case (UnaryOp negates a
// float64 as a float64) — and negation is exact in binary floating point, so
// narrowing the negated double gives the negated real.
//
// An integer box declines: `-r_val` where r_val is an integer column is not a
// real, and applies() has already established the operand's type, so this is
// only a guard against a box the type did not predict.
func realBox(v any) (float32, bool) {
	switch f := v.(type) {
	case float32:
		return f, true
	case float64:
		return float32(f), true
	}
	return 0, false
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

// applies reports whether this batch's probed operand is REAL-typed, and
// raises 22003 for an unrepresentable literal once it is — the error belongs
// to the predicate, not to a row, and PostgreSQL raises it whether or not any
// row would have matched. (The planner refuses the same list before any task
// is dispatched; this is the backstop for a predicate it could not type.)
func (r *realLitSet) applies(b *batch.RecordBatch) bool {
	if r == nil || r.notReal.Load() {
		return false
	}
	real, settled := realTypedOperand(r.probe, b)
	if !real {
		if settled {
			r.notReal.Store(true)
		}
		return false
	}
	if r.hasBadQuoted {
		raiseQuotedLitRefusal(batch.TypeFloat32, r.badQuoted, r.badStatus)
	}
	if r.hasOverflow {
		raiseNumericOutOfRange("real", r.overflow)
	}
	return true
}

// realTypedOperand is physical.realTypedNode's runtime twin: it reports
// whether an operand's own type is REAL, and whether that answer is SETTLED
// (safe to cache for the rest of the query).
//
// The two must keep answering alike — the planner refuses a list this narrows,
// and a disagreement would mean one of them acting on a predicate the other
// calls something else. The shapes are PostgreSQL's, read off EXPLAIN VERBOSE:
// a real column, an explicit cast to real, and unary ± over either are real;
// `r_val + 0` is DOUBLE PRECISION (an integer literal added to a real gives
// float8 there) and must stay widened, which is why this does not simply walk
// down to a column.
//
// Unsettled means "this batch cannot answer" — a column name that resolves in
// no batch yet says nothing about the next one — the same distinction
// classifyOperand draws for boxedPair.
func realTypedOperand(e Expr, b *batch.RecordBatch) (real, settled bool) {
	switch n := e.(type) {
	case *ColRef:
		if n.structField != "" {
			// A ROW field path: its declared type is fieldTyp, and a FLOAT32
			// field reads through the boxed path exactly as a column does.
			n.resolve(b)
			if n.idx < 0 || n.idx >= len(b.Columns) {
				return false, false
			}
			return n.fieldTyp == batch.TypeFloat32, true
		}
		n.resolve(b)
		if n.idx < 0 || n.idx >= len(b.Columns) {
			return false, false
		}
		return n.typ == batch.TypeFloat32, true
	case *Cast:
		switch strings.ToLower(strings.TrimSpace(n.DestType)) {
		case "real", "float4":
			return true, true
		}
		return false, true
	case *UnaryOp:
		if n.Op == "-" || n.Op == "+" {
			return realTypedOperand(n.Operand, b)
		}
		return false, true
	case *BinOp:
		// real OP real is real; anything else widens. `r * CAST(1 AS REAL)`
		// is real and `r * 1` is double precision, both verified with
		// pg_typeof — the pair that says this tests BOTH sides rather than
		// following one down to a column.
		switch n.Op {
		case "+", "-", "*", "/":
			return realTypedPair(n.Left, n.Right, b)
		}
		return false, true
	case *BinOpNumeric:
		switch n.Op {
		case "+", "-", "*", "/":
			return realTypedPair(n.Left, n.Right, b)
		}
		return false, true
	case *Case:
		// caseResultArms is the DECIMAL fold's list and it is the right one
		// here too: the operand and the WHEN conditions only steer, so they
		// contribute no type. A missing ELSE is an implicit untyped NULL and
		// is skipped, the way CommonDeclType skips it.
		return realTypedArms(caseResultArms(n), b)
	case *Coalesce:
		// COALESCE compiles to its own node rather than a FuncCall
		// (compileFuncCall's special form), so it needs its own arm — the
		// shape that made the first version of this walk answer "double" for
		// `COALESCE(r, r2)` while GREATEST over the same pair answered real.
		return realTypedArms(n.Args, b)
	case *FuncCall:
		return realTypedFuncOperand(n, b)
	case *numericFuncCall:
		// Every function the registry declares NUMERIC is wrapped so binary
		// operators over it take the typed path, and ABS is one of them — a
		// type switch on *FuncCall alone misses the wrapper and answered
		// "double" for `ABS(r)`.
		return realTypedFuncOperand(n.FuncCall, b)
	case *decimalScalarFn:
		// ABS compiles to THIS, not to a FuncCall: `abs` is in
		// decimalScalarOps, so the compiler wraps it in the exact-decimal node
		// with the FuncCall kept as `fallback`. When the node resolves to
		// DECIMAL mode the result is numeric and never real; when it does not,
		// the fallback is what runs and the function rule decides. Missing
		// this arm is why `ABS(r) IN (3.1, 7.1)` still answered zero after
		// GREATEST, LEAST, CASE, NULLIF and COALESCE were fixed.
		if n.resolve(b) {
			return false, true
		}
		if n.fallback == nil {
			return false, true
		}
		return realTypedFuncOperand(n.fallback, b)
	}
	return false, true
}

// realTypedPair is `real OP real`, and it is deliberately AND over both sides
// including the settled flag: an unsettled side makes the whole answer
// unsettled, because a column that resolves in no batch yet says nothing about
// the next one.
func realTypedPair(l, r any, b *batch.RecordBatch) (real, settled bool) {
	le, lok := l.(Expr)
	re, rok := r.(Expr)
	if !lok || !rok {
		return false, true
	}
	lReal, lSettled := realTypedOperand(le, b)
	rReal, rSettled := realTypedOperand(re, b)
	return lReal && rReal, lSettled && rSettled
}

// realTypedArms is a choice construct's candidate list: real when at least one
// candidate is real and no candidate forces a wider type.
//
// A numeric LITERAL is neither, and is the correction the review forced — see
// physical.realTypedChoice, this function's twin, for the pg_typeof
// measurements. `COALESCE(r, 0)` is `real` on the server, so the four shapes a
// BI tool writes must narrow like the all-real ones.
func realTypedArms(arms []Expr, b *batch.RecordBatch) (real, settled bool) {
	if len(arms) == 0 {
		return false, true
	}
	settled = true
	sawReal := false
	for _, a := range arms {
		if a == nil {
			return false, settled
		}
		if isNumericLitExpr(a) {
			continue // neutral: coerced into the common type, never widening it
		}
		r, s := realTypedOperand(a, b)
		settled = settled && s
		if !r {
			return false, settled
		}
		sawReal = true
	}
	return sawReal, settled
}

// isNumericLitExpr is physical.isNumericLiteralNode over the COMPILED tree: a
// numeric constant, or one behind a unary sign. Parentheses are folded away by
// the compiler, so there is no Paren arm to walk.
func isNumericLitExpr(e Expr) bool {
	switch n := e.(type) {
	case *UnaryOp:
		return (n.Op == "-" || n.Op == "+") && isNumericLitExpr(n.Operand)
	case *Lit:
		if _, quoted := quotedLitText(n); quoted {
			return false
		}
		_, ok := literalFloat64(n.Val)
		return ok
	}
	return false
}

// realTypedFuncOperand is the FUNCTION half, and physical.realTypedFuncNode's
// twin. ABS is the one scalar function with a float4 overload in PostgreSQL;
// the CHOICE functions the registry names through Ret.SameAsArgs mirror their
// arguments. Everything else widens, including anything this cannot type.
func realTypedFuncOperand(n *FuncCall, b *batch.RecordBatch) (real, settled bool) {
	if strings.EqualFold(strings.TrimSpace(n.Name), "abs") {
		if len(n.Args) != 1 {
			return false, true
		}
		return realTypedOperand(n.Args[0], b)
	}
	idx, poly := DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args))
	if !poly {
		return false, true
	}
	arms := make([]Expr, 0, len(idx))
	for _, i := range idx {
		if i >= 0 && i < len(n.Args) {
			arms = append(arms, n.Args[i])
		}
	}
	return realTypedArms(arms, b)
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
	// `value "X" is out of range for type T` — PostgreSQL's exact wording for
	// a value read INTO a type, which exec.intConstError already emits for the
	// same condition on the vectorized path.
	panic(fatalEval{sqlerr.New("22003", "value %s is out of range for type %s",
		sqlerr.Quote(kernel.RealOverflowText(input)), destType)})
}
