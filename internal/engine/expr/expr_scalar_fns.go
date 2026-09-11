// This file holds expr scalar fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// --- Scalar functions ---

// ArrayLitExpr evaluates to a []any containing the evaluated elements.
type ArrayLitExpr struct {
	Elements []Expr
}

func (e *ArrayLitExpr) Eval(b *batch.RecordBatch, row int) any {
	result := make([]any, len(e.Elements))
	for i, elem := range e.Elements {
		result[i] = elem.Eval(b, row)
	}
	return result
}

// FuncCall represents a scalar function call.
//
// Note: this struct holds NO per-call mutable state. A previous version cached
// an args buffer on the receiver to avoid per-call allocation, but that was
// unsafe under parallel pipeline execution: aggPreProject closures (and other
// wrapped-expression paths) capture the same *FuncCall by pointer rather than
// cloning it per worker, so concurrent goroutines stomped on the shared args
// buffer and produced non-deterministic Q02 row counts at SF0.01 (and worse
// at SF100). The vecFn / prepared lookup caches are still guarded by sync.Once
// (resolved once per BATCH, off EvalVec — not once per row, so the guard
// never sat in a row loop). fn is different: it is resolved off Eval, the
// per-row entry point every one of the 273 scalar functions reaches, so it
// uses the same double-checked atomic.Bool guard as BinOpFloat64/BinOpInt64's
// opCode and BinOpNumeric's mode — small enough that resolveFn inlines into
// Eval (verified with -gcflags='-m'), where sync.Once.Do's closure-plus-load
// did not.
type FuncCall struct {
	Name string
	Args []Expr

	// fnReady publishes fn and the argument-family flags below it: set last
	// under fnMu, read first (and alone) by Eval.
	fnReady atomic.Bool
	fnMu    sync.Mutex
	fn      ScalarFunc
	// Argument-family flags, resolved with fn under fnReady: this function
	// reads its arguments as text (stringInputFuncs) / as network addresses
	// (networkTextFuncs) / as instants (temporalInputFuncs) / as instants
	// that must remember whether they came from a DATE or a TIMESTAMP
	// column (dateArithFuncs, which render their result). Resolved once
	// rather than re-looked-up per row.
	wantsText        bool
	wantsNetworkText bool
	wantsInstant     bool
	wantsDateKind    bool
	// This function picks the extremum of its arguments (GREATEST / LEAST),
	// so it is evaluated with the argument EXPRESSIONS in hand: a numeric
	// literal's exact source text settles an ordering its float64 box cannot
	// (#465). extremumOp is CmpGt for GREATEST and CmpLt for LEAST.
	extremum   bool
	extremumOp CmpOp
	// extremumArms is the per-argument binding for those arguments, armed
	// once alongside extremumOp under fnMu: the non-numeric-literal refusal
	// and the declaration-driven comparison, both keyed by argument index
	// because the (best-so-far, candidate) pair moves between iterations.
	extremumArms *extremumArms
	// nullifArms is the same binding for NULLIF, whose EQUALITY test is the
	// same question GREATEST/LEAST's ordering is: `NULLIF(d_2, d_4)` over two
	// DECIMAL columns at different scales boxes them as their rendered TEXT,
	// and compare() then reads "12.75" and "12.7500" as different strings —
	// so NULLIF answered 12.75 where PostgreSQL answers NULL. The other boxed
	// comparison sites over that pair were bound in #506; this one was
	// missed, and was invisible until a projected NULLIF over a DECIMAL could
	// run at all (ADR-0024 item 2).
	nullifArms *extremumArms
	// choiceArms is the argument list this call CHOOSES its value from, read
	// off the registry's polymorphic declaration (Ret.SameAsArgs) so it
	// cannot drift from the type fold: GREATEST/LEAST/COALESCE/IFNULL mirror
	// every argument, NULLIF argument 0 alone, IF its two branches. nil for
	// every function that declares its own type, which is what keeps the
	// DECIMAL box check off the other 270-odd per-row paths. See dch.
	choiceArms []Expr
	// dch is the DECIMAL box mode for those arms — see Case.dch (#695).
	dch decimalChoice

	vecOnce sync.Once
	vecFn   VecScalarFunc
	// The declared return type, resolved with vecFn: it names the typed
	// slice the kernel writes, which EvalVec checks the output vector can
	// actually hold before handing it over.
	vecRet   batch.TypeID
	vecRetOK bool
	// This function reads its arguments as text (stringInputFuncs), so a
	// non-byte-array column has no bytes for the kernel to read.
	vecTextFn bool
	// The argument positions this function does NOT read as text
	// (typedArgPositions); nil when every argument is text.
	vecTypedArgs map[int]bool

	// Compile-once state for literal-argument regexp_replace (see
	// regexp_prepared.go). Built lazily under prepOnce; nil when the call
	// shape doesn't qualify.
	prepOnce sync.Once
	prepared *preparedRegexp
}

// formatTemporalArgs rewrites boxed TypeDate ColRef argument values to
// their canonical ISO form for string-input functions. Only direct column
// references are covered — a nested expression's output type isn't known
// here (and nothing in the TPC-H or observed customer shapes feeds a
// computed date into a string function).
func (e *FuncCall) formatTemporalArgs(args []any) {
	for i, a := range e.Args {
		// A CAST to a temporal type boxes its result exactly as the matching
		// column does (#340), so it needs the same rendering before a string
		// function reads it — otherwise SUBSTR(CAST(d AS DATE), 1, 4)
		// substrings the epoch-day digits, which is the #273 defect reached
		// through the cast instead of through the column.
		if c, ok := a.(*Cast); ok {
			v, isInt := args[i].(int64)
			if !isInt {
				continue
			}
			switch castTemporalKind(c.DestType) {
			case castToDateKind:
				args[i] = batch.FormatDate(int32(v))
			case castToTimestampKind:
				args[i] = batch.FormatTimestamp(v)
			}
			continue
		}
		// valueType: a ROW FIELD PATH of type DATE boxes as the same epoch
		// day a DATE column does, so it needs the same rendering, and typ
		// names the CONTAINER (#568).
		cr, ok := a.(*ColRef)
		if !ok {
			continue
		}
		v, isInt := args[i].(int64)
		if !isInt {
			continue
		}
		switch cr.valueType() {
		case batch.TypeDate:
			args[i] = batch.FormatDate(int32(v))
		case batch.TypeTimestamp:
			// The TIMESTAMP twin of the DATE arm above, and it was missing:
			// `c_ts || ''`, `CONCAT(c_ts, 'x')` and `UPPER(c_ts)` all read the
			// raw epoch-millisecond box and answered "1700000000000" where
			// pgwire renders the instant for the SAME column. One connection,
			// one column, two answers — which is exactly what #544 is, reached
			// through a string function instead of through CAST (#544).
			args[i] = batch.FormatTimestamp(v)
		}
	}
}

// formatNetworkArgs renders TypeIPv4/TypeMAC ColRef arguments canonically as
// dotted-quad/colon-hex for BOTH networkTextFuncs and stringInputFuncs (#484, #500).
// Read the column through Vector.GetValue's exported rendering boundary because
// formatIPv4/formatMAC are batch-internal; raw int64 boxes are not their text.
// Only direct ColRefs are covered: nested expression output types are unknown here.
// IPv6/CIDR/UUID already use GetValue rendering in ColRef.Eval; PORT/PROTOCOL
// already box their canonical numeric text value and need no rewrite.
// See docs/internals/network-function-argument-rendering.md for the design.
func (e *FuncCall) formatNetworkArgs(b *batch.RecordBatch, row int, args []any) {
	for i, a := range e.Args {
		cr, ok := a.(*ColRef)
		if !ok {
			continue
		}
		// valueType, not typ: a ROW FIELD PATH boxes exactly as a column of
		// the FIELD's type does, so it needs the same unwinding, and typ
		// names the CONTAINER (#568).
		if t := cr.valueType(); t != batch.TypeIPv4 && t != batch.TypeMAC {
			continue
		}
		if dv, ok := cr.displayValue(b, row); ok {
			args[i] = dv
		}
	}
}

// civilDate is a DATE column's value resolved to the instant it denotes (UTC
// midnight of that day), carrying the one fact the instant alone cannot: its
// column has no time-of-day to preserve. Only resolveTemporalArgs mints one,
// and only parseDateArg / parseDateValue read it; every other consumer sees a
// plain time.Time, so this stays inside the date-arithmetic family.
type civilDate struct{ t time.Time }

// resolveTemporalArgs rewrites a boxed DATE/TIMESTAMP column argument to the
// instant it denotes, so the function body's parseTime sees an unambiguous
// time.Time instead of a unit-less number. Resolution goes through
// columnInstant — the same resolver the vectorized kernels use — which is what
// makes the two paths agree by construction rather than by coincidence.
//
// Only direct column references are covered, matching formatTemporalArgs: a
// nested expression's output type isn't known here, and a literal or a
// computed value already carries its own unambiguous form (text, or a number
// that means seconds). Columns of any other type are left alone, so
// year(int_col) keeps reading its int64 as epoch seconds exactly as before.
//
// For the date-arithmetic family a resolved DATE column is tagged civilDate,
// because those functions render their result and a DATE must render as a
// calendar date (issue #322).
func (e *FuncCall) resolveTemporalArgs(b *batch.RecordBatch, row int, args []any) {
	for i, a := range e.Args {
		if args[i] == nil {
			continue
		}
		// A CAST to a temporal type is a column value in all but name once
		// #340 made it box epoch days / epoch milliseconds, and it loses the
		// unit at exactly the same point. Resolve it here for the same reason
		// and by the same rule, or YEAR(CAST(d AS DATE)) reads 9505 days as
		// 9505 seconds and answers 1970 — the #319 defect, reached through
		// the cast instead of through the column.
		if c, ok := a.(*Cast); ok {
			v, isInt := args[i].(int64)
			if !isInt {
				continue
			}
			switch castTemporalKind(c.DestType) {
			case castToDateKind:
				t := time.Unix(v*86400, 0).UTC()
				if e.wantsDateKind {
					args[i] = civilDate{t: t}
				} else {
					args[i] = t
				}
			case castToTimestampKind:
				args[i] = time.UnixMilli(v).UTC()
			}
			continue
		}
		cr, ok := a.(*ColRef)
		if !ok {
			continue
		}
		// A ROW FIELD PATH loses its unit at the same point a column does,
		// and recovers it the same way — from the vector that holds the
		// value. valueType/valueVector are that pair for both (#568).
		vt := cr.valueType()
		if vt != batch.TypeDate && vt != batch.TypeTimestamp {
			continue
		}
		src, r, ok := cr.valueVector(b, row)
		if !ok {
			continue
		}
		if t, ok := columnInstant(src, r); ok {
			if e.wantsDateKind && vt == batch.TypeDate {
				args[i] = civilDate{t: t}
				continue
			}
			args[i] = t
		}
	}
}

// temporalOperand recovers the declared unit for date ± interval:
// DATE columns/fields become civilDate from epoch days; TIMESTAMP columns
// become time.Time from epoch milliseconds (#322, #332).
// Temporal CAST destinations supply the same unit as the matching column,
// including typed date literals lowered to CAST (#340).
// Text passes through with its own rendering. Bare numbers, other column
// types and unsupported expressions decline to unchanged numeric arithmetic.
// See docs/internals/temporal-arithmetic-operand-units.md for the design.
func temporalOperand(b *batch.RecordBatch, row int, e Expr, v any) (any, bool) {
	if s, ok := v.(string); ok {
		return s, true
	}
	if c, ok := e.(*Cast); ok {
		n, isInt := v.(int64)
		if !isInt {
			return nil, false
		}
		switch castTemporalKind(c.DestType) {
		case castToDateKind:
			return civilDate{t: time.Unix(n*86400, 0).UTC()}, true
		case castToTimestampKind:
			return time.UnixMilli(n).UTC(), true
		}
		return nil, false
	}
	cr, ok := e.(*ColRef)
	if !ok {
		return nil, false
	}
	// Same rule as resolveTemporalArgs, for the operator instead of the
	// function: a ROW FIELD PATH's unit lives in the FIELD's declaration and
	// its value in the child vector (#568).
	vt := cr.valueType()
	if vt != batch.TypeDate && vt != batch.TypeTimestamp {
		return nil, false
	}
	src, r, ok := cr.valueVector(b, row)
	if !ok {
		return nil, false
	}
	t, ok := columnInstant(src, r)
	if !ok {
		return nil, false
	}
	if vt == batch.TypeDate {
		return civilDate{t: t}, true
	}
	return t, true
}

func (e *FuncCall) resolveFn() {
	if !e.fnReady.Load() {
		e.resolveFnSlow()
	}
}

// resolveFnSlow runs exactly once per node: the registry lookup plus the
// three argument-family flags derived from it.
func (e *FuncCall) resolveFnSlow() {
	e.fnMu.Lock()
	defer e.fnMu.Unlock()
	if e.fnReady.Load() {
		return
	}
	lower := strings.ToLower(e.Name)
	e.fn = DefaultRegistry.Lookup(e.Name)
	e.wantsText = stringInputFuncs[lower]
	e.wantsNetworkText = networkTextFuncs[lower]
	e.wantsInstant = temporalInputFuncs[lower]
	e.wantsDateKind = dateArithFuncs[lower]
	switch lower {
	case "greatest":
		e.extremum, e.extremumOp = true, CmpGt
		e.extremumArms = armExtremumArms(e.Args)
	case "least":
		e.extremum, e.extremumOp = true, CmpLt
		e.extremumArms = armExtremumArms(e.Args)
	case "nullif":
		if len(e.Args) >= 2 {
			e.nullifArms = armExtremumArms(e.Args)
			e.nullifArms.nullif = true
		}
		// The box follows the DECLARATION's rule, which for NULLIF is the
		// operator its two arguments select rather than a fold over them
		// (#757). Set before choiceArms is built below, so the first batch
		// resolves under the right rule.
		e.dch.nullif = true
	}
	if idx, poly := DefaultRegistry.ReturnType(e.Name).SameAsArgs(len(e.Args)); poly {
		arms := make([]Expr, 0, len(idx))
		for _, i := range idx {
			if i >= 0 && i < len(e.Args) {
				arms = append(arms, e.Args[i])
			}
		}
		if e.dch.nullif {
			// NULLIF's candidate list is [0] — the value comes from argument
			// 0 — but its TYPE is decided by BOTH arguments through the
			// operator they select, so the box resolver needs both (#757).
			arms = append([]Expr(nil), e.Args[:2]...)
		}
		e.choiceArms = arms
	}
	e.fnReady.Store(true)
}

func (e *FuncCall) Eval(b *batch.RecordBatch, row int) any {
	e.resolveFn()
	if e.fn == nil {
		return nil
	}
	// Allocate args on every call so concurrent goroutines sharing this
	// *FuncCall don't stomp on each other. For small N (the common case)
	// the Go compiler routinely stack-allocates this slice, so the cost
	// vs the old per-receiver cache is negligible.
	args := make([]any, len(e.Args))
	for i, a := range e.Args {
		args[i] = a.Eval(b, row)
	}
	if e.wantsText {
		e.formatTemporalArgs(args)
		// stringInputFuncs (length/concat/upper/starts_with/... — #500) has
		// the identical gap networkTextFuncs already closed for a different
		// function family: a TypeIPv4/TypeMAC ColRef argument boxes as its
		// raw encoded int64, and formatNetworkArgs is the one rewrite that
		// turns it back into address text, so it runs for both families
		// rather than being duplicated.
		e.formatNetworkArgs(b, row, args)
	}
	if e.wantsNetworkText {
		e.formatNetworkArgs(b, row, args)
	}
	if e.wantsInstant {
		e.resolveTemporalArgs(b, row, args)
	}
	var out any
	switch {
	case e.extremum:
		out = pickExtremum(b, args, e.extremumOp, e.extremumArms)
	case e.nullifArms != nil:
		out = evalNullIf(b, args, e.nullifArms)
	default:
		out = e.fn(args)
	}
	if e.choiceArms == nil || out == nil {
		// Not a construct that CHOOSES between its arguments. Every other
		// function declares its own result type, so nothing here can be a
		// DECIMAL answered through an integer box (#695).
		return out
	}
	return choiceBox(e.boxMode(b), out)
}

// boxMode reports what this call's chosen box must be rewritten to — see
// Case.boxMode (#695, #555).
func (e *FuncCall) boxMode(b *batch.RecordBatch) choiceBoxMode {
	if e.dch.ready.Load() {
		return e.dch.mode
	}
	return e.dch.resolveSlow(b, e.choiceArms)
}

// evalNullIf is fnNullIf with the argument EXPRESSIONS in hand, so the
// equality test uses the rule the two DECLARATIONS select instead of
// compare()'s reading of two boxes.
//
// A DECIMAL boxes as its rendered text, so compare() called "12.75" and
// "12.7500" — the same number at two scales — different, and NULLIF returned
// its first argument where PostgreSQL returns NULL. extremumArms.order is the
// binding #506 built for exactly this pair; NULLIF is the site it did not
// reach, because the equality is inside a registry function rather than in a
// comparison node. A pair the binding does not apply to (two strings, a text
// column against a quoted literal) falls through to compare() unchanged.
// evalNullIf answers NULLIF's value AT THE CALL'S TYPE.
//
// The materialization is the same one GREATEST/LEAST have had since #646 and
// NULLIF never did: argument 0 can be a QUOTED literal, which arrives as a Go
// string, and PostgreSQL resolves NULLIF's type from the operator its two
// arguments select — `NULLIF('0xC.C', real_col)` is 12.75 there, and the four
// characters had nowhere to go here (#724). It is a no-op for every argument
// that is not a quoted literal, which is every shape the pre-#724 corpus held.
func evalNullIf(b *batch.RecordBatch, args []any, arms *extremumArms) any {
	v := evalNullIfChosen(b, args, arms)
	if v == nil {
		return nil
	}
	return arms.materialize(b, 0, v)
}

func evalNullIfChosen(b *batch.RecordBatch, args []any, arms *extremumArms) any {
	if len(args) < 2 {
		return nil
	}
	// The same refusal every sibling site makes: a literal that names no
	// number beside a DECIMAL column is 22P02 on PostgreSQL and at wadjet's
	// `=`, GREATEST, LEAST and simple CASE, and NULLIF was the one site that
	// answered instead — `NULLIF(d, 'abc')` returned every row. It runs
	// BEFORE the NULL short-circuit for the reason the other sites run it
	// first: the refusal is a property of the operand PAIR's DECLARATIONS,
	// not of the row's values (ADR-0012 item 6's neighbourhood).
	arms.checkRefusal(b)
	if args[0] == nil || args[1] == nil {
		return args[0]
	}
	if c, ok, unknown := arms.order(b, 0, 1, args[0], args[1]); ok {
		// unknown is "these two have no comparable relation" (a stored value
		// naming no address, ADR-0012 item 10). They are not equal, so the
		// first argument stands — the same answer NULL-vs-value gives.
		if !unknown && c == 0 {
			return nil
		}
		return args[0]
	}
	// The pair rule did not apply, which happens when one operand has no
	// declaration this layer can read — a scalar subquery, a container
	// element. compare() would then order two DECIMAL renderings by BYTES,
	// and "12.75" is not "12.7500" bytewise though they are one number:
	// `NULLIF(d, (SELECT d4 …))` answered the row where PostgreSQL answers
	// NULL. A DECIMAL against an UNCLASSIFIABLE operand is therefore
	// compared as the numbers the two boxes name.
	//
	// Against a TEXT operand it is not: a STRING column compares AS TEXT
	// whatever its digits look like (#504), which is what every sibling site
	// does for the same pair, and reading it numerically here made NULLIF
	// disagree with `=`, GREATEST, CASE and IS DISTINCT FROM on one commit.
	if arms.decimalVsUnclassifiable(b) {
		if ls, ok := args[0].(string); ok {
			if rs, ok := args[1].(string); ok {
				if c, ok := batch.CompareDecimalTexts(ls, rs); ok {
					if c == 0 {
						return nil
					}
					return args[0]
				}
			}
		}
	}
	if compare(args[0], args[1], CmpEq) {
		return nil
	}
	return args[0]
}

// pickExtremum is fnGreatest/fnLeast with the argument EXPRESSIONS in hand.
//
// fnGreatest orders through compare(), which reads a DECIMAL column's box as
// text and a numeric literal's as a float64 and has no exact reading of that
// pair — so `GREATEST(d, lit)` picked the wrong operand whenever the literal
// named a value a double cannot hold (#465). The ordering rule is otherwise
// unchanged, NULL skipping included: PostgreSQL's GREATEST/LEAST ignore NULL
// arguments and answer NULL only when every argument is NULL.
//
// A candidate that is not a number is a query ERROR against a DECIMAL
// argument, never a value PostgreSQL would just skip past (#463/#505):
// `GREATEST(d, 'abc')` answered 'abc' as the extremum instead of refusing.
// The refusal checks the pair actually being compared THIS iteration — best
// so far against the new candidate — the same as the comparison itself.
//
// Two DECIMAL arguments are the pair the boxes cannot distinguish from two
// strings, so `GREATEST(d_2, d_4)` picked the LEXICOGRAPHICALLY greater
// rendering (#506). arms.order answers that pair — and every mixed
// DECIMAL/number pair — from the arguments' declarations; anything it declines
// falls through to the literal-side carry-through, exactly as before.
func pickExtremum(b *batch.RecordBatch, args []any, op CmpOp, arms *extremumArms) any {
	// The refusal runs BEFORE the loop, because it is a property of the call's
	// arguments and not of the pairs the values happen to select. Firing it
	// inside the loop is what made `GREATEST(k, 'abc', d)` raise and
	// `LEAST(k, 'abc', d)` answer on the same three arguments (#517), and it
	// also skipped a call with fewer than two non-NULL arguments entirely,
	// where PostgreSQL still raises.
	arms.checkRefusal(b)
	var best any
	bestIdx := -1
	for i, a := range args {
		if a == nil {
			continue
		}
		if best == nil {
			best, bestIdx = a, i
			continue
		}
		better := false
		if arms != nil {
			c, ok, unknown := arms.order(b, i, bestIdx, a, best)
			switch {
			case unknown:
				// A candidate with no place in the order — a stored value
				// that names no address (ADR-0012 item 10) — is SKIPPED, the
				// way PostgreSQL's GREATEST/LEAST skip a NULL argument, and
				// is never text-ordered against the best so far. Residual:
				// such a value arriving FIRST becomes the running best and
				// nothing displaces it, so the answer is the malformed value
				// (#565).
				better = false
			case ok:
				better = cmpOrder(c, op)
			default:
				better = compare(a, best, op)
			}
		} else {
			better = compare(a, best, op)
		}
		if better {
			best, bestIdx = a, i
		}
	}
	// The winner comes back AT THE CALL'S TYPE. A quoted literal arrives as a
	// Go string, and returning that string projected four characters into a
	// FLOAT64 vector; PostgreSQL answers the number.
	return arms.materialize(b, bestIdx, best)
}

// EvalVec evaluates the function for an entire batch, writing results to out.
// Falls back to per-row Eval if no vectorized implementation exists or if
// argument types can't be resolved to column vectors.
func (e *FuncCall) EvalVec(b *batch.RecordBatch, out *batch.Vector, n int) {
	e.vecOnce.Do(func() {
		e.vecFn = DefaultRegistry.LookupVec(e.Name)
		// No argument types to consult here, so a polymorphic declaration
		// answers with its fallback. That is a guess, and it is used the
		// same way a decision is: the guard below re-checks it against the
		// output vector anyway, and a mismatch costs the per-row path.
		vecDecl, c := DefaultRegistry.ReturnType(e.Name).Resolve(0, nil)
		e.vecRet = vecDecl.ID
		e.vecRetOK = c != Undecided
		lower := strings.ToLower(e.Name)
		e.vecTextFn = stringInputFuncs[lower]
		e.vecTypedArgs = typedArgPositions[lower]
	})
	if e.vecFn == nil {
		if e.tryEvalMemoized(b, out, n) {
			return
		}
		for i := 0; i < n; i++ {
			out.SetValue(i, e.Eval(b, i))
		}
		return
	}

	// Resolve argument vectors: ColRef → column directly, Lit → const vector.
	// Allocated per-call to avoid data races across parallel pipeline clones.
	argVecs := make([]*batch.Vector, len(e.Args))
	for i, arg := range e.Args {
		switch a := arg.(type) {
		case *ColRef:
			a.resolve(b)
			if a.idx < 0 || a.idx >= len(b.Columns) {
				e.evalVecPerRow(b, out, n)
				return
			}
			// A ROW FIELD PATH is not vectorizable through these kernels: its
			// idx points at the CONTAINER column, so argVecs[i] below would
			// hand the kernel the ROW vector instead of the field's child —
			// vecYear then read an empty Int64Data and answered NULL, while
			// the same query with only one temporal function took the
			// per-row path and answered correctly (#568). The per-row path
			// resolves the field through ColRef.Eval / resolveTemporalArgs,
			// which already handle a field path; send every field-path arg
			// there, for every vec kernel at once.
			if a.structField != "" {
				e.evalVecPerRow(b, out, n)
				return
			}
			// String-input vec kernels read BytesData; a TypeDate column
			// has none and must render through the (fixed) per-row path
			// (issue #273). The same is true of a column that is not stored
			// as a byte array at all: `SELECT UPPER(int_col)` indexed a nil
			// offsets array and killed the process.
			//
			// TypeIPv4/TypeMAC get the identical guard for the identical
			// reason (#500): both box as a raw encoded int64 with no bytes
			// arena at all, and formatNetworkArgs — the rewrite that turns
			// that back into address text — only runs on the per-row Eval
			// path, never here.
			//
			// Every argument the kernel reads AS TEXT gets that check, not
			// just position 0. Position 0 alone was the premise "the one
			// argument every function in stringInputFuncs reads as text",
			// true for substr/left/right and false for the rest: concat
			// reads all of them, replace three, starts_with/ends_with/
			// contains two. `CONCAT(text_col, int_col)` therefore handed
			// vecConcat a BIGINT vector with a zero-length Offsets slice and
			// took the whole server down on ordinary single-table SQL (#509).
			if e.vecTextFn && (a.typ == batch.TypeDate || a.typ == batch.TypeIPv4 || a.typ == batch.TypeMAC ||
				(!e.vecTypedArgs[i] && !vecTextReadable(b.Columns[a.idx], n))) {
				e.evalVecPerRow(b, out, n)
				return
			}
			argVecs[i] = b.Columns[a.idx]
		case *Lit:
			cv := makeConstVector(a.Val, n)
			if cv == nil {
				e.evalVecPerRow(b, out, n)
				return
			}
			argVecs[i] = cv
		default:
			// Complex expression arg (nested functions, CASE, etc.) —
			// fall back to per-row for safety. Extending to nested VecExpr
			// requires knowing each arg's output type at eval time.
			e.evalVecPerRow(b, out, n)
			return
		}
	}

	// A vec kernel writes a typed slice of out directly — out.Float64Data[i],
	// out.BoolData[i], out.BytesData.Set(i, …) — and which slice is fixed by
	// the function's declared return type. When out cannot hold that type the
	// write runs off the end of a zero-length slice and panics the whole
	// process, every connection with it: four separate functions shipped that
	// way (#310). The declaration is now what types the projection, so the
	// two agree by construction; this guard is what makes that structural
	// rather than a promise, for every kernel at once and every caller that
	// hands a vec expression an output vector of its own choosing. A mismatch
	// costs the per-row path — a slower answer, not a dead server.
	if !vecOutputHolds(out, e.vecRet, e.vecRetOK, n) {
		e.evalVecPerRow(b, out, n)
		return
	}

	e.vecFn(argVecs, out, n)
}

// vecTextReadable reports whether a kernel that reads its argument as text can
// read this vector's bytes directly: values stored as a plain byte array, with
// an offsets array covering the batch. Anything else — an int column, a
// dictionary view whose typed slices are nil — goes through the per-row path,
// which is view- and type-aware.
func vecTextReadable(v *batch.Vector, n int) bool {
	return v != nil && byteArrayShaped(v) && len(v.BytesData.Offsets) > n
}

// vecOutputHolds reports whether out is backed by the storage a kernel
// declared to return t writes into. ok=false (a dynamic declaration) can never
// be safe: nothing says which slice the kernel writes.
func vecOutputHolds(out *batch.Vector, t batch.TypeID, ok bool, n int) bool {
	if out == nil || !ok {
		return false
	}
	switch t {
	case batch.TypeBool:
		return len(out.BoolData) >= n
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		return len(out.Int32Data) >= n
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		return len(out.Int64Data) >= n
	case batch.TypeFloat32:
		return len(out.Float32Data) >= n
	case batch.TypeFloat64:
		return len(out.Float64Data) >= n
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		return len(out.BytesData.Offsets) > n
	case batch.TypeVector:
		return out.Type == batch.TypeVector && out.VectorDim > 0 &&
			len(out.Float32Data) >= n*out.VectorDim
	}
	return out.Type == t
}

// tryEvalMemoized returns false for ineligible shapes so the caller uses its per-row loop.
// The memo belongs exclusively to ONE evaluation of ONE batch by ONE goroutine,
// and dies at return; parallel FuncCall clones share no memo.
// Zero-copy string keys require the input batch to outlive this call and its column
// to remain immutable. Output must be a separate pooled batch column, never alias input;
// that is also required by existing zero-copy probing and sequential offset writes.
// Memo values may also view input bytes (replaceAll/no match or substring replacement);
// copy them into the output arena on emission.
// See docs/internals/scalar-function-memo-arena-lifetime.md for the design.
func (e *FuncCall) tryEvalMemoized(b *batch.RecordBatch, out *batch.Vector, n int) bool {
	if len(e.Args) == 0 || !memoizableFuncs[strings.ToLower(e.Name)] {
		return false
	}
	cr, ok := e.Args[0].(*ColRef)
	if !ok {
		return false
	}
	for _, a := range e.Args[1:] {
		if _, isLit := a.(*Lit); !isLit {
			return false
		}
	}
	cr.resolve(b)
	if cr.idx < 0 || cr.idx >= len(b.Columns) {
		return false
	}
	vec := b.Columns[cr.idx]
	if vec.Type != batch.TypeString && vec.Type != batch.TypeBytes {
		return false
	}
	hasNulls := vec.Nulls.HasNulls()
	// Prepared fast path (regexp_prepared.go): literal-argument
	// regexp_replace evaluates as a direct string→string call — compiled
	// pattern and pre-parsed replacement template, no []any boxing.
	prep := e.preparedReplace()
	if out.Type == batch.TypeString {
		e.evalMemoizedStrings(b, vec, out, n, prep, hasNulls)
		return true
	}
	memo := make(map[string]any, n/2)
	for i := 0; i < n; i++ {
		if hasNulls && vec.Nulls.IsNullFast(i) {
			out.SetValue(i, e.Eval(b, i))
			continue
		}
		s := vec.BytesData.UnsafeStringValue(i)
		if v, hit := memo[s]; hit {
			out.SetValue(i, v)
			continue
		}
		var v any
		if prep != nil {
			v = prep.replaceAll(s)
		} else {
			v = e.Eval(b, i)
		}
		out.SetValue(i, v)
		memo[s] = v
	}
	return true
}

// memoStr is the typed memo value for a string-producing memoized call:
// the result plus its NULL flag, stored by value in the map. The generic
// map[string]any charges an interface box per DISTINCT input, and then a
// string→[]byte conversion per ROW inside Vector.SetValue on the way to
// the output arena. Typed, a distinct input costs the match and nothing
// else, and a row costs an append.
type memoStr struct {
	s    string
	null bool
}

// evalMemoizedStrings is tryEvalMemoized's loop for a string output
// column: typed memo, and results written straight into the output
// BytesColumn. Same per-batch memo lifetime as the generic loop — nothing
// here may outlive the batch, and nothing does: memo values can be
// zero-copy views into the input arena (replaceAll returns its argument
// unchanged when the pattern doesn't match), and SetString copies them
// into the output arena on the way out.
func (e *FuncCall) evalMemoizedStrings(b *batch.RecordBatch, vec, out *batch.Vector, n int, prep *preparedRegexp, hasNulls bool) {
	// Only engine-sized batches recycle their map. An oversized batch
	// (GetForSize's escape hatch) would leave a map whose bucket count —
	// and therefore whose clear cost — outlives it, charged to every
	// normal batch after it.
	var memo map[string]memoStr
	if n <= batch.DefaultBatchSize {
		memo = memoStrPool.Get().(map[string]memoStr)
		defer func() {
			clear(memo)
			memoStrPool.Put(memo)
		}()
	} else {
		memo = make(map[string]memoStr, n/2)
	}
	for i := 0; i < n; i++ {
		if hasNulls && vec.Nulls.IsNullFast(i) {
			out.SetValue(i, e.Eval(b, i))
			continue
		}
		s := vec.BytesData.UnsafeStringValue(i)
		mv, hit := memo[s]
		if !hit {
			if prep != nil {
				mv = memoStr{s: prep.replaceAll(s)}
			} else {
				switch tv := e.Eval(b, i).(type) {
				case string:
					mv = memoStr{s: tv}
				case nil:
					mv = memoStr{null: true}
				default:
					// Mirrors Vector.SetValue's coercion for a string
					// column, so a memoizable function that returns
					// something other than a string can't make the typed
					// path diverge from the generic one.
					mv = memoStr{s: fmt.Sprint(tv)}
				}
			}
			memo[s] = mv
		}
		if mv.null {
			out.WriteNullAt(i)
			continue
		}
		out.Nulls.SetValid(i)
		out.BytesData.SetString(i, mv.s)
	}
}

func (e *FuncCall) evalVecPerRow(b *batch.RecordBatch, out *batch.Vector, n int) {
	for i := 0; i < n; i++ {
		out.SetValue(i, e.Eval(b, i))
	}
}

// makeConstVector creates a vector filled with a constant value.
func makeConstVector(val any, n int) *batch.Vector {
	switch v := val.(type) {
	case float64:
		vec := batch.NewVector(batch.TypeFloat64, n)
		for i := 0; i < n; i++ {
			vec.Float64Data[i] = v
		}
		return vec
	case int64:
		vec := batch.NewVector(batch.TypeInt64, n)
		for i := 0; i < n; i++ {
			vec.Int64Data[i] = v
		}
		return vec
	case int:
		return makeConstVector(int64(v), n)
	case string:
		vec := batch.NewVector(batch.TypeString, n)
		b := []byte(v)
		for i := 0; i < n; i++ {
			vec.BytesData.Set(i, b)
		}
		return vec
	case bool:
		vec := batch.NewVector(batch.TypeBool, n)
		for i := 0; i < n; i++ {
			vec.BoolData[i] = v
		}
		return vec
	case nil:
		vec := batch.NewVector(batch.TypeString, n)
		for i := 0; i < n; i++ {
			vec.Nulls.SetNull(i)
		}
		return vec
	default:
		return nil
	}
}

// numericFuncCall wraps a FuncCall to implement Float64Expr and Int64Expr
// for functions known to return numeric values. Created by the compiler for
// functions like extract, year, length, abs, ceil, floor, round, etc.
type numericFuncCall struct {
	*FuncCall
}

func (e *numericFuncCall) EvalFloat64(b *batch.RecordBatch, row int) (float64, bool) {
	v := e.Eval(b, row)
	if v == nil {
		return 0, false
	}
	return ToFloat64(v), true
}

// EvalInt64 is the seam integer arithmetic reads a numeric function through
// (isIntNative routes `f(x) + 1` here when f is DECLARED integer). It converted
// through a float64, so a function that answers an exact int64 lost its low
// bits on the way into the operator: with BITWISE_OR declared INT64 (#966),
// `BITWISE_OR(f8, 1)` answered 4611686018427387923 and
// `BITWISE_OR(f8, 1) + 0` answered 4611686018427387904 — two spellings of one
// value disagreeing, which is worse than both being wrong. An integer box is
// taken as itself; anything else keeps the old conversion.
func (e *numericFuncCall) EvalInt64(b *batch.RecordBatch, row int) (int64, bool) {
	v := e.Eval(b, row)
	if v == nil {
		return 0, false
	}
	if i, ok := toInt64Safe(v); ok {
		return i, true
	}
	return int64(ToFloat64(v)), true
}

// ScalarFunc is a scalar function implementation.
type ScalarFunc func(args []any) any

// VecScalarFunc is a vectorized scalar function that operates on entire columns
// at once, reading from input vectors and writing to an output vector.
// This avoids per-row interface dispatch and boxing overhead.
type VecScalarFunc func(args []*batch.Vector, out *batch.Vector, n int)

// FuncRegistry is a concurrent-safe registry of scalar functions.
type FuncRegistry struct {
	mu         sync.RWMutex
	funcs      map[string]ScalarFunc
	rets       map[string]Ret // declared return type, one per registered function
	vecFuncs   map[string]VecScalarFunc
	vecReturns map[string]func() int // funcs returning VECTOR; value yields the dimension
}

// NewFuncRegistry creates a new empty function registry.
func NewFuncRegistry() *FuncRegistry {
	return &FuncRegistry{
		funcs:      make(map[string]ScalarFunc),
		rets:       make(map[string]Ret),
		vecFuncs:   make(map[string]VecScalarFunc),
		vecReturns: make(map[string]func() int),
	}
}

// Register adds or replaces a scalar function. ret declares the type the
// function's results are stored as; the planner types projections from it (see
// Ret). Registering without a declaration does not compile, and registering
// the zero value panics here rather than letting a mistyped output vector
// reach a kernel.
func (r *FuncRegistry) Register(name string, fn ScalarFunc, ret Ret) {
	if !ret.Declared() {
		panic(fmt.Sprintf("expr: function %q registered without a declared return type", name))
	}
	r.mu.Lock()
	r.funcs[strings.ToLower(name)] = fn
	r.rets[strings.ToLower(name)] = ret
	r.mu.Unlock()
}

// Unregister removes a scalar function. Returns true if it existed.
func (r *FuncRegistry) Unregister(name string) bool {
	r.mu.Lock()
	_, existed := r.funcs[strings.ToLower(name)]
	delete(r.funcs, strings.ToLower(name))
	delete(r.rets, strings.ToLower(name))
	r.mu.Unlock()
	return existed
}

// ReturnType returns the declared return type of a function. An unregistered
// name yields the zero Ret, which reports Declared() == false and resolves to
// "caller keeps its fallback".
func (r *FuncRegistry) ReturnType(name string) Ret {
	r.mu.RLock()
	ret := r.rets[strings.ToLower(name)]
	r.mu.RUnlock()
	return ret
}

// Lookup returns the function with the given name, or nil if not found.
func (r *FuncRegistry) Lookup(name string) ScalarFunc {
	r.mu.RLock()
	fn := r.funcs[strings.ToLower(name)]
	r.mu.RUnlock()
	return fn
}

// RegisterVec adds a vectorized implementation for a scalar function. A vec
// kernel writes a typed slice of the output vector, so the function it
// accelerates must already be registered with the return type that names that
// slice — registering a kernel for an undeclared function is the exact setup
// that panicked the server four times, and panics here instead.
func (r *FuncRegistry) RegisterVec(name string, fn VecScalarFunc) {
	r.mu.Lock()
	if !r.rets[strings.ToLower(name)].Declared() {
		r.mu.Unlock()
		panic(fmt.Sprintf("expr: vec kernel registered for %q before its return type was declared", name))
	}
	r.vecFuncs[strings.ToLower(name)] = fn
	r.mu.Unlock()
}

// LookupVec returns the vectorized function with the given name, or nil if not found.
func (r *FuncRegistry) LookupVec(name string) VecScalarFunc {
	r.mu.RLock()
	fn := r.vecFuncs[strings.ToLower(name)]
	r.mu.RUnlock()
	return fn
}

// RegisterVecReturn marks a function as returning a VECTOR. dimFn is evaluated
// lazily (at plan time) to obtain the output dimensionality — embed(), for
// example, derives it from the configured embedding provider.
func (r *FuncRegistry) RegisterVecReturn(name string, dimFn func() int) {
	r.mu.Lock()
	r.vecReturns[strings.ToLower(name)] = dimFn
	r.mu.Unlock()
}

// VecReturnDim reports whether the named function returns a VECTOR and, if so,
// its current output dimension. ok is false for non-vector-returning functions.
func (r *FuncRegistry) VecReturnDim(name string) (dim int, ok bool) {
	r.mu.RLock()
	dimFn := r.vecReturns[strings.ToLower(name)]
	r.mu.RUnlock()
	if dimFn == nil {
		return 0, false
	}
	return dimFn(), true
}

// Has returns true if a function with the given name exists.
func (r *FuncRegistry) Has(name string) bool {
	r.mu.RLock()
	_, ok := r.funcs[strings.ToLower(name)]
	r.mu.RUnlock()
	return ok
}

// Names returns all registered function names.
func (r *FuncRegistry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.funcs))
	for name := range r.funcs {
		names = append(names, name)
	}
	r.mu.RUnlock()
	return names
}

// builtin pairs an implementation with its declared return type. Both fields
// are positional in the table below, so an entry that names an implementation
// without saying what it returns does not compile — which is the point: the
// planner reads these declarations to type projections, and for four
// generations of this bug the two lived in different files with nothing
// linking them (#310).
type builtin struct {
	fn  ScalarFunc
	ret Ret
}

// RegisterFunc registers a custom scalar function in the default registry.
// ret declares what the function returns; see Ret.
func RegisterFunc(name string, fn ScalarFunc, ret Ret) {
	DefaultRegistry.Register(name, fn, ret)
}
