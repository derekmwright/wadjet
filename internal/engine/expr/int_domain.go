package expr

import (
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// The integer DOMAIN of an expression is a property of its TYPE, not of the
// SYNTAX that produced its operands (#849, ADR-0024 item 2).
//
// `c_i64 * <int8 max>` raises 22003 here and on PostgreSQL 17.11. Put ANY of
// CAST, a function or a choice construct around the same column and the
// expression used to answer 9.223399706970886e+24 as a float64 — the same
// wrong value projected, filtered, grouped and summed, where the server raises
// `bigint out of range` in every one of those positions. The shapes that do
// NOT overflow were wrong in the same way with the defect invisible:
// `CAST(v AS BIGINT) * 2` answered 200 under OID 701 where PostgreSQL declares
// bigint (measured on the wire, round 0).
//
// The mechanism was the NODE CHOICE. compileBinOp builds the typed
// BinOpNumeric only when both operands satisfy Float64Expr AND Int64Expr;
// *Cast, *Case, *Coalesce, *decimalScalarFn and a polymorphic *FuncCall
// satisfy neither, so every one of those pairs fell to the generic BinOp,
// whose `+ - * %` arms read both sides through ToFloat64. Only `/` had an
// integer arm, added by #369 for exactly this node and exactly these operands.
//
// The predicates below are the RUNTIME MIRROR of physical.intArithAllInt's
// declared-type tail: the planner declares INT64 for what these accept, so the
// two must recognise the same trees or a declaration promises an integer the
// kernel does not produce — which is not a theoretical worry. The first cut of
// this fix moved only the planner, and `ABS(i) * <int8 max>` then computed
// 9.2e24 in float64 and STORED it into the INT64 vector the declaration had
// asked for, answering MinInt64: a wrapped number wearing the right type,
// which is worse than the float it replaced.

// castIsInt reports whether a CAST produces an int64 box.
//
// The destination NAMES an integer type, so the value is an int64 whatever the
// operand was: Cast.Eval's integer arm ends in castIntInRange, which returns
// int64 for every source family and RAISES rather than answering when the
// value has no place in the destination. A cast to anything else — including a
// bare `CAST(x AS NUMERIC)`, whose type is the operand's — is not one.
func castIsInt(e *Cast) bool { return IsIntegerCastDest(e.DestType) }

// caseIsInt reports whether a CASE produces an int64 box: every branch that
// can PRODUCE a value is integer, which is PostgreSQL's select_common_type
// over the same arms (CommonDeclType here, physical.caseDeclaredType over the
// AST). A missing ELSE is the implicit NULL branch and contributes nothing —
// a NULL result exits the arithmetic before either kernel runs.
func caseIsInt(e *Case, b *batch.RecordBatch) bool {
	if len(e.Whens) == 0 {
		return false
	}
	for _, w := range e.Whens {
		if !operandIsInt(w.Result, b) {
			return false
		}
	}
	return e.Else == nil || operandIsInt(e.Else, b)
}

// coalesceIsInt is caseIsInt for COALESCE, which compiles to its own node
// rather than to a FuncCall: every argument can supply the value, so every
// argument has to be integer.
func coalesceIsInt(e *Coalesce, b *batch.RecordBatch) bool {
	if len(e.Args) == 0 {
		return false
	}
	for _, a := range e.Args {
		if !operandIsInt(a, b) {
			return false
		}
	}
	return true
}

// decimalScalarFnIsInt answers for abs/ceil/floor/round/trunc/sign, which
// compile to the exact fixed-point node ahead of the FuncCall wrap. When that
// node's DECIMAL mode is on, its box is a decimal's rendered text and this is
// not integer arithmetic; when it is off the node delegates to the FuncCall it
// carries, and the answer is that call's.
func decimalScalarFnIsInt(e *decimalScalarFn, b *batch.RecordBatch) bool {
	if e.resolve(b) {
		return false
	}
	return e.fallback != nil && funcCallIsInt(e.fallback, b)
}

// funcCallIsInt reports whether a scalar call produces an int64 box.
//
// A FIXED integer declaration answers on its own — that is Ret.Integer(), the
// same test isIntNative and physical.FuncReturnsInteger make. A POLYMORPHIC
// one (RetSameAsArg: abs, greatest, least, nullif, ifnull, if, and the
// domain-preserving math functions) mirrors an argument, so it is integer
// exactly when every argument it can take its value from is — the question
// this layer can now ask, because a batch is in hand and the arguments' own
// types have resolved.
//
// The CONTROL arguments are skipped for the same reason Ret.Resolve skips
// them: NULLIF's second argument steers and never arrives, so
// `NULLIF(i64, 1.5)` is bigint on the server and bigint here.
func funcCallIsInt(fc *FuncCall, b *batch.RecordBatch) bool {
	if fc == nil {
		return false
	}
	r := DefaultRegistry.ReturnType(fc.Name)
	if r.Integer() {
		return true
	}
	// ABS and MOD answer in their argument's OWN domain and are registered
	// RetFloat64 anyway, because the registry has no way to say "integer for
	// an integer, real for a real" (numeric_domain_fn.go, #768). They are
	// asked here rather than through SameAsArgs for that reason: the
	// declaration is fixed and the VALUE is not, and NumericDomainResult is
	// the same rule the planner's declared-type layer reads.
	if n, ok := NumericDomainScalarFn(fc.Name); ok && n == len(fc.Args) {
		for _, a := range fc.Args {
			if !operandIsInt(a, b) {
				return false
			}
		}
		return true
	}
	idx, poly := r.SameAsArgs(len(fc.Args))
	if !poly || len(idx) == 0 {
		return false
	}
	for _, i := range idx {
		if i < 0 || i >= len(fc.Args) {
			return false
		}
		if !operandIsInt(fc.Args[i], b) {
			return false
		}
	}
	return true
}

// intArm is BinOp's integer-domain decision, settled once per node against the
// first batch that can answer it — the same lifecycle decArm and
// BinOpNumeric.resolveMode use, and for the same reason: an operand's type does
// not exist until a batch arrives.
type intArm struct {
	ready atomic.Bool
	mu    sync.Mutex
	on    bool
}

// stamp latches the planner's answer before any batch arrives. It is the
// "decide once" half of StampArithMode: after this, resolve never derives.
func (a *intArm) stamp(integer bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.on = integer && intArithToggle.On()
	a.ready.Store(true)
}

func (a *intArm) resolve(op string, left, right Expr, b *batch.RecordBatch) bool {
	if a.ready.Load() {
		return a.on
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ready.Load() {
		return a.on
	}
	a.on = intArithToggle.On() && intArithOp(op) &&
		operandIsInt(left, b) && operandIsInt(right, b)
	a.ready.Store(true)
	return a.on
}

// intArithOp is the operator set this arm covers. `/` is excluded because
// BinOp's own division arm has carried the integer rule since #369 and decides
// it from the BOXES alone; putting it here too would give one operator two
// gates that can disagree.
func intArithOp(op string) bool {
	switch op {
	case "+", "-", "*", "%":
		return true
	}
	return false
}

// intMode lets a nested BinOp answer the integer question for the node above
// it, the way BinOpNumeric.intMode does.
func (e *BinOp) intMode(b *batch.RecordBatch) bool {
	return e.ints.resolve(e.Op, e.Left, e.Right, b)
}

// StampArithMode tells a compiled arithmetic node what the PLANNER decided its
// output type is, so the runtime does not decide it a second time.
//
// The two decisions are `physical.intArithAllInt` (which picks the output
// VECTOR) and `expr.operandIsInt` (which picks the KERNEL), and they were two
// hand-maintained walks over two representations of one expression. When they
// disagree the value is not merely mislabelled: a float computed under an INT64
// declaration is TRUNCATED into the vector, and at the edge it WRAPS. That is
// how `LEAST(c_i64, 1.5) * 3` answered 4 for PostgreSQL's 4.5 and
// `(CASE … ELSE 1.5 END) * <int8 max>` answered MinInt64 (round-1 review, B3).
//
// So the planner stamps, and `intArm.resolve` returns the stamped answer
// instead of re-deriving one. `integer` is the planner's claim that the output
// vector is INT64.
//
// The stamp cannot manufacture an integer out of a value that is not one:
// `BinOp.intArith` still reads both boxes through `toInt64Safe` and returns
// ok=false for anything else, so a stamp of `true` over a decimal or a float
// box falls through to the float arm exactly as an unstamped node would. What
// it removes is the case where the runtime says integer and the planner did
// not — the direction that used to leave a right value under a declaration
// nothing enforced.
//
// Only the TOP node of a projection is stamped, which is the only one whose
// answer meets a materialized vector; a nested node is an input to this
// decision and keeps deriving its own.
func StampArithMode(e Expr, integer bool) {
	bo, ok := e.(*BinOp)
	if !ok {
		return
	}
	bo.ints.stamp(integer)
}

// intArith is BinOp's checked integer arm: the same kernels the typed node
// uses, so the two spellings of one expression cannot disagree about which
// products have no int64. ok=false means the boxes were not both integers
// after all and the caller's float arm answers.
func (e *BinOp) intArith(lv, rv any) (int64, bool) {
	li, lok := toInt64Safe(lv)
	if !lok {
		return 0, false
	}
	ri, rok := toInt64Safe(rv)
	if !rok {
		return 0, false
	}
	switch e.Op {
	case "+":
		return addInt64Checked(li, ri), true
	case "-":
		return subInt64Checked(li, ri), true
	case "*":
		return mulInt64Checked(li, ri), true
	case "%":
		return modInt64Checked(li, ri), true
	}
	return 0, false
}
