package expr

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// The DECIMAL mode of the constructs that CHOOSE BETWEEN their operands —
// CASE, COALESCE, NULLIF, IFNULL, IF, GREATEST, LEAST (ADR-0024 item 2, #695).
//
// It is the runtime half of what expr.CommonDeclType decides at plan time, and
// it exists for one reason: a choice whose arms are a DECIMAL and an INTEGER
// answers, on the rows the integer wins, with an INTEGER BOX. That box means
// something else entirely to a DECIMAL vector — ADR-0018 §4 makes a DECIMAL
// value an unscaled integer at the column's scale, so SetValue would store the
// integer 100 as 1.00 and SetValueChecked refuses it outright (22003). Neither
// is the value PostgreSQL answers.
//
// So the construct rewrites its chosen box into the spelling every DECIMAL
// producer here already answers with: the value's rendered TEXT, the same box
// a DECIMAL COLUMN and exact arithmetic (binop_decimal.go) hand over. No
// consumer of a boxed value needs teaching, and the store resolves the text at
// the output vector's own scale through ParseDecimalStringChecked — exact, or
// a loud 22003.
//
// The mode is resolved once per node from the first batch, beside
// BinOpNumeric's and for the same reason: an operand's type does not exist
// until a batch arrives.

// decimalChoice is one node's resolved answer to "do my arms fold to a
// DECIMAL". ready publishes on: set last under mu, read first and alone by the
// evaluators.
type decimalChoice struct {
	ready atomic.Bool
	mu    sync.Mutex
	on    bool
}

// resolveSlow runs at most once per node. The arms are built by the CALLER and
// only on this path, so the fast path above it allocates nothing.
func (c *decimalChoice) resolveSlow(b *batch.RecordBatch, arms []Expr) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready.Load() {
		return c.on
	}
	_, _, ok := decimalArmFold(arms, b)
	c.on = ok
	c.ready.Store(true)
	return c.on
}

// decimalChoiceBox rewrites a chosen box into the DECIMAL spelling.
//
// Only an INTEGER box is rewritten, and it is exact by construction: an
// integer is its own digits at scale 0, which is the reading a DECIMAL vector
// must NOT give it (ADR-0018 §4) and the reading its text does.
//
// A FLOAT box is deliberately left alone. It reaches here only from a numeric
// literal — a float COLUMN makes the whole construct float8 — and
// batch.Vector.SetValueChecked already resolves one through the shortest
// decimal text that reads back as the same float, so rewriting it here would
// change nothing at the store and WOULD change what a COMPARISON sees: a
// float box beside a decimal one is a pair the boxed-comparison layer already
// classifies, and handing it text instead put a rounded rendering where the
// literal's exact source text used to be (#452's carrier). The exactness of
// such a literal is decided ahead of the fold instead —
// LiteralChoiceDecimalType.
//
// A string box is already a DECIMAL's text and passes through; so does NULL.
func decimalChoiceBox(v any) any {
	switch n := v.(type) {
	case int64:
		return strconv.FormatInt(n, 10)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case int:
		return strconv.FormatInt(int64(n), 10)
	}
	return v
}

// decimalChoiceArm is one alternative's contribution to a choice's DECIMAL
// fold: the (p,s) it brings, whether it is a genuine DECIMAL rather than an
// integer wearing a fixed-point type, and whether it participates at all.
//
// It is the compiled-tree twin of expr.declFixedPoint (over declared types) and
// physical.decimalArithOperand (over the AST). All three must draw the same
// line or the plan would declare a type the runtime does not produce.
func decimalChoiceArm(e Expr, b *batch.RecordBatch) (batch.DecimalType, bool, bool) {
	if isConstNumericLit(e) {
		// A CONSTANT contributes its spelling and never TRIGGERS the fold:
		// `GREATEST(-2.5, -7.5)` is a float8 expression here, because a bare
		// numeric literal declares FLOAT64 and ADR-0024 leaves that deferral
		// open — the runtime must not answer DECIMAL for something the plan
		// built a float vector for.
		t, ok := constArmDecimalType(e)
		return t, false, ok
	}
	if p, s, ok := DecimalResultOf(e, b); ok {
		return batch.DecimalType{Precision: p, Scale: s}, true, true
	}
	// Not a DECIMAL result. An INTEGER arm still contributes — an integer is
	// DECIMAL(10,0)/(19,0) in the result-type rule — and everything else (a
	// float, a string, an expression with no exact form) makes the fold
	// decline.
	if t, ok := integerArmDecimalType(e, b); ok {
		return t, false, true
	}
	return batch.DecimalType{}, false, false
}

// integerArmDecimalType is the fixed-point contribution of an arm that
// produces an INTEGER, and it must accept every shape the DECLARED side does:
// expr.declFixedPoint takes any arm whose DeclType is INT32/INT64, which is a
// CAST to an integer, a nested choice over integers, integer arithmetic, an
// integer aggregate output and a registry function declared integer — not just
// a bare column or a literal.
//
// The two sides disagreeing is what #695's review found: the plan folded
// `CASE WHEN … THEN d92 ELSE CAST(i32 AS BIGINT) END` to DECIMAL and allocated
// a DECIMAL vector, this function declined because a *Cast is not a
// decimalOperand, so the integer box was never rendered as text and the store
// raised 22003 for a value PostgreSQL answers. Eleven shapes reached it.
//
// The classification is still an enumeration and it can still fall behind a
// new node kind. What it can no longer do is COST A VALUE: the store reads an
// integer box from an expression as a scale-0 value now
// (batch.Vector.SetComputedChecked), so a miss here narrows a declared TYPE
// rather than failing the query.
func integerArmDecimalType(e Expr, b *batch.RecordBatch) (batch.DecimalType, bool) {
	switch v := e.(type) {
	case *UnaryOp:
		if v.Op != "-" && v.Op != "+" {
			return batch.DecimalType{}, false
		}
		return integerArmDecimalType(v.Operand, b)
	case *Cast:
		// A CAST that NAMES an integer type produces one. castIntegerWidth
		// answers the same question expr.Cast's evaluator does.
		if w, ok := castIntegerDecimalType(v); ok {
			return w, true
		}
		return batch.DecimalType{}, false
	case *Case:
		return allIntegerArms(caseResultArms(v), b)
	case *Coalesce:
		return allIntegerArms(v.Args, b)
	case *FuncCall:
		idx, poly := DefaultRegistry.ReturnType(v.Name).SameAsArgs(len(v.Args))
		if poly {
			arms := make([]Expr, 0, len(idx))
			for _, i := range idx {
				if i >= 0 && i < len(v.Args) {
					arms = append(arms, v.Args[i])
				}
			}
			return allIntegerArms(arms, b)
		}
		// A function whose OWN declaration is integer — length(), and the
		// rest of the registry's fixed INT32/INT64 returns (#636).
		if r := DefaultRegistry.ReturnType(v.Name); r.Integer() {
			return batch.DecimalType{Precision: batch.Int64DecimalDigits}, true
		}
		return batch.DecimalType{}, false
	}
	// A bare integer column, an integer literal, int-mode arithmetic: they
	// implement decimalOperand and answer their own range at scale 0.
	o, isOperand := e.(decimalOperand)
	if !isOperand {
		return batch.DecimalType{}, false
	}
	t, ok := o.decimalType(b)
	if !ok {
		return batch.DecimalType{}, false
	}
	return t, true
}

// allIntegerArms is a nested choice's integer contribution: every arm that can
// produce a value must be an integer, and the result carries the widest of
// them. A NULL literal is skipped, exactly as the DECIMAL fold skips it.
func allIntegerArms(arms []Expr, b *batch.RecordBatch) (batch.DecimalType, bool) {
	out, any := batch.DecimalType{}, false
	for _, a := range arms {
		if a == nil {
			continue
		}
		if lit, isLit := a.(*Lit); isLit && lit.Val == nil {
			continue
		}
		if isConstNumericLit(a) {
			t, ok := constArmDecimalType(a)
			if !ok || t.Scale != 0 {
				// A FRACTIONAL constant is not an integer arm; it makes the
				// nested choice a decimal one, which decimalChoiceArm's
				// DecimalResultOf branch above has already been asked about.
				return batch.DecimalType{}, false
			}
			out, any = widerDecimalType(out, t), true
			continue
		}
		t, ok := integerArmDecimalType(a, b)
		if !ok {
			return batch.DecimalType{}, false
		}
		out, any = widerDecimalType(out, t), true
	}
	return out, any
}

// widerDecimalType keeps the larger integer range of two scale-0 operands.
func widerDecimalType(a, b batch.DecimalType) batch.DecimalType {
	if b.Precision > a.Precision {
		return b
	}
	return a
}

// castIntegerDecimalType is a CAST's integer contribution: the range its
// DESTINATION names, at scale 0. A cast to a non-integer type answers false and
// the caller declines, which is what keeps `CAST(x AS TEXT)` out of a numeric
// fold.
func castIntegerDecimalType(c *Cast) (batch.DecimalType, bool) {
	switch strings.ToUpper(strings.TrimSpace(c.DestType)) {
	case "INT", "INT4", "INTEGER", "SMALLINT", "INT2":
		return batch.DecimalType{Precision: batch.Int32DecimalDigits}, true
	case "BIGINT", "INT8", "LONG":
		return batch.DecimalType{Precision: batch.Int64DecimalDigits}, true
	}
	return batch.DecimalType{}, false
}

// constArmDecimalType is a constant numeric arm's contribution — the literal's
// spelling, reached through the unary ± and parentheses that still make it a
// constant.
func constArmDecimalType(e Expr) (batch.DecimalType, bool) {
	switch v := e.(type) {
	case *UnaryOp:
		if v.Op != "-" && v.Op != "+" {
			return batch.DecimalType{}, false
		}
		return constArmDecimalType(v.Operand)
	case *Lit:
		return LiteralChoiceDecimalType(v.Text)
	}
	return batch.DecimalType{}, false
}

// LiteralChoiceDecimalType is the fixed-point type a numeric LITERAL
// contributes to a CHOICE construct's DECIMAL fold: its spelling's (p,s) —
// ADR-0024 item 3 — but only when the BOX compileLit built for it carries that
// spelling exactly.
//
// The box is the qualification, and it is what separates a choice from
// arithmetic. Exact arithmetic reads a literal through its source TEXT
// (litDecimal, ADR-0012 item 6) and is exact for any spelling; a choice
// construct CHOOSES a value and hands over whatever box the winning arm
// produced, which for a literal past a double's ~17 significant digits is
// already rounded. Declaring DECIMAL for
// `GREATEST(d_wide, 493827160549382.7160549350)` would therefore store a
// number nobody wrote on the rows the literal wins. Declining leaves that
// shape exactly where it was — a FLOAT64 declaration and the #361 store
// refusal — which is loud rather than quietly short of digits.
//
// An INTEGER spelling is exact whenever strconv.ParseInt took it, which is
// exactly when compileLit put an int64 in the box.
func LiteralChoiceDecimalType(text string) (batch.DecimalType, bool) {
	t, v, ok := litDecimal(text)
	if !ok {
		return batch.DecimalType{}, false
	}
	trimmed := strings.TrimSpace(text)
	if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return t, true
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return batch.DecimalType{}, false
	}
	d, ok := batch.DecimalTextAt(strconv.FormatFloat(f, 'f', -1, 64), t.Scale)
	if !ok || d.Residual != 0 || d.Sat != 0 || d.Unscaled != v {
		return batch.DecimalType{}, false
	}
	return t, true
}
