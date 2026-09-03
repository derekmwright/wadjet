package expr

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
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

// choiceBoxMode is what a choice construct must do to the box its winning arm
// produced so the value survives the vector the PLAN declared for it.
//
// The two directions are the two halves of one rule, and each exists because a
// box means something else to the other type's vector:
//
//   - choiceBoxDecimal: the arms fold to a DECIMAL, so an INTEGER box becomes
//     the value's TEXT. An integer written into a DECIMAL vector is the
//     already-scaled carrier of ADR-0018 §4 — 100 would read back as 1.00 —
//     and SetValueChecked refuses it outright (#695).
//   - choiceBoxInt64/choiceBoxFloat32/choiceBoxFloat64: the arms fold to a
//     non-DECIMAL number, so a STRING box becomes that number. Two arms can
//     produce one: a DECIMAL column, whose value IS its rendered text, and a
//     QUOTED literal, which arrives as the characters the query spelled.
//     `COALESCE(numeric, float8)` is double precision in PostgreSQL and
//     declares double precision here, and on the rows the DECIMAL wins the box
//     was its text — which the #361 store guard refused, loudly, for as long as
//     nothing converted it (#555's float half); `COALESCE(bigint, '16777217')`
//     is bigint there and the literal's four characters had nowhere to go
//     (#724). GREATEST/LEAST already answered both, through
//     extremumArms.materialize, which is why the defect was invisible to every
//     gate written over those two.
type choiceBoxMode uint8

const (
	choiceBoxNone choiceBoxMode = iota
	choiceBoxDecimal
	choiceBoxInt64
	choiceBoxFloat32
	choiceBoxFloat64
)

// decimalChoice is one node's resolved answer to "what must I do to my chosen
// box". ready publishes on: set last under mu, read first and alone by the
// evaluators.
type decimalChoice struct {
	ready atomic.Bool
	mu    sync.Mutex
	mode  choiceBoxMode
	// nullif routes the mode through NULLIF's operator rule instead of
	// select_common_type (#757). Set by the node that builds the arms, once,
	// before any batch arrives.
	nullif bool
}

// armsBoxMode picks the rule this node's construct resolves under. Every
// construct but NULLIF folds its arms with select_common_type; NULLIF's type
// comes from the comparison OPERATOR its two arguments select, and the box has
// to follow the same rule the declaration does.
func (c *decimalChoice) armsBoxMode(arms []Expr, b *batch.RecordBatch) (choiceBoxMode, bool) {
	if c.nullif {
		return nullifArmsBoxMode(arms, b)
	}
	return choiceArmsBoxMode(arms, b)
}

// resolveSlow runs at most once per node. The arms are built by the CALLER and
// only on this path, so the fast path above it allocates nothing.
func (c *decimalChoice) resolveSlow(b *batch.RecordBatch, arms []Expr) choiceBoxMode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready.Load() {
		return c.mode
	}
	mode, settled := c.armsBoxMode(arms, b)
	if !settled {
		// An arm whose kind this batch could not settle answers for THIS
		// batch only. Latching it would freeze the node's mode on whichever
		// batch happened to arrive first, and the batches of one query do not
		// all carry the same columns: `GREATEST(real, '3.5', integer)`
		// answered 16777216 or 16777217 for the same row between two runs of
		// the same binary before this clause, which is a wrong answer that no
		// gate can reproduce on demand. boxOperand.resolve and
		// extremumArms.commonKind hold the same line, for the same reason.
		return mode
	}
	c.mode = mode
	c.ready.Store(true)
	return mode
}

// choiceArmsBoxMode is that answer, computed from the arms alone.
//
// The DECIMAL mode is gated on decimalArmFold and nothing else, deliberately:
// it is the mode that REWRITES an integer into text, and turning it on for a
// composite the PLAN did not declare DECIMAL would put that text in front of
// an integer vector. joinFoldKinds says numeric for `COALESCE(i32, 1.5)` and
// the plan declares INT32 there (ADR-0024's literal deferral), which is exactly
// that shape.
//
// The non-DECIMAL numeric modes are read off joinFoldKinds — select_common_type
// over the compiled arms, the same fold expr.CommonDeclType runs over the
// DECLARED ones — so the vector the plan builds and the box the runtime hands
// it are decided by one rule.
func choiceArmsBoxMode(arms []Expr, b *batch.RecordBatch) (choiceBoxMode, bool) {
	if _, _, ok := decimalArmFold(arms, b); ok {
		return choiceBoxDecimal, true
	}
	f, settled := joinFoldKinds(arms, b)
	switch f {
	case boxInt32, boxInt64:
		return choiceBoxInt64, settled
	case boxFloat32:
		return choiceBoxFloat32, settled
	case boxFloat64:
		return choiceBoxFloat64, settled
	}
	return choiceBoxNone, settled
}

// nullifArmsBoxMode is choiceArmsBoxMode under NULLIF's own rule (#757).
//
// The DECLARATION side is Ret.operatorResolvedType, and this is its value
// half: PostgreSQL types NULLIF from the `=` OPERATOR its two arguments
// select, not from select_common_type over them, so the box the runtime hands
// the vector has to follow the same rule or the plan allocates one type and
// the kernel writes another. It did: once the declaration moved,
// `NULLIF(numeric(15,2), real)` allocated a FLOAT64 vector — which is what
// PostgreSQL declares — and the kernel still handed it argument 0's DECIMAL
// TEXT, which the #361 store guard refused. Loudly, which is the good
// outcome, but the query stopped answering.
//
// The rule, and it is the same four cases the declaration walks: argument 0's
// own width when both are integers or when argument 0 is a float; float8 when
// argument 0 is an integer or a DECIMAL and argument 1 is a float (there is no
// such operator, so both coerce to the category's preferred type); the DECIMAL
// fold otherwise.
func nullifArmsBoxMode(arms []Expr, b *batch.RecordBatch) (choiceBoxMode, bool) {
	if len(arms) < 2 {
		return choiceBoxNone, false
	}
	_, _, dec0 := decimalArmFold(arms[:1], b)
	_, _, dec1 := decimalArmFold(arms[1:2], b)
	k0, s0 := joinFoldKinds(arms[:1], b)
	k1, s1 := joinFoldKinds(arms[1:2], b)
	if !s0 || !s1 {
		// An arm this batch could not settle answers for THIS batch only,
		// exactly as choiceBoxMode holds the line: latching would freeze the
		// node on whichever batch arrived first.
		return choiceBoxNone, false
	}
	isFloat := func(k boxKind) bool { return k == boxFloat32 || k == boxFloat64 }
	isInt := func(k boxKind) bool { return k == boxInt32 || k == boxInt64 }
	switch {
	case !dec0 && isFloat(k0):
		if k0 == boxFloat32 {
			return choiceBoxFloat32, true
		}
		return choiceBoxFloat64, true
	case !dec1 && isFloat(k1):
		return choiceBoxFloat64, true
	case dec0 || dec1:
		return choiceBoxDecimal, true
	case isInt(k0) && isInt(k1):
		return choiceBoxInt64, true
	}
	return choiceBoxNone, false
}

// choiceBox applies the resolved mode to one chosen box.
func choiceBox(mode choiceBoxMode, v any) any {
	switch mode {
	case choiceBoxDecimal:
		return decimalChoiceBox(v)
	case choiceBoxInt64, choiceBoxFloat32, choiceBoxFloat64:
		return choiceNumberBox(mode, v)
	}
	return v
}

// choiceNumberBox reads a string box as the number the call's folded type
// names.
//
// Only a STRING box is touched, and under a numeric fold a string can only be
// numeric text: a DECIMAL arm's rendering, or a quoted literal's own
// characters. A box that does not parse is left exactly as it arrived — the
// store's #361 guard is what reports it, and replacing a loud refusal with a
// plausible number is the regression in kind the correctness protocol's method
// 8 is about.
//
// FLOAT32 narrows through float32 and hands back a float64, which is how
// ColRef.Eval boxes a real column — so a real-typed choice and a real column
// agree on the box as well as on the value. Both readings are
// extremumArms.materialize's, which has answered them for GREATEST/LEAST since
// #646; this is the same rule for the constructs that never had it.
func choiceNumberBox(mode choiceBoxMode, v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if mode == choiceBoxInt64 {
		n, st := kernel.IntLitText(s)
		if st != kernel.NumConstOK {
			return v
		}
		return n
	}
	bits := 64
	if mode == choiceBoxFloat32 {
		bits = 32
	}
	f, st := kernel.FloatLitText(s, bits)
	if st != kernel.NumConstOK {
		return v
	}
	if mode == choiceBoxFloat32 {
		return float64(float32(f))
	}
	return f
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

// foldDecimalMetas resolves a choice's DECIMAL (p,s) from its arms'
// contributions, and DROPS the CONSTANTS when the whole set does not fit.
//
// The scale is max over the arms — ADR-0012 item 12's rule, and the only
// choice that moves no value, since a narrower one drops digits a wider arm
// holds. A LITERAL is an arm like any other for that purpose: `CASE … THEN
// numeric(9,2) ELSE 0.125 END` needs scale 3 or the 0.125 has nowhere to go,
// and PostgreSQL answers 0.125 there.
//
// The cost, and it is recorded rather than hidden: PostgreSQL's fold carries
// typmod -1, so its columns' rows keep their OWN scale (1.0001) while a
// single-scale vector renders them at the fold's (1.00010). Same number, extra
// zeros. The alternative — taking the scale from the declared operands alone —
// keeps those rows byte-identical and REFUSES the literal, which turns queries
// PostgreSQL and this engine both answer into 22003. A trailing zero is not a
// wrong number; a refused query is no answer at all.
//
// `constrained` is the fallback that makes an UNSELECTED literal free: when
// the full fold does not fit the carrier, the constants are dropped and the
// DECLARED operands decide alone. Without it `COALESCE(numeric(15,2),
// numeric(38,10), '<forty digits>')` fell back to the FIRST arm's (15,2) and
// then failed on the rows the numeric(38,10) column supplied — an arm no row
// selects costing the query its answer, which is the one thing this fold must
// never do.
func foldDecimalMetas(all, constrained []batch.DecimalType) (batch.DecimalType, bool) {
	if m, ok := batch.DecimalCommon(all); ok {
		return m, true
	}
	if len(constrained) == 0 || len(constrained) == len(all) {
		return batch.DecimalType{}, false
	}
	return batch.DecimalCommon(constrained)
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
