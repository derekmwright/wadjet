// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// An INTEGER-valued operand with no exact accessor of its own — a day count
// (`d - DATE '2024-01-01'`), an integer function (`ascii(s)`, `length(s)`,
// `abs(i)`), a subscript of an integer array (`a[1]`), a choice over integers
// (`COALESCE(i, 0)`), and integer arithmetic over those — enters exact
// DECIMAL arithmetic the way an integer column does: as an integer at scale
// 0, `(d - DATE '2024-01-01') * n` numeric as on PostgreSQL. It contributes
// the int64 range, which is what integer arithmetic over columns contributes
// (BinOpNumeric.decimalType's int mode), and its value is its own integer
// box.
//
// Without it these operands made the pair float: `ascii(s) * n` was double
// precision where PostgreSQL says numeric, and so was a scalar subquery
// over it, which PostgreSQL and v0.25.3 declared numeric (CREATE TABLE AS
// stored a double).
//
// physical.decimalArithOperand is the plan's mirror and accepts the same
// shapes: an operand whose declared type is an integer, and a CAST to an
// integer type (#1450), which Cast.decimalType answers first at
// IntegerCastDecimal's width.

// integerBoxOperand reads an integer operand's value as an exact decimal at
// scale 0.
type integerBoxOperand struct{ e Expr }

func (o integerBoxOperand) decimalType(*batch.RecordBatch) (batch.DecimalType, bool) {
	return batch.DecimalType{Precision: batch.Int64DecimalDigits}, true
}

func (o integerBoxOperand) decimalVec(*batch.RecordBatch) (kernel.DecimalOperandVec, bool) {
	return kernel.DecimalOperandVec{}, false
}

func (o integerBoxOperand) evalDecimal(b *batch.RecordBatch, row int) (batch.Int128, bool) {
	v, ok := integerOperandValue(o.e, b, row)
	if !ok {
		return batch.Int128{}, false
	}
	return batch.Int128From(v), true
}

// integerOperand reports whether e is an integer-valued operand the exact
// path may read through integerBoxOperand.
//
// It must stay a SUBSET of what the plan's walk accepts
// (physical.decimalArithOperand): an operand read exactly here under a
// declaration of double precision would put a decimal's text into a float
// vector (#361's guard). So it names the shapes one by one rather than asking
// the integer rule (operandIsInt), which is wider — `ABS(-1)` is an integer
// box that the plan declares double precision, and `NULLIF(i - 1, d)` takes
// its box from argument 0 while the plan declares the common type of both.
func integerOperand(e Expr, b *batch.RecordBatch) bool {
	return integerOperandIn(e, b, true)
}

// integerOperandIn is integerOperand with marks: whether a marked EXTRACT (or
// a nested subquery answering one) counts. Every position reached from an
// operand of numeric arithmetic counts it, as the plan's walk does — the
// operands of arithmetic, unary ±, and the arms of a CASE / COALESCE /
// GREATEST / LEAST / NULLIF and the arguments of abs / mod over such an
// operand (physical.answerIntegerChoice).
func integerOperandIn(e Expr, b *batch.RecordBatch, marks bool) bool {
	switch v := e.(type) {
	case *Cast:
		// Every integer CAST, as the plan's walk reads it
		// (physical.decimalArithOperand); its width is its target's
		// (IntegerCastDecimal), which Cast.decimalType answers first.
		return castIsInt(v)
	case *ColRef:
		v.resolve(b)
		if v.idx < 0 {
			return false
		}
		switch v.valueType() {
		case batch.TypeInt32, batch.TypeInt64:
			return true
		}
		return false
	case *Lit:
		switch v.Val.(type) {
		case int64, int32, int:
			return true
		}
		return false
	case *UnaryOp:
		return (v.Op == "-" || v.Op == "+") && integerOperandIn(v.Operand, b, marks)
	case *BinOp:
		if dateDifference(v, b) {
			return true
		}
		switch v.Op {
		case "+", "-", "*", "%":
			return integerOperandIn(v.Left, b, marks) && integerOperandIn(v.Right, b, marks)
		case "/":
			// A marked EXTRACT is a whole number carried in a double: its
			// quotient is the double's, not an integer division, and the plan
			// declares that node double precision too (binOpDecimalOperand).
			return !answerExtract(v.Left) && !answerExtract(v.Right) &&
				integerOperandIn(v.Left, b, marks) && integerOperandIn(v.Right, b, marks)
		}
		return false
	case *BinOpNumeric:
		return v.intMode(b)
	case *BinOpInt64:
		l, lok := v.Left.(Expr)
		r, rok := v.Right.(Expr)
		return lok && rok && integerOperandIn(l, b, marks) && integerOperandIn(r, b, marks)
	case *ColShapeLen:
		return DefaultRegistry.ReturnType(v.Fallback.Name).Integer()
	case *catalogCall:
		return DefaultRegistry.ReturnType(v.name).Integer()
	case *elementAtExpr:
		if _, col := v.arg0.(*ColRef); !col {
			return false
		}
		c := containerVector(v.arg0, b)
		if c == nil || c.Type != batch.TypeArray || c.Child == nil {
			return false
		}
		switch c.Child.Type {
		case batch.TypeInt32, batch.TypeInt64:
			return true
		}
		return false
	case *Case:
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return integerArms(arms, b, marks)
	case *Coalesce:
		return integerArms(v.Args, b, marks)
	case *ScalarSubquery:
		// A scalar subquery whose declared answer is an integer is an integer
		// operand, as the plan reads its declaration (decimalArithOperand);
		// so is a marked one answering an integral EXTRACT field.
		return marks && v.answer || v.DeclKnown && (v.Decl == batch.TypeInt32 || v.Decl == batch.TypeInt64)
	case *CorrelatedScalarSubquery:
		return marks && v.answer || v.DeclKnown && (v.Decl == batch.TypeInt32 || v.Decl == batch.TypeInt64)
	case *decimalScalarFn:
		return v.fallback != nil && integerCall(v.fallback, b, marks)
	case *numericFuncCall:
		return integerCall(v.FuncCall, b, marks)
	case *FuncCall:
		return integerCall(v, b, marks)
	}
	return false
}

// integerCall is a call whose FIXED declaration is an integer (ascii, length,
// strpos …), abs or mod over integers whose first argument is not a constant
// (the plan keeps `ABS(-1)` on the float path), or a choosing function
// (GREATEST, LEAST, NULLIF, IFNULL) every argument of which is an integer.
func integerCall(fc *FuncCall, b *batch.RecordBatch, marks bool) bool {
	if fc.answer {
		return marks
	}
	r := DefaultRegistry.ReturnType(fc.Name)
	if r.Integer() {
		return true
	}
	if n, ok := NumericDomainScalarFn(fc.Name); ok {
		return n == len(fc.Args) && !isConstNumericLit(fc.Args[0]) && integerArms(fc.Args, b, marks)
	}
	if _, poly := r.SameAsArgs(len(fc.Args)); poly {
		return integerArms(fc.Args, b, marks)
	}
	return false
}

// integerArms reports whether every arm of a choice is an integer operand or
// a NULL, and at least one is an integer.
func integerArms(arms []Expr, b *batch.RecordBatch, marks bool) bool {
	seen := false
	for _, a := range arms {
		if isNullLit(a) {
			continue
		}
		if !integerOperandIn(a, b, marks) {
			return false
		}
		seen = true
	}
	return seen
}

func isNullLit(e Expr) bool {
	l, ok := e.(*Lit)
	return ok && l.Val == nil
}

// dateDifference reports whether a generic BinOp is a DAY COUNT: `-` between
// two DATE producers, or a DATE and an untyped literal that operator
// resolution reads as one (BinOp.unknownTemporalArith).
func dateDifference(e *BinOp, b *batch.RecordBatch) bool {
	if e.Op != "-" {
		return false
	}
	l, r := producedTemporal(e.Left, b), producedTemporal(e.Right, b)
	_, ls := unknownLiteralText(e.Left)
	_, rs := unknownLiteralText(e.Right)
	return (l == castToDateKind || ls) && (r == castToDateKind || rs) && !(ls && rs)
}

// integerOperandValue is one row of an integer operand. Integer arithmetic
// over the generic node computes here, with its 22003 and 22012, because
// that node boxes a double when its operands have no typed protocol; every
// other operand answers its own integer box.
func integerOperandValue(e Expr, b *batch.RecordBatch, row int) (int64, bool) {
	if bo, ok := e.(*BinOp); ok && !dateDifference(bo, b) {
		l, lok := integerOperandValue(bo.Left, b, row)
		if !lok {
			return 0, false
		}
		r, rok := integerOperandValue(bo.Right, b, row)
		if !rok {
			return 0, false
		}
		switch bo.Op {
		case "+":
			return addInt64Checked(l, r), true
		case "-":
			return subInt64Checked(l, r), true
		case "*":
			return mulInt64Checked(l, r), true
		case "/":
			return divInt64Checked(l, r), true
		case "%":
			return modInt64Checked(l, r), true
		}
		return 0, false
	}
	box := e.Eval(b, row)
	if box == nil {
		return 0, false
	}
	v, ok := toInt64Safe(box)
	if f, isFloat := box.(float64); !ok && isFloat && f == math.Trunc(f) && math.Abs(f) < 1<<62 {
		// An integral EXTRACT field is carried in a double: its value is
		// the whole number the double holds.
		v, ok = int64(f), true
	}
	if !ok {
		// integerOperand said integer and the node answered something
		// else: reading it as a number would be a guess.
		panic(fatalEval{fmt.Errorf("integer operand %T answered a %T value", e, box)})
	}
	return v, true
}

// IntegralExtractField reports whether an EXTRACT field (as the parser names
// the call it rewrites EXTRACT into) is a whole number: YEAR, MONTH, DAY,
// HOUR, MINUTE, QUARTER, WEEK, DOW, DOY, ISODOW, ISOYEAR, DECADE, CENTURY,
// MILLENNIUM. SECOND, EPOCH, JULIAN and the sub-second fields carry a
// fraction.
func IntegralExtractField(name string) bool {
	return integralExtractFields[strings.ToLower(name)]
}

var integralExtractFields = map[string]bool{
	"year": true, "month": true, "day": true, "hour": true, "minute": true,
	"quarter": true, "week": true, "day_of_week": true, "day_of_year": true,
	"isodow": true, "isoyear": true, "decade": true, "century": true, "millennium": true,
}

// answerExtract reports whether e is, or is arithmetic over, a marked
// integral EXTRACT field (FuncCall.answer) — physical.answerExtractIn's
// mirror.
func answerExtract(e Expr) bool {
	switch v := e.(type) {
	case *FuncCall:
		return v.answer || anyAnswerExtract(v.Args)
	case *numericFuncCall:
		return v.answer || anyAnswerExtract(v.Args)
	case *decimalScalarFn:
		return answerExtract(v.arg) || (v.modArg != nil && answerExtract(v.modArg))
	case *ScalarSubquery:
		return v.answer
	case *CorrelatedScalarSubquery:
		return v.answer
	case *Case:
		for _, w := range v.Whens {
			if answerExtract(w.Result) {
				return true
			}
		}
		return v.Else != nil && answerExtract(v.Else)
	case *Coalesce:
		return anyAnswerExtract(v.Args)
	case *UnaryOp:
		return answerExtract(v.Operand)
	case *BinOp:
		return answerExtract(v.Left) || answerExtract(v.Right)
	case *BinOpNumeric:
		return answerExtract(v.Left) || answerExtract(v.Right)
	}
	return false
}

func anyAnswerExtract(es []Expr) bool {
	for _, a := range es {
		if answerExtract(a) {
			return true
		}
	}
	return false
}

// SubqueryAnswersIntegralExtract reports whether a scalar subquery's text
// answers a bare integral EXTRACT field (`SELECT extract(year FROM o.d) …`,
// or its rebuilt spelling `year(…)`): the one SELECT item, a call of one
// argument whose name IntegralExtractField accepts.
func SubqueryAnswersIntegralExtract(sql string) bool {
	q, err := plansql.Parse(sql)
	if err != nil {
		return false
	}
	info, err := plansql.ExtractSelect(q)
	if err != nil || info == nil || len(info.Columns) != 1 {
		return false
	}
	n := info.Columns[0].ASTExpr
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	fc, ok := n.(*plansql.FuncCallNode)
	return ok && len(fc.Args) == 1 && IntegralExtractField(fc.Name)
}

// integerCastIn reports whether an integer operand of exact arithmetic takes
// its value from an integer CAST a query wrote (userIntegerCast), at any
// depth of the constructs an integer's type passes through unchanged: unary
// ±, integer arithmetic, the value arms of a CASE / COALESCE / GREATEST /
// LEAST / NULLIF / IFNULL, and abs / mod. resolveDecimalMode asks it of a
// quotient's non-DECIMAL operand, so `t.n / NULLIF(CAST(t.b AS BIGINT), 0)`
// and `(CAST(t.i AS INTEGER) + 0) / t.n` make the decision the bare cast
// makes. physical.integerCastIn is the plan's twin over the AST.
func integerCastIn(e Expr) bool {
	switch v := e.(type) {
	case *Cast:
		return userIntegerCast(v)
	case *UnaryOp:
		return (v.Op == "-" || v.Op == "+") && integerCastIn(v.Operand)
	case *BinOp:
		return integerCastIn(v.Left) || integerCastIn(v.Right)
	case *BinOpInt64:
		l, lok := v.Left.(Expr)
		r, rok := v.Right.(Expr)
		return lok && integerCastIn(l) || rok && integerCastIn(r)
	case *BinOpNumeric:
		l, lok := v.Left.(Expr)
		r, rok := v.Right.(Expr)
		return lok && integerCastIn(l) || rok && integerCastIn(r)
	case *decimalScalarFn:
		return v.fallback != nil && integerCastIn(v.fallback)
	case *numericFuncCall:
		return integerCastIn(v.FuncCall)
	case *FuncCall:
		if _, ok := NumericDomainScalarFn(v.Name); ok {
			return anyIntegerCastIn(v.Args)
		}
	}
	if arms, isChoice := choiceDecimalArms(e); isChoice {
		return anyIntegerCastIn(arms)
	}
	return false
}

func anyIntegerCastIn(es []Expr) bool {
	for _, a := range es {
		if integerCastIn(a) {
			return true
		}
	}
	return false
}
