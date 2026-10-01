// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"math"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
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
// shapes: an operand whose declared type is an integer, except a CAST the
// user wrote, which keeps its own arithmetic rule (a Cast declines here and
// there).

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
	switch v := e.(type) {
	case *Cast:
		return v.answer && castIsInt(v)
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
		return (v.Op == "-" || v.Op == "+") && integerOperand(v.Operand, b)
	case *BinOp:
		if dateDifference(v, b) {
			return true
		}
		switch v.Op {
		case "+", "-", "*", "%":
			return integerOperand(v.Left, b) && integerOperand(v.Right, b)
		case "/":
			// A marked EXTRACT is a whole number carried in a double: its
			// quotient is the double's, not an integer division, and the plan
			// declares that node double precision too (binOpDecimalOperand).
			return !answerExtract(v.Left) && !answerExtract(v.Right) &&
				integerOperand(v.Left, b) && integerOperand(v.Right, b)
		}
		return false
	case *BinOpNumeric:
		return v.intMode(b)
	case *BinOpInt64:
		l, lok := v.Left.(Expr)
		r, rok := v.Right.(Expr)
		return lok && rok && integerOperand(l, b) && integerOperand(r, b)
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
		return integerArms(arms, b)
	case *Coalesce:
		return integerArms(v.Args, b)
	case *decimalScalarFn:
		return v.fallback != nil && integerCall(v.fallback, b)
	case *numericFuncCall:
		return integerCall(v.FuncCall, b)
	case *FuncCall:
		return integerCall(v, b)
	}
	return false
}

// integerCall is a call whose FIXED declaration is an integer (ascii, length,
// strpos …), abs or mod over integers whose first argument is not a constant
// (the plan keeps `ABS(-1)` on the float path), or a choosing function
// (GREATEST, LEAST, NULLIF, IFNULL) every argument of which is an integer.
func integerCall(fc *FuncCall, b *batch.RecordBatch) bool {
	if fc.answer {
		return true
	}
	r := DefaultRegistry.ReturnType(fc.Name)
	if r.Integer() {
		return true
	}
	if n, ok := NumericDomainScalarFn(fc.Name); ok {
		return n == len(fc.Args) && !isConstNumericLit(fc.Args[0]) && integerArms(fc.Args, b)
	}
	if _, poly := r.SameAsArgs(len(fc.Args)); poly {
		return integerArms(fc.Args, b)
	}
	return false
}

// integerArms reports whether every arm of a choice is an integer operand or
// a NULL, and at least one is an integer.
func integerArms(arms []Expr, b *batch.RecordBatch) bool {
	seen := false
	for _, a := range arms {
		if isNullLit(a) {
			continue
		}
		if !integerOperand(a, b) {
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
		return v.answer
	case *numericFuncCall:
		return v.answer
	case *UnaryOp:
		return answerExtract(v.Operand)
	case *BinOp:
		return answerExtract(v.Left) || answerExtract(v.Right)
	case *BinOpNumeric:
		return answerExtract(v.Left) || answerExtract(v.Right)
	}
	return false
}
