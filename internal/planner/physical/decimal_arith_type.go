package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DECLARED type of `+ - * / %` over a DECIMAL operand — ADR-0024 item 3's
// (p,s) rules, and #555's plan half.
//
// It is the AST mirror of expr.resolveDecimalMode, exactly as intArithAllInt
// is the AST mirror of expr.operandIsInt, and it carries the same obligation:
// this must be a STRICT SUBSET of what the runtime takes. Declaring DECIMAL
// for an expression the runtime computes in float64 would allocate an exact
// vector and hand it a rounded number; declaring FLOAT64 for one the runtime
// computes exactly would hand a float vector a decimal TEXT box and trip
// #361's silent-write guard. Both are loud today, and neither is an answer.
//
// The shapes it accepts are therefore precisely the ones whose compiled node
// implements expr's decimalOperand: a plain column reference, a numeric
// literal, unary ±, and nested arithmetic over those. A CASE, a COALESCE, a
// scalar function or a CAST that declares DECIMAL is NOT accepted — those
// compile to nodes with no exact fixed-point form, so they stay on the float
// declaration they had, which is what the runtime still does with them.

// binOpDecimalType returns the DECIMAL type an arithmetic expression declares,
// per batch.DecimalResultType. ok=false means this pair has no fixed-point
// rule and the caller keeps FLOAT64.
func binOpDecimalType(n *plansql.BinaryOp, decls colDecls) (expr.DeclType, bool) {
	t, isDec, ok := binOpDecimalOperand(n, decls)
	if !ok || !isDec {
		// Not decimal arithmetic: either an operand has no exact form, or
		// both are integers — PostgreSQL keeps integer arithmetic in
		// integers, truncating division included (#636), and the int rule
		// decides that one.
		return expr.DeclType{}, false
	}
	return expr.DeclDecimal(t.Precision, t.Scale), true
}

// binOpDecimalOperand answers BOTH questions about an arithmetic node in one
// walk: the fixed-point type it contributes, and whether that type is a
// genuine DECIMAL.
//
// The two are computed together because a nested node is asked both, and
// asking twice makes the walk quadratic in depth over a chain — the shape
// TestNestedChoiceDeclarationIsLinearInDepth exists to catch one layer up.
//
// An all-integer node still CONTRIBUTES: `d * (i + 1)` is numeric in
// PostgreSQL, and the integer arithmetic inside it brings the INT64 range at
// scale 0, exactly as a bare integer column does. expr.BinOpNumeric answers
// the same for its int mode, which is what keeps the two in step.
func binOpDecimalOperand(n *plansql.BinaryOp, decls colDecls) (batch.DecimalType, bool, bool) {
	if _, _, ok := batch.DecimalResultType(n.Op, 1, 0, 1, 0); !ok {
		return batch.DecimalType{}, false, false // not one of + - * / %
	}
	lt, lDec, lok := decimalArithOperand(n.Left, decls)
	rt, rDec, rok := decimalArithOperand(n.Right, decls)
	if !lok || !rok {
		return batch.DecimalType{}, false, false
	}
	if !lDec && !rDec {
		return batch.DecimalType{Precision: batch.Int64DecimalDigits}, false, true
	}
	if n.Op == "/" && isConstNumericLitNode(n.Left) && isConstNumericLitNode(n.Right) {
		// A division between two CONSTANTS keeps the float declaration it has
		// always had — expr.resolveDecimalMode declines the same pair, for the
		// reason spelled out there.
		return batch.DecimalType{}, false, false
	}
	p, s, ok := batch.DecimalResultType(n.Op, lt.Precision, lt.Scale, rt.Precision, rt.Scale)
	if !ok {
		return batch.DecimalType{}, false, false
	}
	return batch.DecimalType{Precision: p, Scale: s}, true, true
}

// decimalArithOperand reports the fixed-point type an operand contributes, and
// whether it is a genuine DECIMAL rather than an integer standing in as one.
//
// The second return is what keeps `i64 + i64` off this path: an integer IS
// DECIMAL(19,0) for a result-type computation (ADR-0024 item 2), so both
// operands answer a type and only this tells the two shapes apart.
//
// ok=false for every operand with no exact form — a float, a string, a
// function call, a CAST, a CASE — and the whole expression then declares what
// it declared before.
func decimalArithOperand(node plansql.Node, decls colDecls) (batch.DecimalType, bool, bool) {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return decimalArithOperand(n.Inner, decls)
	case *plansql.UnaryOp:
		if n.Op != "-" && n.Op != "+" {
			return batch.DecimalType{}, false, false
		}
		return decimalArithOperand(n.Inner, decls)
	case *plansql.BinaryOp:
		return binOpDecimalOperand(n, decls)
	case *plansql.ColRef:
		if decls.isFieldPath(n) {
			// A ROW FIELD PATH contributes its FIELD's declaration: `rw.d + 1`
			// and `d + 1` over the same value are two spellings of one
			// question and must declare the same type (#568's rule).
			// expr.ColRef.decimalType reads the same pair off the field's
			// child vector and the container's schema entry.
			f, ok := decls.field(n)
			if !ok {
				return batch.DecimalType{}, false, false
			}
			return decimalTypeOfColumn(f)
		}
		t, c := colRefDeclaredType(n, decls)
		if c != expr.Decided {
			return batch.DecimalType{}, false, false
		}
		switch t.ID {
		case parquet.TypeDecimal:
			if !t.DecKnown {
				return batch.DecimalType{}, false, false
			}
			return t.Dec(), true, true
		case parquet.TypeInt32:
			return batch.DecimalType{Precision: batch.Int32DecimalDigits}, false, true
		case parquet.TypeInt64:
			return batch.DecimalType{Precision: batch.Int64DecimalDigits}, false, true
		}
		return batch.DecimalType{}, false, false
	case *plansql.FuncCallNode:
		// A scalar math function over a DECIMAL answers a DECIMAL, so it can
		// be an operand of exact arithmetic: `ROUND(d, 1) * 2` is numeric in
		// PostgreSQL and exact here (#668).
		t, ok := scalarFnDeclaredDecimal(n, decls)
		if !ok {
			return batch.DecimalType{}, false, false
		}
		return t.Dec(), true, true
	case *plansql.CastNode:
		// A CAST that NAMES a (p,s) produces an exact DECIMAL and can be an
		// operand of exact arithmetic — `CAST(x AS DECIMAL(10,2)) * 2` is
		// numeric in PostgreSQL and exact here. A BARE cast cannot: its (p,s)
		// is the operand's, resolved per VALUE at runtime, so the declaration
		// this layer would have to commit to does not exist yet.
		p, s, hasParams, ok := expr.DecimalCastDest(n.TypeName)
		if !ok || !hasParams {
			return batch.DecimalType{}, false, false
		}
		return batch.DecimalType{Precision: p, Scale: s}, true, true
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return batch.DecimalType{}, false, false
		}
		t, ok := batch.DecimalTextType(n.Value)
		if !ok {
			return batch.DecimalType{}, false, false
		}
		// An INTEGER literal is not decimal-typed: integer arithmetic owns it
		// and `i * 2` must stay integer. Every OTHER numeric literal is a
		// decimal the user wrote — `i * 1.5` is numeric in PostgreSQL — and
		// so is one too wide for an int64, which no integer path can carry.
		// expr.litIsExactDecimal draws the line with the same ParseInt.
		_, err := strconv.ParseInt(strings.TrimSpace(n.Value), 10, 64)
		return t, err != nil, true
	}
	return batch.DecimalType{}, false, false
}

// scalarFnDeclaredDecimal is the DECIMAL declaration of a scalar math function
// that answers in its argument's OWN domain — abs/ceil/floor/round/trunc/sign
// over a DECIMAL, and mod, which is the `%` operator spelled as a call
// (ADR-0024 items 2 and 3, #668).
//
// PostgreSQL answers all seven in numeric. Wadjet declared every one of them
// FLOAT64 and computed through ToFloat64 of the column's rendered text, so
// ROUND over a DECIMAL made a round trip through a double before any rounding
// happened. The transcendental functions (sqrt/exp/ln/log/power) stay float64
// — a deliberate divergence of ADR-0012 item 9's class, recorded rather than
// closed, because closing it means building an exact tower.
//
// round(x, n) and trunc(x, n) take their result SCALE from n, so n must be a
// constant: a scale that changed per row is not a type. A non-constant second
// argument declines here and the runtime node declines with it.
func scalarFnDeclaredDecimal(n *plansql.FuncCallNode, decls colDecls) (expr.DeclType, bool) {
	if !expr.IsDecimalScalarFn(n.Name) || len(n.Args) < 1 {
		return expr.DeclType{}, false
	}
	if isConstNumericLitNode(n.Args[0]) {
		// A CONSTANT argument stays on the float path, mirroring
		// expr.decimalScalarArg: `SELECT 1.5` declares FLOAT64 here, so
		// `ROUND(0.5)` answering a DECIMAL would make a constant-folded
		// expression change type depending on what wrapped it. Unary ± over a
		// literal is a constant too — `ROUND(-0.5)` parses as a UnaryOp and
		// `ROUND(0.5)` as a Lit, and covering only one of them made the two
		// halves of one query disagree about their own type.
		return expr.DeclType{}, false
	}
	in, isDec, ok := decimalArithOperand(n.Args[0], decls)
	if !ok || !isDec {
		return expr.DeclType{}, false
	}
	if strings.EqualFold(strings.TrimSpace(n.Name), "mod") {
		if len(n.Args) != 2 {
			return expr.DeclType{}, false
		}
		r, _, ok := decimalArithOperand(n.Args[1], decls)
		if !ok {
			return expr.DeclType{}, false
		}
		p, s, ok := batch.DecimalResultType("%", in.Precision, in.Scale, r.Precision, r.Scale)
		if !ok {
			return expr.DeclType{}, false
		}
		return expr.DeclDecimal(p, s), true
	}
	op, ok := expr.DecimalScalarFnOp(n.Name)
	if !ok || len(n.Args) > 2 {
		return expr.DeclType{}, false
	}
	digits := 0
	if len(n.Args) == 2 {
		if op != batch.DecimalScalarRound && op != batch.DecimalScalarTrunc {
			return expr.DeclType{}, false
		}
		if digits, ok = constIntArg(n.Args[1]); !ok {
			return expr.DeclType{}, false
		}
	}
	out, ok := batch.DecimalScalarType(op, in, digits)
	if !ok {
		return expr.DeclType{}, false
	}
	return expr.DeclDecimal(out.Precision, out.Scale), true
}

// isConstNumericLitNode reports whether an AST node is a numeric CONSTANT — a
// literal, or unary ± over one. expr.isConstNumericLit makes the same test one
// layer down, over the compiled node.
func isConstNumericLitNode(node plansql.Node) bool {
	switch n := node.(type) {
	case *plansql.Lit:
		return true
	case *plansql.UnaryOp:
		return (n.Op == "-" || n.Op == "+") && isConstNumericLitNode(n.Inner)
	case *plansql.ParenNode:
		return isConstNumericLitNode(n.Inner)
	}
	return false
}

// constIntArg reads a compile-time integer literal, the only shape a result
// SCALE may be named by. expr.constIntOperand makes the same test one layer
// down, over the compiled node.
func constIntArg(node plansql.Node) (int, bool) {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return constIntArg(n.Inner)
	case *plansql.UnaryOp:
		v, ok := constIntArg(n.Inner)
		if !ok {
			return 0, false
		}
		if n.Op == "-" {
			return -v, true
		}
		return v, n.Op == "+"
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return 0, false
		}
		v, err := strconv.Atoi(strings.TrimSpace(n.Value))
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

// castDeclaredDecimal is a CAST's DECIMAL declaration — ADR-0024 item 3's
// "CAST(x AS DECIMAL(p,s)): exactly (p,s); CAST(x AS DECIMAL): the operand's
// own (p,s), (38,0) from an integer".
//
// A BARE cast over an operand this layer cannot type declines, and the caller
// keeps inferCastType's FLOAT64 — which is what the evaluator still answers
// for that shape, so the declaration and the value agree. That is the residual
// case: `CAST(f AS NUMERIC)` over a float column is numeric in PostgreSQL and
// float8 here, because a float has no scale for the fold to take and any fixed
// one would either truncate the value or invent digits.
func castDeclaredDecimal(n *plansql.CastNode, decls colDecls) (expr.DeclType, bool) {
	p, s, hasParams, ok := expr.DecimalCastDest(n.TypeName)
	if !ok {
		return expr.DeclType{}, false
	}
	if hasParams {
		return expr.DeclDecimal(p, s), true
	}
	t, isDec, ok := decimalArithOperand(n.Inner, decls)
	if !ok {
		return expr.DeclType{}, false
	}
	if !isDec {
		// An INTEGER operand: (38,0), the carrier's full width at scale 0.
		return expr.DeclDecimal(batch.MaxDecimalPrecision, 0), true
	}
	return expr.DeclDecimal(batch.MaxDecimalPrecision, t.Scale), true
}

// decimalTypeOfColumn is a declared column's fixed-point contribution: its own
// (p,s) for a DECIMAL and the whole integer range at scale 0 for an integer,
// with the "is it a genuine DECIMAL" flag decimalArithOperand answers with.
func decimalTypeOfColumn(c parquet.Column) (batch.DecimalType, bool, bool) {
	switch c.Type {
	case parquet.TypeDecimal:
		if c.Precision <= 0 {
			return batch.DecimalType{}, false, false // #458's unconstrained sentinel
		}
		return batch.DecimalType{Precision: c.Precision, Scale: c.Scale}, true, true
	case parquet.TypeInt32:
		return batch.DecimalType{Precision: batch.Int32DecimalDigits}, false, true
	case parquet.TypeInt64:
		return batch.DecimalType{Precision: batch.Int64DecimalDigits}, false, true
	}
	return batch.DecimalType{}, false, false
}

// decimalArithOperandDecided reports whether an operand has an exact
// fixed-point form at RUNTIME as well as a DECIMAL declaration here. Unary
// minus asks it because expr.UnaryOp negates exactly only over the operands
// that implement decimalOperand; over anything else it still goes through
// float64, and declaring DECIMAL there would allocate an exact vector for a
// rounded value.
func decimalArithOperandDecided(node plansql.Node, decls colDecls) bool {
	_, isDec, ok := decimalArithOperand(node, decls)
	return ok && isDec
}
