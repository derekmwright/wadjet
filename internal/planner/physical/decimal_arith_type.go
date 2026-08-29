package physical

import (
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
	if _, _, ok := batch.DecimalResultType(n.Op, 1, 0, 1, 0); !ok {
		return expr.DeclType{}, false // not one of + - * / %
	}
	lt, lDec, lok := decimalArithOperand(n.Left, decls)
	rt, rDec, rok := decimalArithOperand(n.Right, decls)
	if !lok || !rok {
		return expr.DeclType{}, false
	}
	if !lDec && !rDec {
		// Two integers. PostgreSQL keeps integer arithmetic in integers,
		// truncating division included (#636), so this is not a decimal
		// expression at all and the int rule above decides it.
		return expr.DeclType{}, false
	}
	p, s, ok := batch.DecimalResultType(n.Op, lt.Precision, lt.Scale, rt.Precision, rt.Scale)
	if !ok {
		return expr.DeclType{}, false
	}
	return expr.DeclDecimal(p, s), true
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
		t, ok := binOpDecimalType(n, decls)
		if ok {
			return t.Dec(), true, true
		}
		// Nested INTEGER arithmetic still contributes: `d * (i + 1)` is
		// numeric in PostgreSQL. It contributes the integer range at scale 0,
		// the same as a bare integer column, and only when every leaf of it
		// really is an integer — which is what expr.operandIsInt decides at
		// runtime and intArithAllInt mirrors here.
		if intArithAllInt(n, allIntColumns(decls), decls) {
			return batch.DecimalType{Precision: batch.Int64DecimalDigits}, false, true
		}
		return batch.DecimalType{}, false, false
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
		// A literal with a FRACTION is a decimal the user wrote — `i * 1.5`
		// is numeric in PostgreSQL — while a whole number is not, so it
		// leaves `i * 2` integer. expr.operandIsDecimalTyped draws the line
		// in the same place.
		return t, t.Scale > 0, true
	}
	return batch.DecimalType{}, false, false
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

// allIntColumns is the strictly-int column set intArithAllInt asks for, built
// from the declarations this layer holds. The caller's own strictInt map is
// derived from a scan's statistics and is not available at every site that
// needs to type an operand, so the declarations answer instead: a column
// DECLARED INT32/INT64 is integer whatever its values are.
func allIntColumns(decls colDecls) map[string]bool {
	if len(decls.types) == 0 {
		return nil
	}
	out := make(map[string]bool, len(decls.types))
	for name, t := range decls.types {
		if t == parquet.TypeInt32 || t == parquet.TypeInt64 {
			out[strings.ToLower(name)] = true
		}
	}
	return out
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
