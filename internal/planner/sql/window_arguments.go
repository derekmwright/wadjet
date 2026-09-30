// SPDX-License-Identifier: MIT

package sql

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// WindowIntegerArgument reads the INTEGER argument of a window function fn
// (lower-case, for the messages) — LAG / LEAD's offset, NTILE's bucket count,
// NTH_VALUE's n — the way PostgreSQL 17.11 types it: the argument is an
// `integer`, so an integer literal, a signed one, a quoted literal (an
// `unknown` PostgreSQL coerces through int4in), a CAST to integer or
// smallint, or NULL. isNull reports the NULL spelling, which PostgreSQL
// answers with NULL on every row.
//
// It is the one reading of that argument: the parser asks it to refuse what
// the window operator cannot honor (refuseWindowArguments), the physical
// planner for the value (it replaced a strconv.Atoi reading that dropped what
// it could not parse, #1399's mechanism: ADR-0012 amendment log, 2026-09-29).
//
// Any other CONSTANT expression — `2 - 1`, `abs(-1)`, `CAST(1 + 0 AS
// INTEGER)` — is folded at plan time by the planner's constant fold (the one
// table-function arguments use, installed by package logical: this package
// cannot import the expression compiler), and its value read as an integer
// argument. What is refused 0A000 is only an argument that needs a ROW — a
// column reference, a subquery: PostgreSQL evaluates it per row, and the
// operator takes the offset / n as one constant.
func WindowIntegerArgument(fn string, n Node) (v int64, isNull bool, err error) {
	switch e := n.(type) {
	case *ParenNode:
		return WindowIntegerArgument(fn, e.Inner)
	case *UnaryOp:
		if e.Op != "-" && e.Op != "+" {
			break
		}
		// `-2147483648` is ONE signed literal, as PostgreSQL's grammar
		// folds a minus into the constant it precedes (parenthesized too):
		// read apart, its magnitude is past int4 and was refused 42883.
		if lit, ok := Unparen(e.Inner).(*Lit); ok && e.Op == "-" && lit.Kind == LitNumber &&
			!strings.HasPrefix(lit.Value, "-") {
			return WindowIntegerArgument(fn, &Lit{Kind: LitNumber, Value: "-" + lit.Value})
		}
		inner, null, err := WindowIntegerArgument(fn, e.Inner)
		if err != nil || null {
			return 0, null, err
		}
		if e.Op == "-" {
			inner = -inner
		}
		if inner < math.MinInt32 || inner > math.MaxInt32 {
			// -(-2147483648): PostgreSQL negates the literal's text, and
			// 2147483648 is a bigint.
			return 0, false, sqlerr.New("42883",
				"function %s with a bigint argument does not exist: the argument is an integer", fn)
		}
		return inner, false, nil
	case *Lit:
		switch e.Kind {
		case LitNull:
			return 0, true, nil
		case LitString:
			// int4in: surrounding whitespace is accepted, anything else is
			// 22P02, and a number past int4 is 22003 (measured on 17.11:
			// `LAG(x, ' 2 ')` answers offset 2, `LAG(x, '1.5')` raises).
			s := strings.TrimSpace(e.Value)
			i, perr := strconv.ParseInt(s, 10, 64)
			if perr != nil {
				if ne, ok := perr.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
					return 0, false, sqlerr.New("22003", "value %q is out of range for type integer", e.Value)
				}
				return 0, false, sqlerr.New("22P02", "invalid input syntax for type integer: %q", e.Value)
			}
			if i < math.MinInt32 || i > math.MaxInt32 {
				return 0, false, sqlerr.New("22003", "value %q is out of range for type integer", e.Value)
			}
			return i, false, nil
		case LitNumber:
			i, perr := strconv.ParseInt(e.Value, 10, 64)
			if perr == nil && i >= math.MinInt32 && i <= math.MaxInt32 {
				return i, false, nil
			}
			// A literal past int4 is a bigint and one with a fraction or an
			// exponent is a numeric, and PostgreSQL has no window function
			// taking either there.
			argType := "numeric"
			if perr == nil || strings.IndexAny(e.Value, ".eE") < 0 {
				argType = "bigint"
			}
			return 0, false, sqlerr.New("42883",
				"function %s with a %s argument does not exist: the argument is an integer", fn, argType)
		}
	case *CastNode:
		switch t := strings.ToLower(strings.TrimSpace(e.TypeName)); t {
		case "int", "integer", "int4", "smallint", "int2":
			return WindowIntegerArgument(fn, e.Inner)
		default:
			// A cast to any other type is an argument OF that type, which
			// no window function takes there (`LAG(x, CAST(0 AS BIGINT))`
			// is 42883 on 17.11).
			return 0, false, sqlerr.New("42883",
				"function %s with a %s argument does not exist: the argument is an integer", fn, t)
		}
	}
	return foldWindowIntegerArgument(fn, n)
}

// constantFolder is the planner's constant fold (package logical installs it
// with SetConstantFolder): constant reports whether n reads no row, and v is
// its value when it does not.
var constantFolder func(n Node) (v any, constant bool, err error)

// SetConstantFolder installs the planner's constant fold for
// WindowIntegerArgument. Package logical calls it once, at init.
func SetConstantFolder(f func(n Node) (v any, constant bool, err error)) { constantFolder = f }

// foldWindowIntegerArgument reads a constant EXPRESSION as the integer
// argument, typed as PostgreSQL types it: a number literal inside it that is
// past int4 or has a fraction makes the expression a bigint / numeric, which
// no window function takes (42883, `LAG(x, 2147483648 - 1)`), and an int4
// result past int4 is PostgreSQL's integer overflow (22003,
// `LAG(x, 2147483647 + 1)`).
func foldWindowIntegerArgument(fn string, n Node) (int64, bool, error) {
	var v any
	constant := false
	var err error
	if constantFolder != nil {
		v, constant, err = constantFolder(n)
	}
	if !constant {
		return 0, false, sqlerr.New("0A000",
			"the integer argument of %s must be a constant here: %s is not supported "+
				"(PostgreSQL evaluates it per row; write a constant)", fn, n.String())
	}
	if err := windowArgumentOperands(fn, n); err != nil {
		return 0, false, err
	}
	if err != nil {
		return 0, false, err
	}
	var i int64
	switch x := v.(type) {
	case nil:
		return 0, true, nil
	case int64:
		i = x
	case int32:
		i = int64(x)
	case int16:
		i = int64(x)
	case int8:
		i = int64(x)
	default:
		argType := "numeric"
		switch v.(type) {
		case string:
			argType = "text"
		case bool:
			argType = "boolean"
		}
		return 0, false, sqlerr.New("42883",
			"function %s with a %s argument does not exist: the argument is an integer", fn, argType)
	}
	if i < math.MinInt32 || i > math.MaxInt32 {
		return 0, false, sqlerr.New("22003", "integer out of range")
	}
	return i, false, nil
}

// windowArgumentOperands raises the reader's typing error for a number
// literal (or a cast to a type other than an integer) inside a constant
// expression: PostgreSQL types the expression by its operands.
func windowArgumentOperands(fn string, n Node) error {
	switch e := n.(type) {
	case *Lit:
		if e.Kind == LitNumber {
			_, _, err := WindowIntegerArgument(fn, e)
			return err
		}
	case *CastNode:
		switch strings.ToLower(strings.TrimSpace(e.TypeName)) {
		case "int", "integer", "int4", "smallint", "int2":
			return nil // an integer whatever its operand
		}
		_, _, err := WindowIntegerArgument(fn, e)
		return err
	case *ParenNode:
		return windowArgumentOperands(fn, e.Inner)
	case *UnaryOp:
		if lit, ok := Unparen(e.Inner).(*Lit); ok && e.Op == "-" && lit.Kind == LitNumber {
			_, _, err := WindowIntegerArgument(fn, e) // one signed literal
			return err
		}
		return windowArgumentOperands(fn, e.Inner)
	case *BinaryOp:
		if err := windowArgumentOperands(fn, e.Left); err != nil {
			return err
		}
		return windowArgumentOperands(fn, e.Right)
	case *FuncCallNode:
		for _, a := range e.Args {
			if err := windowArgumentOperands(fn, a); err != nil {
				return err
			}
		}
	}
	return nil
}

// refuseWindowArguments raises, at the one site where a call becomes a window
// call, what PostgreSQL raises for a window function's integer argument, and
// refuses what this engine cannot evaluate. NTILE's and NTH_VALUE's
// "must be greater than zero" (22014 / 22016) are NOT raised here: PostgreSQL
// raises them only when a row is evaluated, so a window over no rows answers
// no rows, and the operator raises them per partition.
func refuseWindowArguments(fn *FuncCallNode) error {
	if fn == nil {
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(fn.Name))
	var arg Node
	switch name {
	case "lag", "lead", "nth_value":
		if len(fn.Args) >= 2 {
			arg = fn.Args[1]
		}
	case "ntile":
		if len(fn.Args) >= 1 {
			arg = fn.Args[0]
		}
	}
	if arg == nil {
		return nil
	}
	_, _, err := WindowIntegerArgument(name, arg)
	return err
}
