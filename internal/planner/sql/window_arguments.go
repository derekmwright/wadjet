// SPDX-License-Identifier: MIT

package sql

import (
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// WindowIntegerArgument reads the INTEGER argument of a window function —
// LAG / LEAD's offset, NTILE's bucket count, NTH_VALUE's n — the way
// PostgreSQL 17.11 types it: the argument is an `integer`, so an integer
// literal, a signed one, a quoted literal (an `unknown` PostgreSQL coerces
// through int4in), a CAST to a 4-byte integer, or NULL. isNull reports the
// NULL spelling, which PostgreSQL answers with NULL on every row.
//
// It is the one reading of that argument. The parser asks it to refuse what
// the window operator cannot honor (refuseWindowArguments), and the physical
// planner asks it again for the value — before it, the planner ran strconv.Atoi
// over the argument's text and dropped whatever failed, so `LAG(x, 1 + 1)`,
// `LAG(x, NULL)` and `NTILE(o)` all ran with the zero value, which the
// operator then read as the default (#1399's mechanism).
//
// fn is the function's lower-case name, for the messages.
func WindowIntegerArgument(fn string, n Node) (v int64, isNull bool, err error) {
	switch e := n.(type) {
	case *ParenNode:
		return WindowIntegerArgument(fn, e.Inner)
	case *UnaryOp:
		if e.Op != "-" && e.Op != "+" {
			break
		}
		inner, null, err := WindowIntegerArgument(fn, e.Inner)
		if err != nil || null {
			return 0, null, err
		}
		if e.Op == "-" {
			inner = -inner
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
	return 0, false, sqlerr.New("0A000",
		"the integer argument of %s must be a constant here: %s is not supported "+
			"(PostgreSQL evaluates it per row; write an integer literal)", fn, n.String())
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
