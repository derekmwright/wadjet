package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// checkBooleanContext refuses a non-boolean expression where SQL requires a
// boolean, before any row exists — PostgreSQL's 42804 (#599).
//
// PostgreSQL, measured live on postgres:17-alpine over `cb(id bigint,
// c bigint, s varchar, f double precision, n numeric, b boolean)`:
//
//	WHERE c            42804  argument of WHERE must be type boolean, not type bigint
//	WHERE NOT c        42804  argument of NOT must be type boolean, not type bigint
//	WHERE 1            42804  ... not type integer
//	WHERE s            42804  ... not type character varying   (s is varchar THERE;
//	                          wadjet's STRING declares OID 25, and PostgreSQL says
//	                          "text" for a text column, which is what pgTypeName
//	                          answers)
//	WHERE c AND b      42804  argument of AND must be type boolean, not type bigint
//	WHERE b OR c       42804  argument of OR ...
//	HAVING count(*)    42804  argument of HAVING must be type boolean, not type bigint
//	JOIN ... ON a.c    42804  argument of JOIN/ON ...
//	CASE WHEN 1        42804  argument of CASE/WHEN ...
//
// Wadjet had no type check at all here, and the two evaluators that collapse a
// value to a truth value did not agree: `expr.FilterPredicate`'s generic arm
// takes a failed `v.(bool)` assertion for FALSE, while `expr.toBoolVal` reads
// C truthiness. So `WHERE c` returned 0 rows and `WHERE NOT c` returned the
// row holding 0 — not complements of each other under any reading, which is
// the two-path shape #592 was with the cast removed.
//
// It is as conservative as the rest of this binder (validate.go's contract):
// it refuses only where the expression PROVABLY has a non-boolean type, which
// is a column whose DECLARATION the scope carries, a numeric literal, an
// arithmetic operator, or an aggregate whose result type is fixed. A function
// call, a CAST, a subquery, a container element and anything over a derived
// table or CTE column are left alone — a false positive breaks a working
// query, a false negative merely leaves the shape where it already was.
//
// An UNKNOWN-typed literal is NOT a type error there: PostgreSQL coerces it
// through the boolean input function, so `WHERE 'true'` succeeds, `WHERE NULL`
// succeeds, and `WHERE 'abc'` is 22P02 rather than 42804. That half is
// handled in the parser (plansql.CoerceBooleanLiterals), where the literal can
// be turned into the boolean it names.
func checkBooleanContext(node plansql.Node, scope *colScope, site string) error {
	if node == nil || scope == nil || scope.open {
		return nil
	}
	switch n := plansql.Unparen(node).(type) {
	// The boolean-valued shapes: their operands are checked, not themselves.
	case *plansql.AndNode:
		if err := checkBooleanContext(n.Left, scope, "AND"); err != nil {
			return err
		}
		return checkBooleanContext(n.Right, scope, "AND")
	case *plansql.OrNode:
		if err := checkBooleanContext(n.Left, scope, "OR"); err != nil {
			return err
		}
		return checkBooleanContext(n.Right, scope, "OR")
	case *plansql.NotNode:
		return checkBooleanContext(n.Inner, scope, "NOT")
	case *plansql.CaseNode:
		// Only a SEARCHED CASE's WHEN is a boolean context. `CASE x WHEN 1`
		// compares x against 1 and its WHEN is a VALUE, which is why the
		// Subject test comes first.
		if n.Subject != nil {
			return nil
		}
		for _, w := range n.Whens {
			if err := checkBooleanContext(w.Cond, scope, "CASE/WHEN"); err != nil {
				return err
			}
		}
		return nil
	}
	_, name, ok := provableNonBooleanType(node, scope)
	if !ok {
		return nil
	}
	return sqlerr.New("42804", "argument of %s must be type boolean, not type %s", site, name)
}

// checkCaseWhenContexts finds the SEARCHED CASE conditions anywhere in an
// expression and holds each to the boolean rule. A CASE in the SELECT list is
// not itself a boolean context — its VALUE is the column — but its WHEN is,
// and PostgreSQL refuses `SELECT CASE WHEN 1 THEN 'x' END` with 42804
// (measured live).
func checkCaseWhenContexts(node plansql.Node, scope *colScope) error {
	switch n := node.(type) {
	case nil:
		return nil
	case *plansql.CaseNode:
		if n.Subject == nil {
			for _, w := range n.Whens {
				if err := checkBooleanContext(w.Cond, scope, "CASE/WHEN"); err != nil {
					return err
				}
			}
		} else if err := checkCaseWhenContexts(n.Subject, scope); err != nil {
			return err
		}
		for _, w := range n.Whens {
			if err := checkCaseWhenContexts(w.Result, scope); err != nil {
				return err
			}
		}
		return checkCaseWhenContexts(n.Else, scope)
	case *plansql.ParenNode:
		return checkCaseWhenContexts(n.Inner, scope)
	case *plansql.NotNode:
		return checkCaseWhenContexts(n.Inner, scope)
	case *plansql.AndNode:
		if err := checkCaseWhenContexts(n.Left, scope); err != nil {
			return err
		}
		return checkCaseWhenContexts(n.Right, scope)
	case *plansql.OrNode:
		if err := checkCaseWhenContexts(n.Left, scope); err != nil {
			return err
		}
		return checkCaseWhenContexts(n.Right, scope)
	case *plansql.BinaryOp:
		if err := checkCaseWhenContexts(n.Left, scope); err != nil {
			return err
		}
		return checkCaseWhenContexts(n.Right, scope)
	case *plansql.UnaryOp:
		return checkCaseWhenContexts(n.Inner, scope)
	case *plansql.CmpExpr:
		if err := checkCaseWhenContexts(n.Left, scope); err != nil {
			return err
		}
		return checkCaseWhenContexts(n.Right, scope)
	case *plansql.CastNode:
		return checkCaseWhenContexts(n.Inner, scope)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			if err := checkCaseWhenContexts(a, scope); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// provableNonBooleanType reports the PostgreSQL type name of an expression
// this binder can type with CERTAINTY, when that type is not boolean.
func provableNonBooleanType(node plansql.Node, scope *colScope) (parquet.TypeID, string, bool) {
	switch n := plansql.Unparen(node).(type) {
	case *plansql.ColRef:
		typ, known := scope.provableColType(n)
		if !known || typ == parquet.TypeBool {
			return 0, "", false
		}
		return typ, pgTypeName(typ), true
	case *plansql.Lit:
		switch n.Kind {
		case plansql.LitNumber:
			// PostgreSQL types an unsuffixed integer literal `integer` and a
			// decimal one `numeric`, and names those in the message.
			if strings.ContainsAny(n.Value, ".eE") {
				return parquet.TypeDecimal, "numeric", true
			}
			return parquet.TypeInt32, "integer", true
		}
		// A STRING or NULL literal is UNKNOWN-typed and coerces; see the
		// parser half.
		return 0, "", false
	case *plansql.BinaryOp:
		// Arithmetic and concatenation. Every operator this node carries
		// (+ - * / % ||) yields a non-boolean, and the message names the
		// operand's type the way PostgreSQL's does.
		if typ, name, ok := provableNonBooleanType(n.Left, scope); ok {
			return typ, name, true
		}
		return provableNonBooleanType(n.Right, scope)
	case *plansql.UnaryOp:
		return provableNonBooleanType(n.Inner, scope)
	case *plansql.FuncCallNode:
		// Only the AGGREGATES whose result type is fixed regardless of input.
		// A scalar function's return type is not carried here, and guessing
		// one is how a false positive gets in.
		switch strings.ToLower(n.Name) {
		case "count":
			return parquet.TypeInt64, "bigint", true
		}
		return 0, "", false
	case *plansql.SubqueryNode:
		// A SCALAR SUBQUERY used as a predicate is typed by its single select
		// item, and the same rule applies to it — `WHERE (SELECT COUNT(*)
		// FROM t)` is 42804 `argument of WHERE must be type boolean, not type
		// bigint` in PostgreSQL, measured live, while the bare `HAVING
		// COUNT(*)` was already refused here. Two spellings of one type
		// disagreeing is the shape #599 exists to end (round-1 P6).
		//
		// Only the item this layer can type is refused, which is the same
		// bound the rest of the function keeps: an aggregate with a fixed
		// result type. A subquery selecting a plain COLUMN is typed by its
		// OWN relation, which this scope does not carry, and is left alone.
		return subqueryItemType(n.SQL, scope)
	}
	return 0, "", false
}

// subqueryItemType types a scalar subquery by its single select item, when
// this layer can. Its own FROM is a different scope, so only an item whose
// type is fixed regardless of input — an aggregate like COUNT — is answered.
func subqueryItemType(sql string, scope *colScope) (parquet.TypeID, string, bool) {
	inner := parseSelect(sql)
	if inner == nil || inner.Union != nil || len(inner.Columns) != 1 {
		return 0, "", false
	}
	col := inner.Columns[0]
	if col.Star || col.IsWindow {
		return 0, "", false
	}
	if col.ASTExpr != nil {
		return provableNonBooleanType(col.ASTExpr, scope)
	}
	// An aggregate the extractor recorded without an AST still names itself.
	if col.IsAgg && strings.EqualFold(col.AggFunc, "count") {
		return parquet.TypeInt64, "bigint", true
	}
	return 0, "", false
}

// pgTypeName is PostgreSQL's own spelling of the type wadjet declares, for the
// 42804 message. Measured against `\gdesc` on postgres:17-alpine.
func pgTypeName(t parquet.TypeID) string {
	switch t {
	case parquet.TypeBool:
		return "boolean"
	case parquet.TypeInt32:
		return "integer"
	case parquet.TypeInt64:
		return "bigint"
	case parquet.TypeFloat32:
		return "real"
	case parquet.TypeFloat64:
		return "double precision"
	case parquet.TypeDecimal:
		return "numeric"
	case parquet.TypeString:
		// `text`, which is what the WIRE declares for a STRING column (OID 25)
		// and what pgFormatType reports for it in the catalog. It used to say
		// "character varying" here, which contradicted both, and PostgreSQL's
		// own message for a text column says "text" — measured live on 17.11
		// for `WHERE txt` (42804 "argument of WHERE must be type boolean, not
		// type text") and for `numeric UNION text` (42804 "UNION types numeric
		// and text cannot be matched").
		return "text"
	case parquet.TypeBytes:
		return "bytea"
	case parquet.TypeTimestamp:
		return "timestamp without time zone"
	case parquet.TypeDate:
		return "date"
	case parquet.TypeUUID:
		return "uuid"
	case parquet.TypeMAC:
		return "macaddr"
	case parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR:
		return "inet"
	case parquet.TypeArray:
		return "array"
	case parquet.TypeRow:
		return "record"
	case parquet.TypePort, parquet.TypeProtocol:
		// What the WIRE declares for them since #834 — int4 — and what
		// pgFormatType reports in the catalog. A message naming a type the
		// same query's RowDescription does not is the contradiction #834 is
		// about, one layer over.
		return "integer"
	case parquet.TypeDuration:
		return "bigint"
	}
	// MAP and VECTOR are wadjet-native: PostgreSQL has no such type and
	// therefore no name for it. The refusal still fires — neither is a boolean
	// — and names what wadjet calls it.
	return strings.ToLower(t.String())
}
