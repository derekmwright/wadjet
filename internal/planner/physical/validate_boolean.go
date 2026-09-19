// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// checkBooleanContext refuses provably non-boolean expressions before rows,
// with 42804 and the context/type name (#599, #592).
// In a closed scope, declarations, numeric literals, arithmetic and fixed-result
// aggregates can prove the type; functions, casts, subqueries, container elements
// and derived/CTE columns stay unresolved rather than risk a false positive.
// Unknown string/NULL literals are not 42804: plansql.CoerceBooleanLiterals
// applies boolean input coercion (invalid text is 22P02).
// See docs/internals/boolean-context-type-refusal.md for the design.
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
		// The AGGREGATES whose result type is fixed regardless of input...
		switch strings.ToLower(n.Name) {
		case "count":
			return parquet.TypeInt64, "bigint", true
		}
		// ...and every registered scalar function whose return type is
		// DECLARED and fixed. The declaration is the contract (ADR-0038), not
		// a guess: `upper(s)` is text on this engine and on the server, which
		// refuses `WHERE upper(s)` with 42804 naming it. A POLYMORPHIC
		// declaration mirrors an argument whose type no batch has decided yet
		// and is left alone, which is the bound the rest of this function
		// keeps.
		//
		// The reason it is here: an OPERATOR the parser rewrites into a call
		// reaches this walk as a call. `a # b` is `bitwise_xor(a, b)` and
		// `a ^ b` is `power(a, b)`, and an untyped call in a truth context
		// meant `DELETE FROM t WHERE id > 0 AND n # 3` deleted every row
		// where PostgreSQL raises 42804 (#1179).
		// batch.TypeID is an alias of parquet.TypeID, so the declaration's
		// type is this walk's type with no conversion.
		if typ, ok := expr.FuncFixedNonBooleanType(strings.ToLower(n.Name)); ok {
			return typ, pgTypeName(typ), true
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

// RefuseNonBooleanDMLPredicate holds a DELETE's or an UPDATE's WHERE clause to
// the same truth-context rule a SELECT's is held to: `argument of WHERE must
// be type boolean`, SQLSTATE 42804, PostgreSQL's own sentence.
//
// A DML predicate is COMPILED and not planned (ADR-0031), so nothing on that
// path ran this check — and the per-row closure reads a non-boolean value as
// FALSE only at the TOP: an integer under an AND was evaluated as a condition
// and matched every row, so `DELETE FROM t WHERE id > 0 AND n # 3` emptied a
// table PostgreSQL leaves untouched (#1179; the operator made the clause
// parseable, and the hole under it had been reachable through any non-boolean
// CALL). This is a VALIDATION over the clause the door already parsed, not a
// plan: the target's own columns are the scope, and nothing else is consulted.
func RefuseNonBooleanDMLPredicate(node plansql.Node, alias string, schema []parquet.Column) error {
	if node == nil {
		return nil
	}
	// ONE registration per column. addQualifiedTyped counts sources, and a
	// column registered twice looks AMBIGUOUS to provableColType — which
	// silently turned the refusal off for a bare name whenever the statement
	// wrote an alias (`DELETE FROM t AS a WHERE n`). The qualified form
	// registers the bare name too.
	scope := newColScope()
	for _, c := range schema {
		scope.addQualifiedTyped(alias, c.Name, c.Type)
	}
	if err := checkBooleanContext(node, scope, "WHERE"); err != nil {
		return err
	}
	return checkCaseWhenContexts(node, scope)
}
