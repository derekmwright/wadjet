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
		if n.Subject == nil {
			for _, w := range n.Whens {
				if err := checkBooleanContext(w.Cond, scope, "CASE/WHEN"); err != nil {
					return err
				}
			}
		}
		// ...and the CASE's own VALUE is what the ENCLOSING site reads, so it
		// falls through to the type check below rather than returning here:
		// `WHERE id > 0 AND CASE WHEN id > 0 THEN 1 ELSE 0 END` is 42804
		// integer on the server and DELETED every row here (round-1 review,
		// B1; the arm below is the reviewer's, with credit in the commit).
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
		if !known {
			// A ROW FIELD PATH reaches this walk as a ColRef whose QUALIFIER
			// is a ROW column rather than a relation (ADR-0022), so the
			// relation lookup above misses it. The field's declared type is
			// the one the scope already records for #604's field-existence
			// check: `WHERE (r).a` is 42804 bigint on 17.11 and removed a row
			// here (round-2 review, P2-r2).
			typ, known = scope.provableRowFieldType(n)
		}
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
		case "min", "max":
			// A value the input HELD, so the argument's type: `HAVING CASE
			// WHEN MAX(customer) THEN …` is 42804 text on 17.11 (#1216).
			if len(n.Args) == 1 {
				return provableNonBooleanType(n.Args[0], scope)
			}
			return 0, "", false
		case "string_agg":
			return parquet.TypeString, "text", true
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
		// A POLYMORPHIC declaration mirrors an argument, so the call is typed
		// by the positions it mirrors: `COALESCE(n, 1)` and `GREATEST(n, 1)`
		// are bigint on 17.11, and a DELETE whose WHERE was one of them
		// removed every row here. Only a candidate that is itself PROVABLY
		// non-boolean answers; one that is boolean, or that this scope cannot
		// type, leaves the call alone.
		// A SUBSCRIPT — `arr[1]`, `m['k']` — is lowered to `element_at`, whose
		// declaration mirrors the CONTAINER, so the polymorphic arm below
		// would name the container's type where PostgreSQL names the ELEMENT's
		// (42804 bigint for a bigint[]). The scope records the element type of
		// the columns it was built from, and where it has one, that is the
		// type this position holds — including when the element IS boolean,
		// which is the one shape that must NOT be refused.
		if strings.EqualFold(n.Name, "element_at") && len(n.Args) > 0 {
			if typ, ok := scope.provableElementType(n.Args[0]); ok {
				if typ == parquet.TypeBool {
					return 0, "", false
				}
				return typ, pgTypeName(typ), true
			}
		}
		if positions, ok := expr.FuncPolymorphicArgPositions(strings.ToLower(n.Name)); ok {
			for i, arg := range n.Args {
				if !polymorphicCandidate(positions, i) {
					continue
				}
				if typ, name, ok := provableNonBooleanType(arg, scope); ok {
					return typ, name, true
				}
			}
		}
		return 0, "", false
	case *plansql.CastNode:
		// A CAST DECLARES its result type, which is the strongest declaration
		// in the language: `WHERE CAST(n AS BIGINT)` is 42804 bigint on
		// 17.11 and removed three of four rows here through a DELETE.
		// inferCastType is this package's one reading of a destination name.
		if typ := inferCastType(n.TypeName); typ != parquet.TypeBool {
			return typ, pgTypeName(typ), true
		}
		return 0, "", false
	case *plansql.CaseNode:
		// A CASE's VALUE is its branch results — for a SEARCHED case and for
		// a simple one alike, which is why the Subject is not consulted here.
		// One provably non-boolean branch is enough: a CASE whose branches
		// disagree on type is refused for that reason first.
		for _, w := range n.Whens {
			if typ, name, ok := provableNonBooleanType(w.Result, scope); ok {
				return typ, name, true
			}
		}
		if n.Else != nil {
			return provableNonBooleanType(n.Else, scope)
		}
		return 0, "", false
	case *plansql.ArrayLitNode:
		// An ARRAY constructor is never a boolean. PostgreSQL names the
		// element type — `integer[]` — and this engine has one ARRAY type,
		// so the name it can state truthfully is that.
		return parquet.TypeArray, pgTypeName(parquet.TypeArray), true
	case *plansql.IntervalLit:
		return parquet.TypeDuration, "interval", true
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

// PgTypeName exports pgTypeName for a caller outside this package that needs
// PostgreSQL's own spelling for a 42804 message — wadjet/dml.go's
// datatypeMismatch, which used to print the wadjet-internal TypeID stringer
// ("INT64") in a column that PostgreSQL's own 42804 spells "bigint" (#1252
// review).
func PgTypeName(t parquet.TypeID) string { return pgTypeName(t) }

// RefuseNonBooleanClause is the truth-context rule over a clause whose scope
// is a plain column list and whose SITE has its own name — MERGE's
// `WHEN … AND <cond>`, which PostgreSQL reports as `argument of WHEN must be
// type boolean, not type bigint`.
//
// It exists so the fourth DML verb reads the SAME walk as the other three: the
// bespoke check MERGE had typed a literal, a column, arithmetic and a CAST and
// nothing else, so a CALL, a CASE, an ARRAY or a polymorphic call in a WHEN
// condition fell through and the clause FIRED — destroying a row PostgreSQL
// refuses to touch (#1179 round 2, the node-kind census).
func RefuseNonBooleanClause(node plansql.Node, site string, cols []parquet.Column) error {
	if node == nil {
		return nil
	}
	scope := newColScope()
	for _, c := range cols {
		scope.addQualifiedTyped("", c.Name, c.Type)
		scope.addRowColumn(c)
		scope.addElementType(c)
	}
	if err := checkBooleanContext(node, scope, site); err != nil {
		return err
	}
	return checkCaseWhenContexts(node, scope)
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
		scope.addRowColumn(c)
		scope.addElementType(c)
	}
	if err := checkBooleanContext(node, scope, "WHERE"); err != nil {
		return err
	}
	return checkCaseWhenContexts(node, scope)
}

// polymorphicCandidate reports whether argument i is one of the positions a
// polymorphic declaration mirrors. An EMPTY list means every argument, which
// is what the declaration itself means (expr.FuncPolymorphicArgPositions).
func polymorphicCandidate(positions []int, i int) bool {
	if len(positions) == 0 {
		return true
	}
	for _, p := range positions {
		if p == i {
			return true
		}
	}
	return false
}

// RefuseWindowInADMLPredicate refuses a WINDOW function in a DELETE's or an
// UPDATE's WHERE — 42P20, BEFORE column resolution, which is the server's
// order: `WHERE COUNT(*) OVER (PARTITION BY zz)` is 42P20 there even though
// `zz` does not exist. Its SELECT twin is
// plansql.RefuseWindowInARowFilteringClause, which runs in the parser's own
// post-extract hook for the same reason.
func RefuseWindowInADMLPredicate(node plansql.Node) error {
	if node == nil || len(plansql.FindAllWindowFuncs(node)) == 0 {
		return nil
	}
	return sqlerr.New("42P20", "window functions are not allowed in WHERE")
}

// RefuseAggregateInADMLPredicate refuses an AGGREGATE in the same clause —
// 42803, but AFTER column resolution, which is the other half of the server's
// order and the half round 2 got wrong: `DELETE … WHERE SUM(zz)` is
// `42703 column "zz" does not exist` on 17.11 and on the SELECT door here,
// and running the aggregate check first made it 42803, so the two doors
// disagreed about the same statement (round-2 review, P3-r2).
func RefuseAggregateInADMLPredicate(node plansql.Node) error {
	if node == nil {
		return nil
	}
	found := plansql.FindAllAggregates(node)
	if len(found) == 0 {
		return nil
	}
	kind := "aggregate functions"
	if strings.EqualFold(found[0].Name, "grouping") {
		kind = "grouping operations"
	}
	return sqlerr.New("42803", "%s are not allowed in WHERE", kind)
}
