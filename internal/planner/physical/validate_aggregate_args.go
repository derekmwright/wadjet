// SPDX-License-Identifier: MIT

package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// AN AGGREGATE OVER AN ARGUMENT IT HAS NO OVERLOAD FOR IS REFUSED, NOT
// ANSWERED (#1249, #1061).
//
// The aggregate layer computed whatever its accumulator did with a value of
// the wrong class: SUM/AVG/STDDEV/VARIANCE/CORR/COVAR/MEDIAN/MODE over TEXT,
// BYTEA, UUID, INET, MACADDR or a container answered NULL on every arm; over
// TIMESTAMP and DATE they answered the sum of the internal epoch integers
// (`SUM(ts)` = 3.4e13) under a numeric type; AVG over IPV4 and MACADDR
// answered a number that differed by arm; BOOL_AND/BOOL_OR/EVERY over a
// non-boolean answered `false`/`true`; STRING_AGG rendered each value with
// Go's fmt (`[98 121 …]` for bytes, `map[…]` for a row); MIN/MAX over a ROW
// answered a FIELD-WISE minimum presented as one row. PostgreSQL 17.11 raises
// 42883 `function sum(text) does not exist` for every one of them (measured
// over the 22-type matrix; the table is in arc BR's landing notes).
//
// The accept-sets are PostgreSQL's, read over this engine's types by what the
// WIRE declares them as: PORT and PROTOCOL are int4 and DURATION is int8 on
// the wire, so they are numbers here too. Two sets are wider than
// PostgreSQL's and stay so, recorded in ADR-0012 §5: MIN/MAX over BOOL, UUID,
// MACADDR, BYTEA, MAP and VECTOR (a defined total order, identical on every
// arm). ROW is removed from that list by this rule — its order was never a
// row order.
//
// A QUOTED or NULL literal is SQL's `unknown`. PostgreSQL resolves it against
// the overloads: SUM and AVG have several numeric ones and none preferred, so
// `SUM('5')` and `SUM(NULL)` are 42725 `function sum(unknown) is not unique`
// (wadjet answered NULL and 0); MEDIAN has none at all. Everywhere else an
// unknown literal takes the one type the aggregate demands, which is the
// literal rule's business and not this one's.
//
// The deciding site is here, in the binder, for refuseUnknownFlagNames'
// reason: a DAG stage compiles when a task runs, so a refusal made there is
// made only where a row arrives. An argument whose type this layer cannot
// decide is not refused.
type aggArgClass int

const (
	aggArgAny aggArgClass = iota
	aggArgNumeric
	aggArgBool
	aggArgText
	// aggArgOrdered is every type with a defined order — everything but ROW.
	aggArgOrdered
)

// aggArgPositions names, per aggregate, the class each argument position must
// hold. A position past the list is not this rule's (a separator, a fraction).
var aggArgPositions = map[string][]aggArgClass{
	"sum": {aggArgNumeric}, "avg": {aggArgNumeric},
	"stddev": {aggArgNumeric}, "stddev_samp": {aggArgNumeric}, "stddev_pop": {aggArgNumeric},
	"variance": {aggArgNumeric}, "var_samp": {aggArgNumeric}, "var_pop": {aggArgNumeric},
	"corr": {aggArgNumeric, aggArgNumeric}, "covar_samp": {aggArgNumeric, aggArgNumeric},
	"covar_pop": {aggArgNumeric, aggArgNumeric},
	"bool_and":  {aggArgBool}, "bool_or": {aggArgBool}, "every": {aggArgBool},
	"string_agg": {aggArgText},
	// DuckDB's spellings, which PostgreSQL has not (or only as ordered-set
	// aggregates): the specification is the oracle, and the value is a
	// number over a number. Over anything else they answered NULL.
	"median": {aggArgNumeric}, "mode": {aggArgNumeric},
	"quantile_cont": {aggArgNumeric}, "quantile_disc": {aggArgNumeric},
	"percentile_cont": {aggArgAny, aggArgNumeric}, "percentile_disc": {aggArgAny, aggArgNumeric},
	"min": {aggArgOrdered}, "max": {aggArgOrdered},
	"min_by": {aggArgAny, aggArgOrdered}, "max_by": {aggArgAny, aggArgOrdered},
}

// aggUnknownIsNotUnique are the aggregates whose `unknown` argument PostgreSQL
// cannot resolve to one overload (42725); aggUnknownDoesNotExist are the ones
// with no overload an unknown could take (42883).
var (
	aggUnknownIsNotUnique  = map[string]bool{"sum": true, "avg": true}
	aggUnknownDoesNotExist = map[string]bool{"median": true,
		"quantile_cont": true, "quantile_disc": true}
)

func isNumericClass(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64,
		parquet.TypeDecimal, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		return true
	}
	return false
}

func aggArgFits(c aggArgClass, t parquet.TypeID) bool {
	switch c {
	case aggArgNumeric:
		return isNumericClass(t)
	case aggArgBool:
		return t == parquet.TypeBool
	case aggArgText:
		// TEXT is PostgreSQL's set. The rest is the KEPT superset, measured
		// per type at base 260fc569 (arc BR round 2): each of these rendered
		// every value as its own text — the ISO date, the dotted address, the
		// number — identically on all five arms. TIMESTAMP rendered the epoch
		// milliseconds and the containers Go's fmt (`map[…]`), so those stay
		// refused; BYTEA is the 0A000 below.
		switch t {
		case parquet.TypeString, parquet.TypeBool, parquet.TypeInt32, parquet.TypeInt64,
			parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal, parquet.TypeIPv4,
			parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC, parquet.TypePort,
			parquet.TypeProtocol, parquet.TypeDuration, parquet.TypeUUID, parquet.TypeDate:
			return true
		}
		return false
	case aggArgOrdered:
		return t != parquet.TypeRow
	}
	return true
}

// aggArgTypeOf is the binder's TYPE of an argument node, for the class
// question alone: a column's declared TypeID whatever its parameters (a
// container or an unconstrained DECIMAL declines as a projection declaration,
// and is still certainly not text), anything else through the ordinary
// declaration.
func aggArgTypeOf(decls ColDecls) func(plansql.Node) (parquet.TypeID, bool) {
	return func(n plansql.Node) (parquet.TypeID, bool) {
		switch v := plansql.Unparen(n).(type) {
		case *plansql.ColRef:
			c, ok := decls.colDecl(v)
			return c.Type, ok
		case *plansql.CastNode:
			// A cast to a name this layer does not recognise is not TEXT
			// (structuralCastType).
			return structuralCastType(v.TypeName)
		}
		d, c := fieldContainerDeclaredType(n, decls)
		if c != expr.Decided || d.Untyped {
			return 0, false
		}
		return d.ID, true
	}
}

// refuseAggregateArgument applies the rule to one call.
func refuseAggregateArgument(fc *plansql.FuncCallNode, typeOf func(plansql.Node) (parquet.TypeID, bool)) error {
	if fc == nil || fc.Star {
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(fc.Name))
	positions, ok := aggArgPositions[name]
	if !ok {
		return nil
	}
	names := func() string {
		out := make([]string, len(fc.Args))
		for i, a := range fc.Args {
			out[i] = aggArgTypeName(a, typeOf)
		}
		return strings.Join(out, ", ")
	}
	for i, class := range positions {
		if i >= len(fc.Args) || class == aggArgAny {
			continue
		}
		arg := plansql.Unparen(fc.Args[i])
		if lit, ok := arg.(*plansql.Lit); ok && (lit.Kind == plansql.LitString || lit.Kind == plansql.LitNull) {
			switch {
			case aggOrderedSet[name]:
				return orderedSetRefusal(name)
			case aggUnknownIsNotUnique[name]:
				return sqlerr.New("42725", "function %s(%s) is not unique", name, names())
			case aggUnknownDoesNotExist[name]:
				return sqlerr.New("42883", "function %s(%s) does not exist", name, names())
			case class == aggArgBool && lit.Kind == plansql.LitString:
				// The one overload takes the literal as BOOLEAN INPUT, and
				// text that is no boolean is PostgreSQL's 22P02 — measured:
				// `bool_and('5')`. This engine answered `false`.
				if _, valid := plansql.ParseBoolText(lit.Value); !valid {
					return sqlerr.New("22P02", "invalid input syntax for type boolean: %q", lit.Value)
				}
			case class == aggArgNumeric && lit.Kind == plansql.LitString:
				// The statistical family resolves `unknown` to double
				// precision, so text that is no number is 22P02 there —
				// `stddev('t')`, measured. This engine answered NULL. (Text
				// that IS a number is answered by the server and NULL here;
				// that value defect is recorded in arc BR's notes.)
				if _, err := strconv.ParseFloat(strings.TrimSpace(lit.Value), 64); err != nil {
					return sqlerr.New("22P02", "invalid input syntax for type double precision: %q", lit.Value)
				}
			}
			continue
		}
		t, ok := typeOf(fc.Args[i])
		if !ok || aggArgFits(class, t) {
			continue
		}
		if name == "string_agg" && t == parquet.TypeBytes {
			// PostgreSQL has string_agg(bytea, bytea) and answers it; this
			// engine's accumulator renders each value with Go's fmt, so the
			// answer would be `[98 121 …]`. Loud, and named, where the server
			// answers — never that value.
			return sqlerr.New("0A000", "string_agg over bytea is not supported")
		}
		if aggOrderedSet[name] {
			return orderedSetRefusal(name)
		}
		return sqlerr.New("42883", "function %s(%s) does not exist", name, names())
	}
	return nil
}

// aggOrderedSet are PostgreSQL's ORDERED-SET aggregates, which this engine
// also takes in DuckDB's plain call form. Over a number the plain form answers
// (a kept extension, ADR-0012 §5); where it is refused the state is the one
// PostgreSQL gives the plain form, 42809 (measured on 17.11: `mode(c_str)`,
// `percentile_disc(0.5, c_str)`).
var aggOrderedSet = map[string]bool{"mode": true, "percentile_cont": true, "percentile_disc": true}

func orderedSetRefusal(name string) error {
	return sqlerr.New("42809", "WITHIN GROUP is required for ordered-set aggregate %s", name)
}

// aggArgTypeName renders one argument the way PostgreSQL's 42883 message does:
// `unknown` for a quoted or NULL literal and anything undecided, the type's
// own PostgreSQL name otherwise.
func aggArgTypeName(arg plansql.Node, typeOf func(plansql.Node) (parquet.TypeID, bool)) string {
	if lit, ok := plansql.Unparen(arg).(*plansql.Lit); ok {
		switch lit.Kind {
		case plansql.LitString, plansql.LitNull:
			return "unknown"
		case plansql.LitNumber:
			if strings.ContainsAny(lit.Value, ".eE") {
				return "numeric"
			}
			return "integer"
		}
	}
	t, ok := typeOf(arg)
	if !ok {
		return "unknown"
	}
	return pgTypeName(t)
}

// structuralCastType is a CAST's target class, or false for a name this layer
// does not recognise. inferCastType answers STRING for every name it does not
// know — `DECIMAL(9,2)`, `INET`, `INTERVAL` — which is a projection's safe
// fallback and a false TEXT here, so the text names are matched explicitly and
// an unknown name decides nothing.
func structuralCastType(typeName string) (parquet.TypeID, bool) {
	base := strings.ToUpper(strings.TrimSpace(typeName))
	if strings.Contains(base, "[") {
		return 0, false
	}
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	switch base {
	case "TEXT", "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING", "STRING", "BPCHAR", "NAME":
		return parquet.TypeString, true
	case "DECIMAL", "NUMERIC":
		return parquet.TypeDecimal, true
	}
	if t := inferCastType(typeName); t != parquet.TypeString {
		return t, true
	}
	return 0, false
}
