package expr

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// CAST(<x> AS BOOLEAN).
//
// Before this existed, `Cast.Eval`'s switch had no boolean arm at all, so the
// destination type was silently DROPPED and the operand came back unconverted.
// Two consumers then read that unconverted value by two different rules and
// answered two different things about the same expression (#592):
//
//   - the PROJECTION allocated a BOOL output vector (inferCastType maps
//     BOOLEAN to batch.TypeBool) and wrote the raw box through
//     Vector.SetValue, whose TypeBool arm coerces an int64/int32/float64 to
//     `!= 0` — so `SELECT (c)::BOOLEAN` looked correct, by accident, and a
//     STRING operand hit the #361 silent-write guard and failed the query;
//   - the FILTER asked `v.(bool)` and took the failed assertion for FALSE, so
//     `WHERE (c)::BOOLEAN` excluded EVERY row while `WHERE NOT (c)::BOOLEAN`
//     and `WHERE (c)::BOOLEAN IS NULL` were right — those two go through
//     `evalBoolNull`, whose `toBoolVal` DOES read an integer's truthiness.
//
// One expression with three readings is the two-path defect class, and it is
// worse than an ordinary wrong answer here: TLP-WHERE's partition
// (`p` UNION ALL `NOT p` UNION ALL `p IS NULL`) is exactly these three
// readings, so the `p` arm contributed nothing and the partition permanently
// undercounted.
//
// What the cast now answers, per ADR-0012 item 1 and item 5's new entry:
//
//	BOOL              itself
//	INT32 / INT64     0 is FALSE, every other value TRUE
//	STRING            PostgreSQL's own boolean input function (parseBoolText),
//	                  and 22P02 for a string that names no boolean
//	NULL              NULL, whatever the source type
//	anything else     42846 cannot_coerce, the error PostgreSQL raises
//
// The rule is selected from the operand's DECLARATION, never from the Go box
// a row happens to produce, because the box cannot tell the cases apart:
// ADR-0012 item 8's boxed-value rule. A DECIMAL column and a STRING column
// both box as a Go string, and PostgreSQL answers them differently (42846
// against its boolean input function) — reading the box would give
// `DECIMAL(9,0)` holding 1 the answer TRUE where PostgreSQL refuses the cast
// outright. DATE, IPv4 and MAC box as their raw integer encodings and would
// have taken the integer arm for the same reason.

// castBoolRule is the conversion one operand DECLARATION selects.
type castBoolRule int8

const (
	// castBoolByBox: nothing here declares a type, so the value's own box
	// decides. Reached for the operand shapes castBoolDeclared does not
	// resolve — arithmetic, a scalar subquery, an ordinary function call.
	// None of those produce a DECIMAL in this engine (no registered scalar
	// function declares one — expr.Ret has no DECIMAL constant), which is
	// why the box is safe where a column's would not be.
	castBoolByBox castBoolRule = iota
	castBoolIdentity
	castBoolFromInt
	castBoolFromText
	castBoolRefuse
)

// castToBool converts one already-non-nil operand value under the rule its
// declaration selects.
func (e *Cast) castToBool(b *batch.RecordBatch, v any) any {
	typ, declared := e.boolSourceType(b)
	if declared {
		switch castBoolRuleFor(typ) {
		case castBoolIdentity:
			if bv, ok := v.(bool); ok {
				return bv
			}
		case castBoolFromInt:
			if i, ok := toInt64Safe(v); ok {
				return i != 0
			}
		case castBoolFromText:
			if s, ok := stringOperand(v); ok {
				return boolFromText(s)
			}
		case castBoolRefuse:
			raiseCannotCastToBoolean(pgCastSourceName(typ))
		}
		// The declaration and the box disagree about this row's shape. Fall
		// through rather than guess: the box rules below answer the same way
		// for every shape they share with the declaration, and refuse the
		// rest.
	}
	switch tv := v.(type) {
	case bool:
		return tv
	case int64:
		return tv != 0
	case int32:
		return tv != 0
	case int:
		return tv != 0
	case string:
		return boolFromText(tv)
	case float64, float32:
		raiseCannotCastToBoolean("double precision")
	case []byte:
		raiseCannotCastToBoolean("bytea")
	}
	raiseCannotCastToBoolean(fmt.Sprintf("%T", v))
	return nil
}

// boolFromText is PostgreSQL's boolean input function as a value conversion:
// a string that names no boolean is SQLSTATE 22P02, never a value. That is
// #463's rule for DECIMAL one type family over — a match-nothing answer to
// `WHERE s::BOOLEAN` over a column holding "maybe" deletes rows silently.
func boolFromText(s string) bool {
	v, ok := parseBoolText(s)
	if !ok {
		raiseInvalidTextRepresentation("boolean", s)
	}
	return v
}

// parseBoolText is `parse_bool_with_len` (src/backend/utils/adt/bool.c),
// which is what PostgreSQL's `text::boolean` runs.
//
// It accepts, case-insensitively and after trimming C `isspace` whitespace,
// any non-empty PREFIX of "true", "false", "yes" or "no", plus "on"/"off"
// and the single characters "1" and "0". The prefix rule is not decoration:
// `'tr'::boolean` and `'fals'::boolean` answer on live PostgreSQL 17, and a
// stricter reader would raise 22P02 for values PostgreSQL accepts.
//
// "o" alone is the one prefix REFUSED, because it cannot choose between "on"
// and "off" — PostgreSQL spells that as comparing at least two characters,
// and `SELECT 'o'::boolean` is an error there.
func parseBoolText(s string) (val, ok bool) {
	t := strings.Trim(s, " \t\n\v\f\r")
	if t == "" {
		return false, false
	}
	switch t[0] {
	case 't', 'T':
		if isPrefixFold(t, "true") {
			return true, true
		}
	case 'f', 'F':
		if isPrefixFold(t, "false") {
			return false, true
		}
	case 'y', 'Y':
		if isPrefixFold(t, "yes") {
			return true, true
		}
	case 'n', 'N':
		if isPrefixFold(t, "no") {
			return false, true
		}
	case 'o', 'O':
		if len(t) < 2 {
			return false, false
		}
		if isPrefixFold(t, "on") {
			return true, true
		}
		if isPrefixFold(t, "off") {
			return false, true
		}
	case '1':
		if len(t) == 1 {
			return true, true
		}
	case '0':
		if len(t) == 1 {
			return false, true
		}
	}
	return false, false
}

// isPrefixFold reports whether s is a case-insensitive prefix of word.
func isPrefixFold(s, word string) bool {
	return len(s) <= len(word) && strings.EqualFold(s, word[:len(s)])
}

// castBoolRuleFor is the rule table, keyed by the operand's declared type.
func castBoolRuleFor(t batch.TypeID) castBoolRule {
	switch t {
	case batch.TypeBool:
		return castBoolIdentity
	case batch.TypeInt32, batch.TypeInt64:
		return castBoolFromInt
	case batch.TypeString:
		return castBoolFromText
	}
	return castBoolRefuse
}

// raiseCannotCastToBoolean aborts the query with SQLSTATE 42846,
// PostgreSQL's cannot_coerce, in PostgreSQL's own wording.
func raiseCannotCastToBoolean(from string) {
	panic(fatalEval{sqlerr.New("42846", "cannot cast type %s to boolean", from)})
}

// pgCastSourceName names the source type the way PostgreSQL's error would,
// falling back to wadjet's own spelling for the types PostgreSQL does not
// have. IPv4/IPv6/CIDR answer "inet" and MAC "macaddr" because those are the
// PostgreSQL types whose semantics ADR-0012 item 10 adopted for them.
func pgCastSourceName(t batch.TypeID) string {
	switch t {
	case batch.TypeBool:
		return "boolean"
	case batch.TypeInt32:
		return "integer"
	case batch.TypeInt64:
		return "bigint"
	case batch.TypeFloat32:
		return "real"
	case batch.TypeFloat64:
		return "double precision"
	case batch.TypeDecimal:
		return "numeric"
	case batch.TypeString:
		return "text"
	case batch.TypeBytes:
		return "bytea"
	case batch.TypeTimestamp:
		return "timestamp without time zone"
	case batch.TypeDate:
		return "date"
	case batch.TypeUUID:
		return "uuid"
	case batch.TypeIPv4, batch.TypeIPv6, batch.TypeCIDR:
		return "inet"
	case batch.TypeMAC:
		return "macaddr"
	case batch.TypePort:
		return "port"
	case batch.TypeProtocol:
		return "protocol"
	case batch.TypeDuration:
		return "duration"
	case batch.TypeArray:
		return "array"
	case batch.TypeRow:
		return "record"
	case batch.TypeMap:
		return "map"
	case batch.TypeVector:
		return "vector"
	}
	return "unknown"
}

// boolSourceType resolves the operand's DECLARED type once and caches it.
//
// The atomic is the publish boxOperand.kind uses, for the same reason: these
// nodes are shared across the parallel pipeline workers evaluating one batch,
// and concurrent writers can only ever agree, because the answer is a pure
// function of the operand's fixed declaration.
func (e *Cast) boolSourceType(b *batch.RecordBatch) (batch.TypeID, bool) {
	switch v := e.boolSrc.Load(); {
	case v == castBoolSrcNone:
		return 0, false
	case v != castBoolSrcUnset:
		return batch.TypeID(v - castBoolSrcBias), true
	}
	typ, have, settled := castBoolDeclared(e.Operand, b)
	if settled {
		if have {
			e.boolSrc.Store(int32(typ) + castBoolSrcBias)
		} else {
			e.boolSrc.Store(castBoolSrcNone)
		}
	}
	return typ, have
}

// The three states Cast.boolSrc encodes. A resolved type is stored biased so
// that the zero value can mean "not resolved yet" — batch.TypeID's own zero is
// TypeBool, a perfectly ordinary answer.
const (
	castBoolSrcUnset int32 = 0
	castBoolSrcNone  int32 = 1
	castBoolSrcBias  int32 = 2
)

// castBoolDeclared reports the type an operand's DECLARATION says its values
// have, and whether that answer is SETTLED — safe to cache for the rest of the
// query. Unsettled means "this batch cannot answer": a column name that
// resolves in no batch yet says nothing about the next one.
//
// It mirrors classifyOperand (boxed_pair.go), which answers the same question
// at a coarser grain — that one only has to separate DECIMAL from text from
// number, while a cast to BOOLEAN has to separate INTEGER from FLOAT, which
// share classifyOperand's boxNumber.
func castBoolDeclared(e Expr, b *batch.RecordBatch) (typ batch.TypeID, have, settled bool) {
	switch v := e.(type) {
	case *ColRef:
		if v.structField != "" {
			// A ROW field access reads a boxed value out of a container; the
			// container's declaration does not type the field here.
			return 0, false, true
		}
		v.resolve(b)
		if v.idx < 0 || v.idx >= len(b.Columns) {
			return 0, false, false
		}
		return v.typ, true, true
	case *Lit:
		switch v.Val.(type) {
		case bool:
			return batch.TypeBool, true, true
		case int64, int32, int:
			return batch.TypeInt64, true, true
		case float64, float32:
			// PostgreSQL types an unsuffixed decimal literal as `numeric`
			// (ADR-0012 item 6), and `numeric::boolean` is the 42846 this
			// reports — not `double precision::boolean`, which is a
			// different message for the same refusal.
			return batch.TypeDecimal, true, true
		case string:
			// An unknown-typed literal takes the type its context demands,
			// which here is the boolean input function — `'t'::BOOLEAN`.
			return batch.TypeString, true, true
		}
		return 0, false, true
	case *Cast:
		if t, ok := castDestType(v.DestType); ok {
			return t, true, true
		}
		return 0, false, true
	case *Coalesce:
		return joinCastBoolDeclared(v.Args, b)
	case *Case:
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return joinCastBoolDeclared(arms, b)
	case *FuncCall:
		// GREATEST/LEAST answer WITH one of their arguments; every other
		// function declares its own result type, and none of those
		// declarations is a DECIMAL, so the box is a safe reading for them.
		switch strings.ToLower(v.Name) {
		case "greatest", "least":
			return joinCastBoolDeclared(v.Args, b)
		}
	}
	return 0, false, true
}

// joinCastBoolDeclared is castBoolDeclared over a set of alternatives that one
// value is chosen from. They must AGREE for the join to decide anything; a
// NULL alternative is skipped, because `COALESCE(c, NULL)` is as much an
// integer expression as `c` is (the same rule joinOperandKinds states).
func joinCastBoolDeclared(args []Expr, b *batch.RecordBatch) (typ batch.TypeID, have, settled bool) {
	settled = true
	for _, a := range args {
		if a == nil {
			continue
		}
		if lit, ok := a.(*Lit); ok && lit.Val == nil {
			continue
		}
		t, h, s := castBoolDeclared(a, b)
		settled = settled && s
		if !h {
			return 0, false, settled
		}
		if !have {
			typ, have = t, true
			continue
		}
		if t != typ {
			return 0, false, settled
		}
	}
	return typ, have, settled
}

// castDestType maps a nested CAST's destination spelling to the type its
// values will have.
//
// It is expr's own reading of the same names physical.inferCastType maps, and
// the two are separate on purpose because they answer different questions:
// that one types the OUTPUT COLUMN a projection allocates, so a DECIMAL
// destination is FLOAT64 there (the cast evaluator produces a float64); this
// one types the VALUES an enclosing cast will read, so the same destination is
// DECIMAL here, which is the type the 42846 message has to name.
//
// The one answer they MUST share is BOOLEAN — if the projection allocated a
// vector of another type for a cast this file converts to a bool, SetValue
// would refuse the write. `TestCastDestTypeBooleanSpellings` (here) and
// `TestInferCastTypeBooleanSpellings` (physical) hold each side to it.
func castDestType(dest string) (batch.TypeID, bool) {
	switch strings.ToUpper(strings.TrimSpace(dest)) {
	case "BOOLEAN", "BOOL":
		return batch.TypeBool, true
	case "INTEGER", "INT", "BIGINT", "INT64", "SIGNED":
		return batch.TypeInt64, true
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "FLOAT64":
		return batch.TypeFloat64, true
	case "NUMERIC", "DECIMAL":
		return batch.TypeDecimal, true
	case "CHAR", "VARCHAR", "TEXT", "STRING":
		return batch.TypeString, true
	case "DATE":
		return batch.TypeDate, true
	case "TIMESTAMP", "DATETIME", "TIMESTAMPTZ":
		return batch.TypeTimestamp, true
	}
	return 0, false
}
