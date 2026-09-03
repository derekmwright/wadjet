package expr

import (
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The re-run's outer values are rendered TYPED.
//
// A correlated subquery this engine cannot express as a join is re-executed
// per outer row, and the outer row's values reach the re-run as LITERAL TEXT
// substituted into the subquery's WHERE. What that text MEANS is decided by
// the literal's own spelling, so a value whose Go box has lost its wadjet
// type is re-typed by whatever the box happens to look like:
// `batch.Vector.GetValue` hands a DECIMAL back as its rendered TEXT
// (vector.go, `case TypeDecimal`), and the old renderer's `default:` arm
// wrapped anything it did not recognize in quotes. `a.w_d2 = b.k` against a
// BIGINT inner therefore became `'2.00' = b.k` and raised 22P02 — a query
// PostgreSQL answers with 3 rows (#679). DATE, TIMESTAMP, the six
// network-native types, UUID and BYTES reached the same arm.
//
// Two rules, and the second is the one that keeps this honest:
//
//  1. Every value is rendered as a literal THIS engine's own parser reads
//     back as the SAME value at the SAME type — a CAST where the bare
//     spelling would re-type it (DECIMAL, DATE, TIMESTAMP, REAL), a bare
//     numeric where the type is already the literal's, a quoted string where
//     the type resolves a string operand (the network types, UUID).
//     `TestOuterLiteralRoundTripsEveryType` compares each rendering against
//     the value read straight out of the column.
//  2. A type with NO literal spelling in this dialect is a REFUSAL, not a
//     guess. ARRAY, ROW, MAP and VECTOR have no literal at all, and a BYTES
//     value that is not valid UTF-8 (or holds a NUL) has none either: the
//     only bytea spelling the parser accepts is a quoted string, and the
//     bytes that do not survive that round trip would come back as different
//     bytes. Rendering them anyway is exactly the trade — a plausible wrong
//     answer for a loud one — that protocol item 8 forbids.
//
// A NULL is `null` for every type: it is the value the outer row holds, and
// every comparison over it is UNKNOWN, which is what PostgreSQL answers.

// outerLiteral renders one outer-row value as the literal node the re-run's
// SQL carries in place of the correlated column reference.
func outerLiteral(v *batch.Vector, row int) (plansql.Node, error) {
	val := v.GetValue(row)
	if val == nil {
		return &plansql.Lit{Value: "null", Kind: plansql.LitNull}, nil
	}
	num := func(s string) plansql.Node { return &plansql.Lit{Value: s, Kind: plansql.LitNumber} }
	str := func(s string) plansql.Node { return &plansql.Lit{Value: s, Kind: plansql.LitString} }
	cast := func(s, typeName string) plansql.Node {
		return &plansql.CastNode{Inner: str(s), TypeName: typeName}
	}

	switch v.Type {
	case batch.TypeBool:
		b, ok := val.(bool)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		if b {
			return &plansql.Lit{Value: "true", Kind: plansql.LitBool}, nil
		}
		return &plansql.Lit{Value: "false", Kind: plansql.LitBool}, nil

	case batch.TypeInt32, batch.TypeInt64, batch.TypePort, batch.TypeProtocol, batch.TypeDuration:
		// PORT and PROTOCOL box as int32 and DURATION as int64, and an
		// integer literal IS their type — no cast can narrow or widen the
		// comparison away from where an equality already lands.
		switch n := val.(type) {
		case int32:
			return num(strconv.FormatInt(int64(n), 10)), nil
		case int64:
			return num(strconv.FormatInt(n, 10)), nil
		case int:
			return num(strconv.FormatInt(int64(n), 10)), nil
		}
		return nil, unrenderableOuterValue(v.Type, val)

	case batch.TypeFloat64:
		f, ok := val.(float64)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			// The same rule the IN-list materialization applies (ADR-0021
			// §2): this dialect has no numeric literal for either, so there
			// is nothing to substitute.
			return nil, unrenderableOuterValue(v.Type, val)
		}
		return cast(strconv.FormatFloat(f, 'g', -1, 64), "double precision"), nil

	case batch.TypeFloat32:
		f, ok := val.(float32)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		// The CAST is load-bearing, not decoration. A bare numeric literal
		// is float8 (ADR-0024's literal-typing rule and PostgreSQL's), so
		// `c_f32 = 0.14285715` compares the column's float4 WIDENED against
		// a float8 that is a different number — measured 0 rows here for the
		// 1 the same row's own value gives (#631's rule, reached through the
		// re-run).
		return cast(strconv.FormatFloat(float64(f), 'g', -1, 32), "real"), nil

	case batch.TypeString:
		s, ok := val.(string)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		return str(s), nil

	case batch.TypeDecimal:
		s, ok := val.(string)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		// DECIMAL(38, scale): 38 is the Int128 carrier's own width, so it
		// never narrows a value the column could hold, and the SCALE is the
		// column's — which is what decides the comparison's exactness. The
		// bare spelling would make this a float8 literal and compare the
		// column's exact digits against a double.
		return cast(s, fmt.Sprintf("decimal(38, %d)", outerDecimalScale(v))), nil

	case batch.TypeTimestamp:
		ms, ok := val.(int64)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		return cast(batch.FormatTimestamp(ms), "timestamp"), nil

	case batch.TypeDate:
		s, ok := val.(string)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		return cast(s, "date"), nil

	case batch.TypeIPv4, batch.TypeIPv6, batch.TypeCIDR, batch.TypeMAC, batch.TypeUUID:
		// These box as their canonical TEXT and the comparison kernels
		// resolve a string operand against the column's declared network
		// type — the same path a hand-written `c_ipv4 = '10.0.0.1'` takes.
		s, ok := val.(string)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		return str(s), nil

	case batch.TypeBytes:
		raw, ok := val.([]byte)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		if !bytesHaveALiteral(raw) {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		return str(string(raw)), nil
	}

	// ARRAY, ROW, MAP, VECTOR: no literal spelling at all.
	return nil, unrenderableOuterValue(v.Type, val)
}

// outerDecimalScale is the column's DECIMAL scale, resolved through a view —
// a view carries Type but no typed storage, so its scale lives on Base.
func outerDecimalScale(v *batch.Vector) int {
	for v.Base != nil {
		v = v.Base
	}
	return v.DecimalData.Scale
}

// bytesHaveALiteral reports whether these bytes survive the round trip
// through the only bytea spelling this dialect has, a quoted string.
//
// A NUL cannot travel through the wire's text format at all (#570), and
// invalid UTF-8 does not survive the parser's string handling. Both come back
// as DIFFERENT bytes, which is a wrong answer rather than a failure — so the
// renderer refuses instead.
func bytesHaveALiteral(raw []byte) bool {
	for _, b := range raw {
		if b == 0 {
			return false
		}
	}
	return utf8.Valid(raw)
}

// UnrenderableOuterValueError reports an outer-row value with no literal
// spelling this engine's parser reads back as the same value.
//
// It is fatal, and deliberately: the alternative is substituting a literal
// that means something else, which turns a query this engine cannot run into
// one that answers the wrong number.
type UnrenderableOuterValueError struct {
	Type batch.TypeID
}

func (e *UnrenderableOuterValueError) Error() string {
	return fmt.Sprintf("a correlated subquery this engine cannot express as a join re-runs per "+
		"outer row with the outer values substituted as literals, and a %s value has no literal "+
		"spelling that reads back as the same value; rewrite the correlation as a join",
		e.Type)
}

// SQLState is PostgreSQL's feature_not_supported: the query is legal SQL this
// engine has no lowering for, which is what 0A000 says.
func (e *UnrenderableOuterValueError) SQLState() string { return "0A000" }

// FatalEvalError satisfies the marker the pipeline drivers recover on.
func (e *UnrenderableOuterValueError) FatalEvalError() error { return e }

func unrenderableOuterValue(t batch.TypeID, _ any) error {
	return &UnrenderableOuterValueError{Type: t}
}
