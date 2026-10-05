// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Correlated reruns substitute typed literals that this parser reads back
// with the outer value's own meaning (#679).
// Use CAST for DECIMAL/DATE/TIMESTAMP/REAL, numeric spelling for numeric
// values and quoted text for network/UUID values; the round-trip gate is
// TestOuterLiteralRendersEveryTypeAsItsOwnType.
// An ARRAY is its typed array literal (ArrayValueLiteral), BYTES its typed
// hex value (BytesValueLiteral). Refuse ROW/MAP/VECTOR and an ARRAY whose
// element has no exact cast spelling: this substitution path has no faithful
// literal spelling for them.
// Every CAST spelling here is COLUMN-TYPED (plansql.CastNode.Column): the
// literal is the value of an outer column of that type, and the re-run types
// it as that column — an int4's width and DECIMAL(10,0), a NUMERIC(10,2)'s
// (p,s), an int4[]'s element — not by the rules a CAST expression has.
// NULL renders as a NULL of the column's type where the value is a CAST or a
// typed literal (outerNull), and as the bare null where it is a quoted string;
// comparisons remain UNKNOWN.
// See docs/internals/correlated-outer-literal-roundtrip.md for the design.

// outerLiteral renders one outer-row value as the literal node the re-run's
// SQL carries in place of the correlated column reference.
func outerLiteral(v *batch.Vector, row int) (plansql.Node, error) {
	return outerColumnLiteral(v, 0, row)
}

// outerColumnLiteral is outerLiteral for a column whose declared DECIMAL
// precision is known (0 = not carried: the Int128 carrier's 38, as a column
// reference reads it — colRefDecimalType).
func outerColumnLiteral(v *batch.Vector, precision, row int) (plansql.Node, error) {
	val := v.GetValue(row)
	if val == nil {
		return outerNull(v, precision), nil
	}
	num := func(s string) plansql.Node { return &plansql.Lit{Value: s, Kind: plansql.LitNumber} }
	str := func(s string) plansql.Node { return &plansql.Lit{Value: s, Kind: plansql.LitString} }
	cast := func(s, typeName string) plansql.Node {
		return &plansql.CastNode{Inner: str(s), TypeName: typeName, Column: true}
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
		var text string
		switch n := val.(type) {
		case int32:
			text = strconv.FormatInt(int64(n), 10)
		case int64:
			text = strconv.FormatInt(n, 10)
		case int:
			text = strconv.FormatInt(int64(n), 10)
		default:
			return nil, unrenderableOuterValue(v.Type, val)
		}
		// An INTEGER or a BIGINT is a column-typed value of its type: a bare
		// literal's type is read off its digits, and in DECIMAL arithmetic
		// that is a DECIMAL(1,0) for a 3 where the int4 column is
		// DECIMAL(10,0) — so `x.m / o.i` re-ran as `x.m / 3` at a six-digit
		// scale while the subquery was declared at the column's thirteen
		// (0.4166670000000). It is the stand-in the declaration is made from
		// (outerStandIn). PORT and PROTOCOL box as int32 and DURATION as
		// int64; no cast spelling names those types here, so they stay the
		// bare literal.
		switch v.Type {
		case batch.TypeInt32:
			return &plansql.CastNode{Inner: num(text), TypeName: "integer", Column: true}, nil
		case batch.TypeInt64:
			return &plansql.CastNode{Inner: num(text), TypeName: "bigint", Column: true}, nil
		}
		return num(text), nil

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
		// The column's DECIMAL(p, s): the SCALE decides the comparison's
		// exactness, and the PRECISION the (p,s) arithmetic over the value
		// takes, as the column's own does (`x.v / o.n` divides at the scale
		// `v / n` has). The bare spelling would make this a float8 literal
		// and compare the column's exact digits against a double.
		return cast(s, outerDecimalName(precision, outerDecimalScale(v))), nil

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
		// The typed BYTES value (BytesValueLiteral), column-typed like every
		// other CAST here. The raw bytes as a quoted literal were read by
		// byteain a second time beside a BYTES operand, so an outer value
		// holding `\x41` matched the row holding `A` and one holding two
		// backslashes matched nothing (#1501); a NUL or invalid UTF-8 had no
		// spelling at all and refused the query.
		raw, ok := val.([]byte)
		if !ok {
			return nil, unrenderableOuterValue(v.Type, val)
		}
		lit := BytesValueLiteral(raw)
		lit.Column = true
		return lit, nil

	case batch.TypeArray:
		// An ARRAY outer value is its typed array literal (arc CW,
		// review P2): `CAST('{1,2}' AS BIGINT[])`, the spelling the DAG's
		// scalar-subquery substitution already writes (ArrayValueLiteral), so
		// `(SELECT count(*) FROM ca c2 WHERE c2.ai = ca.ai)` over a stored
		// array column is a plain array comparison per outer row — it
		// refused here once CREATE TABLE AS stored a real array instead of
		// its text. The vector's own declaration names the element; an
		// element with no exact cast spelling keeps the refusal below.
		base := v
		for base.Base != nil {
			base = base.Base
		}
		if n, ok := ArrayValueLiteral(val, batch.VectorDecl("", base)); ok {
			if c, ok := n.(*plansql.CastNode); ok {
				// A one-dimensional value is a value of its array column.
				return &plansql.CastNode{Inner: c.Inner, TypeName: c.TypeName, Column: true}, nil
			}
			return n, nil
		}
	}

	// ROW, MAP, VECTOR (and an ARRAY whose element has no exact cast
	// spelling): no literal spelling at all.
	return nil, unrenderableOuterValue(v.Type, val)
}

// outerNull is a NULL outer value's spelling: a NULL of the column's type
// wherever the value's own spelling is a CAST or a typed literal (an
// integer, a float, a DECIMAL, a DATE, a TIMESTAMP, a boolean, an ARRAY) —
// outerStandIn's spelling, so a NULL row re-runs the expression the
// subquery was declared from. The bare `null` is SQL's unknown, and it typed
// the expression around it by the other operand: `sum(x.v + null)` over an
// INTEGER outer column answered the sum of x.v where PostgreSQL answers NULL,
// and `max(null + x.v)` under a DATE declaration answered a number. A type
// whose value is spelled as a quoted string (text, UUID, the network types,
// bytes) keeps the bare `null`, the twin of that unknown-typed literal.
func outerNull(v *batch.Vector, precision int) plansql.Node {
	untyped := &plansql.Lit{Value: "null", Kind: plansql.LitNull}
	base := v
	for base.Base != nil {
		base = base.Base
	}
	col := batch.VectorDecl("", base)
	if col.Type == batch.TypeDecimal && col.Precision <= 0 {
		col.Precision = precision
	}
	switch col.Type {
	case batch.TypeBool:
		return &plansql.CastNode{Inner: untyped, TypeName: "boolean", Column: true}
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat64, batch.TypeFloat32, batch.TypeDecimal,
		batch.TypeDate, batch.TypeTimestamp, batch.TypeArray:
		if n, ok := outerStandIn(col); ok {
			return n
		}
	}
	return untyped
}

// outerDecimalName is a DECIMAL column's cast spelling: its declared
// precision, or the Int128 carrier's 38 where the declaration carries none —
// the width a column reference reads it at then (colRefDecimalType).
func outerDecimalName(precision, scale int) string {
	if precision <= 0 {
		precision = batch.MaxDecimalPrecision
	}
	return fmt.Sprintf("decimal(%d, %d)", precision, scale)
}

// outerDecimalScale is the column's DECIMAL scale, resolved through a view —
// a view carries Type but no typed storage, so its scale lives on Base.
func outerDecimalScale(v *batch.Vector) int {
	for v.Base != nil {
		v = v.Base
	}
	return v.DecimalData.Scale
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

// OuterDeclFunc answers the declared column an outer reference reads.
type OuterDeclFunc func(ref plansql.OuterRef) (parquet.Column, bool)

// OuterTypedSubquerySQL is a correlated subquery's text with every OUTER
// reference spelled as a column-typed NULL of the outer column's DECLARED
// type — `__column_value(cast(null as double precision))` for a DOUBLE column
// (outerStandIn) — for the planner to DECLARE the subquery from. It is the per-row re-run's spelling
// (outerLiteral) with the value left out: the re-run substitutes a typed
// literal, so the text the subquery is declared from must type its outer
// operands the same way, or the declaration and the value describe two
// different expressions. Planned with its outer names left in, `(SELECT c.f
// + x.v …)` resolved `c.f` to nothing and the sum was declared by the
// operand it could read — INT32 — so 6.5 was written into an integer vector
// as 6 (#1422).
//
// ok=false — declare from sql as written — when the text has no outer
// reference, when a reference's declaration is unknown, or when its type has
// no exact cast spelling here (outerCastName).
func OuterTypedSubquerySQL(sql string, outerTables map[string]bool, outerCols map[string]string,
	innerCols plansql.TableColumns, outerDecl OuterDeclFunc) (string, bool) {
	if outerDecl == nil || len(outerTables) == 0 {
		return "", false
	}
	var refs []plansql.OuterRef
	var err error
	if len(outerCols) > 0 {
		refs, err = plansql.FindCorrelatedRefsWithScope(sql, outerTables, outerCols, innerCols)
	} else {
		refs, err = plansql.FindCorrelatedRefs(sql, outerTables)
	}
	if err != nil || len(refs) == 0 {
		return "", false
	}
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return "", false
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return "", false
	}
	return outerTypedSQL(info, refs, outerTables, buildUnqualOuterCols(refs, outerCols), outerDecl)
}

// outerTypedSQL is OuterTypedSubquerySQL over an already-parsed subquery,
// substituted exactly as rerunSQL substitutes the per-row values.
func outerTypedSQL(info *plansql.SelectInfo, refs []plansql.OuterRef, outerTables map[string]bool,
	unqual map[string]string, outerDecl OuterDeclFunc) (string, bool) {
	if info == nil || outerDecl == nil || len(refs) == 0 {
		return "", false
	}
	vals := make(map[string]any, len(refs))
	for _, ref := range refs {
		col, ok := outerDecl(ref)
		if !ok {
			return "", false
		}
		stand, ok := outerStandIn(col)
		if !ok {
			return "", false
		}
		vals[ref.Table+"."+ref.Column] = stand
	}
	rewrite := func(n plansql.Node) plansql.Node {
		out := plansql.RewriteOuterRefs(n, outerTables, vals)
		if len(unqual) > 0 {
			out = plansql.RewriteUnqualifiedOuterRefs(out, unqual, vals)
		}
		return out
	}
	return plansql.RebuildSQLForRerun(info, rewrite)
}

// outerStandIn is the value-free twin of outerLiteral's spelling for a column
// of this declared type, so the subquery is declared from the expression the
// re-run evaluates: a column-typed NULL of the column's type
// (plansql.CastNode.Column) — typed as the outer COLUMN is, its int4 width,
// its DECIMAL (p,s), its array element, by every walk that types a column
// reference — a boolean its bare literal. ok=false for a type the re-run has
// no spelling for, or that this does not name (the declaration is then made
// from the text as written).
func outerStandIn(col parquet.Column) (plansql.Node, bool) {
	cast := func(name string) plansql.Node {
		return &plansql.CastNode{Inner: &plansql.Lit{Value: "null", Kind: plansql.LitNull}, TypeName: name, Column: true}
	}
	switch col.Type {
	case batch.TypeBool:
		return &plansql.Lit{Value: "false", Kind: plansql.LitBool}, true
	case batch.TypeInt32:
		return cast("integer"), true
	case batch.TypeInt64:
		return cast("bigint"), true
	case batch.TypeFloat64:
		return cast("double precision"), true
	case batch.TypeFloat32:
		return cast("real"), true
	case batch.TypeDecimal:
		return cast(outerDecimalName(col.Precision, col.Scale)), true
	case batch.TypeString:
		return cast("text"), true
	case batch.TypeDate:
		return cast("date"), true
	case batch.TypeTimestamp:
		return cast("timestamp"), true
	case batch.TypeUUID:
		return cast("uuid"), true
	case batch.TypeArray:
		// One dimension of a scalar element: ArrayValueLiteral's own cast.
		if col.ElementType != nil && col.ElementType.Type != batch.TypeArray {
			if el, ok := arrayElementCastName(col.ElementType); ok {
				return cast(el + "[]"), true
			}
		}
	}
	return nil, false
}

// ColumnOfCastName is the column a column-typed cast's type name declares
// (plansql.CastNode.Column): the spellings outerLiteral and outerStandIn
// write, an INTEGER an int4 column, a DECIMAL(p, s) that (p,s), an `el[]` /
// ARRAY(el) the array of that element. ok=false for any other name, and for a
// bare DECIMAL (no (p,s) to be a column of).
func ColumnOfCastName(name string) (parquet.Column, bool) {
	t := strings.ToLower(strings.TrimSpace(name))
	elem, isArray := ArrayCastElement(t)
	if !isArray && strings.HasSuffix(t, "[]") {
		elem, isArray = strings.TrimSuffix(t, "[]"), true
	}
	if isArray {
		el, ok := ColumnOfCastName(elem)
		if !ok || el.Type == parquet.TypeArray {
			return parquet.Column{}, false
		}
		el.Nullable = true
		return parquet.Column{Type: parquet.TypeArray, Nullable: true, ElementType: &el}, true
	}
	if p, sc, hasParams, ok := DecimalCastDest(t); ok {
		if !hasParams {
			return parquet.Column{}, false
		}
		return parquet.Column{Type: parquet.TypeDecimal, Nullable: true, Precision: p, Scale: sc}, true
	}
	var typ parquet.TypeID
	switch t {
	case "integer", "int", "int4":
		typ = parquet.TypeInt32
	case "bigint", "int8":
		typ = parquet.TypeInt64
	case "double precision", "double", "float8":
		typ = parquet.TypeFloat64
	case "real", "float4":
		typ = parquet.TypeFloat32
	case "text":
		typ = parquet.TypeString
	case "boolean", "bool":
		typ = parquet.TypeBool
	case "date":
		typ = parquet.TypeDate
	case "timestamp":
		typ = parquet.TypeTimestamp
	case "uuid":
		typ = parquet.TypeUUID
	case "ipv4":
		typ = parquet.TypeIPv4
	default:
		return parquet.Column{}, false
	}
	return parquet.Column{Type: typ, Nullable: true}, true
}
