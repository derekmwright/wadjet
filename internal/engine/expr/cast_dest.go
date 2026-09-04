package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A CAST to a type name this engine does not have is 42704, not a STRING
// column over the operand (#652).
//
// `CAST(1 AS bogustype)` answered the string "1" under DataTypeOID 25, and
// `CAST(c_f64 AS bogustype)` answered "0.3333333333333333" the same way:
// Cast.Eval's switch fell to `default: return v` and physical.inferCastType's
// fell to `default: return TypeString`, so the two layers agreed with each
// other about a column PostgreSQL says cannot be described at all — the
// #310/#443 shape, a numeric VALUE published under a STRING declaration.
// PostgreSQL 17.11 raises `type "bogustype" does not exist` at parse time.
//
// The accept-set is deliberately the UNION of the two doors:
//
//   - every name parquet.ParseTypeID takes, which is what a CREATE TABLE
//     column type may be. `CAST(x AS ARRAY(INT))` is not implemented by
//     Cast.Eval and passes its operand through, and that stays exactly as it
//     was: this pass refuses names that name NOTHING, not casts that are
//     unimplemented.
//   - the PostgreSQL spellings Cast.Eval implements that the DDL door has no
//     column type for — `int4`, `smallint`, `real`, `double precision`,
//     `signed`, `timestamptz` and the rest of the list below.
//
// The union means the two doors agree about which NAMES exist, which is the
// property #838's both-doors gate already states for the length modifier:
// one type name, one disposition. `bytea`, `inet`, `json`, `money` and `xml`
// are in NEITHER set and are refused on both doors now — a refusal PostgreSQL
// does not make, and the CREATE TABLE door has made it all along. Recorded in
// ADR-0012's divergence list, because the alternative is the wrong VALUE this
// closes.

// castOnlyDestTypes are the destination spellings expr.Cast implements which
// parquet.ParseTypeID does not take as a column type. Keep it in step with
// Cast.Eval's switch labels and castTemporalKindLower: a label there that is
// missing here is a cast this refuses.
var castOnlyDestTypes = map[string]bool{
	"int4":             true,
	"int8":             true,
	"int2":             true,
	"smallint":         true,
	"signed":           true,
	"real":             true,
	"float4":           true,
	"float8":           true,
	"double precision": true,
	"numeric":          true,
	"char":             true,
	"character":        true,
	"timestamptz":      true,
}

// KnownCastDest reports whether name is a type this engine has. It is the
// question `CAST(x AS name)` asks before anything else, and the answer for a
// name in neither door's set is PostgreSQL's 42704.
func KnownCastDest(name string) bool {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return false
	}
	if castOnlyDestTypes[strings.ToLower(trimmed)] {
		return true
	}
	// A PARAMETERIZED spelling names its type in the part before the
	// parenthesis, so RECOGNIZING the shape is the whole answer this function
	// owes — the modifier's own refusal is a different question with a
	// different code, and PostgreSQL keeps them apart: `VARCHAR(0)` is 22023
	// `length for type varchar must be at least 1` and `FLOAT(54)` is 22023
	// `precision for type float must be less than 54 bits`, NOT 42704. The
	// first cut of this returned `err == nil` here and turned five wire-oracle
	// entries from 22023 into 42704 — a right refusal under the wrong code,
	// which is the defect #366 is about.
	if _, _, ok := parquet.StringTypeLength(trimmed); ok {
		return true
	}
	if _, _, ok := parquet.FloatTypePrecision(trimmed); ok {
		return true
	}
	if _, _, _, ok := DecimalCastDest(trimmed); ok {
		return true
	}
	return parquet.KnownTypeName(trimmed)
}

// UnknownTypeError names a type name no type in this engine answers to. It is
// a compile REFUSAL in the sense fatal.go's IsCompileRefusal means: an error
// that has decided its own PostgreSQL class is an answer, not a hint the
// planner may fall back from.
type UnknownTypeError struct{ Name string }

func (e *UnknownTypeError) Error() string {
	return "type " + `"` + strings.TrimSpace(e.Name) + `"` + " does not exist"
}

// SQLState returns PostgreSQL's undefined_object code.
func (e *UnknownTypeError) SQLState() string { return "42704" }
