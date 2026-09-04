package coordinator

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The NUMERIC-FOLD fixture (#724): every numeric width in ONE table, so a
// polymorphic call (CASE/COALESCE/GREATEST/LEAST/NULLIF) can be written over
// each ORDERED PAIR of them with a quoted literal in each argument position.
//
// No existing fixture can stand in. typemx has one column per numeric type but
// a single DECIMAL, so no fold over TWO decimals at different scales is
// expressible there; decpair has the two decimals and the two floats but no
// INTEGER column at all, and realwidth has bigint/real/double and no decimal.
// The fold this issue is about — PostgreSQL's select_common_type over the
// numeric category, INT32 -> INT64 -> DECIMAL -> FLOAT32 -> FLOAT64 — needs
// all six in scope at once, because the defect is exactly that the first
// argument answered for the call instead of the fold.
const nfTable = "numfold"

func nfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "n_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "n_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "n_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "n_d152", Type: parquet.TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
		{Name: "n_d3810", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		// A genuine TEXT column, so a fold that lands on the string rung can
		// be told apart from one that fell back to the string FALLBACK.
		{Name: "n_s", Type: parquet.TypeString, Nullable: true},
	}}
}

// nfData mirrors, row for row, the `nm` table every PostgreSQL 17.11 answer in
// this file was read off. Values are chosen so the fold's rung is VISIBLE in
// the answer rather than only in the OID:
//
//   - row 1 holds 0.1 in the real column, which renders 0.1 as a real and
//     0.10000000149011612 as a double, so a fold that widens real to double
//     when PostgreSQL keeps it real is a wrong VALUE, not just a wrong type.
//   - row 3 holds 16777216 (2^24) in the real column and 16777217 in the
//     integer and double ones: the first integer real cannot follow.
//   - row 2 is all-NULL, the row on which COALESCE takes its last argument.
//   - row 4 is negative, so LEAST and GREATEST do not agree by accident.
func nfData() []map[string]any {
	dec := func(unscaled int64) parquet.Decimal128 {
		hi := int64(0)
		if unscaled < 0 {
			hi = -1
		}
		return parquet.Decimal128{Hi: hi, Lo: uint64(unscaled)}
	}
	return []map[string]any{
		{
			"id": int64(1), "n_i32": int32(3), "n_i64": int64(4),
			"n_f32": float32(0.1), "n_f64": float64(0.2),
			"n_d152": dec(1275), "n_d3810": dec(127500000001), "n_s": "x",
		},
		{
			"id": int64(2), "n_i32": nil, "n_i64": nil,
			"n_f32": nil, "n_f64": nil,
			"n_d152": nil, "n_d3810": nil, "n_s": nil,
		},
		{
			"id": int64(3), "n_i32": int32(16777217), "n_i64": int64(16777217),
			"n_f32": float32(16777216), "n_f64": float64(16777217),
			"n_d152": dec(100), "n_d3810": dec(10000000000), "n_s": "y",
		},
		{
			"id": int64(4), "n_i32": int32(-5), "n_i64": int64(-6),
			"n_f32": float32(-0.5), "n_f64": float64(-0.25),
			"n_d152": dec(-350), "n_d3810": dec(-35000000000), "n_s": "z",
		},
	}
}

// nfEntry is one composite and PostgreSQL 17.11's answer for it.
//
// pg is the type PostgreSQL DECLARES for the expression — read off
// pg_attribute for a view over `SELECT <expr> AS v FROM nm`, not from
// pg_typeof, so it carries the TYPMOD as well as the type — or "ERR <state>"
// when PostgreSQL refuses the composite outright. want is the four rows'
// values in id order, PostgreSQL's own ::text rendering, "<NULL>" for a NULL.
type nfEntry struct {
	name string
	expr string
	pg   string
	want string
}

// nfCarrierRefusals are the composites PostgreSQL ANSWERS and wadjet refuses,
// on purpose, with PostgreSQL's answer recorded beside each.
//
// Every one is ADR-0024 item 1's finite carrier meeting a value PostgreSQL's
// unbounded `numeric` holds: the fold lands on DECIMAL and the literal is
// '1e39' (forty digits, past the 38 an Int128 at scale 0 can spell), 'NaN' or
// 'Infinity' (no bit pattern in a fixed-point integer at all).
//
// A literal that is merely FINER than the columns is NOT here: the fold's
// scale is max over the arms, the literal's included (ADR-0012 item 12), so
// `GREATEST(numeric(15,2), '12.750000000000000001')` answers all twenty
// digits. What that costs is the COLUMNS' rendering, which
// TestLiteralScaleInADecimalFold pins.
//
// The refusal is AT THE STORE, per row, never at plan time: the same
// composite over a range that excludes the literal's row ANSWERS, and so does
// a WHERE over it, which projects nothing. TestNumericFoldRefusalIsPerRow
// holds that line.
//
// The answer is loud rather than wrong — the position ADR-0024 took when it
// chose the carrier, and the one ADR-0012 item 12 records as a divergence
// rather than a defect.
//
// They are LISTED rather than skipped because the list is a claim that can
// fail: the gate asserts each still refuses, so if the carrier ever grows —
// or if a fold starts answering these some other way — these entries fail and
// the recorded PostgreSQL answer is right there to assert instead (ADR-0013).
//
// Before #724 most of them "agreed" with PostgreSQL by accident: the quoted
// literal made the whole call declare STRING, so its own characters went out
// unread and happened to match PostgreSQL's rendering. That agreement was the
// defect, not the absence of one — the same declaration made
// `GREATEST(numeric, '3.5')` a text column that no GROUP BY, SUM or wire OID
// could read as a number.
var nfCarrierRefusals = map[string]string{
	"COALESCE|n_i32|LIT1e39|n_d152":   "3|1000000000000000000000000000000000000000|16777217|-5",
	"GREATEST|n_i32|LIT1e39|n_d152":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_i32|n_d152":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_i32|n_d152|LIT1e39":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_i32|LIT1e39|n_d152":      "3|1000000000000000000000000000000000000000|1.00|-5",
	"CASE|n_i32|LIT1e39|n_d152":       "3|1000000000000000000000000000000000000000|1.00|-3.50",
	"COALESCE|n_i32|LIT1e39|n_d3810":  "3|1000000000000000000000000000000000000000|16777217|-5",
	"GREATEST|n_i32|LIT1e39|n_d3810":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_i32|n_d3810":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_i32|n_d3810|LIT1e39":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_i32|LIT1e39|n_d3810":     "3|1000000000000000000000000000000000000000|1.0000000000|-5",
	"CASE|n_i32|LIT1e39|n_d3810":      "3|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"COALESCE|n_i64|LIT1e39|n_d152":   "4|1000000000000000000000000000000000000000|16777217|-6",
	"GREATEST|n_i64|LIT1e39|n_d152":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_i64|n_d152":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_i64|n_d152|LIT1e39":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_i64|LIT1e39|n_d152":      "4|1000000000000000000000000000000000000000|1.00|-6",
	"CASE|n_i64|LIT1e39|n_d152":       "4|1000000000000000000000000000000000000000|1.00|-3.50",
	"COALESCE|n_i64|LIT1e39|n_d3810":  "4|1000000000000000000000000000000000000000|16777217|-6",
	"GREATEST|n_i64|LIT1e39|n_d3810":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_i64|n_d3810":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_i64|n_d3810|LIT1e39":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_i64|LIT1e39|n_d3810":     "4|1000000000000000000000000000000000000000|1.0000000000|-6",
	"CASE|n_i64|LIT1e39|n_d3810":      "4|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"COALESCE|n_d152|LIT1e39|n_i32":   "12.75|1000000000000000000000000000000000000000|1.00|-3.50",
	"GREATEST|n_d152|LIT1e39|n_i32":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_d152|n_i32":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d152|n_i32|LIT1e39":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_d152|LIT1e39|n_i32":      "3|1000000000000000000000000000000000000000|1.00|-5",
	"CASE|n_d152|LIT1e39|n_i32":       "12.75|1000000000000000000000000000000000000000|16777217|-5",
	"COALESCE|n_d152|LIT1e39|n_i64":   "12.75|1000000000000000000000000000000000000000|1.00|-3.50",
	"GREATEST|n_d152|LIT1e39|n_i64":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_d152|n_i64":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d152|n_i64|LIT1e39":   "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_d152|LIT1e39|n_i64":      "4|1000000000000000000000000000000000000000|1.00|-6",
	"CASE|n_d152|LIT1e39|n_i64":       "12.75|1000000000000000000000000000000000000000|16777217|-6",
	"COALESCE|n_d152|LIT1e39|n_d3810": "12.75|1000000000000000000000000000000000000000|1.00|-3.50",
	"GREATEST|n_d152|LIT1e39|n_d3810": "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_d152|n_d3810": "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d152|n_d3810|LIT1e39": "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_d152|LIT1e39|n_d3810":    "12.75|1000000000000000000000000000000000000000|1.00|-3.50",
	"CASE|n_d152|LIT1e39|n_d3810":     "12.75|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"COALESCE|n_d3810|LIT1e39|n_i32":  "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"GREATEST|n_d3810|LIT1e39|n_i32":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_d3810|n_i32":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d3810|n_i32|LIT1e39":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_d3810|LIT1e39|n_i32":     "3|1000000000000000000000000000000000000000|1.0000000000|-5",
	"CASE|n_d3810|LIT1e39|n_i32":      "12.7500000001|1000000000000000000000000000000000000000|16777217|-5",
	"COALESCE|n_d3810|LIT1e39|n_i64":  "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"GREATEST|n_d3810|LIT1e39|n_i64":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_d3810|n_i64":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d3810|n_i64|LIT1e39":  "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_d3810|LIT1e39|n_i64":     "4|1000000000000000000000000000000000000000|1.0000000000|-6",
	"CASE|n_d3810|LIT1e39|n_i64":      "12.7500000001|1000000000000000000000000000000000000000|16777217|-6",
	"COALESCE|n_d3810|LIT1e39|n_d152": "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"GREATEST|n_d3810|LIT1e39|n_d152": "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|LIT1e39|n_d3810|n_d152": "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d3810|n_d152|LIT1e39": "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"LEAST|n_d3810|LIT1e39|n_d152":    "12.75|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"CASE|n_d3810|LIT1e39|n_d152":     "12.7500000001|1000000000000000000000000000000000000000|1.00|-3.50",
	"GREATEST|n_d152|LIT1e39":         "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"COALESCE|n_d152|LIT1e39":         "12.75|1000000000000000000000000000000000000000|1.00|-3.50",
	"NULLIF|LIT1e39|n_d152":           "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"CASE|n_d152|LIT1e39":             "12.75|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d152|LITNaN":          "NaN|NaN|NaN|NaN",
	"COALESCE|n_d152|LITNaN":          "12.75|NaN|1.00|-3.50",
	"NULLIF|LITNaN|n_d152":            "NaN|NaN|NaN|NaN",
	"CASE|n_d152|LITNaN":              "12.75|NaN|NaN|NaN",
	"GREATEST|n_d152|LITInfinity":     "Infinity|Infinity|Infinity|Infinity",
	"COALESCE|n_d152|LITInfinity":     "12.75|Infinity|1.00|-3.50",
	"NULLIF|LITInfinity|n_d152":       "Infinity|Infinity|Infinity|Infinity",
	"CASE|n_d152|LITInfinity":         "12.75|Infinity|Infinity|Infinity",
	"GREATEST|n_d3810|LIT1e39":        "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"COALESCE|n_d3810|LIT1e39":        "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000",
	"NULLIF|LIT1e39|n_d3810":          "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"CASE|n_d3810|LIT1e39":            "12.7500000001|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000",
	"GREATEST|n_d3810|LITNaN":         "NaN|NaN|NaN|NaN",
	"COALESCE|n_d3810|LITNaN":         "12.7500000001|NaN|1.0000000000|-3.5000000000",
	"NULLIF|LITNaN|n_d3810":           "NaN|NaN|NaN|NaN",
	"CASE|n_d3810|LITNaN":             "12.7500000001|NaN|NaN|NaN",
	"GREATEST|n_d3810|LITInfinity":    "Infinity|Infinity|Infinity|Infinity",
	"COALESCE|n_d3810|LITInfinity":    "12.7500000001|Infinity|1.0000000000|-3.5000000000",
	"NULLIF|LITInfinity|n_d3810":      "Infinity|Infinity|Infinity|Infinity",
	"CASE|n_d3810|LITInfinity":        "12.7500000001|Infinity|Infinity|Infinity",
}

// nfBoxPins are the composites whose VALUES agree with PostgreSQL and whose
// DECLARED type does not, with the box wadjet produces.
//
// Every one is NULLIF's, and one rule: PostgreSQL takes NULLIF's type from the
// OPERATOR its two arguments select — `NULLIF(integer, real)` resolves
// `float8 = float8`, so the result is double precision — while wadjet takes it
// from argument 0, whose value the call actually returns. It is orthogonal to
// #724 (no quoted literal is involved and the fold is not consulted), it moved
// neither way with it, and it is filed as #757.
//
// A pin that starts agreeing FAILS, which is how the fix will announce itself.
var nfBoxPins = map[string]string{
	// The eight NULLIF rows are GONE (#757, 2026-09-03). NULLIF's type comes
	// from the comparison OPERATOR its two arguments select — argument 0's
	// own width within the integer or float family, float8 for an integer or
	// a DECIMAL against a float — and both the declaration
	// (expr.Ret.operatorResolvedType) and the box (expr.nullifArmsBoxMode)
	// follow it now. They agree with PostgreSQL outright rather than needing
	// an exemption.

	// --- The arm-kind rows, none of them the fold's own doing ------------
	//
	// INT32 arithmetic is computed and declared in int64: expr.BinOpNumeric's
	// integer mode has one integer width, so `SELECT n_i32 + 1` has boxed
	// int64 since #369 and these fold to the same INT64. The DOMAIN is right
	// and the width is the standing int4/int8 divergence, not a value.
	"CASE|n_i32|ARITH":  "int64", // PostgreSQL declares integer
	"CASE|n_i32|NESTED": "int64", // PostgreSQL declares integer
	// The three FUNCABS rows are GONE (#768, 2026-09-03). abs() answers in
	// its argument's own domain now — integer in, integer out; real in, real
	// out — declared by physical.scalarFnDeclaredNumericDomain and computed
	// by expr.absKeepsDomain / expr.vecAbsDomain, which move together
	// because declaring bigint over a ToFloat64 computation would put a right
	// OID on a rounded number. Only abs and mod: CEIL, FLOOR, ROUND, TRUNC,
	// SIGN, SQRT, POWER, LN and EXP over an integer ARE double precision in
	// PostgreSQL, measured, and still are here.
	// The two LITNUM rows are GONE (#849, 2026-09-04). A bare numeric
	// LITERAL arm used to decide nothing here, so the column's own type
	// answered and `100.5` was read at it — declared int32/int64, valued 100.
	// PostgreSQL resolves a FRACTIONAL literal against a typed integer
	// operand to numeric, and both halves follow it now:
	// expr.fractionalLitTriggersFold widens the declaration and its runtime
	// twin expr.fracLitArmTriggersFold widens the fold, so the arms agree
	// with the server outright. ADR-0024's deferral for a fold of literals
	// ALONE is untouched — it takes a non-literal arm to trigger this.
}

// nfValuePins are entries whose VALUES this engine answers differently from
// PostgreSQL, with PostgreSQL's answer recorded in the corpus entry beside
// each. Every one is a defect OUTSIDE the numeric fold — the fold picks the
// rung correctly and another layer then produces a different number on it —
// and every one reproduces with the fold's integer-arithmetic rule reverted,
// which is how they were classified rather than assumed.
//
// They are PINNED rather than dropped for the reason nfCarrierRefusals is: a
// pin is a claim that can fail. The gate asserts each still diverges, so the
// day the underlying defect is fixed these entries fail and the recorded
// PostgreSQL answer is already there to assert instead (ADR-0013).
var nfValuePins = map[string]string{
	// The two numeric-literal VALUE rows are GONE with their box pins (#849,
	// 2026-09-04): `100.5` beside an integer column is numeric on the server
	// and numeric here, so the half survives instead of being truncated to
	// 100. It was the clearest case in this corpus of a wrong declared RUNG
	// being a wrong NUMBER — the declaration decided the vector the value was
	// stored into, and an int32 vector has nowhere to put a half.
	// The float -> integer CAST rounding row is GONE (#768, 2026-09-03).
	// PostgreSQL's float-to-integer cast is rint(), half to EVEN, and this
	// engine rounded halves away from zero: -0.5 answered -1 where the
	// server answers 0. The NUMERIC-to-integer cast rounds the other way on
	// the same server (-0.5::numeric::int is -1) and still does here, which
	// is why only the float arm moved.
	// The FUNCABS value row is GONE with its box pin (#768): abs() over a
	// real is computed in float32 now, so 0.1 stays 0.1. It was the clearest
	// case in the corpus of a wrong return DECLARATION being a wrong NUMBER
	// and not only a wrong OID.
}

// nfBox is the Go box a value of PostgreSQL's declared type arrives in when
// wadjet declares the same thing. It is how this gate observes a DECLARED type
// through a result set: an INT32 vector answers int32 and a FLOAT32 one
// float32, which is a different fact from the digits.
//
// `numeric` and `text` share the string box, so this channel alone cannot tell
// a DECIMAL declaration from the STRING fallback. Two other things do: the
// DIGITS, because the fallback renders a DECIMAL column's own text where the
// fold's (p,s) renders the widened scale, and
// physical.TestQuotedLiteralIsUnknownInTheFold, which asserts the expr.DeclType
// directly.
func nfBox(pgType string) string {
	switch pgType {
	case "integer":
		return "int32"
	case "bigint":
		return "int64"
	case "real":
		return "float32"
	case "double precision":
		return "float64"
	}
	return "string"
}

// TestNumericFoldTwoPath is the #724 gate: every polymorphic composite over
// every ordered pair of the six numeric widths, with a quoted literal in each
// argument position and in nine spellings, on both execution paths, against
// PostgreSQL 17.11.
//
// The defect it closes is one line in physical.nodeDeclaredType — a QUOTED
// literal was typed `Decl(TypeString), Decided` — and the reason it needs a
// corpus this size is that the consequence was invisible wherever the corpus
// was thin. A call holding such a literal had a non-numeric decider,
// expr.CommonDeclType could not fold, and the call fell back to its FIRST
// argument. The answer therefore depended on ARGUMENT ORDER: `GREATEST(real,
// '16777217', double)` narrowed to real where PostgreSQL answers the double,
// and the same three arguments in the other order did not. With a bigint
// first it did not narrow at all — it WRAPPED, to int64's minimum, a number
// from nowhere that ADR-0012 item 6 forbids and that reached GROUP BY keys
// built from the same projection.
//
// Both arms are held to PostgreSQL rather than to each other, so an engine
// that agrees with the other arm and with nothing else still fails.
func TestNumericFoldTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	corpus := nfCorpus()
	seen := make(map[string]bool, len(corpus))
	for _, c := range corpus {
		if seen[c.name] {
			t.Fatalf("duplicate corpus entry %q", c.name)
		}
		seen[c.name] = true
	}
	for name := range nfCarrierRefusals {
		if !seen[name] {
			t.Errorf("carrier-refusal pin %q matches no corpus entry — it exempts nothing "+
				"and hides nothing. Delete it or fix the name.", name)
		}
	}
	for name := range nfBoxPins {
		if !seen[name] {
			t.Errorf("box pin %q matches no corpus entry — delete it or fix the name.", name)
		}
	}
	for name := range nfValuePins {
		if !seen[name] {
			t.Errorf("value pin %q matches no corpus entry — delete it or fix the name.", name)
		}
	}

	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT id, (%s) AS v FROM %s ORDER BY id", c.expr, nfTable)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				res, err := nfRun(ctx, single, coord, sql, arm.dag)
				if state, ok := strings.CutPrefix(c.pg, "ERR "); ok {
					if err == nil {
						t.Errorf("%s: %s answered %s; PostgreSQL raises %s — an unknown-typed "+
							"literal that names no value of the fold's type is refused at parse "+
							"analysis there, whatever the rows hold",
							arm.name, sql, nfRender(res), state)
					}
					continue
				}
				if pgWant, pinned := nfCarrierRefusals[c.name]; pinned {
					if err == nil {
						t.Errorf("%s: %s answered %s and PostgreSQL answers %s — the finite "+
							"carrier no longer refuses this, so delete the nfCarrierRefusals "+
							"entry and assert the value (ADR-0024 item 1)",
							arm.name, sql, nfRender(res), pgWant)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s: %s refused: %v\n  PostgreSQL declares %s and answers %s",
						arm.name, sql, err, c.pg, c.want)
				}
				// A box-pinned entry is compared as the NUMBER: the pin
				// says wadjet declares another type for these, and a
				// DECIMAL declaration renders 1.00 where PostgreSQL's
				// double prints 1. The pin below is what holds the
				// declaration itself to account.
				mode := c.pg
				if _, pinned := nfBoxPins[c.name]; pinned {
					mode = "numeric"
				}
				got := nfValues(res)
				if pin, pinned := nfValuePins[c.name]; pinned {
					if nfSameValues(mode, c.want, got) {
						t.Errorf("%s: %s now answers %s, which is PostgreSQL's answer — "+
							"the value pin AGREES, so delete this nfValuePins entry and "+
							"let the corpus assert it (ADR-0013)", arm.name, sql, got)
					} else if got != pin {
						t.Errorf("%s: %s\n  got  %s\n  pin  %s\n  want %s (PostgreSQL 17.11, declared %s)"+
							"\n  the divergence moved: neither PostgreSQL's answer nor the pinned one",
							arm.name, sql, got, pin, c.want, c.pg)
					}
				} else if !nfSameValues(mode, c.want, got) {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11, declared %s)",
						arm.name, sql, got, c.want, c.pg)
				}
				want, box := nfBox(c.pg), nfBoxOf(res)
				if pin, pinned := nfBoxPins[c.name]; pinned {
					if box == want {
						t.Errorf("%s: %s now boxes as %s, which is what PostgreSQL's %s "+
							"declares — the pin AGREES, so delete this nfBoxPins entry",
							arm.name, sql, box, c.pg)
					} else if box != pin {
						t.Errorf("%s: %s boxes as %s; the pin records %s and PostgreSQL "+
							"declares %s", arm.name, sql, box, pin, c.pg)
					}
					continue
				}
				if box != "" && box != want {
					t.Errorf("%s: %s boxed as %s; PostgreSQL declares %s, which is the %s box",
						arm.name, sql, box, c.pg, want)
				}
			}
		})
	}
}

func nfRun(ctx context.Context, single *wadjet.DB, coord *Coordinator, sql string, dag bool) (*oracle.Result, error) {
	if dag {
		return tmdRunDAG(ctx, coord, sql)
	}
	return tmdRunSingle(ctx, single, sql)
}

// nfValues renders a result the way the PostgreSQL side was recorded: the four
// rows' values in id order, "|"-separated, "<NULL>" for a NULL.
//
// A float is rendered at the box's OWN width, which is what makes a real that
// was widened to double visible: PostgreSQL prints a real 0.1 as 0.1 and the
// same number as a double as 0.10000000149011612.
func nfValues(res *oracle.Result) string {
	parts := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		parts = append(parts, nfCell(row["v"]))
	}
	return strings.Join(parts, "|")
}

func nfCell(v any) string {
	switch n := v.(type) {
	case nil:
		return "<NULL>"
	case float32:
		return nfFloatText(float64(n), 32)
	case float64:
		return nfFloatText(n, 64)
	case string:
		return n
	}
	return fmt.Sprint(v)
}

// nfFloatText is PostgreSQL's float output at the default
// extra_float_digits: the SHORTEST text that reads back as the same value at
// the box's own width, in fixed notation while the decimal exponent is inside
// the type's range and in scientific notation with a two-digit signed exponent
// outside it. Every threshold read live off postgres:17-alpine, and the two
// widths do NOT share the upper one:
//
//	float8: 1e14 -> 100000000000000   1e15 -> 1e+15   16777217 -> 16777217
//	float4: 100000 -> 100000          1e6  -> 1e+06   16777216 -> 1.6777216e+07
//	both:   0.0001 -> 0.0001          1e-5 -> 1e-05
//
// Go's own %g switches somewhere else again (a double 16777217 comes out
// 1.6777217e+07), which is why this is spelled out rather than delegated.
//
// Comparing the TEXT rather than the number is deliberate for a float: the
// question this gate asks is at what WIDTH a value was materialized, and a
// real 0.1 widened to double is 0.10000000149011612 — a different text for
// what a numeric comparison would call the same number.
func nfFloatText(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	hi := 14
	if bits == 32 {
		hi = 5
	}
	sci := strconv.FormatFloat(f, 'e', -1, bits)
	i := strings.IndexByte(sci, 'e')
	exp, err := strconv.Atoi(sci[i+1:])
	if err != nil {
		return sci
	}
	if exp >= -4 && exp <= hi {
		return strconv.FormatFloat(f, 'f', -1, bits)
	}
	sign, mag := "+", exp
	if exp < 0 {
		sign, mag = "-", -exp
	}
	return fmt.Sprintf("%se%s%02d", sci[:i], sign, mag)
}

// nfSameValues compares wadjet's four rows against PostgreSQL's.
//
// Exact text, with ONE exception: where PostgreSQL declares `numeric`, the two
// engines render the same number differently, and the difference is the
// carrier, not the answer. PostgreSQL's computed numeric carries typmod −1 and
// prints every value at ITS OWN scale — `GREATEST(numeric(15,2), i32)` prints
// 3 for the integer row and 12.75 for the decimal one — while wadjet declares
// a (p,s) for the fold and its vector renders every value at that scale, so
// the same rows read 3.00 and 12.75. That is ADR-0024's finite fixed-point
// meeting an unbounded one, recorded in ADR-0012 item 12, and it is not what
// this gate is about; the DIGITS are, so a numeric cell is compared as the
// exact number both texts name.
//
// Nothing weaker: the comparison is exact rational equality, so 12.75 against
// 12.7500 passes and 12.75 against 12.7501 does not, and a truncated scale —
// the failure mode a too-narrow fold produces — is still caught.
func nfSameValues(pgType, want, got string) bool {
	if want == got {
		return true
	}
	if pgType != "numeric" {
		return false
	}
	w, g := strings.Split(want, "|"), strings.Split(got, "|")
	if len(w) != len(g) {
		return false
	}
	for i := range w {
		if w[i] == g[i] {
			continue
		}
		a, aok := new(big.Rat).SetString(w[i])
		b, bok := new(big.Rat).SetString(g[i])
		if !aok || !bok || a.Cmp(b) != 0 {
			return false
		}
	}
	return true
}

// nfBoxOf is the Go type of the first non-NULL value in the result — the
// output vector's type, made observable.
func nfBoxOf(res *oracle.Result) string {
	for _, row := range res.Rows {
		if row["v"] == nil {
			continue
		}
		return fmt.Sprintf("%T", row["v"])
	}
	return ""
}

func nfRender(res *oracle.Result) string {
	if res == nil {
		return "<no rows>"
	}
	return nfValues(res)
}

// nfCorpus is the matrix, read live off postgres:17-alpine (17.11) over the
// `nm` table nfData mirrors row for row. It is GENERATED shape by shape rather
// than hand-picked, because the defect it exists for was ORDER-dependent: the
// call took its type from whichever argument came first, so a corpus that
// wrote each composite once would have caught between a third and none of it
// depending on how the examples happened to be spelled.
//
// The shapes:
//
//   - every one of COALESCE/GREATEST/LEAST/NULLIF/CASE over each ORDERED PAIR
//     of the six numeric widths (150 entries) — the ladder itself,
//     INT32 -> INT64 -> DECIMAL -> FLOAT32 -> FLOAT64, in both directions;
//   - the same pairs with a quoted literal in the FIRST, MIDDLE and LAST
//     argument position, in two spellings (360);
//   - nine literal SPELLINGS against each width (270): '1e39' (out of int64
//     and real, in float8 and numeric), 'NaN' and 'Infinity' (float and
//     numeric only), '12.750000000000000001' (exact as numeric, rounded as
//     float8), '0xC.C' (the float input grammar reads it, numeric and the
//     integers do not), '16777217' (exact as double, not as real), '3.5', ”
//     and 'abc';
//   - composites with NO typed operand at all (23), which PostgreSQL resolves
//     to text;
//   - ten three-way folds that mix a DECIMAL, a FLOAT and a literal.
func nfCorpus() []nfEntry {
	return []nfEntry{
		{"COALESCE|n_i32|n_i64", "COALESCE(n_i32, n_i64)", "bigint", "3|<NULL>|16777217|-5"},
		{"GREATEST|n_i32|n_i64", "GREATEST(n_i32, n_i64)", "bigint", "4|<NULL>|16777217|-5"},
		{"LEAST|n_i32|n_i64", "LEAST(n_i32, n_i64)", "bigint", "3|<NULL>|16777217|-6"},
		{"NULLIF|n_i32|n_i64", "NULLIF(n_i32, n_i64)", "integer", "3|<NULL>|<NULL>|-5"},
		{"CASE|n_i32|n_i64", "CASE WHEN id=1 THEN n_i32 ELSE n_i64 END", "bigint", "3|<NULL>|16777217|-6"},
		{"COALESCE|n_i32|n_f32", "COALESCE(n_i32, n_f32)", "real", "3|<NULL>|1.6777216e+07|-5"},
		{"GREATEST|n_i32|n_f32", "GREATEST(n_i32, n_f32)", "real", "3|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_i32|n_f32", "LEAST(n_i32, n_f32)", "real", "0.1|<NULL>|1.6777216e+07|-5"},
		{"NULLIF|n_i32|n_f32", "NULLIF(n_i32, n_f32)", "double precision", "3|<NULL>|16777217|-5"},
		{"CASE|n_i32|n_f32", "CASE WHEN id=1 THEN n_i32 ELSE n_f32 END", "real", "3|<NULL>|1.6777216e+07|-0.5"},
		{"COALESCE|n_i32|n_f64", "COALESCE(n_i32, n_f64)", "double precision", "3|<NULL>|16777217|-5"},
		{"GREATEST|n_i32|n_f64", "GREATEST(n_i32, n_f64)", "double precision", "3|<NULL>|16777217|-0.25"},
		{"LEAST|n_i32|n_f64", "LEAST(n_i32, n_f64)", "double precision", "0.2|<NULL>|16777217|-5"},
		{"NULLIF|n_i32|n_f64", "NULLIF(n_i32, n_f64)", "double precision", "3|<NULL>|<NULL>|-5"},
		{"CASE|n_i32|n_f64", "CASE WHEN id=1 THEN n_i32 ELSE n_f64 END", "double precision", "3|<NULL>|16777217|-0.25"},
		{"COALESCE|n_i32|n_d152", "COALESCE(n_i32, n_d152)", "numeric", "3|<NULL>|16777217|-5"},
		{"GREATEST|n_i32|n_d152", "GREATEST(n_i32, n_d152)", "numeric", "12.75|<NULL>|16777217|-3.50"},
		{"LEAST|n_i32|n_d152", "LEAST(n_i32, n_d152)", "numeric", "3|<NULL>|1.00|-5"},
		{"NULLIF|n_i32|n_d152", "NULLIF(n_i32, n_d152)", "numeric", "3|<NULL>|16777217|-5"},
		{"CASE|n_i32|n_d152", "CASE WHEN id=1 THEN n_i32 ELSE n_d152 END", "numeric", "3|<NULL>|1.00|-3.50"},
		{"COALESCE|n_i32|n_d3810", "COALESCE(n_i32, n_d3810)", "numeric", "3|<NULL>|16777217|-5"},
		{"GREATEST|n_i32|n_d3810", "GREATEST(n_i32, n_d3810)", "numeric", "12.7500000001|<NULL>|16777217|-3.5000000000"},
		{"LEAST|n_i32|n_d3810", "LEAST(n_i32, n_d3810)", "numeric", "3|<NULL>|1.0000000000|-5"},
		{"NULLIF|n_i32|n_d3810", "NULLIF(n_i32, n_d3810)", "numeric", "3|<NULL>|16777217|-5"},
		{"CASE|n_i32|n_d3810", "CASE WHEN id=1 THEN n_i32 ELSE n_d3810 END", "numeric", "3|<NULL>|1.0000000000|-3.5000000000"},
		{"COALESCE|n_i64|n_i32", "COALESCE(n_i64, n_i32)", "bigint", "4|<NULL>|16777217|-6"},
		{"GREATEST|n_i64|n_i32", "GREATEST(n_i64, n_i32)", "bigint", "4|<NULL>|16777217|-5"},
		{"LEAST|n_i64|n_i32", "LEAST(n_i64, n_i32)", "bigint", "3|<NULL>|16777217|-6"},
		{"NULLIF|n_i64|n_i32", "NULLIF(n_i64, n_i32)", "bigint", "4|<NULL>|<NULL>|-6"},
		{"CASE|n_i64|n_i32", "CASE WHEN id=1 THEN n_i64 ELSE n_i32 END", "bigint", "4|<NULL>|16777217|-5"},
		{"COALESCE|n_i64|n_f32", "COALESCE(n_i64, n_f32)", "real", "4|<NULL>|1.6777216e+07|-6"},
		{"GREATEST|n_i64|n_f32", "GREATEST(n_i64, n_f32)", "real", "4|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_i64|n_f32", "LEAST(n_i64, n_f32)", "real", "0.1|<NULL>|1.6777216e+07|-6"},
		{"NULLIF|n_i64|n_f32", "NULLIF(n_i64, n_f32)", "double precision", "4|<NULL>|16777217|-6"},
		{"CASE|n_i64|n_f32", "CASE WHEN id=1 THEN n_i64 ELSE n_f32 END", "real", "4|<NULL>|1.6777216e+07|-0.5"},
		{"COALESCE|n_i64|n_f64", "COALESCE(n_i64, n_f64)", "double precision", "4|<NULL>|16777217|-6"},
		{"GREATEST|n_i64|n_f64", "GREATEST(n_i64, n_f64)", "double precision", "4|<NULL>|16777217|-0.25"},
		{"LEAST|n_i64|n_f64", "LEAST(n_i64, n_f64)", "double precision", "0.2|<NULL>|16777217|-6"},
		{"NULLIF|n_i64|n_f64", "NULLIF(n_i64, n_f64)", "double precision", "4|<NULL>|<NULL>|-6"},
		{"CASE|n_i64|n_f64", "CASE WHEN id=1 THEN n_i64 ELSE n_f64 END", "double precision", "4|<NULL>|16777217|-0.25"},
		{"COALESCE|n_i64|n_d152", "COALESCE(n_i64, n_d152)", "numeric", "4|<NULL>|16777217|-6"},
		{"GREATEST|n_i64|n_d152", "GREATEST(n_i64, n_d152)", "numeric", "12.75|<NULL>|16777217|-3.50"},
		{"LEAST|n_i64|n_d152", "LEAST(n_i64, n_d152)", "numeric", "4|<NULL>|1.00|-6"},
		{"NULLIF|n_i64|n_d152", "NULLIF(n_i64, n_d152)", "numeric", "4|<NULL>|16777217|-6"},
		{"CASE|n_i64|n_d152", "CASE WHEN id=1 THEN n_i64 ELSE n_d152 END", "numeric", "4|<NULL>|1.00|-3.50"},
		{"COALESCE|n_i64|n_d3810", "COALESCE(n_i64, n_d3810)", "numeric", "4|<NULL>|16777217|-6"},
		{"GREATEST|n_i64|n_d3810", "GREATEST(n_i64, n_d3810)", "numeric", "12.7500000001|<NULL>|16777217|-3.5000000000"},
		{"LEAST|n_i64|n_d3810", "LEAST(n_i64, n_d3810)", "numeric", "4|<NULL>|1.0000000000|-6"},
		{"NULLIF|n_i64|n_d3810", "NULLIF(n_i64, n_d3810)", "numeric", "4|<NULL>|16777217|-6"},
		{"CASE|n_i64|n_d3810", "CASE WHEN id=1 THEN n_i64 ELSE n_d3810 END", "numeric", "4|<NULL>|1.0000000000|-3.5000000000"},
		{"COALESCE|n_f32|n_i32", "COALESCE(n_f32, n_i32)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|n_i32", "GREATEST(n_f32, n_i32)", "real", "3|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_f32|n_i32", "LEAST(n_f32, n_i32)", "real", "0.1|<NULL>|1.6777216e+07|-5"},
		{"NULLIF|n_f32|n_i32", "NULLIF(n_f32, n_i32)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"CASE|n_f32|n_i32", "CASE WHEN id=1 THEN n_f32 ELSE n_i32 END", "real", "0.1|<NULL>|1.6777216e+07|-5"},
		{"COALESCE|n_f32|n_i64", "COALESCE(n_f32, n_i64)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|n_i64", "GREATEST(n_f32, n_i64)", "real", "4|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_f32|n_i64", "LEAST(n_f32, n_i64)", "real", "0.1|<NULL>|1.6777216e+07|-6"},
		{"NULLIF|n_f32|n_i64", "NULLIF(n_f32, n_i64)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"CASE|n_f32|n_i64", "CASE WHEN id=1 THEN n_f32 ELSE n_i64 END", "real", "0.1|<NULL>|1.6777216e+07|-6"},
		{"COALESCE|n_f32|n_f64", "COALESCE(n_f32, n_f64)", "double precision", "0.10000000149011612|<NULL>|16777216|-0.5"},
		{"GREATEST|n_f32|n_f64", "GREATEST(n_f32, n_f64)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"LEAST|n_f32|n_f64", "LEAST(n_f32, n_f64)", "double precision", "0.10000000149011612|<NULL>|16777216|-0.5"},
		{"NULLIF|n_f32|n_f64", "NULLIF(n_f32, n_f64)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"CASE|n_f32|n_f64", "CASE WHEN id=1 THEN n_f32 ELSE n_f64 END", "double precision", "0.10000000149011612|<NULL>|16777217|-0.25"},
		{"COALESCE|n_f32|n_d152", "COALESCE(n_f32, n_d152)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|n_d152", "GREATEST(n_f32, n_d152)", "real", "12.75|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_f32|n_d152", "LEAST(n_f32, n_d152)", "real", "0.1|<NULL>|1|-3.5"},
		{"NULLIF|n_f32|n_d152", "NULLIF(n_f32, n_d152)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"CASE|n_f32|n_d152", "CASE WHEN id=1 THEN n_f32 ELSE n_d152 END", "real", "0.1|<NULL>|1|-3.5"},
		{"COALESCE|n_f32|n_d3810", "COALESCE(n_f32, n_d3810)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|n_d3810", "GREATEST(n_f32, n_d3810)", "real", "12.75|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_f32|n_d3810", "LEAST(n_f32, n_d3810)", "real", "0.1|<NULL>|1|-3.5"},
		{"NULLIF|n_f32|n_d3810", "NULLIF(n_f32, n_d3810)", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"CASE|n_f32|n_d3810", "CASE WHEN id=1 THEN n_f32 ELSE n_d3810 END", "real", "0.1|<NULL>|1|-3.5"},
		{"COALESCE|n_f64|n_i32", "COALESCE(n_f64, n_i32)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"GREATEST|n_f64|n_i32", "GREATEST(n_f64, n_i32)", "double precision", "3|<NULL>|16777217|-0.25"},
		{"LEAST|n_f64|n_i32", "LEAST(n_f64, n_i32)", "double precision", "0.2|<NULL>|16777217|-5"},
		{"NULLIF|n_f64|n_i32", "NULLIF(n_f64, n_i32)", "double precision", "0.2|<NULL>|<NULL>|-0.25"},
		{"CASE|n_f64|n_i32", "CASE WHEN id=1 THEN n_f64 ELSE n_i32 END", "double precision", "0.2|<NULL>|16777217|-5"},
		{"COALESCE|n_f64|n_i64", "COALESCE(n_f64, n_i64)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"GREATEST|n_f64|n_i64", "GREATEST(n_f64, n_i64)", "double precision", "4|<NULL>|16777217|-0.25"},
		{"LEAST|n_f64|n_i64", "LEAST(n_f64, n_i64)", "double precision", "0.2|<NULL>|16777217|-6"},
		{"NULLIF|n_f64|n_i64", "NULLIF(n_f64, n_i64)", "double precision", "0.2|<NULL>|<NULL>|-0.25"},
		{"CASE|n_f64|n_i64", "CASE WHEN id=1 THEN n_f64 ELSE n_i64 END", "double precision", "0.2|<NULL>|16777217|-6"},
		{"COALESCE|n_f64|n_f32", "COALESCE(n_f64, n_f32)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"GREATEST|n_f64|n_f32", "GREATEST(n_f64, n_f32)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"LEAST|n_f64|n_f32", "LEAST(n_f64, n_f32)", "double precision", "0.10000000149011612|<NULL>|16777216|-0.5"},
		{"NULLIF|n_f64|n_f32", "NULLIF(n_f64, n_f32)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"CASE|n_f64|n_f32", "CASE WHEN id=1 THEN n_f64 ELSE n_f32 END", "double precision", "0.2|<NULL>|16777216|-0.5"},
		{"COALESCE|n_f64|n_d152", "COALESCE(n_f64, n_d152)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"GREATEST|n_f64|n_d152", "GREATEST(n_f64, n_d152)", "double precision", "12.75|<NULL>|16777217|-0.25"},
		{"LEAST|n_f64|n_d152", "LEAST(n_f64, n_d152)", "double precision", "0.2|<NULL>|1|-3.5"},
		{"NULLIF|n_f64|n_d152", "NULLIF(n_f64, n_d152)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"CASE|n_f64|n_d152", "CASE WHEN id=1 THEN n_f64 ELSE n_d152 END", "double precision", "0.2|<NULL>|1|-3.5"},
		{"COALESCE|n_f64|n_d3810", "COALESCE(n_f64, n_d3810)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"GREATEST|n_f64|n_d3810", "GREATEST(n_f64, n_d3810)", "double precision", "12.7500000001|<NULL>|16777217|-0.25"},
		{"LEAST|n_f64|n_d3810", "LEAST(n_f64, n_d3810)", "double precision", "0.2|<NULL>|1|-3.5"},
		{"NULLIF|n_f64|n_d3810", "NULLIF(n_f64, n_d3810)", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"CASE|n_f64|n_d3810", "CASE WHEN id=1 THEN n_f64 ELSE n_d3810 END", "double precision", "0.2|<NULL>|1|-3.5"},
		{"COALESCE|n_d152|n_i32", "COALESCE(n_d152, n_i32)", "numeric", "12.75|<NULL>|1.00|-3.50"},
		{"GREATEST|n_d152|n_i32", "GREATEST(n_d152, n_i32)", "numeric", "12.75|<NULL>|16777217|-3.50"},
		{"LEAST|n_d152|n_i32", "LEAST(n_d152, n_i32)", "numeric", "3|<NULL>|1.00|-5"},
		{"NULLIF|n_d152|n_i32", "NULLIF(n_d152, n_i32)", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"CASE|n_d152|n_i32", "CASE WHEN id=1 THEN n_d152 ELSE n_i32 END", "numeric", "12.75|<NULL>|16777217|-5"},
		{"COALESCE|n_d152|n_i64", "COALESCE(n_d152, n_i64)", "numeric", "12.75|<NULL>|1.00|-3.50"},
		{"GREATEST|n_d152|n_i64", "GREATEST(n_d152, n_i64)", "numeric", "12.75|<NULL>|16777217|-3.50"},
		{"LEAST|n_d152|n_i64", "LEAST(n_d152, n_i64)", "numeric", "4|<NULL>|1.00|-6"},
		{"NULLIF|n_d152|n_i64", "NULLIF(n_d152, n_i64)", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"CASE|n_d152|n_i64", "CASE WHEN id=1 THEN n_d152 ELSE n_i64 END", "numeric", "12.75|<NULL>|16777217|-6"},
		{"COALESCE|n_d152|n_f32", "COALESCE(n_d152, n_f32)", "real", "12.75|<NULL>|1|-3.5"},
		{"GREATEST|n_d152|n_f32", "GREATEST(n_d152, n_f32)", "real", "12.75|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_d152|n_f32", "LEAST(n_d152, n_f32)", "real", "0.1|<NULL>|1|-3.5"},
		{"NULLIF|n_d152|n_f32", "NULLIF(n_d152, n_f32)", "double precision", "12.75|<NULL>|1|-3.5"},
		{"CASE|n_d152|n_f32", "CASE WHEN id=1 THEN n_d152 ELSE n_f32 END", "real", "12.75|<NULL>|1.6777216e+07|-0.5"},
		{"COALESCE|n_d152|n_f64", "COALESCE(n_d152, n_f64)", "double precision", "12.75|<NULL>|1|-3.5"},
		{"GREATEST|n_d152|n_f64", "GREATEST(n_d152, n_f64)", "double precision", "12.75|<NULL>|16777217|-0.25"},
		{"LEAST|n_d152|n_f64", "LEAST(n_d152, n_f64)", "double precision", "0.2|<NULL>|1|-3.5"},
		{"NULLIF|n_d152|n_f64", "NULLIF(n_d152, n_f64)", "double precision", "12.75|<NULL>|1|-3.5"},
		{"CASE|n_d152|n_f64", "CASE WHEN id=1 THEN n_d152 ELSE n_f64 END", "double precision", "12.75|<NULL>|16777217|-0.25"},
		{"COALESCE|n_d152|n_d3810", "COALESCE(n_d152, n_d3810)", "numeric", "12.75|<NULL>|1.00|-3.50"},
		{"GREATEST|n_d152|n_d3810", "GREATEST(n_d152, n_d3810)", "numeric", "12.7500000001|<NULL>|1.00|-3.50"},
		{"LEAST|n_d152|n_d3810", "LEAST(n_d152, n_d3810)", "numeric", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|n_d152|n_d3810", "NULLIF(n_d152, n_d3810)", "numeric(15,2)", "12.75|<NULL>|<NULL>|<NULL>"},
		{"CASE|n_d152|n_d3810", "CASE WHEN id=1 THEN n_d152 ELSE n_d3810 END", "numeric", "12.75|<NULL>|1.0000000000|-3.5000000000"},
		{"COALESCE|n_d3810|n_i32", "COALESCE(n_d3810, n_i32)", "numeric", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|n_i32", "GREATEST(n_d3810, n_i32)", "numeric", "12.7500000001|<NULL>|16777217|-3.5000000000"},
		{"LEAST|n_d3810|n_i32", "LEAST(n_d3810, n_i32)", "numeric", "3|<NULL>|1.0000000000|-5"},
		{"NULLIF|n_d3810|n_i32", "NULLIF(n_d3810, n_i32)", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"CASE|n_d3810|n_i32", "CASE WHEN id=1 THEN n_d3810 ELSE n_i32 END", "numeric", "12.7500000001|<NULL>|16777217|-5"},
		{"COALESCE|n_d3810|n_i64", "COALESCE(n_d3810, n_i64)", "numeric", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|n_i64", "GREATEST(n_d3810, n_i64)", "numeric", "12.7500000001|<NULL>|16777217|-3.5000000000"},
		{"LEAST|n_d3810|n_i64", "LEAST(n_d3810, n_i64)", "numeric", "4|<NULL>|1.0000000000|-6"},
		{"NULLIF|n_d3810|n_i64", "NULLIF(n_d3810, n_i64)", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"CASE|n_d3810|n_i64", "CASE WHEN id=1 THEN n_d3810 ELSE n_i64 END", "numeric", "12.7500000001|<NULL>|16777217|-6"},
		{"COALESCE|n_d3810|n_f32", "COALESCE(n_d3810, n_f32)", "real", "12.75|<NULL>|1|-3.5"},
		{"GREATEST|n_d3810|n_f32", "GREATEST(n_d3810, n_f32)", "real", "12.75|<NULL>|1.6777216e+07|-0.5"},
		{"LEAST|n_d3810|n_f32", "LEAST(n_d3810, n_f32)", "real", "0.1|<NULL>|1|-3.5"},
		{"NULLIF|n_d3810|n_f32", "NULLIF(n_d3810, n_f32)", "double precision", "12.7500000001|<NULL>|1|-3.5"},
		{"CASE|n_d3810|n_f32", "CASE WHEN id=1 THEN n_d3810 ELSE n_f32 END", "real", "12.75|<NULL>|1.6777216e+07|-0.5"},
		{"COALESCE|n_d3810|n_f64", "COALESCE(n_d3810, n_f64)", "double precision", "12.7500000001|<NULL>|1|-3.5"},
		{"GREATEST|n_d3810|n_f64", "GREATEST(n_d3810, n_f64)", "double precision", "12.7500000001|<NULL>|16777217|-0.25"},
		{"LEAST|n_d3810|n_f64", "LEAST(n_d3810, n_f64)", "double precision", "0.2|<NULL>|1|-3.5"},
		{"NULLIF|n_d3810|n_f64", "NULLIF(n_d3810, n_f64)", "double precision", "12.7500000001|<NULL>|1|-3.5"},
		{"CASE|n_d3810|n_f64", "CASE WHEN id=1 THEN n_d3810 ELSE n_f64 END", "double precision", "12.7500000001|<NULL>|16777217|-0.25"},
		{"COALESCE|n_d3810|n_d152", "COALESCE(n_d3810, n_d152)", "numeric", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|n_d152", "GREATEST(n_d3810, n_d152)", "numeric", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"LEAST|n_d3810|n_d152", "LEAST(n_d3810, n_d152)", "numeric", "12.75|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|n_d152", "NULLIF(n_d3810, n_d152)", "numeric(38,10)", "12.7500000001|<NULL>|<NULL>|<NULL>"},
		{"CASE|n_d3810|n_d152", "CASE WHEN id=1 THEN n_d3810 ELSE n_d152 END", "numeric", "12.7500000001|<NULL>|1.00|-3.50"},
		{"COALESCE|n_i32|LIT3.5|n_i64", "COALESCE(n_i32, '3.5', n_i64)", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT3.5|n_i64", "GREATEST(n_i32, '3.5', n_i64)", "ERR 22P02", ""},
		{"GREATEST|LIT3.5|n_i32|n_i64", "GREATEST('3.5', n_i32, n_i64)", "ERR 22P02", ""},
		{"GREATEST|n_i32|n_i64|LIT3.5", "GREATEST(n_i32, n_i64, '3.5')", "ERR 22P02", ""},
		{"LEAST|n_i32|LIT3.5|n_i64", "LEAST(n_i32, '3.5', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT3.5|n_i64", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '3.5' ELSE n_i64 END", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT1e39|n_i64", "COALESCE(n_i32, '1e39', n_i64)", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT1e39|n_i64", "GREATEST(n_i32, '1e39', n_i64)", "ERR 22P02", ""},
		{"GREATEST|LIT1e39|n_i32|n_i64", "GREATEST('1e39', n_i32, n_i64)", "ERR 22P02", ""},
		{"GREATEST|n_i32|n_i64|LIT1e39", "GREATEST(n_i32, n_i64, '1e39')", "ERR 22P02", ""},
		{"LEAST|n_i32|LIT1e39|n_i64", "LEAST(n_i32, '1e39', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT1e39|n_i64", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '1e39' ELSE n_i64 END", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT3.5|n_f32", "COALESCE(n_i32, '3.5', n_f32)", "real", "3|3.5|1.6777216e+07|-5"},
		{"GREATEST|n_i32|LIT3.5|n_f32", "GREATEST(n_i32, '3.5', n_f32)", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_i32|n_f32", "GREATEST('3.5', n_i32, n_f32)", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_i32|n_f32|LIT3.5", "GREATEST(n_i32, n_f32, '3.5')", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_i32|LIT3.5|n_f32", "LEAST(n_i32, '3.5', n_f32)", "real", "0.1|3.5|3.5|-5"},
		{"CASE|n_i32|LIT3.5|n_f32", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '3.5' ELSE n_f32 END", "real", "3|3.5|1.6777216e+07|-0.5"},
		{"COALESCE|n_i32|LIT1e39|n_f32", "COALESCE(n_i32, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|n_i32|LIT1e39|n_f32", "GREATEST(n_i32, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_i32|n_f32", "GREATEST('1e39', n_i32, n_f32)", "ERR 22003", ""},
		{"GREATEST|n_i32|n_f32|LIT1e39", "GREATEST(n_i32, n_f32, '1e39')", "ERR 22003", ""},
		{"LEAST|n_i32|LIT1e39|n_f32", "LEAST(n_i32, '1e39', n_f32)", "ERR 22003", ""},
		{"CASE|n_i32|LIT1e39|n_f32", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '1e39' ELSE n_f32 END", "ERR 22003", ""},
		{"COALESCE|n_i32|LIT3.5|n_f64", "COALESCE(n_i32, '3.5', n_f64)", "double precision", "3|3.5|16777217|-5"},
		{"GREATEST|n_i32|LIT3.5|n_f64", "GREATEST(n_i32, '3.5', n_f64)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_i32|n_f64", "GREATEST('3.5', n_i32, n_f64)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|n_i32|n_f64|LIT3.5", "GREATEST(n_i32, n_f64, '3.5')", "double precision", "3.5|3.5|16777217|3.5"},
		{"LEAST|n_i32|LIT3.5|n_f64", "LEAST(n_i32, '3.5', n_f64)", "double precision", "0.2|3.5|3.5|-5"},
		{"CASE|n_i32|LIT3.5|n_f64", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '3.5' ELSE n_f64 END", "double precision", "3|3.5|16777217|-0.25"},
		{"COALESCE|n_i32|LIT1e39|n_f64", "COALESCE(n_i32, '1e39', n_f64)", "double precision", "3|1e+39|16777217|-5"},
		{"GREATEST|n_i32|LIT1e39|n_f64", "GREATEST(n_i32, '1e39', n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_i32|n_f64", "GREATEST('1e39', n_i32, n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_i32|n_f64|LIT1e39", "GREATEST(n_i32, n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_i32|LIT1e39|n_f64", "LEAST(n_i32, '1e39', n_f64)", "double precision", "0.2|1e+39|16777217|-5"},
		{"CASE|n_i32|LIT1e39|n_f64", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '1e39' ELSE n_f64 END", "double precision", "3|1e+39|16777217|-0.25"},
		{"COALESCE|n_i32|LIT3.5|n_d152", "COALESCE(n_i32, '3.5', n_d152)", "numeric", "3|3.5|16777217|-5"},
		{"GREATEST|n_i32|LIT3.5|n_d152", "GREATEST(n_i32, '3.5', n_d152)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_i32|n_d152", "GREATEST('3.5', n_i32, n_d152)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|n_i32|n_d152|LIT3.5", "GREATEST(n_i32, n_d152, '3.5')", "numeric", "12.75|3.5|16777217|3.5"},
		{"LEAST|n_i32|LIT3.5|n_d152", "LEAST(n_i32, '3.5', n_d152)", "numeric", "3|3.5|1.00|-5"},
		{"CASE|n_i32|LIT3.5|n_d152", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '3.5' ELSE n_d152 END", "numeric", "3|3.5|1.00|-3.50"},
		{"COALESCE|n_i32|LIT1e39|n_d152", "COALESCE(n_i32, '1e39', n_d152)", "numeric", "3|1000000000000000000000000000000000000000|16777217|-5"},
		{"GREATEST|n_i32|LIT1e39|n_d152", "GREATEST(n_i32, '1e39', n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_i32|n_d152", "GREATEST('1e39', n_i32, n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_i32|n_d152|LIT1e39", "GREATEST(n_i32, n_d152, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_i32|LIT1e39|n_d152", "LEAST(n_i32, '1e39', n_d152)", "numeric", "3|1000000000000000000000000000000000000000|1.00|-5"},
		{"CASE|n_i32|LIT1e39|n_d152", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '1e39' ELSE n_d152 END", "numeric", "3|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"COALESCE|n_i32|LIT3.5|n_d3810", "COALESCE(n_i32, '3.5', n_d3810)", "numeric", "3|3.5|16777217|-5"},
		{"GREATEST|n_i32|LIT3.5|n_d3810", "GREATEST(n_i32, '3.5', n_d3810)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_i32|n_d3810", "GREATEST('3.5', n_i32, n_d3810)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|n_i32|n_d3810|LIT3.5", "GREATEST(n_i32, n_d3810, '3.5')", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"LEAST|n_i32|LIT3.5|n_d3810", "LEAST(n_i32, '3.5', n_d3810)", "numeric", "3|3.5|1.0000000000|-5"},
		{"CASE|n_i32|LIT3.5|n_d3810", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '3.5' ELSE n_d3810 END", "numeric", "3|3.5|1.0000000000|-3.5000000000"},
		{"COALESCE|n_i32|LIT1e39|n_d3810", "COALESCE(n_i32, '1e39', n_d3810)", "numeric", "3|1000000000000000000000000000000000000000|16777217|-5"},
		{"GREATEST|n_i32|LIT1e39|n_d3810", "GREATEST(n_i32, '1e39', n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_i32|n_d3810", "GREATEST('1e39', n_i32, n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_i32|n_d3810|LIT1e39", "GREATEST(n_i32, n_d3810, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_i32|LIT1e39|n_d3810", "LEAST(n_i32, '1e39', n_d3810)", "numeric", "3|1000000000000000000000000000000000000000|1.0000000000|-5"},
		{"CASE|n_i32|LIT1e39|n_d3810", "CASE WHEN id=1 THEN n_i32 WHEN id=2 THEN '1e39' ELSE n_d3810 END", "numeric", "3|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"COALESCE|n_i64|LIT3.5|n_i32", "COALESCE(n_i64, '3.5', n_i32)", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT3.5|n_i32", "GREATEST(n_i64, '3.5', n_i32)", "ERR 22P02", ""},
		{"GREATEST|LIT3.5|n_i64|n_i32", "GREATEST('3.5', n_i64, n_i32)", "ERR 22P02", ""},
		{"GREATEST|n_i64|n_i32|LIT3.5", "GREATEST(n_i64, n_i32, '3.5')", "ERR 22P02", ""},
		{"LEAST|n_i64|LIT3.5|n_i32", "LEAST(n_i64, '3.5', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT3.5|n_i32", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '3.5' ELSE n_i32 END", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT1e39|n_i32", "COALESCE(n_i64, '1e39', n_i32)", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT1e39|n_i32", "GREATEST(n_i64, '1e39', n_i32)", "ERR 22P02", ""},
		{"GREATEST|LIT1e39|n_i64|n_i32", "GREATEST('1e39', n_i64, n_i32)", "ERR 22P02", ""},
		{"GREATEST|n_i64|n_i32|LIT1e39", "GREATEST(n_i64, n_i32, '1e39')", "ERR 22P02", ""},
		{"LEAST|n_i64|LIT1e39|n_i32", "LEAST(n_i64, '1e39', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT1e39|n_i32", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '1e39' ELSE n_i32 END", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT3.5|n_f32", "COALESCE(n_i64, '3.5', n_f32)", "real", "4|3.5|1.6777216e+07|-6"},
		{"GREATEST|n_i64|LIT3.5|n_f32", "GREATEST(n_i64, '3.5', n_f32)", "real", "4|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_i64|n_f32", "GREATEST('3.5', n_i64, n_f32)", "real", "4|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_i64|n_f32|LIT3.5", "GREATEST(n_i64, n_f32, '3.5')", "real", "4|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_i64|LIT3.5|n_f32", "LEAST(n_i64, '3.5', n_f32)", "real", "0.1|3.5|3.5|-6"},
		{"CASE|n_i64|LIT3.5|n_f32", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '3.5' ELSE n_f32 END", "real", "4|3.5|1.6777216e+07|-0.5"},
		{"COALESCE|n_i64|LIT1e39|n_f32", "COALESCE(n_i64, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|n_i64|LIT1e39|n_f32", "GREATEST(n_i64, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_i64|n_f32", "GREATEST('1e39', n_i64, n_f32)", "ERR 22003", ""},
		{"GREATEST|n_i64|n_f32|LIT1e39", "GREATEST(n_i64, n_f32, '1e39')", "ERR 22003", ""},
		{"LEAST|n_i64|LIT1e39|n_f32", "LEAST(n_i64, '1e39', n_f32)", "ERR 22003", ""},
		{"CASE|n_i64|LIT1e39|n_f32", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '1e39' ELSE n_f32 END", "ERR 22003", ""},
		{"COALESCE|n_i64|LIT3.5|n_f64", "COALESCE(n_i64, '3.5', n_f64)", "double precision", "4|3.5|16777217|-6"},
		{"GREATEST|n_i64|LIT3.5|n_f64", "GREATEST(n_i64, '3.5', n_f64)", "double precision", "4|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_i64|n_f64", "GREATEST('3.5', n_i64, n_f64)", "double precision", "4|3.5|16777217|3.5"},
		{"GREATEST|n_i64|n_f64|LIT3.5", "GREATEST(n_i64, n_f64, '3.5')", "double precision", "4|3.5|16777217|3.5"},
		{"LEAST|n_i64|LIT3.5|n_f64", "LEAST(n_i64, '3.5', n_f64)", "double precision", "0.2|3.5|3.5|-6"},
		{"CASE|n_i64|LIT3.5|n_f64", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '3.5' ELSE n_f64 END", "double precision", "4|3.5|16777217|-0.25"},
		{"COALESCE|n_i64|LIT1e39|n_f64", "COALESCE(n_i64, '1e39', n_f64)", "double precision", "4|1e+39|16777217|-6"},
		{"GREATEST|n_i64|LIT1e39|n_f64", "GREATEST(n_i64, '1e39', n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_i64|n_f64", "GREATEST('1e39', n_i64, n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_i64|n_f64|LIT1e39", "GREATEST(n_i64, n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_i64|LIT1e39|n_f64", "LEAST(n_i64, '1e39', n_f64)", "double precision", "0.2|1e+39|16777217|-6"},
		{"CASE|n_i64|LIT1e39|n_f64", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '1e39' ELSE n_f64 END", "double precision", "4|1e+39|16777217|-0.25"},
		{"COALESCE|n_i64|LIT3.5|n_d152", "COALESCE(n_i64, '3.5', n_d152)", "numeric", "4|3.5|16777217|-6"},
		{"GREATEST|n_i64|LIT3.5|n_d152", "GREATEST(n_i64, '3.5', n_d152)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_i64|n_d152", "GREATEST('3.5', n_i64, n_d152)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|n_i64|n_d152|LIT3.5", "GREATEST(n_i64, n_d152, '3.5')", "numeric", "12.75|3.5|16777217|3.5"},
		{"LEAST|n_i64|LIT3.5|n_d152", "LEAST(n_i64, '3.5', n_d152)", "numeric", "3.5|3.5|1.00|-6"},
		{"CASE|n_i64|LIT3.5|n_d152", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '3.5' ELSE n_d152 END", "numeric", "4|3.5|1.00|-3.50"},
		{"COALESCE|n_i64|LIT1e39|n_d152", "COALESCE(n_i64, '1e39', n_d152)", "numeric", "4|1000000000000000000000000000000000000000|16777217|-6"},
		{"GREATEST|n_i64|LIT1e39|n_d152", "GREATEST(n_i64, '1e39', n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_i64|n_d152", "GREATEST('1e39', n_i64, n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_i64|n_d152|LIT1e39", "GREATEST(n_i64, n_d152, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_i64|LIT1e39|n_d152", "LEAST(n_i64, '1e39', n_d152)", "numeric", "4|1000000000000000000000000000000000000000|1.00|-6"},
		{"CASE|n_i64|LIT1e39|n_d152", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '1e39' ELSE n_d152 END", "numeric", "4|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"COALESCE|n_i64|LIT3.5|n_d3810", "COALESCE(n_i64, '3.5', n_d3810)", "numeric", "4|3.5|16777217|-6"},
		{"GREATEST|n_i64|LIT3.5|n_d3810", "GREATEST(n_i64, '3.5', n_d3810)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_i64|n_d3810", "GREATEST('3.5', n_i64, n_d3810)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|n_i64|n_d3810|LIT3.5", "GREATEST(n_i64, n_d3810, '3.5')", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"LEAST|n_i64|LIT3.5|n_d3810", "LEAST(n_i64, '3.5', n_d3810)", "numeric", "3.5|3.5|1.0000000000|-6"},
		{"CASE|n_i64|LIT3.5|n_d3810", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '3.5' ELSE n_d3810 END", "numeric", "4|3.5|1.0000000000|-3.5000000000"},
		{"COALESCE|n_i64|LIT1e39|n_d3810", "COALESCE(n_i64, '1e39', n_d3810)", "numeric", "4|1000000000000000000000000000000000000000|16777217|-6"},
		{"GREATEST|n_i64|LIT1e39|n_d3810", "GREATEST(n_i64, '1e39', n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_i64|n_d3810", "GREATEST('1e39', n_i64, n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_i64|n_d3810|LIT1e39", "GREATEST(n_i64, n_d3810, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_i64|LIT1e39|n_d3810", "LEAST(n_i64, '1e39', n_d3810)", "numeric", "4|1000000000000000000000000000000000000000|1.0000000000|-6"},
		{"CASE|n_i64|LIT1e39|n_d3810", "CASE WHEN id=1 THEN n_i64 WHEN id=2 THEN '1e39' ELSE n_d3810 END", "numeric", "4|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"COALESCE|n_f32|LIT3.5|n_i32", "COALESCE(n_f32, '3.5', n_i32)", "real", "0.1|3.5|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|LIT3.5|n_i32", "GREATEST(n_f32, '3.5', n_i32)", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_f32|n_i32", "GREATEST('3.5', n_f32, n_i32)", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_f32|n_i32|LIT3.5", "GREATEST(n_f32, n_i32, '3.5')", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_f32|LIT3.5|n_i32", "LEAST(n_f32, '3.5', n_i32)", "real", "0.1|3.5|3.5|-5"},
		{"CASE|n_f32|LIT3.5|n_i32", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '3.5' ELSE n_i32 END", "real", "0.1|3.5|1.6777216e+07|-5"},
		{"COALESCE|n_f32|LIT1e39|n_i32", "COALESCE(n_f32, '1e39', n_i32)", "ERR 22003", ""},
		{"GREATEST|n_f32|LIT1e39|n_i32", "GREATEST(n_f32, '1e39', n_i32)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_f32|n_i32", "GREATEST('1e39', n_f32, n_i32)", "ERR 22003", ""},
		{"GREATEST|n_f32|n_i32|LIT1e39", "GREATEST(n_f32, n_i32, '1e39')", "ERR 22003", ""},
		{"LEAST|n_f32|LIT1e39|n_i32", "LEAST(n_f32, '1e39', n_i32)", "ERR 22003", ""},
		{"CASE|n_f32|LIT1e39|n_i32", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '1e39' ELSE n_i32 END", "ERR 22003", ""},
		{"COALESCE|n_f32|LIT3.5|n_i64", "COALESCE(n_f32, '3.5', n_i64)", "real", "0.1|3.5|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|LIT3.5|n_i64", "GREATEST(n_f32, '3.5', n_i64)", "real", "4|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_f32|n_i64", "GREATEST('3.5', n_f32, n_i64)", "real", "4|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_f32|n_i64|LIT3.5", "GREATEST(n_f32, n_i64, '3.5')", "real", "4|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_f32|LIT3.5|n_i64", "LEAST(n_f32, '3.5', n_i64)", "real", "0.1|3.5|3.5|-6"},
		{"CASE|n_f32|LIT3.5|n_i64", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '3.5' ELSE n_i64 END", "real", "0.1|3.5|1.6777216e+07|-6"},
		{"COALESCE|n_f32|LIT1e39|n_i64", "COALESCE(n_f32, '1e39', n_i64)", "ERR 22003", ""},
		{"GREATEST|n_f32|LIT1e39|n_i64", "GREATEST(n_f32, '1e39', n_i64)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_f32|n_i64", "GREATEST('1e39', n_f32, n_i64)", "ERR 22003", ""},
		{"GREATEST|n_f32|n_i64|LIT1e39", "GREATEST(n_f32, n_i64, '1e39')", "ERR 22003", ""},
		{"LEAST|n_f32|LIT1e39|n_i64", "LEAST(n_f32, '1e39', n_i64)", "ERR 22003", ""},
		{"CASE|n_f32|LIT1e39|n_i64", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '1e39' ELSE n_i64 END", "ERR 22003", ""},
		{"COALESCE|n_f32|LIT3.5|n_f64", "COALESCE(n_f32, '3.5', n_f64)", "double precision", "0.10000000149011612|3.5|16777216|-0.5"},
		{"GREATEST|n_f32|LIT3.5|n_f64", "GREATEST(n_f32, '3.5', n_f64)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_f32|n_f64", "GREATEST('3.5', n_f32, n_f64)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|n_f32|n_f64|LIT3.5", "GREATEST(n_f32, n_f64, '3.5')", "double precision", "3.5|3.5|16777217|3.5"},
		{"LEAST|n_f32|LIT3.5|n_f64", "LEAST(n_f32, '3.5', n_f64)", "double precision", "0.10000000149011612|3.5|3.5|-0.5"},
		{"CASE|n_f32|LIT3.5|n_f64", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '3.5' ELSE n_f64 END", "double precision", "0.10000000149011612|3.5|16777217|-0.25"},
		{"COALESCE|n_f32|LIT1e39|n_f64", "COALESCE(n_f32, '1e39', n_f64)", "double precision", "0.10000000149011612|1e+39|16777216|-0.5"},
		{"GREATEST|n_f32|LIT1e39|n_f64", "GREATEST(n_f32, '1e39', n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_f32|n_f64", "GREATEST('1e39', n_f32, n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f32|n_f64|LIT1e39", "GREATEST(n_f32, n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_f32|LIT1e39|n_f64", "LEAST(n_f32, '1e39', n_f64)", "double precision", "0.10000000149011612|1e+39|16777216|-0.5"},
		{"CASE|n_f32|LIT1e39|n_f64", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '1e39' ELSE n_f64 END", "double precision", "0.10000000149011612|1e+39|16777217|-0.25"},
		{"COALESCE|n_f32|LIT3.5|n_d152", "COALESCE(n_f32, '3.5', n_d152)", "real", "0.1|3.5|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|LIT3.5|n_d152", "GREATEST(n_f32, '3.5', n_d152)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_f32|n_d152", "GREATEST('3.5', n_f32, n_d152)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_f32|n_d152|LIT3.5", "GREATEST(n_f32, n_d152, '3.5')", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_f32|LIT3.5|n_d152", "LEAST(n_f32, '3.5', n_d152)", "real", "0.1|3.5|1|-3.5"},
		{"CASE|n_f32|LIT3.5|n_d152", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '3.5' ELSE n_d152 END", "real", "0.1|3.5|1|-3.5"},
		{"COALESCE|n_f32|LIT1e39|n_d152", "COALESCE(n_f32, '1e39', n_d152)", "ERR 22003", ""},
		{"GREATEST|n_f32|LIT1e39|n_d152", "GREATEST(n_f32, '1e39', n_d152)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_f32|n_d152", "GREATEST('1e39', n_f32, n_d152)", "ERR 22003", ""},
		{"GREATEST|n_f32|n_d152|LIT1e39", "GREATEST(n_f32, n_d152, '1e39')", "ERR 22003", ""},
		{"LEAST|n_f32|LIT1e39|n_d152", "LEAST(n_f32, '1e39', n_d152)", "ERR 22003", ""},
		{"CASE|n_f32|LIT1e39|n_d152", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '1e39' ELSE n_d152 END", "ERR 22003", ""},
		{"COALESCE|n_f32|LIT3.5|n_d3810", "COALESCE(n_f32, '3.5', n_d3810)", "real", "0.1|3.5|1.6777216e+07|-0.5"},
		{"GREATEST|n_f32|LIT3.5|n_d3810", "GREATEST(n_f32, '3.5', n_d3810)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_f32|n_d3810", "GREATEST('3.5', n_f32, n_d3810)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_f32|n_d3810|LIT3.5", "GREATEST(n_f32, n_d3810, '3.5')", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_f32|LIT3.5|n_d3810", "LEAST(n_f32, '3.5', n_d3810)", "real", "0.1|3.5|1|-3.5"},
		{"CASE|n_f32|LIT3.5|n_d3810", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '3.5' ELSE n_d3810 END", "real", "0.1|3.5|1|-3.5"},
		{"COALESCE|n_f32|LIT1e39|n_d3810", "COALESCE(n_f32, '1e39', n_d3810)", "ERR 22003", ""},
		{"GREATEST|n_f32|LIT1e39|n_d3810", "GREATEST(n_f32, '1e39', n_d3810)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_f32|n_d3810", "GREATEST('1e39', n_f32, n_d3810)", "ERR 22003", ""},
		{"GREATEST|n_f32|n_d3810|LIT1e39", "GREATEST(n_f32, n_d3810, '1e39')", "ERR 22003", ""},
		{"LEAST|n_f32|LIT1e39|n_d3810", "LEAST(n_f32, '1e39', n_d3810)", "ERR 22003", ""},
		{"CASE|n_f32|LIT1e39|n_d3810", "CASE WHEN id=1 THEN n_f32 WHEN id=2 THEN '1e39' ELSE n_d3810 END", "ERR 22003", ""},
		{"COALESCE|n_f64|LIT3.5|n_i32", "COALESCE(n_f64, '3.5', n_i32)", "double precision", "0.2|3.5|16777217|-0.25"},
		{"GREATEST|n_f64|LIT3.5|n_i32", "GREATEST(n_f64, '3.5', n_i32)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_f64|n_i32", "GREATEST('3.5', n_f64, n_i32)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|n_f64|n_i32|LIT3.5", "GREATEST(n_f64, n_i32, '3.5')", "double precision", "3.5|3.5|16777217|3.5"},
		{"LEAST|n_f64|LIT3.5|n_i32", "LEAST(n_f64, '3.5', n_i32)", "double precision", "0.2|3.5|3.5|-5"},
		{"CASE|n_f64|LIT3.5|n_i32", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '3.5' ELSE n_i32 END", "double precision", "0.2|3.5|16777217|-5"},
		{"COALESCE|n_f64|LIT1e39|n_i32", "COALESCE(n_f64, '1e39', n_i32)", "double precision", "0.2|1e+39|16777217|-0.25"},
		{"GREATEST|n_f64|LIT1e39|n_i32", "GREATEST(n_f64, '1e39', n_i32)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_f64|n_i32", "GREATEST('1e39', n_f64, n_i32)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f64|n_i32|LIT1e39", "GREATEST(n_f64, n_i32, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_f64|LIT1e39|n_i32", "LEAST(n_f64, '1e39', n_i32)", "double precision", "0.2|1e+39|16777217|-5"},
		{"CASE|n_f64|LIT1e39|n_i32", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '1e39' ELSE n_i32 END", "double precision", "0.2|1e+39|16777217|-5"},
		{"COALESCE|n_f64|LIT3.5|n_i64", "COALESCE(n_f64, '3.5', n_i64)", "double precision", "0.2|3.5|16777217|-0.25"},
		{"GREATEST|n_f64|LIT3.5|n_i64", "GREATEST(n_f64, '3.5', n_i64)", "double precision", "4|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_f64|n_i64", "GREATEST('3.5', n_f64, n_i64)", "double precision", "4|3.5|16777217|3.5"},
		{"GREATEST|n_f64|n_i64|LIT3.5", "GREATEST(n_f64, n_i64, '3.5')", "double precision", "4|3.5|16777217|3.5"},
		{"LEAST|n_f64|LIT3.5|n_i64", "LEAST(n_f64, '3.5', n_i64)", "double precision", "0.2|3.5|3.5|-6"},
		{"CASE|n_f64|LIT3.5|n_i64", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '3.5' ELSE n_i64 END", "double precision", "0.2|3.5|16777217|-6"},
		{"COALESCE|n_f64|LIT1e39|n_i64", "COALESCE(n_f64, '1e39', n_i64)", "double precision", "0.2|1e+39|16777217|-0.25"},
		{"GREATEST|n_f64|LIT1e39|n_i64", "GREATEST(n_f64, '1e39', n_i64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_f64|n_i64", "GREATEST('1e39', n_f64, n_i64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f64|n_i64|LIT1e39", "GREATEST(n_f64, n_i64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_f64|LIT1e39|n_i64", "LEAST(n_f64, '1e39', n_i64)", "double precision", "0.2|1e+39|16777217|-6"},
		{"CASE|n_f64|LIT1e39|n_i64", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '1e39' ELSE n_i64 END", "double precision", "0.2|1e+39|16777217|-6"},
		{"COALESCE|n_f64|LIT3.5|n_f32", "COALESCE(n_f64, '3.5', n_f32)", "double precision", "0.2|3.5|16777217|-0.25"},
		{"GREATEST|n_f64|LIT3.5|n_f32", "GREATEST(n_f64, '3.5', n_f32)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_f64|n_f32", "GREATEST('3.5', n_f64, n_f32)", "double precision", "3.5|3.5|16777217|3.5"},
		{"GREATEST|n_f64|n_f32|LIT3.5", "GREATEST(n_f64, n_f32, '3.5')", "double precision", "3.5|3.5|16777217|3.5"},
		{"LEAST|n_f64|LIT3.5|n_f32", "LEAST(n_f64, '3.5', n_f32)", "double precision", "0.10000000149011612|3.5|3.5|-0.5"},
		{"CASE|n_f64|LIT3.5|n_f32", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '3.5' ELSE n_f32 END", "double precision", "0.2|3.5|16777216|-0.5"},
		{"COALESCE|n_f64|LIT1e39|n_f32", "COALESCE(n_f64, '1e39', n_f32)", "double precision", "0.2|1e+39|16777217|-0.25"},
		{"GREATEST|n_f64|LIT1e39|n_f32", "GREATEST(n_f64, '1e39', n_f32)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_f64|n_f32", "GREATEST('1e39', n_f64, n_f32)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f64|n_f32|LIT1e39", "GREATEST(n_f64, n_f32, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_f64|LIT1e39|n_f32", "LEAST(n_f64, '1e39', n_f32)", "double precision", "0.10000000149011612|1e+39|16777216|-0.5"},
		{"CASE|n_f64|LIT1e39|n_f32", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '1e39' ELSE n_f32 END", "double precision", "0.2|1e+39|16777216|-0.5"},
		{"COALESCE|n_f64|LIT3.5|n_d152", "COALESCE(n_f64, '3.5', n_d152)", "double precision", "0.2|3.5|16777217|-0.25"},
		{"GREATEST|n_f64|LIT3.5|n_d152", "GREATEST(n_f64, '3.5', n_d152)", "double precision", "12.75|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_f64|n_d152", "GREATEST('3.5', n_f64, n_d152)", "double precision", "12.75|3.5|16777217|3.5"},
		{"GREATEST|n_f64|n_d152|LIT3.5", "GREATEST(n_f64, n_d152, '3.5')", "double precision", "12.75|3.5|16777217|3.5"},
		{"LEAST|n_f64|LIT3.5|n_d152", "LEAST(n_f64, '3.5', n_d152)", "double precision", "0.2|3.5|1|-3.5"},
		{"CASE|n_f64|LIT3.5|n_d152", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '3.5' ELSE n_d152 END", "double precision", "0.2|3.5|1|-3.5"},
		{"COALESCE|n_f64|LIT1e39|n_d152", "COALESCE(n_f64, '1e39', n_d152)", "double precision", "0.2|1e+39|16777217|-0.25"},
		{"GREATEST|n_f64|LIT1e39|n_d152", "GREATEST(n_f64, '1e39', n_d152)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_f64|n_d152", "GREATEST('1e39', n_f64, n_d152)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f64|n_d152|LIT1e39", "GREATEST(n_f64, n_d152, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_f64|LIT1e39|n_d152", "LEAST(n_f64, '1e39', n_d152)", "double precision", "0.2|1e+39|1|-3.5"},
		{"CASE|n_f64|LIT1e39|n_d152", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '1e39' ELSE n_d152 END", "double precision", "0.2|1e+39|1|-3.5"},
		{"COALESCE|n_f64|LIT3.5|n_d3810", "COALESCE(n_f64, '3.5', n_d3810)", "double precision", "0.2|3.5|16777217|-0.25"},
		{"GREATEST|n_f64|LIT3.5|n_d3810", "GREATEST(n_f64, '3.5', n_d3810)", "double precision", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_f64|n_d3810", "GREATEST('3.5', n_f64, n_d3810)", "double precision", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|n_f64|n_d3810|LIT3.5", "GREATEST(n_f64, n_d3810, '3.5')", "double precision", "12.7500000001|3.5|16777217|3.5"},
		{"LEAST|n_f64|LIT3.5|n_d3810", "LEAST(n_f64, '3.5', n_d3810)", "double precision", "0.2|3.5|1|-3.5"},
		{"CASE|n_f64|LIT3.5|n_d3810", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '3.5' ELSE n_d3810 END", "double precision", "0.2|3.5|1|-3.5"},
		{"COALESCE|n_f64|LIT1e39|n_d3810", "COALESCE(n_f64, '1e39', n_d3810)", "double precision", "0.2|1e+39|16777217|-0.25"},
		{"GREATEST|n_f64|LIT1e39|n_d3810", "GREATEST(n_f64, '1e39', n_d3810)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_f64|n_d3810", "GREATEST('1e39', n_f64, n_d3810)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f64|n_d3810|LIT1e39", "GREATEST(n_f64, n_d3810, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_f64|LIT1e39|n_d3810", "LEAST(n_f64, '1e39', n_d3810)", "double precision", "0.2|1e+39|1|-3.5"},
		{"CASE|n_f64|LIT1e39|n_d3810", "CASE WHEN id=1 THEN n_f64 WHEN id=2 THEN '1e39' ELSE n_d3810 END", "double precision", "0.2|1e+39|1|-3.5"},
		{"COALESCE|n_d152|LIT3.5|n_i32", "COALESCE(n_d152, '3.5', n_i32)", "numeric", "12.75|3.5|1.00|-3.50"},
		{"GREATEST|n_d152|LIT3.5|n_i32", "GREATEST(n_d152, '3.5', n_i32)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_d152|n_i32", "GREATEST('3.5', n_d152, n_i32)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|n_d152|n_i32|LIT3.5", "GREATEST(n_d152, n_i32, '3.5')", "numeric", "12.75|3.5|16777217|3.5"},
		{"LEAST|n_d152|LIT3.5|n_i32", "LEAST(n_d152, '3.5', n_i32)", "numeric", "3|3.5|1.00|-5"},
		{"CASE|n_d152|LIT3.5|n_i32", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '3.5' ELSE n_i32 END", "numeric", "12.75|3.5|16777217|-5"},
		{"COALESCE|n_d152|LIT1e39|n_i32", "COALESCE(n_d152, '1e39', n_i32)", "numeric", "12.75|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"GREATEST|n_d152|LIT1e39|n_i32", "GREATEST(n_d152, '1e39', n_i32)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_d152|n_i32", "GREATEST('1e39', n_d152, n_i32)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d152|n_i32|LIT1e39", "GREATEST(n_d152, n_i32, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_d152|LIT1e39|n_i32", "LEAST(n_d152, '1e39', n_i32)", "numeric", "3|1000000000000000000000000000000000000000|1.00|-5"},
		{"CASE|n_d152|LIT1e39|n_i32", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '1e39' ELSE n_i32 END", "numeric", "12.75|1000000000000000000000000000000000000000|16777217|-5"},
		{"COALESCE|n_d152|LIT3.5|n_i64", "COALESCE(n_d152, '3.5', n_i64)", "numeric", "12.75|3.5|1.00|-3.50"},
		{"GREATEST|n_d152|LIT3.5|n_i64", "GREATEST(n_d152, '3.5', n_i64)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_d152|n_i64", "GREATEST('3.5', n_d152, n_i64)", "numeric", "12.75|3.5|16777217|3.5"},
		{"GREATEST|n_d152|n_i64|LIT3.5", "GREATEST(n_d152, n_i64, '3.5')", "numeric", "12.75|3.5|16777217|3.5"},
		{"LEAST|n_d152|LIT3.5|n_i64", "LEAST(n_d152, '3.5', n_i64)", "numeric", "3.5|3.5|1.00|-6"},
		{"CASE|n_d152|LIT3.5|n_i64", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '3.5' ELSE n_i64 END", "numeric", "12.75|3.5|16777217|-6"},
		{"COALESCE|n_d152|LIT1e39|n_i64", "COALESCE(n_d152, '1e39', n_i64)", "numeric", "12.75|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"GREATEST|n_d152|LIT1e39|n_i64", "GREATEST(n_d152, '1e39', n_i64)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_d152|n_i64", "GREATEST('1e39', n_d152, n_i64)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d152|n_i64|LIT1e39", "GREATEST(n_d152, n_i64, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_d152|LIT1e39|n_i64", "LEAST(n_d152, '1e39', n_i64)", "numeric", "4|1000000000000000000000000000000000000000|1.00|-6"},
		{"CASE|n_d152|LIT1e39|n_i64", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '1e39' ELSE n_i64 END", "numeric", "12.75|1000000000000000000000000000000000000000|16777217|-6"},
		{"COALESCE|n_d152|LIT3.5|n_f32", "COALESCE(n_d152, '3.5', n_f32)", "real", "12.75|3.5|1|-3.5"},
		{"GREATEST|n_d152|LIT3.5|n_f32", "GREATEST(n_d152, '3.5', n_f32)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_d152|n_f32", "GREATEST('3.5', n_d152, n_f32)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_d152|n_f32|LIT3.5", "GREATEST(n_d152, n_f32, '3.5')", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_d152|LIT3.5|n_f32", "LEAST(n_d152, '3.5', n_f32)", "real", "0.1|3.5|1|-3.5"},
		{"CASE|n_d152|LIT3.5|n_f32", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '3.5' ELSE n_f32 END", "real", "12.75|3.5|1.6777216e+07|-0.5"},
		{"COALESCE|n_d152|LIT1e39|n_f32", "COALESCE(n_d152, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|n_d152|LIT1e39|n_f32", "GREATEST(n_d152, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_d152|n_f32", "GREATEST('1e39', n_d152, n_f32)", "ERR 22003", ""},
		{"GREATEST|n_d152|n_f32|LIT1e39", "GREATEST(n_d152, n_f32, '1e39')", "ERR 22003", ""},
		{"LEAST|n_d152|LIT1e39|n_f32", "LEAST(n_d152, '1e39', n_f32)", "ERR 22003", ""},
		{"CASE|n_d152|LIT1e39|n_f32", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '1e39' ELSE n_f32 END", "ERR 22003", ""},
		{"COALESCE|n_d152|LIT3.5|n_f64", "COALESCE(n_d152, '3.5', n_f64)", "double precision", "12.75|3.5|1|-3.5"},
		{"GREATEST|n_d152|LIT3.5|n_f64", "GREATEST(n_d152, '3.5', n_f64)", "double precision", "12.75|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_d152|n_f64", "GREATEST('3.5', n_d152, n_f64)", "double precision", "12.75|3.5|16777217|3.5"},
		{"GREATEST|n_d152|n_f64|LIT3.5", "GREATEST(n_d152, n_f64, '3.5')", "double precision", "12.75|3.5|16777217|3.5"},
		{"LEAST|n_d152|LIT3.5|n_f64", "LEAST(n_d152, '3.5', n_f64)", "double precision", "0.2|3.5|1|-3.5"},
		{"CASE|n_d152|LIT3.5|n_f64", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '3.5' ELSE n_f64 END", "double precision", "12.75|3.5|16777217|-0.25"},
		{"COALESCE|n_d152|LIT1e39|n_f64", "COALESCE(n_d152, '1e39', n_f64)", "double precision", "12.75|1e+39|1|-3.5"},
		{"GREATEST|n_d152|LIT1e39|n_f64", "GREATEST(n_d152, '1e39', n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_d152|n_f64", "GREATEST('1e39', n_d152, n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_d152|n_f64|LIT1e39", "GREATEST(n_d152, n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_d152|LIT1e39|n_f64", "LEAST(n_d152, '1e39', n_f64)", "double precision", "0.2|1e+39|1|-3.5"},
		{"CASE|n_d152|LIT1e39|n_f64", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '1e39' ELSE n_f64 END", "double precision", "12.75|1e+39|16777217|-0.25"},
		{"COALESCE|n_d152|LIT3.5|n_d3810", "COALESCE(n_d152, '3.5', n_d3810)", "numeric", "12.75|3.5|1.00|-3.50"},
		{"GREATEST|n_d152|LIT3.5|n_d3810", "GREATEST(n_d152, '3.5', n_d3810)", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"GREATEST|LIT3.5|n_d152|n_d3810", "GREATEST('3.5', n_d152, n_d3810)", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"GREATEST|n_d152|n_d3810|LIT3.5", "GREATEST(n_d152, n_d3810, '3.5')", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"LEAST|n_d152|LIT3.5|n_d3810", "LEAST(n_d152, '3.5', n_d3810)", "numeric", "3.5|3.5|1.00|-3.50"},
		{"CASE|n_d152|LIT3.5|n_d3810", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '3.5' ELSE n_d3810 END", "numeric", "12.75|3.5|1.0000000000|-3.5000000000"},
		{"COALESCE|n_d152|LIT1e39|n_d3810", "COALESCE(n_d152, '1e39', n_d3810)", "numeric", "12.75|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"GREATEST|n_d152|LIT1e39|n_d3810", "GREATEST(n_d152, '1e39', n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_d152|n_d3810", "GREATEST('1e39', n_d152, n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d152|n_d3810|LIT1e39", "GREATEST(n_d152, n_d3810, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_d152|LIT1e39|n_d3810", "LEAST(n_d152, '1e39', n_d3810)", "numeric", "12.75|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"CASE|n_d152|LIT1e39|n_d3810", "CASE WHEN id=1 THEN n_d152 WHEN id=2 THEN '1e39' ELSE n_d3810 END", "numeric", "12.75|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"COALESCE|n_d3810|LIT3.5|n_i32", "COALESCE(n_d3810, '3.5', n_i32)", "numeric", "12.7500000001|3.5|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|LIT3.5|n_i32", "GREATEST(n_d3810, '3.5', n_i32)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_d3810|n_i32", "GREATEST('3.5', n_d3810, n_i32)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|n_d3810|n_i32|LIT3.5", "GREATEST(n_d3810, n_i32, '3.5')", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"LEAST|n_d3810|LIT3.5|n_i32", "LEAST(n_d3810, '3.5', n_i32)", "numeric", "3|3.5|1.0000000000|-5"},
		{"CASE|n_d3810|LIT3.5|n_i32", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '3.5' ELSE n_i32 END", "numeric", "12.7500000001|3.5|16777217|-5"},
		{"COALESCE|n_d3810|LIT1e39|n_i32", "COALESCE(n_d3810, '1e39', n_i32)", "numeric", "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|LIT1e39|n_i32", "GREATEST(n_d3810, '1e39', n_i32)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_d3810|n_i32", "GREATEST('1e39', n_d3810, n_i32)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d3810|n_i32|LIT1e39", "GREATEST(n_d3810, n_i32, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_d3810|LIT1e39|n_i32", "LEAST(n_d3810, '1e39', n_i32)", "numeric", "3|1000000000000000000000000000000000000000|1.0000000000|-5"},
		{"CASE|n_d3810|LIT1e39|n_i32", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '1e39' ELSE n_i32 END", "numeric", "12.7500000001|1000000000000000000000000000000000000000|16777217|-5"},
		{"COALESCE|n_d3810|LIT3.5|n_i64", "COALESCE(n_d3810, '3.5', n_i64)", "numeric", "12.7500000001|3.5|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|LIT3.5|n_i64", "GREATEST(n_d3810, '3.5', n_i64)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_d3810|n_i64", "GREATEST('3.5', n_d3810, n_i64)", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|n_d3810|n_i64|LIT3.5", "GREATEST(n_d3810, n_i64, '3.5')", "numeric", "12.7500000001|3.5|16777217|3.5"},
		{"LEAST|n_d3810|LIT3.5|n_i64", "LEAST(n_d3810, '3.5', n_i64)", "numeric", "3.5|3.5|1.0000000000|-6"},
		{"CASE|n_d3810|LIT3.5|n_i64", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '3.5' ELSE n_i64 END", "numeric", "12.7500000001|3.5|16777217|-6"},
		{"COALESCE|n_d3810|LIT1e39|n_i64", "COALESCE(n_d3810, '1e39', n_i64)", "numeric", "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|LIT1e39|n_i64", "GREATEST(n_d3810, '1e39', n_i64)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_d3810|n_i64", "GREATEST('1e39', n_d3810, n_i64)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d3810|n_i64|LIT1e39", "GREATEST(n_d3810, n_i64, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_d3810|LIT1e39|n_i64", "LEAST(n_d3810, '1e39', n_i64)", "numeric", "4|1000000000000000000000000000000000000000|1.0000000000|-6"},
		{"CASE|n_d3810|LIT1e39|n_i64", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '1e39' ELSE n_i64 END", "numeric", "12.7500000001|1000000000000000000000000000000000000000|16777217|-6"},
		{"COALESCE|n_d3810|LIT3.5|n_f32", "COALESCE(n_d3810, '3.5', n_f32)", "real", "12.75|3.5|1|-3.5"},
		{"GREATEST|n_d3810|LIT3.5|n_f32", "GREATEST(n_d3810, '3.5', n_f32)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|LIT3.5|n_d3810|n_f32", "GREATEST('3.5', n_d3810, n_f32)", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"GREATEST|n_d3810|n_f32|LIT3.5", "GREATEST(n_d3810, n_f32, '3.5')", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"LEAST|n_d3810|LIT3.5|n_f32", "LEAST(n_d3810, '3.5', n_f32)", "real", "0.1|3.5|1|-3.5"},
		{"CASE|n_d3810|LIT3.5|n_f32", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '3.5' ELSE n_f32 END", "real", "12.75|3.5|1.6777216e+07|-0.5"},
		{"COALESCE|n_d3810|LIT1e39|n_f32", "COALESCE(n_d3810, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|n_d3810|LIT1e39|n_f32", "GREATEST(n_d3810, '1e39', n_f32)", "ERR 22003", ""},
		{"GREATEST|LIT1e39|n_d3810|n_f32", "GREATEST('1e39', n_d3810, n_f32)", "ERR 22003", ""},
		{"GREATEST|n_d3810|n_f32|LIT1e39", "GREATEST(n_d3810, n_f32, '1e39')", "ERR 22003", ""},
		{"LEAST|n_d3810|LIT1e39|n_f32", "LEAST(n_d3810, '1e39', n_f32)", "ERR 22003", ""},
		{"CASE|n_d3810|LIT1e39|n_f32", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '1e39' ELSE n_f32 END", "ERR 22003", ""},
		{"COALESCE|n_d3810|LIT3.5|n_f64", "COALESCE(n_d3810, '3.5', n_f64)", "double precision", "12.7500000001|3.5|1|-3.5"},
		{"GREATEST|n_d3810|LIT3.5|n_f64", "GREATEST(n_d3810, '3.5', n_f64)", "double precision", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|LIT3.5|n_d3810|n_f64", "GREATEST('3.5', n_d3810, n_f64)", "double precision", "12.7500000001|3.5|16777217|3.5"},
		{"GREATEST|n_d3810|n_f64|LIT3.5", "GREATEST(n_d3810, n_f64, '3.5')", "double precision", "12.7500000001|3.5|16777217|3.5"},
		{"LEAST|n_d3810|LIT3.5|n_f64", "LEAST(n_d3810, '3.5', n_f64)", "double precision", "0.2|3.5|1|-3.5"},
		{"CASE|n_d3810|LIT3.5|n_f64", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '3.5' ELSE n_f64 END", "double precision", "12.7500000001|3.5|16777217|-0.25"},
		{"COALESCE|n_d3810|LIT1e39|n_f64", "COALESCE(n_d3810, '1e39', n_f64)", "double precision", "12.7500000001|1e+39|1|-3.5"},
		{"GREATEST|n_d3810|LIT1e39|n_f64", "GREATEST(n_d3810, '1e39', n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|LIT1e39|n_d3810|n_f64", "GREATEST('1e39', n_d3810, n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"GREATEST|n_d3810|n_f64|LIT1e39", "GREATEST(n_d3810, n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"LEAST|n_d3810|LIT1e39|n_f64", "LEAST(n_d3810, '1e39', n_f64)", "double precision", "0.2|1e+39|1|-3.5"},
		{"CASE|n_d3810|LIT1e39|n_f64", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '1e39' ELSE n_f64 END", "double precision", "12.7500000001|1e+39|16777217|-0.25"},
		{"COALESCE|n_d3810|LIT3.5|n_d152", "COALESCE(n_d3810, '3.5', n_d152)", "numeric", "12.7500000001|3.5|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|LIT3.5|n_d152", "GREATEST(n_d3810, '3.5', n_d152)", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"GREATEST|LIT3.5|n_d3810|n_d152", "GREATEST('3.5', n_d3810, n_d152)", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"GREATEST|n_d3810|n_d152|LIT3.5", "GREATEST(n_d3810, n_d152, '3.5')", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"LEAST|n_d3810|LIT3.5|n_d152", "LEAST(n_d3810, '3.5', n_d152)", "numeric", "3.5|3.5|1.0000000000|-3.5000000000"},
		{"CASE|n_d3810|LIT3.5|n_d152", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '3.5' ELSE n_d152 END", "numeric", "12.7500000001|3.5|1.00|-3.50"},
		{"COALESCE|n_d3810|LIT1e39|n_d152", "COALESCE(n_d3810, '1e39', n_d152)", "numeric", "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"GREATEST|n_d3810|LIT1e39|n_d152", "GREATEST(n_d3810, '1e39', n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|LIT1e39|n_d3810|n_d152", "GREATEST('1e39', n_d3810, n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d3810|n_d152|LIT1e39", "GREATEST(n_d3810, n_d152, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"LEAST|n_d3810|LIT1e39|n_d152", "LEAST(n_d3810, '1e39', n_d152)", "numeric", "12.75|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"CASE|n_d3810|LIT1e39|n_d152", "CASE WHEN id=1 THEN n_d3810 WHEN id=2 THEN '1e39' ELSE n_d152 END", "numeric", "12.7500000001|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"GREATEST|n_i32|LIT1e39", "GREATEST(n_i32, '1e39')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT1e39", "COALESCE(n_i32, '1e39')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LIT1e39", "NULLIF(n_i32, '1e39')", "ERR 22P02", ""},
		{"NULLIF|LIT1e39|n_i32", "NULLIF('1e39', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT1e39", "CASE WHEN id=1 THEN n_i32 ELSE '1e39' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LITNaN", "GREATEST(n_i32, 'NaN')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LITNaN", "COALESCE(n_i32, 'NaN')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LITNaN", "NULLIF(n_i32, 'NaN')", "ERR 22P02", ""},
		{"NULLIF|LITNaN|n_i32", "NULLIF('NaN', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LITNaN", "CASE WHEN id=1 THEN n_i32 ELSE 'NaN' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LITInfinity", "GREATEST(n_i32, 'Infinity')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LITInfinity", "COALESCE(n_i32, 'Infinity')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LITInfinity", "NULLIF(n_i32, 'Infinity')", "ERR 22P02", ""},
		{"NULLIF|LITInfinity|n_i32", "NULLIF('Infinity', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LITInfinity", "CASE WHEN id=1 THEN n_i32 ELSE 'Infinity' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT12.750000000000000001", "GREATEST(n_i32, '12.750000000000000001')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT12.750000000000000001", "COALESCE(n_i32, '12.750000000000000001')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LIT12.750000000000000001", "NULLIF(n_i32, '12.750000000000000001')", "ERR 22P02", ""},
		{"NULLIF|LIT12.750000000000000001|n_i32", "NULLIF('12.750000000000000001', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT12.750000000000000001", "CASE WHEN id=1 THEN n_i32 ELSE '12.750000000000000001' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT0xC.C", "GREATEST(n_i32, '0xC.C')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT0xC.C", "COALESCE(n_i32, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LIT0xC.C", "NULLIF(n_i32, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|LIT0xC.C|n_i32", "NULLIF('0xC.C', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT0xC.C", "CASE WHEN id=1 THEN n_i32 ELSE '0xC.C' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT", "GREATEST(n_i32, '')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT", "COALESCE(n_i32, '')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LIT", "NULLIF(n_i32, '')", "ERR 22P02", ""},
		{"NULLIF|LIT|n_i32", "NULLIF('', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT", "CASE WHEN id=1 THEN n_i32 ELSE '' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LITabc", "GREATEST(n_i32, 'abc')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LITabc", "COALESCE(n_i32, 'abc')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LITabc", "NULLIF(n_i32, 'abc')", "ERR 22P02", ""},
		{"NULLIF|LITabc|n_i32", "NULLIF('abc', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LITabc", "CASE WHEN id=1 THEN n_i32 ELSE 'abc' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT3.5", "GREATEST(n_i32, '3.5')", "ERR 22P02", ""},
		{"COALESCE|n_i32|LIT3.5", "COALESCE(n_i32, '3.5')", "ERR 22P02", ""},
		{"NULLIF|n_i32|LIT3.5", "NULLIF(n_i32, '3.5')", "ERR 22P02", ""},
		{"NULLIF|LIT3.5|n_i32", "NULLIF('3.5', n_i32)", "ERR 22P02", ""},
		{"CASE|n_i32|LIT3.5", "CASE WHEN id=1 THEN n_i32 ELSE '3.5' END", "ERR 22P02", ""},
		{"GREATEST|n_i32|LIT16777217", "GREATEST(n_i32, '16777217')", "integer", "16777217|16777217|16777217|16777217"},
		{"COALESCE|n_i32|LIT16777217", "COALESCE(n_i32, '16777217')", "integer", "3|16777217|16777217|-5"},
		{"NULLIF|n_i32|LIT16777217", "NULLIF(n_i32, '16777217')", "integer", "3|<NULL>|<NULL>|-5"},
		{"NULLIF|LIT16777217|n_i32", "NULLIF('16777217', n_i32)", "integer", "16777217|16777217|<NULL>|16777217"},
		{"CASE|n_i32|LIT16777217", "CASE WHEN id=1 THEN n_i32 ELSE '16777217' END", "integer", "3|16777217|16777217|16777217"},
		{"GREATEST|n_i64|LIT1e39", "GREATEST(n_i64, '1e39')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT1e39", "COALESCE(n_i64, '1e39')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LIT1e39", "NULLIF(n_i64, '1e39')", "ERR 22P02", ""},
		{"NULLIF|LIT1e39|n_i64", "NULLIF('1e39', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT1e39", "CASE WHEN id=1 THEN n_i64 ELSE '1e39' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LITNaN", "GREATEST(n_i64, 'NaN')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LITNaN", "COALESCE(n_i64, 'NaN')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LITNaN", "NULLIF(n_i64, 'NaN')", "ERR 22P02", ""},
		{"NULLIF|LITNaN|n_i64", "NULLIF('NaN', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LITNaN", "CASE WHEN id=1 THEN n_i64 ELSE 'NaN' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LITInfinity", "GREATEST(n_i64, 'Infinity')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LITInfinity", "COALESCE(n_i64, 'Infinity')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LITInfinity", "NULLIF(n_i64, 'Infinity')", "ERR 22P02", ""},
		{"NULLIF|LITInfinity|n_i64", "NULLIF('Infinity', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LITInfinity", "CASE WHEN id=1 THEN n_i64 ELSE 'Infinity' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT12.750000000000000001", "GREATEST(n_i64, '12.750000000000000001')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT12.750000000000000001", "COALESCE(n_i64, '12.750000000000000001')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LIT12.750000000000000001", "NULLIF(n_i64, '12.750000000000000001')", "ERR 22P02", ""},
		{"NULLIF|LIT12.750000000000000001|n_i64", "NULLIF('12.750000000000000001', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT12.750000000000000001", "CASE WHEN id=1 THEN n_i64 ELSE '12.750000000000000001' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT0xC.C", "GREATEST(n_i64, '0xC.C')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT0xC.C", "COALESCE(n_i64, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LIT0xC.C", "NULLIF(n_i64, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|LIT0xC.C|n_i64", "NULLIF('0xC.C', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT0xC.C", "CASE WHEN id=1 THEN n_i64 ELSE '0xC.C' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT", "GREATEST(n_i64, '')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT", "COALESCE(n_i64, '')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LIT", "NULLIF(n_i64, '')", "ERR 22P02", ""},
		{"NULLIF|LIT|n_i64", "NULLIF('', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT", "CASE WHEN id=1 THEN n_i64 ELSE '' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LITabc", "GREATEST(n_i64, 'abc')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LITabc", "COALESCE(n_i64, 'abc')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LITabc", "NULLIF(n_i64, 'abc')", "ERR 22P02", ""},
		{"NULLIF|LITabc|n_i64", "NULLIF('abc', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LITabc", "CASE WHEN id=1 THEN n_i64 ELSE 'abc' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT3.5", "GREATEST(n_i64, '3.5')", "ERR 22P02", ""},
		{"COALESCE|n_i64|LIT3.5", "COALESCE(n_i64, '3.5')", "ERR 22P02", ""},
		{"NULLIF|n_i64|LIT3.5", "NULLIF(n_i64, '3.5')", "ERR 22P02", ""},
		{"NULLIF|LIT3.5|n_i64", "NULLIF('3.5', n_i64)", "ERR 22P02", ""},
		{"CASE|n_i64|LIT3.5", "CASE WHEN id=1 THEN n_i64 ELSE '3.5' END", "ERR 22P02", ""},
		{"GREATEST|n_i64|LIT16777217", "GREATEST(n_i64, '16777217')", "bigint", "16777217|16777217|16777217|16777217"},
		{"COALESCE|n_i64|LIT16777217", "COALESCE(n_i64, '16777217')", "bigint", "4|16777217|16777217|-6"},
		{"NULLIF|n_i64|LIT16777217", "NULLIF(n_i64, '16777217')", "bigint", "4|<NULL>|<NULL>|-6"},
		{"NULLIF|LIT16777217|n_i64", "NULLIF('16777217', n_i64)", "bigint", "16777217|16777217|<NULL>|16777217"},
		{"CASE|n_i64|LIT16777217", "CASE WHEN id=1 THEN n_i64 ELSE '16777217' END", "bigint", "4|16777217|16777217|16777217"},
		{"GREATEST|n_f32|LIT1e39", "GREATEST(n_f32, '1e39')", "ERR 22003", ""},
		{"COALESCE|n_f32|LIT1e39", "COALESCE(n_f32, '1e39')", "ERR 22003", ""},
		{"NULLIF|n_f32|LIT1e39", "NULLIF(n_f32, '1e39')", "ERR 22003", ""},
		{"NULLIF|LIT1e39|n_f32", "NULLIF('1e39', n_f32)", "ERR 22003", ""},
		{"CASE|n_f32|LIT1e39", "CASE WHEN id=1 THEN n_f32 ELSE '1e39' END", "ERR 22003", ""},
		{"GREATEST|n_f32|LITNaN", "GREATEST(n_f32, 'NaN')", "real", "NaN|NaN|NaN|NaN"},
		{"COALESCE|n_f32|LITNaN", "COALESCE(n_f32, 'NaN')", "real", "0.1|NaN|1.6777216e+07|-0.5"},
		{"NULLIF|n_f32|LITNaN", "NULLIF(n_f32, 'NaN')", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"NULLIF|LITNaN|n_f32", "NULLIF('NaN', n_f32)", "real", "NaN|NaN|NaN|NaN"},
		{"CASE|n_f32|LITNaN", "CASE WHEN id=1 THEN n_f32 ELSE 'NaN' END", "real", "0.1|NaN|NaN|NaN"},
		{"GREATEST|n_f32|LITInfinity", "GREATEST(n_f32, 'Infinity')", "real", "Infinity|Infinity|Infinity|Infinity"},
		{"COALESCE|n_f32|LITInfinity", "COALESCE(n_f32, 'Infinity')", "real", "0.1|Infinity|1.6777216e+07|-0.5"},
		{"NULLIF|n_f32|LITInfinity", "NULLIF(n_f32, 'Infinity')", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"NULLIF|LITInfinity|n_f32", "NULLIF('Infinity', n_f32)", "real", "Infinity|Infinity|Infinity|Infinity"},
		{"CASE|n_f32|LITInfinity", "CASE WHEN id=1 THEN n_f32 ELSE 'Infinity' END", "real", "0.1|Infinity|Infinity|Infinity"},
		{"GREATEST|n_f32|LIT12.750000000000000001", "GREATEST(n_f32, '12.750000000000000001')", "real", "12.75|12.75|1.6777216e+07|12.75"},
		{"COALESCE|n_f32|LIT12.750000000000000001", "COALESCE(n_f32, '12.750000000000000001')", "real", "0.1|12.75|1.6777216e+07|-0.5"},
		{"NULLIF|n_f32|LIT12.750000000000000001", "NULLIF(n_f32, '12.750000000000000001')", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"NULLIF|LIT12.750000000000000001|n_f32", "NULLIF('12.750000000000000001', n_f32)", "real", "12.75|12.75|12.75|12.75"},
		{"CASE|n_f32|LIT12.750000000000000001", "CASE WHEN id=1 THEN n_f32 ELSE '12.750000000000000001' END", "real", "0.1|12.75|12.75|12.75"},
		{"GREATEST|n_f32|LIT0xC.C", "GREATEST(n_f32, '0xC.C')", "real", "12.75|12.75|1.6777216e+07|12.75"},
		{"COALESCE|n_f32|LIT0xC.C", "COALESCE(n_f32, '0xC.C')", "real", "0.1|12.75|1.6777216e+07|-0.5"},
		{"NULLIF|n_f32|LIT0xC.C", "NULLIF(n_f32, '0xC.C')", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"NULLIF|LIT0xC.C|n_f32", "NULLIF('0xC.C', n_f32)", "real", "12.75|12.75|12.75|12.75"},
		{"CASE|n_f32|LIT0xC.C", "CASE WHEN id=1 THEN n_f32 ELSE '0xC.C' END", "real", "0.1|12.75|12.75|12.75"},
		{"GREATEST|n_f32|LIT", "GREATEST(n_f32, '')", "ERR 22P02", ""},
		{"COALESCE|n_f32|LIT", "COALESCE(n_f32, '')", "ERR 22P02", ""},
		{"NULLIF|n_f32|LIT", "NULLIF(n_f32, '')", "ERR 22P02", ""},
		{"NULLIF|LIT|n_f32", "NULLIF('', n_f32)", "ERR 22P02", ""},
		{"CASE|n_f32|LIT", "CASE WHEN id=1 THEN n_f32 ELSE '' END", "ERR 22P02", ""},
		{"GREATEST|n_f32|LITabc", "GREATEST(n_f32, 'abc')", "ERR 22P02", ""},
		{"COALESCE|n_f32|LITabc", "COALESCE(n_f32, 'abc')", "ERR 22P02", ""},
		{"NULLIF|n_f32|LITabc", "NULLIF(n_f32, 'abc')", "ERR 22P02", ""},
		{"NULLIF|LITabc|n_f32", "NULLIF('abc', n_f32)", "ERR 22P02", ""},
		{"CASE|n_f32|LITabc", "CASE WHEN id=1 THEN n_f32 ELSE 'abc' END", "ERR 22P02", ""},
		{"GREATEST|n_f32|LIT3.5", "GREATEST(n_f32, '3.5')", "real", "3.5|3.5|1.6777216e+07|3.5"},
		{"COALESCE|n_f32|LIT3.5", "COALESCE(n_f32, '3.5')", "real", "0.1|3.5|1.6777216e+07|-0.5"},
		{"NULLIF|n_f32|LIT3.5", "NULLIF(n_f32, '3.5')", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"NULLIF|LIT3.5|n_f32", "NULLIF('3.5', n_f32)", "real", "3.5|3.5|3.5|3.5"},
		{"CASE|n_f32|LIT3.5", "CASE WHEN id=1 THEN n_f32 ELSE '3.5' END", "real", "0.1|3.5|3.5|3.5"},
		{"GREATEST|n_f32|LIT16777217", "GREATEST(n_f32, '16777217')", "real", "1.6777216e+07|1.6777216e+07|1.6777216e+07|1.6777216e+07"},
		{"COALESCE|n_f32|LIT16777217", "COALESCE(n_f32, '16777217')", "real", "0.1|1.6777216e+07|1.6777216e+07|-0.5"},
		{"NULLIF|n_f32|LIT16777217", "NULLIF(n_f32, '16777217')", "real", "0.1|<NULL>|<NULL>|-0.5"},
		{"NULLIF|LIT16777217|n_f32", "NULLIF('16777217', n_f32)", "real", "1.6777216e+07|1.6777216e+07|<NULL>|1.6777216e+07"},
		{"CASE|n_f32|LIT16777217", "CASE WHEN id=1 THEN n_f32 ELSE '16777217' END", "real", "0.1|1.6777216e+07|1.6777216e+07|1.6777216e+07"},
		{"GREATEST|n_f64|LIT1e39", "GREATEST(n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"COALESCE|n_f64|LIT1e39", "COALESCE(n_f64, '1e39')", "double precision", "0.2|1e+39|16777217|-0.25"},
		{"NULLIF|n_f64|LIT1e39", "NULLIF(n_f64, '1e39')", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"NULLIF|LIT1e39|n_f64", "NULLIF('1e39', n_f64)", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"CASE|n_f64|LIT1e39", "CASE WHEN id=1 THEN n_f64 ELSE '1e39' END", "double precision", "0.2|1e+39|1e+39|1e+39"},
		{"GREATEST|n_f64|LITNaN", "GREATEST(n_f64, 'NaN')", "double precision", "NaN|NaN|NaN|NaN"},
		{"COALESCE|n_f64|LITNaN", "COALESCE(n_f64, 'NaN')", "double precision", "0.2|NaN|16777217|-0.25"},
		{"NULLIF|n_f64|LITNaN", "NULLIF(n_f64, 'NaN')", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"NULLIF|LITNaN|n_f64", "NULLIF('NaN', n_f64)", "double precision", "NaN|NaN|NaN|NaN"},
		{"CASE|n_f64|LITNaN", "CASE WHEN id=1 THEN n_f64 ELSE 'NaN' END", "double precision", "0.2|NaN|NaN|NaN"},
		{"GREATEST|n_f64|LITInfinity", "GREATEST(n_f64, 'Infinity')", "double precision", "Infinity|Infinity|Infinity|Infinity"},
		{"COALESCE|n_f64|LITInfinity", "COALESCE(n_f64, 'Infinity')", "double precision", "0.2|Infinity|16777217|-0.25"},
		{"NULLIF|n_f64|LITInfinity", "NULLIF(n_f64, 'Infinity')", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"NULLIF|LITInfinity|n_f64", "NULLIF('Infinity', n_f64)", "double precision", "Infinity|Infinity|Infinity|Infinity"},
		{"CASE|n_f64|LITInfinity", "CASE WHEN id=1 THEN n_f64 ELSE 'Infinity' END", "double precision", "0.2|Infinity|Infinity|Infinity"},
		{"GREATEST|n_f64|LIT12.750000000000000001", "GREATEST(n_f64, '12.750000000000000001')", "double precision", "12.75|12.75|16777217|12.75"},
		{"COALESCE|n_f64|LIT12.750000000000000001", "COALESCE(n_f64, '12.750000000000000001')", "double precision", "0.2|12.75|16777217|-0.25"},
		{"NULLIF|n_f64|LIT12.750000000000000001", "NULLIF(n_f64, '12.750000000000000001')", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"NULLIF|LIT12.750000000000000001|n_f64", "NULLIF('12.750000000000000001', n_f64)", "double precision", "12.75|12.75|12.75|12.75"},
		{"CASE|n_f64|LIT12.750000000000000001", "CASE WHEN id=1 THEN n_f64 ELSE '12.750000000000000001' END", "double precision", "0.2|12.75|12.75|12.75"},
		{"GREATEST|n_f64|LIT0xC.C", "GREATEST(n_f64, '0xC.C')", "double precision", "12.75|12.75|16777217|12.75"},
		{"COALESCE|n_f64|LIT0xC.C", "COALESCE(n_f64, '0xC.C')", "double precision", "0.2|12.75|16777217|-0.25"},
		{"NULLIF|n_f64|LIT0xC.C", "NULLIF(n_f64, '0xC.C')", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"NULLIF|LIT0xC.C|n_f64", "NULLIF('0xC.C', n_f64)", "double precision", "12.75|12.75|12.75|12.75"},
		{"CASE|n_f64|LIT0xC.C", "CASE WHEN id=1 THEN n_f64 ELSE '0xC.C' END", "double precision", "0.2|12.75|12.75|12.75"},
		{"GREATEST|n_f64|LIT", "GREATEST(n_f64, '')", "ERR 22P02", ""},
		{"COALESCE|n_f64|LIT", "COALESCE(n_f64, '')", "ERR 22P02", ""},
		{"NULLIF|n_f64|LIT", "NULLIF(n_f64, '')", "ERR 22P02", ""},
		{"NULLIF|LIT|n_f64", "NULLIF('', n_f64)", "ERR 22P02", ""},
		{"CASE|n_f64|LIT", "CASE WHEN id=1 THEN n_f64 ELSE '' END", "ERR 22P02", ""},
		{"GREATEST|n_f64|LITabc", "GREATEST(n_f64, 'abc')", "ERR 22P02", ""},
		{"COALESCE|n_f64|LITabc", "COALESCE(n_f64, 'abc')", "ERR 22P02", ""},
		{"NULLIF|n_f64|LITabc", "NULLIF(n_f64, 'abc')", "ERR 22P02", ""},
		{"NULLIF|LITabc|n_f64", "NULLIF('abc', n_f64)", "ERR 22P02", ""},
		{"CASE|n_f64|LITabc", "CASE WHEN id=1 THEN n_f64 ELSE 'abc' END", "ERR 22P02", ""},
		{"GREATEST|n_f64|LIT3.5", "GREATEST(n_f64, '3.5')", "double precision", "3.5|3.5|16777217|3.5"},
		{"COALESCE|n_f64|LIT3.5", "COALESCE(n_f64, '3.5')", "double precision", "0.2|3.5|16777217|-0.25"},
		{"NULLIF|n_f64|LIT3.5", "NULLIF(n_f64, '3.5')", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"NULLIF|LIT3.5|n_f64", "NULLIF('3.5', n_f64)", "double precision", "3.5|3.5|3.5|3.5"},
		{"CASE|n_f64|LIT3.5", "CASE WHEN id=1 THEN n_f64 ELSE '3.5' END", "double precision", "0.2|3.5|3.5|3.5"},
		{"GREATEST|n_f64|LIT16777217", "GREATEST(n_f64, '16777217')", "double precision", "16777217|16777217|16777217|16777217"},
		{"COALESCE|n_f64|LIT16777217", "COALESCE(n_f64, '16777217')", "double precision", "0.2|16777217|16777217|-0.25"},
		{"NULLIF|n_f64|LIT16777217", "NULLIF(n_f64, '16777217')", "double precision", "0.2|<NULL>|<NULL>|-0.25"},
		{"NULLIF|LIT16777217|n_f64", "NULLIF('16777217', n_f64)", "double precision", "16777217|16777217|<NULL>|16777217"},
		{"CASE|n_f64|LIT16777217", "CASE WHEN id=1 THEN n_f64 ELSE '16777217' END", "double precision", "0.2|16777217|16777217|16777217"},
		{"GREATEST|n_d152|LIT1e39", "GREATEST(n_d152, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"COALESCE|n_d152|LIT1e39", "COALESCE(n_d152, '1e39')", "numeric", "12.75|1000000000000000000000000000000000000000|1.00|-3.50"},
		{"NULLIF|n_d152|LIT1e39", "NULLIF(n_d152, '1e39')", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|LIT1e39|n_d152", "NULLIF('1e39', n_d152)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"CASE|n_d152|LIT1e39", "CASE WHEN id=1 THEN n_d152 ELSE '1e39' END", "numeric", "12.75|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d152|LITNaN", "GREATEST(n_d152, 'NaN')", "numeric", "NaN|NaN|NaN|NaN"},
		{"COALESCE|n_d152|LITNaN", "COALESCE(n_d152, 'NaN')", "numeric", "12.75|NaN|1.00|-3.50"},
		{"NULLIF|n_d152|LITNaN", "NULLIF(n_d152, 'NaN')", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|LITNaN|n_d152", "NULLIF('NaN', n_d152)", "numeric", "NaN|NaN|NaN|NaN"},
		{"CASE|n_d152|LITNaN", "CASE WHEN id=1 THEN n_d152 ELSE 'NaN' END", "numeric", "12.75|NaN|NaN|NaN"},
		{"GREATEST|n_d152|LITInfinity", "GREATEST(n_d152, 'Infinity')", "numeric", "Infinity|Infinity|Infinity|Infinity"},
		{"COALESCE|n_d152|LITInfinity", "COALESCE(n_d152, 'Infinity')", "numeric", "12.75|Infinity|1.00|-3.50"},
		{"NULLIF|n_d152|LITInfinity", "NULLIF(n_d152, 'Infinity')", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|LITInfinity|n_d152", "NULLIF('Infinity', n_d152)", "numeric", "Infinity|Infinity|Infinity|Infinity"},
		{"CASE|n_d152|LITInfinity", "CASE WHEN id=1 THEN n_d152 ELSE 'Infinity' END", "numeric", "12.75|Infinity|Infinity|Infinity"},
		{"GREATEST|n_d152|LIT12.750000000000000001", "GREATEST(n_d152, '12.750000000000000001')", "numeric", "12.750000000000000001|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"COALESCE|n_d152|LIT12.750000000000000001", "COALESCE(n_d152, '12.750000000000000001')", "numeric", "12.75|12.750000000000000001|1.00|-3.50"},
		{"NULLIF|n_d152|LIT12.750000000000000001", "NULLIF(n_d152, '12.750000000000000001')", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|LIT12.750000000000000001|n_d152", "NULLIF('12.750000000000000001', n_d152)", "numeric", "12.750000000000000001|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"CASE|n_d152|LIT12.750000000000000001", "CASE WHEN id=1 THEN n_d152 ELSE '12.750000000000000001' END", "numeric", "12.75|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"GREATEST|n_d152|LIT0xC.C", "GREATEST(n_d152, '0xC.C')", "ERR 22P02", ""},
		{"COALESCE|n_d152|LIT0xC.C", "COALESCE(n_d152, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|n_d152|LIT0xC.C", "NULLIF(n_d152, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|LIT0xC.C|n_d152", "NULLIF('0xC.C', n_d152)", "ERR 22P02", ""},
		{"CASE|n_d152|LIT0xC.C", "CASE WHEN id=1 THEN n_d152 ELSE '0xC.C' END", "ERR 22P02", ""},
		{"GREATEST|n_d152|LIT", "GREATEST(n_d152, '')", "ERR 22P02", ""},
		{"COALESCE|n_d152|LIT", "COALESCE(n_d152, '')", "ERR 22P02", ""},
		{"NULLIF|n_d152|LIT", "NULLIF(n_d152, '')", "ERR 22P02", ""},
		{"NULLIF|LIT|n_d152", "NULLIF('', n_d152)", "ERR 22P02", ""},
		{"CASE|n_d152|LIT", "CASE WHEN id=1 THEN n_d152 ELSE '' END", "ERR 22P02", ""},
		{"GREATEST|n_d152|LITabc", "GREATEST(n_d152, 'abc')", "ERR 22P02", ""},
		{"COALESCE|n_d152|LITabc", "COALESCE(n_d152, 'abc')", "ERR 22P02", ""},
		{"NULLIF|n_d152|LITabc", "NULLIF(n_d152, 'abc')", "ERR 22P02", ""},
		{"NULLIF|LITabc|n_d152", "NULLIF('abc', n_d152)", "ERR 22P02", ""},
		{"CASE|n_d152|LITabc", "CASE WHEN id=1 THEN n_d152 ELSE 'abc' END", "ERR 22P02", ""},
		{"GREATEST|n_d152|LIT3.5", "GREATEST(n_d152, '3.5')", "numeric", "12.75|3.5|3.5|3.5"},
		{"COALESCE|n_d152|LIT3.5", "COALESCE(n_d152, '3.5')", "numeric", "12.75|3.5|1.00|-3.50"},
		{"NULLIF|n_d152|LIT3.5", "NULLIF(n_d152, '3.5')", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|LIT3.5|n_d152", "NULLIF('3.5', n_d152)", "numeric", "3.5|3.5|3.5|3.5"},
		{"CASE|n_d152|LIT3.5", "CASE WHEN id=1 THEN n_d152 ELSE '3.5' END", "numeric", "12.75|3.5|3.5|3.5"},
		{"GREATEST|n_d152|LIT16777217", "GREATEST(n_d152, '16777217')", "numeric", "16777217|16777217|16777217|16777217"},
		{"COALESCE|n_d152|LIT16777217", "COALESCE(n_d152, '16777217')", "numeric", "12.75|16777217|1.00|-3.50"},
		{"NULLIF|n_d152|LIT16777217", "NULLIF(n_d152, '16777217')", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"NULLIF|LIT16777217|n_d152", "NULLIF('16777217', n_d152)", "numeric", "16777217|16777217|16777217|16777217"},
		{"CASE|n_d152|LIT16777217", "CASE WHEN id=1 THEN n_d152 ELSE '16777217' END", "numeric", "12.75|16777217|16777217|16777217"},
		{"GREATEST|n_d3810|LIT1e39", "GREATEST(n_d3810, '1e39')", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"COALESCE|n_d3810|LIT1e39", "COALESCE(n_d3810, '1e39')", "numeric", "12.7500000001|1000000000000000000000000000000000000000|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|LIT1e39", "NULLIF(n_d3810, '1e39')", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|LIT1e39|n_d3810", "NULLIF('1e39', n_d3810)", "numeric", "1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"CASE|n_d3810|LIT1e39", "CASE WHEN id=1 THEN n_d3810 ELSE '1e39' END", "numeric", "12.7500000001|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000|1000000000000000000000000000000000000000"},
		{"GREATEST|n_d3810|LITNaN", "GREATEST(n_d3810, 'NaN')", "numeric", "NaN|NaN|NaN|NaN"},
		{"COALESCE|n_d3810|LITNaN", "COALESCE(n_d3810, 'NaN')", "numeric", "12.7500000001|NaN|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|LITNaN", "NULLIF(n_d3810, 'NaN')", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|LITNaN|n_d3810", "NULLIF('NaN', n_d3810)", "numeric", "NaN|NaN|NaN|NaN"},
		{"CASE|n_d3810|LITNaN", "CASE WHEN id=1 THEN n_d3810 ELSE 'NaN' END", "numeric", "12.7500000001|NaN|NaN|NaN"},
		{"GREATEST|n_d3810|LITInfinity", "GREATEST(n_d3810, 'Infinity')", "numeric", "Infinity|Infinity|Infinity|Infinity"},
		{"COALESCE|n_d3810|LITInfinity", "COALESCE(n_d3810, 'Infinity')", "numeric", "12.7500000001|Infinity|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|LITInfinity", "NULLIF(n_d3810, 'Infinity')", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|LITInfinity|n_d3810", "NULLIF('Infinity', n_d3810)", "numeric", "Infinity|Infinity|Infinity|Infinity"},
		{"CASE|n_d3810|LITInfinity", "CASE WHEN id=1 THEN n_d3810 ELSE 'Infinity' END", "numeric", "12.7500000001|Infinity|Infinity|Infinity"},
		{"GREATEST|n_d3810|LIT12.750000000000000001", "GREATEST(n_d3810, '12.750000000000000001')", "numeric", "12.7500000001|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"COALESCE|n_d3810|LIT12.750000000000000001", "COALESCE(n_d3810, '12.750000000000000001')", "numeric", "12.7500000001|12.750000000000000001|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|LIT12.750000000000000001", "NULLIF(n_d3810, '12.750000000000000001')", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|LIT12.750000000000000001|n_d3810", "NULLIF('12.750000000000000001', n_d3810)", "numeric", "12.750000000000000001|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"CASE|n_d3810|LIT12.750000000000000001", "CASE WHEN id=1 THEN n_d3810 ELSE '12.750000000000000001' END", "numeric", "12.7500000001|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"GREATEST|n_d3810|LIT0xC.C", "GREATEST(n_d3810, '0xC.C')", "ERR 22P02", ""},
		{"COALESCE|n_d3810|LIT0xC.C", "COALESCE(n_d3810, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|n_d3810|LIT0xC.C", "NULLIF(n_d3810, '0xC.C')", "ERR 22P02", ""},
		{"NULLIF|LIT0xC.C|n_d3810", "NULLIF('0xC.C', n_d3810)", "ERR 22P02", ""},
		{"CASE|n_d3810|LIT0xC.C", "CASE WHEN id=1 THEN n_d3810 ELSE '0xC.C' END", "ERR 22P02", ""},
		{"GREATEST|n_d3810|LIT", "GREATEST(n_d3810, '')", "ERR 22P02", ""},
		{"COALESCE|n_d3810|LIT", "COALESCE(n_d3810, '')", "ERR 22P02", ""},
		{"NULLIF|n_d3810|LIT", "NULLIF(n_d3810, '')", "ERR 22P02", ""},
		{"NULLIF|LIT|n_d3810", "NULLIF('', n_d3810)", "ERR 22P02", ""},
		{"CASE|n_d3810|LIT", "CASE WHEN id=1 THEN n_d3810 ELSE '' END", "ERR 22P02", ""},
		{"GREATEST|n_d3810|LITabc", "GREATEST(n_d3810, 'abc')", "ERR 22P02", ""},
		{"COALESCE|n_d3810|LITabc", "COALESCE(n_d3810, 'abc')", "ERR 22P02", ""},
		{"NULLIF|n_d3810|LITabc", "NULLIF(n_d3810, 'abc')", "ERR 22P02", ""},
		{"NULLIF|LITabc|n_d3810", "NULLIF('abc', n_d3810)", "ERR 22P02", ""},
		{"CASE|n_d3810|LITabc", "CASE WHEN id=1 THEN n_d3810 ELSE 'abc' END", "ERR 22P02", ""},
		{"GREATEST|n_d3810|LIT3.5", "GREATEST(n_d3810, '3.5')", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"COALESCE|n_d3810|LIT3.5", "COALESCE(n_d3810, '3.5')", "numeric", "12.7500000001|3.5|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|LIT3.5", "NULLIF(n_d3810, '3.5')", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|LIT3.5|n_d3810", "NULLIF('3.5', n_d3810)", "numeric", "3.5|3.5|3.5|3.5"},
		{"CASE|n_d3810|LIT3.5", "CASE WHEN id=1 THEN n_d3810 ELSE '3.5' END", "numeric", "12.7500000001|3.5|3.5|3.5"},
		{"GREATEST|n_d3810|LIT16777217", "GREATEST(n_d3810, '16777217')", "numeric", "16777217|16777217|16777217|16777217"},
		{"COALESCE|n_d3810|LIT16777217", "COALESCE(n_d3810, '16777217')", "numeric", "12.7500000001|16777217|1.0000000000|-3.5000000000"},
		{"NULLIF|n_d3810|LIT16777217", "NULLIF(n_d3810, '16777217')", "numeric(38,10)", "12.7500000001|<NULL>|1.0000000000|-3.5000000000"},
		{"NULLIF|LIT16777217|n_d3810", "NULLIF('16777217', n_d3810)", "numeric", "16777217|16777217|16777217|16777217"},
		{"CASE|n_d3810|LIT16777217", "CASE WHEN id=1 THEN n_d3810 ELSE '16777217' END", "numeric", "12.7500000001|16777217|16777217|16777217"},
		{"ALLLIT|GREATEST|1e39", "GREATEST('1e39', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|1e39", "COALESCE('1e39', 'zz')", "text", "1e39|1e39|1e39|1e39"},
		{"ALLLIT|GREATEST|NaN", "GREATEST('NaN', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|NaN", "COALESCE('NaN', 'zz')", "text", "NaN|NaN|NaN|NaN"},
		{"ALLLIT|GREATEST|Infinity", "GREATEST('Infinity', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|Infinity", "COALESCE('Infinity', 'zz')", "text", "Infinity|Infinity|Infinity|Infinity"},
		{"ALLLIT|GREATEST|12.750000000000000001", "GREATEST('12.750000000000000001', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|12.750000000000000001", "COALESCE('12.750000000000000001', 'zz')", "text", "12.750000000000000001|12.750000000000000001|12.750000000000000001|12.750000000000000001"},
		{"ALLLIT|GREATEST|0xC.C", "GREATEST('0xC.C', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|0xC.C", "COALESCE('0xC.C', 'zz')", "text", "0xC.C|0xC.C|0xC.C|0xC.C"},
		{"ALLLIT|GREATEST|", "GREATEST('', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|", "COALESCE('', 'zz')", "text", "|||"},
		{"ALLLIT|GREATEST|abc", "GREATEST('abc', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|abc", "COALESCE('abc', 'zz')", "text", "abc|abc|abc|abc"},
		{"ALLLIT|GREATEST|3.5", "GREATEST('3.5', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|3.5", "COALESCE('3.5', 'zz')", "text", "3.5|3.5|3.5|3.5"},
		{"ALLLIT|GREATEST|16777217", "GREATEST('16777217', 'zz')", "text", "zz|zz|zz|zz"},
		{"ALLLIT|COALESCE|16777217", "COALESCE('16777217', 'zz')", "text", "16777217|16777217|16777217|16777217"},
		{"ALLLIT|COALESCE|two", "COALESCE('a','b')", "text", "a|a|a|a"},
		{"ALLLIT|GREATEST|two", "GREATEST('a','b')", "text", "b|b|b|b"},
		{"ALLLIT|CASE|two", "CASE WHEN id=1 THEN 'a' ELSE 'b' END", "text", "a|b|b|b"},
		{"ALLLIT|COALESCE|numlit", "COALESCE('3.5', 1)", "ERR 22P02", ""},
		{"ALLLIT|GREATEST|numlit", "GREATEST('3.5', 1)", "ERR 22P02", ""},
		{"MIX|dec_f64_lit", "COALESCE(n_d152, n_f64, '3.5')", "double precision", "12.75|3.5|1|-3.5"},
		{"MIX|f64_dec_lit", "COALESCE(n_f64, n_d152, '3.5')", "double precision", "0.2|3.5|16777217|-0.25"},
		{"MIX|dec_f32_lit", "GREATEST(n_d152, n_f32, '3.5')", "real", "12.75|3.5|1.6777216e+07|3.5"},
		{"MIX|i64_f32_f64_lit1e39", "GREATEST(n_i64, n_f32, n_f64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"MIX|i64_f32_f64_litNaN", "GREATEST(n_i64, n_f32, n_f64, 'NaN')", "double precision", "NaN|NaN|NaN|NaN"},
		{"MIX|f32_f64_i64_lit1e39", "GREATEST(n_f32, n_f64, n_i64, '1e39')", "double precision", "1e+39|1e+39|1e+39|1e+39"},
		{"MIX|f32_lit_f64", "GREATEST(n_f32, '16777217', n_f64)", "double precision", "16777217|16777217|16777217|16777217"},
		{"MIX|i64_lit_f64", "GREATEST(n_i64, '3.5', n_f64)", "double precision", "4|3.5|16777217|3.5"},
		{"MIX|dec152_dec3810", "COALESCE(n_d152, n_d3810)", "numeric", "12.75|<NULL>|1.00|-3.50"},
		{"MIX|dec152_dec3810_lit", "COALESCE(n_d152, n_d3810, '12.750000000000000001')", "numeric", "12.75|12.750000000000000001|1.00|-3.50"},

		// --- The ARM KINDS a CASE can hold, over each numeric width -------
		//
		// Every entry above folds over BARE COLUMNS and quoted literals. That
		// left the fold's INPUT untested: `nodeDeclaredType` answers per arm,
		// and an arm that is not a bare column went through a different arm of
		// that switch. Arithmetic was the one that lost its type — `n_i64 +
		// 100` declared FLOAT64, so `CASE WHEN … THEN n_i64 ELSE n_i64 + 100
		// END` folded {INT64, FLOAT64} and answered double precision where
		// PostgreSQL answers bigint. It read as correct for as long as
		// CommonDeclType answered from the FIRST decider rather than the
		// ladder, which is why the #724 stack exposed it rather than caused it.
		//
		// Eight arm kinds x five widths, every answer read off PostgreSQL 17
		// (format_type over a view, so the typmod is carried). The controls
		// matter as much as the subject: LITINT/LITNUM/LITQ pin ADR-0024's
		// literal rules, and the float rows pin that arithmetic must NOT be
		// declared integer — `real + 100` is double precision on PostgreSQL,
		// and both the NESTED and ARITH rows for n_f32 say so.
		{"CASE|n_i32|SELF", "CASE WHEN id=1 THEN n_i32 ELSE n_i32 END", "integer", "3|<NULL>|16777217|-5"},
		{"CASE|n_i32|ARITH", "CASE WHEN id=1 THEN n_i32 ELSE n_i32 + 100 END", "integer", "3|<NULL>|16777317|95"},
		{"CASE|n_i32|CASTI64", "CASE WHEN id=1 THEN n_i32 ELSE CAST(n_i32 AS BIGINT) END", "bigint", "3|<NULL>|16777217|-5"},
		{"CASE|n_i32|LITINT", "CASE WHEN id=1 THEN n_i32 ELSE 100 END", "integer", "3|100|100|100"},
		{"CASE|n_i32|LITNUM", "CASE WHEN id=1 THEN n_i32 ELSE 100.5 END", "numeric", "3|100.5|100.5|100.5"},
		{"CASE|n_i32|LITQ", "CASE WHEN id=1 THEN n_i32 ELSE 100 END", "integer", "3|100|100|100"},
		{"CASE|n_i32|NESTED", "CASE WHEN id=1 THEN n_i32 ELSE CASE WHEN id = 3 THEN n_i32 ELSE n_i32 + 1 END END", "integer", "3|<NULL>|16777217|-4"},
		{"CASE|n_i32|FUNCABS", "CASE WHEN id=1 THEN n_i32 ELSE ABS(n_i32) END", "integer", "3|<NULL>|16777217|5"},
		{"CASE|n_i64|SELF", "CASE WHEN id=1 THEN n_i64 ELSE n_i64 END", "bigint", "4|<NULL>|16777217|-6"},
		{"CASE|n_i64|ARITH", "CASE WHEN id=1 THEN n_i64 ELSE n_i64 + 100 END", "bigint", "4|<NULL>|16777317|94"},
		{"CASE|n_i64|CASTI64", "CASE WHEN id=1 THEN n_i64 ELSE CAST(n_i64 AS BIGINT) END", "bigint", "4|<NULL>|16777217|-6"},
		{"CASE|n_i64|LITINT", "CASE WHEN id=1 THEN n_i64 ELSE 100 END", "bigint", "4|100|100|100"},
		{"CASE|n_i64|LITNUM", "CASE WHEN id=1 THEN n_i64 ELSE 100.5 END", "numeric", "4|100.5|100.5|100.5"},
		{"CASE|n_i64|LITQ", "CASE WHEN id=1 THEN n_i64 ELSE 100 END", "bigint", "4|100|100|100"},
		{"CASE|n_i64|NESTED", "CASE WHEN id=1 THEN n_i64 ELSE CASE WHEN id = 3 THEN n_i64 ELSE n_i64 + 1 END END", "bigint", "4|<NULL>|16777217|-5"},
		{"CASE|n_i64|FUNCABS", "CASE WHEN id=1 THEN n_i64 ELSE ABS(n_i64) END", "bigint", "4|<NULL>|16777217|6"},
		{"CASE|n_f32|SELF", "CASE WHEN id=1 THEN n_f32 ELSE n_f32 END", "real", "0.1|<NULL>|1.6777216e+07|-0.5"},
		{"CASE|n_f32|ARITH", "CASE WHEN id=1 THEN n_f32 ELSE n_f32 + 100 END", "double precision", "0.10000000149011612|<NULL>|16777316|99.5"},
		{"CASE|n_f32|CASTI64", "CASE WHEN id=1 THEN n_f32 ELSE CAST(n_f32 AS BIGINT) END", "real", "0.1|<NULL>|1.6777216e+07|0"},
		{"CASE|n_f32|LITINT", "CASE WHEN id=1 THEN n_f32 ELSE 100 END", "real", "0.1|100|100|100"},
		{"CASE|n_f32|LITNUM", "CASE WHEN id=1 THEN n_f32 ELSE 100.5 END", "real", "0.1|100.5|100.5|100.5"},
		{"CASE|n_f32|LITQ", "CASE WHEN id=1 THEN n_f32 ELSE 100 END", "real", "0.1|100|100|100"},
		{"CASE|n_f32|NESTED", "CASE WHEN id=1 THEN n_f32 ELSE CASE WHEN id = 3 THEN n_f32 ELSE n_f32 + 1 END END", "double precision", "0.10000000149011612|<NULL>|16777216|0.5"},
		{"CASE|n_f32|FUNCABS", "CASE WHEN id=1 THEN n_f32 ELSE ABS(n_f32) END", "real", "0.1|<NULL>|1.6777216e+07|0.5"},
		{"CASE|n_f64|SELF", "CASE WHEN id=1 THEN n_f64 ELSE n_f64 END", "double precision", "0.2|<NULL>|16777217|-0.25"},
		{"CASE|n_f64|ARITH", "CASE WHEN id=1 THEN n_f64 ELSE n_f64 + 100 END", "double precision", "0.2|<NULL>|16777317|99.75"},
		{"CASE|n_f64|CASTI64", "CASE WHEN id=1 THEN n_f64 ELSE CAST(n_f64 AS BIGINT) END", "double precision", "0.2|<NULL>|16777217|0"},
		{"CASE|n_f64|LITINT", "CASE WHEN id=1 THEN n_f64 ELSE 100 END", "double precision", "0.2|100|100|100"},
		{"CASE|n_f64|LITNUM", "CASE WHEN id=1 THEN n_f64 ELSE 100.5 END", "double precision", "0.2|100.5|100.5|100.5"},
		{"CASE|n_f64|LITQ", "CASE WHEN id=1 THEN n_f64 ELSE 100 END", "double precision", "0.2|100|100|100"},
		{"CASE|n_f64|NESTED", "CASE WHEN id=1 THEN n_f64 ELSE CASE WHEN id = 3 THEN n_f64 ELSE n_f64 + 1 END END", "double precision", "0.2|<NULL>|16777217|0.75"},
		{"CASE|n_f64|FUNCABS", "CASE WHEN id=1 THEN n_f64 ELSE ABS(n_f64) END", "double precision", "0.2|<NULL>|16777217|0.25"},
		{"CASE|n_d152|SELF", "CASE WHEN id=1 THEN n_d152 ELSE n_d152 END", "numeric(15,2)", "12.75|<NULL>|1.00|-3.50"},
		{"CASE|n_d152|ARITH", "CASE WHEN id=1 THEN n_d152 ELSE n_d152 + 100 END", "numeric", "12.75|<NULL>|101.00|96.50"},
		{"CASE|n_d152|CASTI64", "CASE WHEN id=1 THEN n_d152 ELSE CAST(n_d152 AS BIGINT) END", "numeric", "12.75|<NULL>|1|-4"},
		{"CASE|n_d152|LITINT", "CASE WHEN id=1 THEN n_d152 ELSE 100 END", "numeric", "12.75|100|100|100"},
		{"CASE|n_d152|LITNUM", "CASE WHEN id=1 THEN n_d152 ELSE 100.5 END", "numeric", "12.75|100.5|100.5|100.5"},
		{"CASE|n_d152|LITQ", "CASE WHEN id=1 THEN n_d152 ELSE 100 END", "numeric", "12.75|100|100|100"},
		{"CASE|n_d152|NESTED", "CASE WHEN id=1 THEN n_d152 ELSE CASE WHEN id = 3 THEN n_d152 ELSE n_d152 + 1 END END", "numeric", "12.75|<NULL>|1.00|-2.50"},
		{"CASE|n_d152|FUNCABS", "CASE WHEN id=1 THEN n_d152 ELSE ABS(n_d152) END", "numeric", "12.75|<NULL>|1.00|3.50"},
	}
}

// nfPos is one composite in one VALUE-PRODUCING position, with PostgreSQL
// 17.11's whole result: rows in result order separated by ';', a row's cells
// in column order separated by ',', "<NULL>" for a NULL.
type nfPos struct {
	name string
	sql  string
	want string
}

// nfPosRefusals are the positions PostgreSQL refuses too, with its SQLSTATE.
// Every one is `CAST(<a fold holding 1e39> AS DECIMAL(30,6))`, which overflows
// the destination on both engines.
var nfPosRefusals = map[string]string{
	"BigintFirstOverRange|CastOfTheComposite": "22003",
}

// nfPosPins are keyed "<entry>|<arm>", because the two arms do not always
// diverge together and a pin that claimed both would hide one of them starting
// to agree. Every entry records wadjet's answer; PostgreSQL's is in the corpus.
//
// Four shapes, one residual, and it is NOT this fix's: a REAL-typed composite's
// value is not NARROWED to real once it leaves the projection. It reproduces
// with no quoted literal in the query at all, on a composite whose declared
// type #724 did not move. Measured on this fixture, single-process arm:
//
//	SELECT GREATEST(n_f32, n_i32) * 2 FROM numfold WHERE id = 3
//	  -> 33554434        PostgreSQL: 33554432 (the real is 16777216)
//	SELECT CAST(GREATEST(n_f32, n_i32) AS DECIMAL(30,6)) FROM numfold WHERE id = 3
//	  -> 16777217.000000 PostgreSQL: 16777200.000000
//
// The composite DECLARES real and the PROJECTION narrows on the way into that
// vector — every Projection, GroupByKey, AggregateInput, WindowArgAndOrder and
// Distinct entry here passes — but the value handed to a CAST, an arithmetic
// operand or (on the DAG) a set-operation arm is the winning arm's own box, at
// its own width. `GREATEST(real, integer)` on the row where the integer wins
// hands over an int64 that real cannot hold exactly, and nothing between there
// and those consumers narrows it. Filed as #758.
//
// The CastOfTheComposite rows carry a SECOND divergence, PostgreSQL's own: its
// float4 -> numeric cast renders the real with six significant digits first,
// so 16777216 becomes 16777200 there. Recorded rather than chased; wadjet's
// 16777216.000000 is the real's exact value.
//
// SetOpArm is pinned on the DAG ONLY: the single-process arm narrows the arm's
// value to real before the union widens it, as PostgreSQL does, and the DAG's
// arm reconciliation does not. That asymmetry is part of #758.
var nfPosPins = map[string]string{
	// The ARITHMETIC and SET-OP rows are GONE (#758, 2026-09-03).
	// extremumArms.materialize rewrote only a QUOTED literal's box, so a
	// COLUMN won at its OWN width: `GREATEST(real, '3.5', integer)` folds to
	// real, the integer arm won over 16777216/16777217 — the same real — and
	// came back as the integer. The projection narrowed it into a real
	// vector, which is why the bare call looked right and `* 2` answered
	// 33554434. The winner is brought to the fold's width now
	// (extremumWinnerBox), and COALESCE over the same pair — which was
	// ALREADY right, through choiceNumberBox — is what localized it.
	//
	// The two CAST rows stay, at a NEW value, and the pin's own comment
	// predicted it: PostgreSQL renders a `float4 -> numeric` cast at six
	// SIGNIFICANT digits (16777200.000000), which is a rendering rule this
	// engine does not have. What moved is the DIGIT the fold was wrong
	// about — 16777217 was the integer's value at the integer's width, and
	// 16777216 is the real the fold resolves to. Closer, and still not
	// PostgreSQL's; the residual is the float4-to-numeric rendering alone.
	"RealBesideDecimal|CastOfTheComposite|dag":        "1,0.100000;2,<NULL>;3,16777216.000000;4,-0.500000",
	"RealBesideDecimal|CastOfTheComposite|single":     "1,0.100000;2,<NULL>;3,16777216.000000;4,-0.500000",
	"RealQuotedFractionInt|CastOfTheComposite|dag":    "1,3.500000;2,3.500000;3,16777216.000000;4,3.500000",
	"RealQuotedFractionInt|CastOfTheComposite|single": "1,3.500000;2,3.500000;3,16777216.000000;4,3.500000",
}

// TestNumericFoldValuePositionsTwoPath takes the composites this fix moves
// through every position a VALUE can be produced in, because a declared type
// is not a display property: the projection is only the first consumer.
//
// The wrap this closes reached a GROUP BY key built from the same projection —
// int64's minimum became a GROUP — and the same declaration decides the sort
// key a window ORDER BY builds, the accumulator an aggregate opens, the column
// a set operation reconciles its arms to, the vector a CAST reads from and the
// operand arithmetic runs on. A fix asserted on `SELECT <expr>` alone would
// leave every one of those untested, and three of them (the group key, the
// window key, the set-op arm) take a DIFFERENT mechanism on the stage DAG.
//
// Values are compared as NUMBERS here rather than as text: the projection
// entries in TestNumericFoldTwoPath already hold the exact rendering, and a
// value narrowed or wrapped by a wrong declaration is a different NUMBER —
// 16777216 for 16777217, a real 0.1 read at double width, int64's minimum for
// 1e39 — so nothing this gate exists for can slip through.
func TestNumericFoldValuePositionsTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	names := map[string]bool{}
	for _, c := range nfPositions() {
		names[c.name] = true
	}
	for name := range nfPosRefusals {
		if !names[name] {
			t.Errorf("position refusal %q matches no entry — delete it or fix the name.", name)
		}
	}
	for key := range nfPosPins {
		name, _, ok := strings.Cut(key, "|single")
		if !ok {
			name, _, ok = strings.Cut(key, "|dag")
		}
		if !ok || !names[name] {
			t.Errorf("position pin %q names no entry|arm — delete it or fix the key.", key)
		}
	}

	for _, c := range nfPositions() {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				res, err := nfRun(ctx, single, coord, c.sql, arm.dag)
				if state, refuses := nfPosRefusals[c.name]; refuses {
					if err == nil {
						t.Errorf("%s: %s answered %s; PostgreSQL raises %s",
							arm.name, c.sql, nfRows(res), state)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s: %s refused: %v\n  PostgreSQL answers %s",
						arm.name, c.sql, err, c.want)
				}
				got := nfRows(res)
				if pin, pinned := nfPosPins[c.name+"|"+arm.name]; pinned {
					if nfSameRows(c.want, got) {
						t.Errorf("%s: %s now AGREES with PostgreSQL (%s) — delete this "+
							"nfPosPins entry", arm.name, c.sql, c.want)
					} else if !nfSameRows(pin, got) {
						t.Errorf("%s: %s\n  got  %s\n  pin  %s\n  PostgreSQL 17.11: %s",
							arm.name, c.sql, got, pin, c.want)
					}
					continue
				}
				if !nfSameRows(c.want, got) {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						arm.name, c.sql, got, c.want)
				}
			}
		})
	}
}

// nfRows renders a whole result the way the PostgreSQL side was recorded.
func nfRows(res *oracle.Result) string {
	if res == nil {
		return ""
	}
	rows := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		cells := make([]string, 0, len(res.Columns))
		for _, col := range res.Columns {
			cells = append(cells, nfCell(row[col]))
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	return strings.Join(rows, ";")
}

// nfSameRows compares the two renderings cell by cell, as the NUMBERS both
// texts name where both parse as one and as text otherwise. See the gate's
// own comment for why the numeric reading is the right strength here and the
// exact one is the right strength for the projection corpus.
func nfSameRows(want, got string) bool {
	if want == got {
		return true
	}
	w, g := strings.Split(want, ";"), strings.Split(got, ";")
	if len(w) != len(g) {
		return false
	}
	for i := range w {
		wc, gc := strings.Split(w[i], ","), strings.Split(g[i], ",")
		if len(wc) != len(gc) {
			return false
		}
		for j := range wc {
			if wc[j] == gc[j] {
				continue
			}
			a, aok := new(big.Rat).SetString(wc[j])
			b, bok := new(big.Rat).SetString(gc[j])
			if !aok || !bok || a.Cmp(b) != 0 {
				return false
			}
		}
	}
	return true
}

// TestInsertSelectIsNotAValuePositionYet is method 10's fixture for the one
// value-producing position the corpus above does NOT cover: a composite stored
// through `INSERT INTO … SELECT` or `CREATE TABLE … AS SELECT`, where the
// declared type decides the COLUMN a table keeps rather than a vector a query
// throws away.
//
// It is not covered because the parser has neither form — INSERT takes VALUES
// only (dml_parser.go) — so there is no shape to write. That is a claim about
// the engine, and this is the fixture that ATTEMPTS it: when either form
// lands, this test fails and the corpus above gets the position it is missing.
func TestInsertSelectIsNotAValuePositionYet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	db := tmdStandalone(t, ctx)
	for _, sql := range []string{
		"INSERT INTO " + nfTable + " SELECT id, GREATEST(n_i64, n_f32, n_f64, '1e39') FROM " + nfTable,
		"CREATE TABLE nffold AS SELECT GREATEST(n_i64, n_f32, n_f64, '1e39') AS v FROM " + nfTable,
	} {
		if _, err := tmdRunSingle(ctx, db, sql); err == nil {
			t.Errorf("%q was accepted — a composite can now be STORED, which is a "+
				"value-producing position TestNumericFoldValuePositionsTwoPath does not "+
				"cover. Add it there and delete this test.", sql)
		}
	}
}

// nfPositions is every composite above through every position a value can be
// produced in, with PostgreSQL 17.11's whole result for each.
func nfPositions() []nfPos {
	return []nfPos{
		{"BigintFirstOverRange|Projection", "SELECT id, (GREATEST(n_i64, n_f32, n_f64, '1e39')) AS v FROM numfold ORDER BY id", "1,1e+39;2,1e+39;3,1e+39;4,1e+39"},
		{"BigintFirstOverRange|GroupByKey", "SELECT (GREATEST(n_i64, n_f32, n_f64, '1e39')) AS v, COUNT(*) AS n FROM numfold GROUP BY (GREATEST(n_i64, n_f32, n_f64, '1e39')) ORDER BY 1", "1e+39,4"},
		{"BigintFirstOverRange|AggregateInput", "SELECT MAX(GREATEST(n_i64, n_f32, n_f64, '1e39')) AS mx, MIN(GREATEST(n_i64, n_f32, n_f64, '1e39')) AS mn, COUNT(GREATEST(n_i64, n_f32, n_f64, '1e39')) AS c, COUNT(DISTINCT GREATEST(n_i64, n_f32, n_f64, '1e39')) AS d FROM numfold", "1e+39,1e+39,4,1"},
		{"BigintFirstOverRange|WindowArgAndOrder", "SELECT id, MAX(GREATEST(n_i64, n_f32, n_f64, '1e39')) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (GREATEST(n_i64, n_f32, n_f64, '1e39')), id) AS r FROM numfold ORDER BY id", "1,1e+39,1;2,1e+39,2;3,1e+39,3;4,1e+39,4"},
		{"BigintFirstOverRange|SetOpArm", "SELECT (GREATEST(n_i64, n_f32, n_f64, '1e39')) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-0.25;0.2;16777217;1e+39;1e+39;1e+39;1e+39;<NULL>"},
		{"BigintFirstOverRange|CastOfTheComposite", "SELECT id, CAST((GREATEST(n_i64, n_f32, n_f64, '1e39')) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "ERR 22003"},
		{"BigintFirstOverRange|ArithmeticOverIt", "SELECT id, (GREATEST(n_i64, n_f32, n_f64, '1e39')) * 2 AS v FROM numfold ORDER BY id", "1,2e+39;2,2e+39;3,2e+39;4,2e+39"},
		{"BigintFirstOverRange|Distinct", "SELECT DISTINCT (GREATEST(n_i64, n_f32, n_f64, '1e39')) AS v FROM numfold ORDER BY 1", "1e+39"},
		{"RealFirstQuotedInt|Projection", "SELECT id, (GREATEST(n_f32, '16777217', n_f64)) AS v FROM numfold ORDER BY id", "1,16777217;2,16777217;3,16777217;4,16777217"},
		{"RealFirstQuotedInt|GroupByKey", "SELECT (GREATEST(n_f32, '16777217', n_f64)) AS v, COUNT(*) AS n FROM numfold GROUP BY (GREATEST(n_f32, '16777217', n_f64)) ORDER BY 1", "16777217,4"},
		{"RealFirstQuotedInt|AggregateInput", "SELECT MAX(GREATEST(n_f32, '16777217', n_f64)) AS mx, MIN(GREATEST(n_f32, '16777217', n_f64)) AS mn, COUNT(GREATEST(n_f32, '16777217', n_f64)) AS c, COUNT(DISTINCT GREATEST(n_f32, '16777217', n_f64)) AS d FROM numfold", "16777217,16777217,4,1"},
		{"RealFirstQuotedInt|WindowArgAndOrder", "SELECT id, MAX(GREATEST(n_f32, '16777217', n_f64)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (GREATEST(n_f32, '16777217', n_f64)), id) AS r FROM numfold ORDER BY id", "1,16777217,1;2,16777217,2;3,16777217,3;4,16777217,4"},
		{"RealFirstQuotedInt|SetOpArm", "SELECT (GREATEST(n_f32, '16777217', n_f64)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-0.25;0.2;16777217;16777217;16777217;16777217;16777217;<NULL>"},
		{"RealFirstQuotedInt|CastOfTheComposite", "SELECT id, CAST((GREATEST(n_f32, '16777217', n_f64)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,16777217.000000;2,16777217.000000;3,16777217.000000;4,16777217.000000"},
		{"RealFirstQuotedInt|ArithmeticOverIt", "SELECT id, (GREATEST(n_f32, '16777217', n_f64)) * 2 AS v FROM numfold ORDER BY id", "1,33554434;2,33554434;3,33554434;4,33554434"},
		{"RealFirstQuotedInt|Distinct", "SELECT DISTINCT (GREATEST(n_f32, '16777217', n_f64)) AS v FROM numfold ORDER BY 1", "16777217"},
		{"BigintFirstQuotedFraction|Projection", "SELECT id, (GREATEST(n_i64, '3.5', n_f64)) AS v FROM numfold ORDER BY id", "1,4;2,3.5;3,16777217;4,3.5"},
		{"BigintFirstQuotedFraction|GroupByKey", "SELECT (GREATEST(n_i64, '3.5', n_f64)) AS v, COUNT(*) AS n FROM numfold GROUP BY (GREATEST(n_i64, '3.5', n_f64)) ORDER BY 1", "3.5,2;4,1;16777217,1"},
		{"BigintFirstQuotedFraction|AggregateInput", "SELECT MAX(GREATEST(n_i64, '3.5', n_f64)) AS mx, MIN(GREATEST(n_i64, '3.5', n_f64)) AS mn, COUNT(GREATEST(n_i64, '3.5', n_f64)) AS c, COUNT(DISTINCT GREATEST(n_i64, '3.5', n_f64)) AS d FROM numfold", "16777217,3.5,4,3"},
		{"BigintFirstQuotedFraction|WindowArgAndOrder", "SELECT id, MAX(GREATEST(n_i64, '3.5', n_f64)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (GREATEST(n_i64, '3.5', n_f64)), id) AS r FROM numfold ORDER BY id", "1,16777217,3;2,16777217,1;3,16777217,4;4,16777217,2"},
		{"BigintFirstQuotedFraction|SetOpArm", "SELECT (GREATEST(n_i64, '3.5', n_f64)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-0.25;0.2;3.5;3.5;4;16777217;16777217;<NULL>"},
		{"BigintFirstQuotedFraction|CastOfTheComposite", "SELECT id, CAST((GREATEST(n_i64, '3.5', n_f64)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,4.000000;2,3.500000;3,16777217.000000;4,3.500000"},
		{"BigintFirstQuotedFraction|ArithmeticOverIt", "SELECT id, (GREATEST(n_i64, '3.5', n_f64)) * 2 AS v FROM numfold ORDER BY id", "1,8;2,7;3,33554434;4,7"},
		{"BigintFirstQuotedFraction|Distinct", "SELECT DISTINCT (GREATEST(n_i64, '3.5', n_f64)) AS v FROM numfold ORDER BY 1", "3.5;4;16777217"},
		{"DecimalBesideDouble|Projection", "SELECT id, (COALESCE(n_d152, n_f64)) AS v FROM numfold ORDER BY id", "1,12.75;2,<NULL>;3,1;4,-3.5"},
		{"DecimalBesideDouble|GroupByKey", "SELECT (COALESCE(n_d152, n_f64)) AS v, COUNT(*) AS n FROM numfold GROUP BY (COALESCE(n_d152, n_f64)) ORDER BY 1", "-3.5,1;1,1;12.75,1;<NULL>,1"},
		{"DecimalBesideDouble|AggregateInput", "SELECT MAX(COALESCE(n_d152, n_f64)) AS mx, MIN(COALESCE(n_d152, n_f64)) AS mn, COUNT(COALESCE(n_d152, n_f64)) AS c, COUNT(DISTINCT COALESCE(n_d152, n_f64)) AS d FROM numfold", "12.75,-3.5,3,3"},
		{"DecimalBesideDouble|WindowArgAndOrder", "SELECT id, MAX(COALESCE(n_d152, n_f64)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (COALESCE(n_d152, n_f64)), id) AS r FROM numfold ORDER BY id", "1,12.75,3;2,12.75,4;3,12.75,2;4,12.75,1"},
		{"DecimalBesideDouble|SetOpArm", "SELECT (COALESCE(n_d152, n_f64)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-3.5;-0.25;0.2;1;12.75;16777217;<NULL>;<NULL>"},
		{"DecimalBesideDouble|CastOfTheComposite", "SELECT id, CAST((COALESCE(n_d152, n_f64)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,12.750000;2,<NULL>;3,1.000000;4,-3.500000"},
		{"DecimalBesideDouble|ArithmeticOverIt", "SELECT id, (COALESCE(n_d152, n_f64)) * 2 AS v FROM numfold ORDER BY id", "1,25.5;2,<NULL>;3,2;4,-7"},
		{"DecimalBesideDouble|Distinct", "SELECT DISTINCT (COALESCE(n_d152, n_f64)) AS v FROM numfold ORDER BY 1", "-3.5;1;12.75;<NULL>"},
		{"RealBesideDecimal|Projection", "SELECT id, (COALESCE(n_f32, n_d152)) AS v FROM numfold ORDER BY id", "1,0.1;2,<NULL>;3,1.6777216e+07;4,-0.5"},
		{"RealBesideDecimal|GroupByKey", "SELECT (COALESCE(n_f32, n_d152)) AS v, COUNT(*) AS n FROM numfold GROUP BY (COALESCE(n_f32, n_d152)) ORDER BY 1", "-0.5,1;0.1,1;1.6777216e+07,1;<NULL>,1"},
		{"RealBesideDecimal|AggregateInput", "SELECT MAX(COALESCE(n_f32, n_d152)) AS mx, MIN(COALESCE(n_f32, n_d152)) AS mn, COUNT(COALESCE(n_f32, n_d152)) AS c, COUNT(DISTINCT COALESCE(n_f32, n_d152)) AS d FROM numfold", "1.6777216e+07,-0.5,3,3"},
		{"RealBesideDecimal|WindowArgAndOrder", "SELECT id, MAX(COALESCE(n_f32, n_d152)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (COALESCE(n_f32, n_d152)), id) AS r FROM numfold ORDER BY id", "1,1.6777216e+07,2;2,1.6777216e+07,4;3,1.6777216e+07,3;4,1.6777216e+07,1"},
		{"RealBesideDecimal|SetOpArm", "SELECT (COALESCE(n_f32, n_d152)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-0.5;-0.25;0.10000000149011612;0.2;16777216;16777217;<NULL>;<NULL>"},
		{"RealBesideDecimal|CastOfTheComposite", "SELECT id, CAST((COALESCE(n_f32, n_d152)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,0.100000;2,<NULL>;3,16777200.000000;4,-0.500000"},
		{"RealBesideDecimal|ArithmeticOverIt", "SELECT id, (COALESCE(n_f32, n_d152)) * 2 AS v FROM numfold ORDER BY id", "1,0.20000000298023224;2,<NULL>;3,33554432;4,-1"},
		{"RealBesideDecimal|Distinct", "SELECT DISTINCT (COALESCE(n_f32, n_d152)) AS v FROM numfold ORDER BY 1", "-0.5;0.1;1.6777216e+07;<NULL>"},
		{"IntWidths|Projection", "SELECT id, (COALESCE(n_i32, n_i64)) AS v FROM numfold ORDER BY id", "1,3;2,<NULL>;3,16777217;4,-5"},
		{"IntWidths|GroupByKey", "SELECT (COALESCE(n_i32, n_i64)) AS v, COUNT(*) AS n FROM numfold GROUP BY (COALESCE(n_i32, n_i64)) ORDER BY 1", "-5,1;3,1;16777217,1;<NULL>,1"},
		{"IntWidths|AggregateInput", "SELECT MAX(COALESCE(n_i32, n_i64)) AS mx, MIN(COALESCE(n_i32, n_i64)) AS mn, COUNT(COALESCE(n_i32, n_i64)) AS c, COUNT(DISTINCT COALESCE(n_i32, n_i64)) AS d FROM numfold", "16777217,-5,3,3"},
		{"IntWidths|WindowArgAndOrder", "SELECT id, MAX(COALESCE(n_i32, n_i64)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (COALESCE(n_i32, n_i64)), id) AS r FROM numfold ORDER BY id", "1,16777217,2;2,16777217,4;3,16777217,3;4,16777217,1"},
		{"IntWidths|SetOpArm", "SELECT (COALESCE(n_i32, n_i64)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-5;-0.25;0.2;3;16777217;16777217;<NULL>;<NULL>"},
		{"IntWidths|CastOfTheComposite", "SELECT id, CAST((COALESCE(n_i32, n_i64)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,3.000000;2,<NULL>;3,16777217.000000;4,-5.000000"},
		{"IntWidths|ArithmeticOverIt", "SELECT id, (COALESCE(n_i32, n_i64)) * 2 AS v FROM numfold ORDER BY id", "1,6;2,<NULL>;3,33554434;4,-10"},
		{"IntWidths|Distinct", "SELECT DISTINCT (COALESCE(n_i32, n_i64)) AS v FROM numfold ORDER BY 1", "-5;3;16777217;<NULL>"},
		{"TwoDecimalScales|Projection", "SELECT id, (COALESCE(n_d152, n_d3810)) AS v FROM numfold ORDER BY id", "1,12.75;2,<NULL>;3,1.00;4,-3.50"},
		{"TwoDecimalScales|GroupByKey", "SELECT (COALESCE(n_d152, n_d3810)) AS v, COUNT(*) AS n FROM numfold GROUP BY (COALESCE(n_d152, n_d3810)) ORDER BY 1", "-3.50,1;1.00,1;12.75,1;<NULL>,1"},
		{"TwoDecimalScales|AggregateInput", "SELECT MAX(COALESCE(n_d152, n_d3810)) AS mx, MIN(COALESCE(n_d152, n_d3810)) AS mn, COUNT(COALESCE(n_d152, n_d3810)) AS c, COUNT(DISTINCT COALESCE(n_d152, n_d3810)) AS d FROM numfold", "12.75,-3.50,3,3"},
		{"TwoDecimalScales|WindowArgAndOrder", "SELECT id, MAX(COALESCE(n_d152, n_d3810)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (COALESCE(n_d152, n_d3810)), id) AS r FROM numfold ORDER BY id", "1,12.75,3;2,12.75,4;3,12.75,2;4,12.75,1"},
		{"TwoDecimalScales|SetOpArm", "SELECT (COALESCE(n_d152, n_d3810)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-3.5;-0.25;0.2;1;12.75;16777217;<NULL>;<NULL>"},
		{"TwoDecimalScales|CastOfTheComposite", "SELECT id, CAST((COALESCE(n_d152, n_d3810)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,12.750000;2,<NULL>;3,1.000000;4,-3.500000"},
		{"TwoDecimalScales|ArithmeticOverIt", "SELECT id, (COALESCE(n_d152, n_d3810)) * 2 AS v FROM numfold ORDER BY id", "1,25.50;2,<NULL>;3,2.00;4,-7.00"},
		{"TwoDecimalScales|Distinct", "SELECT DISTINCT (COALESCE(n_d152, n_d3810)) AS v FROM numfold ORDER BY 1", "-3.50;1.00;12.75;<NULL>"},
		{"CaseDecimalElseDouble|Projection", "SELECT id, (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS v FROM numfold ORDER BY id", "1,12.75;2,<NULL>;3,16777217;4,-0.25"},
		{"CaseDecimalElseDouble|GroupByKey", "SELECT (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS v, COUNT(*) AS n FROM numfold GROUP BY (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) ORDER BY 1", "-0.25,1;12.75,1;16777217,1;<NULL>,1"},
		{"CaseDecimalElseDouble|AggregateInput", "SELECT MAX(CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS mx, MIN(CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS mn, COUNT(CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS c, COUNT(DISTINCT CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS d FROM numfold", "16777217,-0.25,3,3"},
		{"CaseDecimalElseDouble|WindowArgAndOrder", "SELECT id, MAX(CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END), id) AS r FROM numfold ORDER BY id", "1,16777217,2;2,16777217,4;3,16777217,3;4,16777217,1"},
		{"CaseDecimalElseDouble|SetOpArm", "SELECT (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-0.25;-0.25;0.2;12.75;16777217;16777217;<NULL>;<NULL>"},
		{"CaseDecimalElseDouble|CastOfTheComposite", "SELECT id, CAST((CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,12.750000;2,<NULL>;3,16777217.000000;4,-0.250000"},
		{"CaseDecimalElseDouble|ArithmeticOverIt", "SELECT id, (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) * 2 AS v FROM numfold ORDER BY id", "1,25.5;2,<NULL>;3,33554434;4,-0.5"},
		{"CaseDecimalElseDouble|Distinct", "SELECT DISTINCT (CASE WHEN id = 1 THEN n_d152 ELSE n_f64 END) AS v FROM numfold ORDER BY 1", "-0.25;12.75;16777217;<NULL>"},
		{"BigintQuotedInt|Projection", "SELECT id, (COALESCE(n_i64, '16777217')) AS v FROM numfold ORDER BY id", "1,4;2,16777217;3,16777217;4,-6"},
		{"BigintQuotedInt|GroupByKey", "SELECT (COALESCE(n_i64, '16777217')) AS v, COUNT(*) AS n FROM numfold GROUP BY (COALESCE(n_i64, '16777217')) ORDER BY 1", "-6,1;4,1;16777217,2"},
		{"BigintQuotedInt|AggregateInput", "SELECT MAX(COALESCE(n_i64, '16777217')) AS mx, MIN(COALESCE(n_i64, '16777217')) AS mn, COUNT(COALESCE(n_i64, '16777217')) AS c, COUNT(DISTINCT COALESCE(n_i64, '16777217')) AS d FROM numfold", "16777217,-6,4,3"},
		{"BigintQuotedInt|WindowArgAndOrder", "SELECT id, MAX(COALESCE(n_i64, '16777217')) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (COALESCE(n_i64, '16777217')), id) AS r FROM numfold ORDER BY id", "1,16777217,2;2,16777217,3;3,16777217,4;4,16777217,1"},
		{"BigintQuotedInt|SetOpArm", "SELECT (COALESCE(n_i64, '16777217')) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-6;-0.25;0.2;4;16777217;16777217;16777217;<NULL>"},
		{"BigintQuotedInt|CastOfTheComposite", "SELECT id, CAST((COALESCE(n_i64, '16777217')) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,4.000000;2,16777217.000000;3,16777217.000000;4,-6.000000"},
		{"BigintQuotedInt|ArithmeticOverIt", "SELECT id, (COALESCE(n_i64, '16777217')) * 2 AS v FROM numfold ORDER BY id", "1,8;2,33554434;3,33554434;4,-12"},
		{"BigintQuotedInt|Distinct", "SELECT DISTINCT (COALESCE(n_i64, '16777217')) AS v FROM numfold ORDER BY 1", "-6;4;16777217"},
		{"RealQuotedFractionInt|Projection", "SELECT id, (GREATEST(n_f32, '3.5', n_i32)) AS v FROM numfold ORDER BY id", "1,3.5;2,3.5;3,1.6777216e+07;4,3.5"},
		{"RealQuotedFractionInt|GroupByKey", "SELECT (GREATEST(n_f32, '3.5', n_i32)) AS v, COUNT(*) AS n FROM numfold GROUP BY (GREATEST(n_f32, '3.5', n_i32)) ORDER BY 1", "3.5,3;1.6777216e+07,1"},
		{"RealQuotedFractionInt|AggregateInput", "SELECT MAX(GREATEST(n_f32, '3.5', n_i32)) AS mx, MIN(GREATEST(n_f32, '3.5', n_i32)) AS mn, COUNT(GREATEST(n_f32, '3.5', n_i32)) AS c, COUNT(DISTINCT GREATEST(n_f32, '3.5', n_i32)) AS d FROM numfold", "1.6777216e+07,3.5,4,2"},
		{"RealQuotedFractionInt|WindowArgAndOrder", "SELECT id, MAX(GREATEST(n_f32, '3.5', n_i32)) OVER () AS w, ROW_NUMBER() OVER (ORDER BY (GREATEST(n_f32, '3.5', n_i32)), id) AS r FROM numfold ORDER BY id", "1,1.6777216e+07,1;2,1.6777216e+07,2;3,1.6777216e+07,4;4,1.6777216e+07,3"},
		{"RealQuotedFractionInt|SetOpArm", "SELECT (GREATEST(n_f32, '3.5', n_i32)) AS v FROM numfold UNION ALL SELECT n_f64 FROM numfold ORDER BY 1", "-0.25;0.2;3.5;3.5;3.5;16777216;16777217;<NULL>"},
		{"RealQuotedFractionInt|CastOfTheComposite", "SELECT id, CAST((GREATEST(n_f32, '3.5', n_i32)) AS DECIMAL(30,6)) AS v FROM numfold ORDER BY id", "1,3.500000;2,3.500000;3,16777200.000000;4,3.500000"},
		{"RealQuotedFractionInt|ArithmeticOverIt", "SELECT id, (GREATEST(n_f32, '3.5', n_i32)) * 2 AS v FROM numfold ORDER BY id", "1,7;2,7;3,33554432;4,7"},
		{"RealQuotedFractionInt|Distinct", "SELECT DISTINCT (GREATEST(n_f32, '3.5', n_i32)) AS v FROM numfold ORDER BY 1", "3.5;1.6777216e+07"},
	}
}
