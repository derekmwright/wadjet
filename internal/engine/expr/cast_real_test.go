package expr

import (
	"math"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCastToRealNarrowsToFloat32 is the regression for `CAST(x AS REAL)` being
// a NO-OP: the arm sat beside "float" and "double" and answered ToFloat64, so
// the cast the user wrote changed nothing at all.
//
// PostgreSQL types the result float4 and rounds the value into it, which is
// visible both in the value and in what it then compares equal to:
//
//	SELECT CAST(1.0/3 AS REAL)          -> 0.33333334   (not 0.3333333333333333)
//	SELECT pg_typeof(CAST(1 AS REAL))   -> real
//
// Bare FLOAT is deliberately NOT narrowed: PostgreSQL's unqualified `float` is
// double precision (pg_typeof(CAST(1 AS FLOAT)) is `double precision`;
// float(1..24) is real and float(25..53) is double), so only REAL and FLOAT4
// name float4.
func TestCastToRealNarrowsToFloat32(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{Name: "x", Type: parquet.TypeInt64}}, 1)
	b.Columns[0].SetValue(0, int64(1))
	b.Len = 1

	narrowing := []string{"CAST(3.1 AS REAL)", "CAST(3.1 AS real)", "CAST(3.1 AS FLOAT4)"}
	for _, sql := range narrowing {
		v := compileExprSQL(t, sql).Eval(b, 0)
		f, ok := v.(float32)
		if !ok {
			t.Errorf("%s produced %#v (%T), want a float32 — the cast is a no-op", sql, v, v)
			continue
		}
		if f != float32(3.1) {
			t.Errorf("%s = %v, want %v", sql, f, float32(3.1))
		}
	}

	widening := []string{"CAST(3.1 AS FLOAT)", "CAST(3.1 AS DOUBLE PRECISION)", "CAST(3.1 AS FLOAT8)"}
	for _, sql := range widening {
		v := compileExprSQL(t, sql).Eval(b, 0)
		f, ok := v.(float64)
		if !ok {
			t.Errorf("%s produced %#v (%T), want a float64 — PostgreSQL's bare FLOAT is double precision", sql, v, v)
			continue
		}
		if f != 3.1 {
			t.Errorf("%s = %v, want 3.1", sql, f)
		}
	}

	// The rounding is visible, not just the box: 1/3 at float4 precision.
	v := compileExprSQL(t, "CAST(1.0/3 AS REAL)").Eval(b, 0)
	if f, ok := v.(float32); !ok || f != float32(1.0/3.0) {
		t.Errorf("CAST(1.0/3 AS REAL) = %#v, want %v", v, float32(1.0/3.0))
	}
}

// TestCastToRealRefusesUnrepresentableValues follows PostgreSQL's float8->float4
// conversion, which raises rather than answering ±Inf or 0 (utils/adt/float.c,
// both SQLSTATE 22003, verified live):
//
//	SELECT CAST(1e40::float8 AS real)   -> ERROR: value out of range: overflow
//	SELECT CAST(1e-50::float8 AS real)  -> ERROR: value out of range: underflow
//
// An already-infinite value is representable and passes through — the
// distinction between "too big to be a real" and "is infinity", which the
// literal-side refusal draws too (kernel.RealLitTextUnrepresentable).
func TestCastToRealRefusesUnrepresentableValues(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{Name: "x", Type: parquet.TypeInt64}}, 1)
	b.Columns[0].SetValue(0, int64(1))
	b.Len = 1

	for _, c := range []struct{ sql, want string }{
		{"CAST(1e40 AS REAL)", "overflow"},
		{"CAST(-1e40 AS REAL)", "overflow"},
		{"CAST(1e-50 AS REAL)", "underflow"},
		// The boundary: real's smallest denormal is about 1.4e-45, and
		// PostgreSQL draws the line in the gap below it —
		// `CAST(1e-46 AS real)` raises where `CAST(1e-45 AS real)` answers.
		{"CAST(1e-46 AS REAL)", "underflow"},
		{"CAST(-1e-46 AS REAL)", "underflow"},
	} {
		err := catchFatalEval(t, func() { compileExprSQL(t, c.sql).Eval(b, 0) })
		if err == nil {
			t.Errorf("%s did not raise; PostgreSQL raises 22003 %s", c.sql, c.want)
			continue
		}
		if got := sqlerr.StateOf(err); got != "22003" {
			t.Errorf("%s: SQLSTATE %q, want 22003 (%v)", c.sql, got, err)
		}
	}

	// Infinity and zero are legal float4 values, not conversion failures.
	for _, sql := range []string{
		"CAST(CAST('Infinity' AS DOUBLE PRECISION) AS REAL)",
		"CAST(0 AS REAL)",
		"CAST(0.0 AS REAL)",
		// A denormal is a value, not a failure: PostgreSQL answers 1e-45.
		"CAST(1e-45 AS REAL)",
		"CAST(-1e-45 AS REAL)",
	} {
		if err := catchFatalEval(t, func() { compileExprSQL(t, sql).Eval(b, 0) }); err != nil {
			t.Errorf("%s raised %v; the value is representable", sql, err)
		}
	}
}

// TestMixedNumericComparisonKeepsPostgresFloatOrder is the regression for the
// last escape from #459: compare()'s "mixed numeric types" branch used Go's
// IEEE operators, so a FLOAT column against an INTEGER literal lost the total
// order the both-float64 branch beside it already had. `f > 1` dropped every
// NaN row while `f > 1.0` kept it — one predicate, two answers, decided by the
// literal's spelling. It is the ROW path, which is what the stage DAG compiles
// every scan-pushed filter to.
//
// Wants are PostgreSQL 17's over a real column holding {NaN, 0.1, 2.0}
// (ADR-0012 item 8: NaN is the greatest value and equal to itself).
func TestMixedNumericComparisonKeepsPostgresFloatOrder(t *testing.T) {
	b := realColBatch(float32(math.NaN()), float32(0.1), float32(2))

	cases := []struct {
		sql  string
		want []bool
	}{
		{"r > 1", []bool{true, false, true}},
		{"r >= 1", []bool{true, false, true}},
		{"r < 1", []bool{false, true, false}},
		{"r <= 1", []bool{false, true, false}},
		{"r = 1", []bool{false, false, false}},
		{"r <> 1", []bool{true, true, true}},
		// The float-literal spelling of the same predicates, which took the
		// both-float64 fast path and was already right. The two must agree.
		{"r > 1.0", []bool{true, false, true}},
		{"r <> 1.0", []bool{true, true, true}},
	}
	for _, c := range cases {
		pred := FilterPredicate(compileExprSQL(t, c.sql))
		for row := 0; row < b.Len; row++ {
			if got := pred(b, row); got != c.want[row] {
				t.Errorf("row %d: %s = %v, want %v (PostgreSQL 17)", row, c.sql, got, c.want[row])
			}
		}
	}
}

// TestCastDestTypeRealSpellings holds expr's own type map to the answer
// Cast.Eval now gives, for the reason TestCastDestTypeBooleanSpellings already
// records: this map and physical.inferCastType decide different things (the
// values an enclosing cast reads, and the column a projection allocates) and
// must agree about what a spelling NAMES — otherwise the write is refused, or
// the rounded value is silently widened straight back and the cast looks like
// the no-op it used to be. physical.TestInferCastTypeRealSpellings is the
// other half.
func TestCastDestTypeRealSpellings(t *testing.T) {
	for _, spelling := range []string{"REAL", "real", " Real ", "FLOAT4", "float4"} {
		got, ok := castDestType(spelling)
		if !ok || got != batch.TypeFloat32 {
			t.Errorf("castDestType(%q) = (%v, %v), want (FLOAT32, true)", spelling, got, ok)
		}
	}
	// PostgreSQL's bare FLOAT is DOUBLE PRECISION, not real.
	for _, spelling := range []string{"FLOAT", "float", "DOUBLE", "DOUBLE PRECISION", "FLOAT8", "FLOAT64"} {
		got, ok := castDestType(spelling)
		if !ok || got != batch.TypeFloat64 {
			t.Errorf("castDestType(%q) = (%v, %v), want (FLOAT64, true)", spelling, got, ok)
		}
	}
}

// TestCastToRealMessageFollowsTheOperand pins WHICH 22003 message a refused
// cast gives, because PostgreSQL has two and picks by what is being cast:
//
//	CAST(1e40 AS real)          -> "1000…000" is out of range for type real
//	CAST(1e40::float8 AS real)  -> value out of range: overflow
//
// The first is the numeric->real cast failing during constant folding, so it
// names the numeric's own digits — the same digits, and the same renderer
// (kernel.RealOverflowText), the IN-list refusal prints, so one query cannot
// produce two spellings of one refusal. The second is float8->real at runtime,
// which has no literal to name. Wadjet gave the runtime text for both.
//
// Every want below is postgres:17's, read off the server verbatim.
func TestCastToRealMessageFollowsTheOperand(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{{Name: "x", Type: parquet.TypeInt64}}, 1)
	b.Columns[0].SetValue(0, int64(1))
	b.Len = 1

	cases := []struct{ sql, want string }{
		// LITERAL operand: the numeric's digits, expanded, never in exponent
		// form, whatever the query spelled.
		{"CAST(1e40 AS REAL)", `"10000000000000000000000000000000000000000" is out of range for type real`},
		{"CAST(1e+40 AS REAL)", `"10000000000000000000000000000000000000000" is out of range for type real`},
		{"CAST(-1e40 AS REAL)", `"-10000000000000000000000000000000000000000" is out of range for type real`},
		{"CAST(1e-46 AS REAL)", `"0.0000000000000000000000000000000000000000000001" is out of range for type real`},
		// NON-LITERAL operand: no literal to name, so the runtime text, in
		// whichever direction the conversion failed.
		{"CAST(CAST(1e40 AS DOUBLE PRECISION) AS REAL)", "value out of range: overflow"},
		{"CAST(CAST(1e-46 AS DOUBLE PRECISION) AS REAL)", "value out of range: underflow"},
		{"CAST(x * 1e40 AS REAL)", "value out of range: overflow"},
		{"CAST(x * 1e-46 AS REAL)", "value out of range: underflow"},
	}
	for _, c := range cases {
		err := catchFatalEval(t, func() { compileExprSQL(t, c.sql).Eval(b, 0) })
		if err == nil {
			t.Errorf("%s did not raise; PostgreSQL raises 22003 %s", c.sql, c.want)
			continue
		}
		if got := sqlerr.StateOf(err); got != "22003" {
			t.Errorf("%s: SQLSTATE %q, want 22003 (%v)", c.sql, got, err)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s raised %q, want the message naming %s", c.sql, err.Error(), c.want)
		}
	}
}
