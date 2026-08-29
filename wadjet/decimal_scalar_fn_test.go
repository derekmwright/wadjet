package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The scalar math functions that answer in their argument's OWN domain, over a
// DECIMAL — #668, ADR-0024 items 2 and 3.
//
// PostgreSQL answers abs/ceil/floor/round/trunc/sign/mod in `numeric` over a
// numeric argument. Wadjet declared every one of them RetFloat64 and computed
// through ToFloat64, whose default arm parses a DECIMAL column's rendered TEXT
// with fmt.Sscanf — so the value made a round trip through a double before any
// rounding happened, and on the paths where that parse fails it produced 0 for
// every row. ROUND was the visible one; the whole family shared the cause.
//
// Every expected value here was verified live against postgres:17.11.

// TestDecimalScalarFunctionsAreExact is the value gate.
func TestDecimalScalarFunctionsAreExact(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// a = 12.75 at DECIMAL(9,2); row 5 holds -0.01.
		{"round to one digit", "SELECT ROUND(a, 1) AS v FROM decdecl WHERE id = 1", "12.8"},
		{"round the wider column", "SELECT ROUND(b, 1) AS v FROM decdecl WHERE id = 1", "12.8"},
		{"round to integer", "SELECT ROUND(a) AS v FROM decdecl WHERE id = 1", "13"},
		// PostgreSQL's round(1234.56, -2) is 1200 and round(12.75, -1) is 10:
		// a negative digit count rounds to a power of ten ABOVE the point.
		{"round to a negative digit", "SELECT ROUND(a, -1) AS v FROM decdecl WHERE id = 1", "10"},
		{"abs", "SELECT ABS(a) AS v FROM decdecl WHERE id = 5", "0.01"},
		{"ceil", "SELECT CEIL(a) AS v FROM decdecl WHERE id = 1", "13"},
		{"ceil of a negative", "SELECT CEILING(a) AS v FROM decdecl WHERE id = 5", "0"},
		{"floor", "SELECT FLOOR(a) AS v FROM decdecl WHERE id = 1", "12"},
		{"floor of a negative", "SELECT FLOOR(a) AS v FROM decdecl WHERE id = 5", "-1"},
		{"sign", "SELECT SIGN(a) AS v FROM decdecl WHERE id = 5", "-1"},
		// TRUNC cuts toward zero and cannot carry; ROUND at the same digit
		// would answer 12.8.
		{"trunc", "SELECT TRUNC(a, 1) AS v FROM decdecl WHERE id = 1", "12.7"},
		{"the truncate spelling", "SELECT TRUNCATE(a, 1) AS v FROM decdecl WHERE id = 1", "12.7"},
		// mod is the `%` operator spelled as a call, so it takes the same
		// rule and the same kernel.
		{"mod by a literal", "SELECT MOD(a, 3) AS v FROM decdecl WHERE id = 1", "0.75"},
		{"mod across scales", "SELECT MOD(b, a) AS v FROM decdecl WHERE id = 2", "0.0001"},
		// The result is an exact operand of everything downstream.
		{"in arithmetic", "SELECT ROUND(a, 1) * 2 AS v FROM decdecl WHERE id = 1", "25.6"},
		{"over arithmetic", "SELECT ROUND(a + b, 1) AS v FROM decdecl WHERE id = 1", "25.5"},
		{"inside an aggregate", "SELECT SUM(ROUND(a, 1)) AS v FROM decdecl", "53.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.sql, len(res.Rows))
			}
			got, ok := res.Rows[0]["v"].(string)
			if !ok {
				t.Fatalf("%s: v = %#v (%T), want the DECIMAL text — a non-string box means "+
					"the answer came back through float64 (#668)", tc.sql, res.Rows[0]["v"], res.Rows[0]["v"])
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalScalarFunctionsDeclareDecimal is the (p,s) each one names, per
// batch.DecimalScalarType. a is DECIMAL(9,2), so its integer part is 7 digits.
func TestDecimalScalarFunctionsDeclareDecimal(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		sql              string
		precision, scale int
	}{
		// abs keeps the argument's type exactly.
		{"SELECT ABS(a) AS v FROM decdecl", 9, 2},
		// ceil/floor/round-to-integer can CARRY, so the integer part grows by
		// one: 7 + 1 = 8 digits, scale 0.
		{"SELECT CEIL(a) AS v FROM decdecl", 8, 0},
		{"SELECT FLOOR(a) AS v FROM decdecl", 8, 0},
		{"SELECT ROUND(a) AS v FROM decdecl", 8, 0},
		// round(x, n) keeps n fraction digits, and can still carry.
		{"SELECT ROUND(a, 1) AS v FROM decdecl", 9, 1},
		// trunc cannot carry, so the integer part does not grow.
		{"SELECT TRUNC(a, 1) AS v FROM decdecl", 8, 1},
		// sign is -1, 0 or 1 whatever the argument's width.
		{"SELECT SIGN(a) AS v FROM decdecl", 1, 0},
		// mod is the `%` rule: s = max(2,0), p = min(7,1) + 2.
		{"SELECT MOD(a, 3) AS v FROM decdecl", 3, 2},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d column metas, want 1", len(res.ColumnMetas))
			}
			m := res.ColumnMetas[0]
			if m.TypeID != parquet.TypeDecimal {
				t.Fatalf("declared %s, want DECIMAL", m.TypeID)
			}
			if m.Precision != tc.precision || m.Scale != tc.scale {
				t.Errorf("declared DECIMAL(%d,%d), want DECIMAL(%d,%d)",
					m.Precision, m.Scale, tc.precision, tc.scale)
			}
		})
	}
}

// TestTranscendentalFunctionsStayFloat64 pins the DELIBERATE divergence.
//
// PostgreSQL answers sqrt/exp/ln/log/power over a numeric in numeric. Wadjet
// keeps them in float64, which is ADR-0012 item 9's recorded class — the same
// one STDDEV/VARIANCE/CORR/MEDIAN sit in. Closing it means building an exact
// fixed-point tower, not widening an accumulator, so it is pinned here rather
// than left to be discovered.
func TestTranscendentalFunctionsStayFloat64(t *testing.T) {
	db := ddrOpen(t)
	for _, sql := range []string{
		"SELECT SQRT(a) AS v FROM decdecl WHERE id = 1",
		"SELECT POWER(a, 2) AS v FROM decdecl WHERE id = 1",
		"SELECT LN(a) AS v FROM decdecl WHERE id = 1",
		"SELECT EXP(a) AS v FROM decdecl WHERE id = 5",
		"SELECT LOG(a) AS v FROM decdecl WHERE id = 1",
	} {
		t.Run(sql, func(t *testing.T) {
			res := ddrQuery(t, db, sql)
			if len(res.ColumnMetas) != 1 || res.ColumnMetas[0].TypeID != parquet.TypeFloat64 {
				t.Errorf("%s declared %v, want FLOAT64 — an exact numeric tower is a separate "+
					"change (ADR-0012 item 9's class)", sql, res.ColumnMetas)
			}
		})
	}
}

// TestIntegerFunctionArithmeticTruncates is #636: `length(s) / 2` is 2 in
// PostgreSQL, not 2.5.
//
// compileBinOp chose the arithmetic node from the operands' COMPILE-TIME
// shape, and a function call had none — `possiblyIntAtRuntime` answered false
// for every one — so the expression compiled to BinOpFloat64 and answered a
// fraction. The registry's declaration is the shape it was missing.
func TestIntegerFunctionArithmeticTruncates(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		// s is "12.75" on row 1: five characters.
		{"length over two", "SELECT LENGTH(s) / 2 AS v FROM decdecl WHERE id = 1", 2},
		{"octet_length over two", "SELECT OCTET_LENGTH(s) / 2 AS v FROM decdecl WHERE id = 1", 2},
		{"char_length over two", "SELECT CHAR_LENGTH(s) / 2 AS v FROM decdecl WHERE id = 1", 2},
		{"nested", "SELECT (LENGTH(s) + 1) / 2 AS v FROM decdecl WHERE id = 1", 3},
		{"modulo", "SELECT LENGTH(s) % 2 AS v FROM decdecl WHERE id = 1", 1},
		// The plain integer forms, unchanged, beside them.
		{"column over literal", "SELECT id / 2 AS v FROM decdecl WHERE id = 7", 3},
		{"two literals", "SELECT 7 / 2 AS v FROM decdecl WHERE id = 1", 3},
		{"negative truncates toward zero", "SELECT (0 - 7) / 2 AS v FROM decdecl WHERE id = 1", -3},
		{"modulo takes the dividend's sign", "SELECT 7 % (0 - 3) AS v FROM decdecl WHERE id = 1", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.sql, len(res.Rows))
			}
			got := res.Rows[0]["v"]
			if got != any(tc.want) {
				t.Errorf("%s = %#v (%T), want %d — integer division TRUNCATES (#636)",
					tc.sql, got, got, tc.want)
			}
			if len(res.ColumnMetas) == 1 && res.ColumnMetas[0].TypeID != parquet.TypeInt64 {
				t.Errorf("%s declared %s, want INT64", tc.sql, res.ColumnMetas[0].TypeID)
			}
		})
	}
}

// TestIntegerArithmeticOverflowIsAnError is #637's reachable half: an integer
// result with no int64 is 22003, PostgreSQL's `bigint out of range`, and never
// the wrapped number Go's operators answer.
//
// A wrapped total is a different number wearing the right type — the same
// class of silent wrong answer ADR-0024 item 4 closes for DECIMAL — and this
// node's own doc comment used to promise it out loud.
func TestIntegerArithmeticOverflowIsAnError(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"addition", "SELECT 9223372036854775807 + id AS v FROM decdecl WHERE id = 1"},
		{"subtraction", "SELECT (0 - 9223372036854775807) - id AS v FROM decdecl WHERE id = 7"},
		{"multiplication", "SELECT 4611686018427387904 * id AS v FROM decdecl WHERE id = 7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Query(context.Background(), tc.sql)
			if err == nil {
				t.Fatalf("%s answered instead of failing — a wrapped integer is a different "+
					"number wearing the right type (#637)", tc.sql)
			}
			if got := sqlerr.StateOf(err); got != "22003" {
				t.Errorf("%s failed with SQLSTATE %q (%v), want 22003", tc.sql, got, err)
			}
		})
	}
}

// TestConstantDecimalProjectionIsNotZero pins the defect the pg-oracle found
// in the first cut of this work: a DECIMAL-declared projection whose
// VECTORIZED arm did not apply left the output vector's ZEROS standing, and
// every row came back 0.
//
// `SELECT ROUND(0.5), ROUND(-0.5)` answered float 1 beside decimal 0 — two
// halves of one query disagreeing about their own type, and the decimal half
// silently wrong. Two things were wrong and both are fixed: unary minus over a
// literal took a path its positive twin did not, and the vec arm returned
// without writing and without saying so. The vec arm now REPORTS whether it
// wrote, and exec.Project falls back to the checked boxed writer when it did
// not — so a declaration the runtime cannot honour is at worst slow, never
// silently zero.
//
// The shapes here have no FROM clause, which is where the output vector's
// carrier slice is not sized the way a scan's is.
func TestConstantDecimalProjectionIsNotZero(t *testing.T) {
	db := ddrOpen(t)
	t.Run("the oracle's own repro", func(t *testing.T) {
		res := ddrQuery(t, db, `SELECT ROUND(0.5) AS a, ROUND(-0.5) AS d`)
		if got := fmt.Sprint(res.Rows[0]["a"]); got != "1" {
			t.Errorf("ROUND(0.5) = %q, want 1", got)
		}
		if got := fmt.Sprint(res.Rows[0]["d"]); got != "-1" {
			t.Errorf("ROUND(-0.5) = %q, want -1 — a 0 here is the output vector's own "+
				"zero, which nothing ever wrote", got)
		}
	})
	// A DECIMAL-declared expression with no FROM, through the boxed writer
	// (CAST) and through the vectorized one (arithmetic over a cast, which IS
	// an exact operand).
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{`SELECT CAST('12.75' AS DECIMAL(9,2)) AS v`, "12.75"},
		{`SELECT CAST('12.75' AS DECIMAL(9,2)) * 2 AS v`, "25.50"},
		{`SELECT CAST('12.75' AS DECIMAL(9,2)) + CAST('0.25' AS DECIMAL(9,2)) AS v`, "13.00"},
		{`SELECT ROUND(CAST('12.755' AS DECIMAL(9,3)), 2) AS v`, "12.76"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.sql, len(res.Rows))
			}
			if got := res.Rows[0]["v"]; got != any(tc.want) {
				t.Errorf("%s = %#v (%T), want %q", tc.sql, got, got, tc.want)
			}
		})
	}
}
