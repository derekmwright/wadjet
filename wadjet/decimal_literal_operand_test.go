package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A numeric LITERAL is an exact DECIMAL operand — #555's review finding R2,
// ADR-0024 item 3.
//
// compileBinOp chose the arithmetic node from the operands' COMPILE-TIME
// shape, and a fractional literal is a float64 box that isIntNative rejects,
// so the pair pinned to BinOpFloat64 — while the planner declared DECIMAL from
// the same literal's spelling. The float answer then met an exact vector:
// `d * 1.05` was 13.387500000000001 and failed the checked store with 22003,
// and `SELECT 0.1 + 0.2` did the same with no DECIMAL column in the query at
// all.
//
// Every value here is one a float64 CANNOT represent, which is the point: the
// fixture's own values (12.75, 2.00) are all float-exact, so nothing built on
// them could see this. Every expectation is what postgres:17.11 answers.
func TestNumericLiteralIsAnExactDecimalOperand(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// 1.05 has no float64; the product's last digit is the proof.
		{"times a non-representable literal",
			"SELECT a * 1.05 AS v FROM decdecl WHERE id = 1", "13.3875"},
		{"the wider column too",
			"SELECT b * 1.1 AS v FROM decdecl WHERE id = 1", "14.02500"},
		// 12.75 + 1e-20 needs 22 digits: a float64 holds 16 and drops the 1.
		{"a literal below the double's last place",
			"SELECT a + 0.00000000000000000001 AS v FROM decdecl WHERE id = 1",
			"12.75000000000000000001"},
		// TRAILING ZEROS are part of the spelling and so part of the type:
		// PostgreSQL renders `12.75 * 100.0` as 1275.000, three fraction
		// digits, because the literal contributed one.
		{"trailing zeros are digits", "SELECT a * 100.0 AS v FROM decdecl WHERE id = 1", "1275.000"},
		// Literal OP literal, with no column anywhere: PostgreSQL types both
		// as numeric and answers exactly.
		{"the canonical float trap", "SELECT 0.1 + 0.2 AS v", "0.3"},
		{"and its neighbour", "SELECT 1.1 + 2.2 AS v", "3.3"},
		// A division between two CONSTANTS keeps the float path: item 3's
		// division scale is a policy floor of six fraction digits, chosen
		// for column operands, and between two narrow literals the floor is
		// all there is — `1.0 / 3` would keep six digits where the double,
		// and PostgreSQL, keep sixteen or more. Every other operator is
		// exact at a scale derived from the operands' own, so only this one
		// declines (TestCompileBinOpDivision has pinned it since #369).
		{"a constant division stays float", "SELECT 10.0 / 4 AS v", ""},
		// An INTEGER literal stays integer, so integer division still
		// truncates — the rule this must not have broken. (Over a table: a
		// no-FROM projection types its constants float64 whatever they are,
		// which is a separate, pre-existing shape.)
		{"an integer literal stays integer", "SELECT 7 / 2 AS v FROM decdecl WHERE id = 1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.sql, len(res.Rows))
			}
			if tc.want == "" {
				// The two shapes that deliberately stay off the exact path:
				// an integer literal pair (integer division, 3) and a
				// constant division (float, 2.5).
				switch got := res.Rows[0]["v"]; got {
				case any(int64(3)), any(float64(2.5)):
				default:
					t.Errorf("%s = %#v (%T), want the integer 3 or the float 2.5",
						tc.sql, got, got)
				}
				return
			}
			got, ok := res.Rows[0]["v"].(string)
			if !ok {
				t.Fatalf("%s: v = %#v (%T), want the DECIMAL text — a non-string box means "+
					"the answer came back through float64", tc.sql, res.Rows[0]["v"], res.Rows[0]["v"])
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestFloatTrapIsExact is the one-line statement of the same thing, in the
// form a user would write it. PostgreSQL answers true.
func TestFloatTrapIsExact(t *testing.T) {
	db := ddrOpen(t)
	res := ddrQuery(t, db, "SELECT 0.1 + 0.2 = 0.3 AS v")
	if got := res.Rows[0]["v"]; got != any(true) {
		t.Errorf("0.1 + 0.2 = 0.3 answered %#v, want true — the sum is exact numeric in "+
			"PostgreSQL and must be here (#555 review, R2)", got)
	}
}

// TestModuloOverDecimalAndFractions is #555's review findings N1 and N2.
//
// `%` was the one operator ff7aeced's "same rule, same kernel" claim did not
// hold for: `MOD(d, 0.5)` took the decimal kernel and `d % 0.5` did not. The
// float arm it fell to computed `float64(int64(lf) % int64(rf))`, which
// truncates BOTH operands to integers first — so `d % 1.5` was 0 instead of
// 0.75, and `d % 0.5` divided by a zero the truncation created and CRASHED the
// query with a runtime panic.
func TestModuloOverDecimalAndFractions(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		sql  string
		want string
	}{
		{"SELECT a % 0.5 AS v FROM decdecl WHERE id = 1", "0.25"},
		{"SELECT a % 1.5 AS v FROM decdecl WHERE id = 1", "0.75"},
		{"SELECT 0.5 % a AS v FROM decdecl WHERE id = 1", "0.50"},
		{"SELECT MOD(a, 0.5) AS v FROM decdecl WHERE id = 1", "0.25"},
		{"SELECT a % b AS v FROM decdecl WHERE id = 2", "12.7500"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			got, ok := res.Rows[0]["v"].(string)
			if !ok {
				t.Fatalf("%s: v = %#v (%T), want the DECIMAL text", tc.sql, res.Rows[0]["v"], res.Rows[0]["v"])
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
	// A FLOAT operand keeps the float operator, which PostgreSQL does not
	// have at all (`double precision % numeric` is 42883) — so this is a
	// documented SUPERSET. What it must not be is a crash: math.Mod answers
	// where truncating to integers divided by zero.
	res := ddrQuery(t, db, "SELECT CAST(a AS DOUBLE PRECISION) % 0.5 AS v FROM decdecl WHERE id = 1")
	if got := res.Rows[0]["v"]; got != any(float64(0.25)) {
		t.Errorf("float %% 0.5 = %#v, want 0.25 (math.Mod) — PostgreSQL has no such "+
			"operator, so this is a superset, but never a panic", got)
	}
}

// TestBareDecimalCastOverTextDoesNotBreakTheStore is review finding R3: a bare
// `CAST(text AS DECIMAL)` produced a decimal box while its DECLARATION stayed
// FLOAT64, and the store refused it — "cannot store string into FLOAT64
// vector", the #361 guard firing on a value the engine produced itself.
//
// The declaration is the authority: a bare destination takes the operand's own
// scale and a TEXT operand has none this layer can name, so the value follows
// the declaration to float64. That is ADR-0024's recorded residual, and it is
// now what both halves do.
func TestBareDecimalCastOverTextDoesNotBreakTheStore(t *testing.T) {
	db := ddrOpen(t)
	res := ddrQuery(t, db, "SELECT CAST(s AS DECIMAL) AS v FROM decdecl WHERE id = 1")
	if got := res.Rows[0]["v"]; got != any(float64(12.75)) {
		t.Errorf("CAST(text AS DECIMAL) = %#v (%T), want the float64 12.75 its declaration "+
			"names", got, got)
	}
	if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeFloat64 {
		t.Errorf("declared %s, want FLOAT64 — the value and the declaration must agree", m.TypeID)
	}
}

// TestNumericCastRefusalsMatchPostgres covers review findings N3, N4 and S1:
// the refusals a cast owes, in the spelling a user writes.
func TestNumericCastRefusalsMatchPostgres(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name  string
		sql   string
		state string
	}{
		// N3: PostgreSQL has no boolean-to-numeric cast in ANY spelling. The
		// BARE one answered 1, because a bare destination declines to name a
		// type before the refusal is ever reached.
		{"bare cast from boolean", "SELECT CAST(TRUE AS NUMERIC) AS v", "42846"},
		{"parameterized cast from boolean", "SELECT CAST(TRUE AS NUMERIC(9,2)) AS v", "42846"},
		{"the :: spelling", "SELECT TRUE::numeric AS v", "42846"},
		// S1: a well-formed number that is simply TOO WIDE is a RANGE
		// condition. Reporting 22P02 sends a client hunting a typo in a
		// number it read correctly.
		{"a number past the carrier", "SELECT CAST('1e40' AS DECIMAL(38,0)) AS v", "22003"},
		{"a 40-digit integer text",
			"SELECT CAST('1234567890123456789012345678901234567890' AS DECIMAL(38,10)) AS v", "22003"},
		// Still 22P02 for text that names no number at all.
		{"text that is not a number", "SELECT CAST('abc' AS DECIMAL(9,2)) AS v", "22P02"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Query(context.Background(), tc.sql)
			if err == nil {
				t.Fatalf("%s answered instead of refusing", tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s failed with SQLSTATE %q (%v), want %q", tc.sql, got, err, tc.state)
			}
		})
	}
}

// TestSmallintCastAndWideRound are the two shapes that answered nothing useful:
// N4, a SMALLINT destination that was a declared-STRING pass-through, and S1's
// other half, a ROUND whose result type capped the precision while KEEPING the
// scale — DECIMAL(38,38), whose bound is |v| < 10^0, so no value with an
// integer digit could be declared at all.
func TestSmallintCastAndWideRound(t *testing.T) {
	db := ddrOpen(t)
	t.Run("cast to smallint rounds", func(t *testing.T) {
		res := ddrQuery(t, db, "SELECT CAST(a AS SMALLINT) AS v FROM decdecl WHERE id = 1")
		if got := res.Rows[0]["v"]; got != any(int64(13)) {
			t.Errorf("CAST(12.75 AS SMALLINT) = %#v (%T), want 13", got, got)
		}
	})
	t.Run("smallint refuses past its range", func(t *testing.T) {
		_, err := db.Query(context.Background(), "SELECT CAST('40000' AS DECIMAL(9,0))::smallint AS v")
		if err == nil {
			t.Fatal("40000::smallint answered; PostgreSQL says smallint out of range")
		}
		if got := sqlerr.StateOf(err); got != "22003" {
			t.Errorf("SQLSTATE %q (%v), want 22003", got, err)
		}
	})
	t.Run("round past the carrier keeps the value", func(t *testing.T) {
		// PostgreSQL's numeric keeps all 40 fraction digits; the carrier gives
		// them up down to ADR-0024 item 3's floor. The VALUE is the same
		// number either way, which is what the oracle compares.
		res := ddrQuery(t, db, "SELECT ROUND(a, 40) AS v FROM decdecl WHERE id = 1")
		got, ok := res.Rows[0]["v"].(string)
		if !ok {
			t.Fatalf("v = %#v (%T), want the DECIMAL text", res.Rows[0]["v"], res.Rows[0]["v"])
		}
		if want := "12.7500000000000000000000000000000"; got != want {
			t.Errorf("ROUND(a, 40) = %q, want %q", got, want)
		}
	})
}
