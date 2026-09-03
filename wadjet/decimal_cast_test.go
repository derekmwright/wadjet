package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// CAST to and from DECIMAL, exactly — ADR-0024 items 3, 4 and 6, #555's cast
// half.
//
// Before this the evaluator had two answers and neither was a DECIMAL:
// `"decimal", "numeric"` returned ToFloat64(v), and a PARAMETERIZED
// destination matched no case at all and fell to `default: return v` — the
// value passed through untouched with the (p,s) silently ignored, so
// `CAST(numeric(18,4) '12.7501' AS DECIMAL(9,2))` answered 12.7501 where
// PostgreSQL answers 12.75. The declared type followed: `inferCastType` put
// NUMERIC/DECIMAL in its float8 arm and had no arm at all for
// `"DECIMAL(10,2)"`, so the projection allocated a STRING column.
//
// Every expected value here was verified live against postgres:17.11.

// TestDecimalCastIsExact is the value gate.
func TestDecimalCastIsExact(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want any
	}{
		// From DECIMAL: rescaled, rounded half away from zero, exactly once.
		{"narrowing rounds", "SELECT CAST(b AS DECIMAL(9,2)) AS v FROM decdecl WHERE id = 2", "12.75"},
		{"widening is exact", "SELECT CAST(a AS DECIMAL(10,4)) AS v FROM decdecl WHERE id = 1", "12.7500"},
		{"same scale passes through", "SELECT CAST(a AS DECIMAL(10,2)) AS v FROM decdecl WHERE id = 1", "12.75"},
		{"the :: spelling", "SELECT a::numeric(9,1) AS v FROM decdecl WHERE id = 1", "12.8"},
		// From INTEGER: PostgreSQL's `7::numeric(10,2)` is 7.00.
		{"from an integer column", "SELECT CAST(id AS DECIMAL(10,2)) AS v FROM decdecl WHERE id = 7", "7.00"},
		// From TEXT: the checked parse, rounding at the target scale.
		{"from numeric text", "SELECT CAST('12.755' AS DECIMAL(9,2)) AS v FROM decdecl WHERE id = 1", "12.76"},
		{"from negative text", "SELECT CAST('-12.755' AS DECIMAL(9,2)) AS v FROM decdecl WHERE id = 1", "-12.76"},
		// A BARE destination keeps the operand's own scale at the carrier's
		// full width, and (38,0) from an integer (ADR-0024 item 3).
		{"bare keeps the operand's scale", "SELECT CAST(a AS DECIMAL) AS v FROM decdecl WHERE id = 1", "12.75"},
		{"bare over an integer is scale 0", "SELECT CAST(id AS DECIMAL) AS v FROM decdecl WHERE id = 7", "7"},
		{"bare over arithmetic", "SELECT CAST(a * b AS NUMERIC) AS v FROM decdecl WHERE id = 1", "162.562500"},
		// A cast that names its type is an exact arithmetic operand.
		{"cast in arithmetic", "SELECT CAST(a AS DECIMAL(10,2)) * 2 AS v FROM decdecl WHERE id = 1", "25.50"},
		// FROM a DECIMAL to the other families. The integer cast ROUNDS half
		// away from zero, which is PostgreSQL's rule for numeric->int (#373).
		{"to bigint rounds", "SELECT CAST(a AS BIGINT) AS v FROM decdecl WHERE id = 1", int64(13)},
		{"to text renders at the declared scale",
			"SELECT CAST(a AS TEXT) AS v FROM decdecl WHERE id = 1", "12.75"},
		{"to double precision", "SELECT CAST(a AS DOUBLE PRECISION) AS v FROM decdecl WHERE id = 1", 12.75},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.sql, len(res.Rows))
			}
			if got := res.Rows[0]["v"]; got != tc.want {
				t.Errorf("%s = %#v (%T), want %#v (%T)", tc.sql, got, got, tc.want, tc.want)
			}
		})
	}
}

// TestDecimalCastDeclaresItsDestination is the declared type at the wire,
// where a client meets it.
//
// A CAST that NAMES a (p,s) imposes it — that is the cast's own modifier,
// not select_common_typmod over its inputs, and PostgreSQL 17's \gdesc says
// so: `numeric(9,0)` for `CAST(x AS DECIMAL(9))`, `numeric(18,4)` for
// `CAST(x AS NUMERIC(18,4))`. Only a BARE destination drops to plain numeric.
//
// This test asserted the opposite until #708, which is where ADR-0024 item 5
// was corrected: it listed a CAST among the typmod--1 constructs. The three
// bare spellings below are the half that was always right, and they are the
// control — an arm that imposed a modifier for EVERY cast would be a new
// divergence in the other direction.
func TestDecimalCastDeclaresItsDestination(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		sql              string
		precision, scale int
		// wantUnconstrained is PostgreSQL's answer, measured live: true
		// where \gdesc says plain `numeric`, false where it names (p,s).
		wantUnconstrained bool
	}{
		{"SELECT CAST(a AS DECIMAL(10,2)) AS v FROM decdecl", 10, 2, false},
		{"SELECT CAST(a AS NUMERIC(18,4)) AS v FROM decdecl", 18, 4, false},
		{"SELECT CAST(a AS DECIMAL(9)) AS v FROM decdecl", 9, 0, false},
		{"SELECT CAST(a AS DECIMAL) AS v FROM decdecl", 38, 2, true},
		{"SELECT CAST(b AS NUMERIC) AS v FROM decdecl", 38, 4, true},
		{"SELECT CAST(id AS DECIMAL) AS v FROM decdecl", 38, 0, true},
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
			if m.WireUnconstrained != tc.wantUnconstrained {
				t.Errorf("WireUnconstrained = %v, want %v — PostgreSQL 17 describes this "+
					"as %s (ADR-0024 item 5, corrected by #708)",
					m.WireUnconstrained, tc.wantUnconstrained,
					map[bool]string{true: "plain numeric", false: "numeric(p,s)"}[tc.wantUnconstrained])
			}
		})
	}
}

// TestStringCastEnforcesItsLengthAndStillDropsTheDeclaration is #838, one half
// closed and one half pinned — in the ORDER ADR-0012 item 5 sets down, because
// declaring `character varying(4)` while returning six characters is a worse
// lie than declaring nothing.
//
// The VALUE half is FIXED. `expr.Cast.Eval`'s switch matches the lowered type
// name exactly, so `varchar(4)` matched no case label at all and the whole
// cast reached `default: return v` — the length was parsed by the SQL parser
// and then dropped, and a client casting to bound a width got its operand
// back. It now truncates to n CHARACTERS (PostgreSQL counts characters:
// `CAST('éàüxyz' AS VARCHAR(3))` is `éàü`, six octets), and `VARCHAR(0)` is
// 22023 as it is on the server.
//
// The DECLARATION half is still pinned below: `physical.declaredTypmod`
// answers only for DECIMAL and `pgwire.TypeMod` has no string arm, so
// RowDescription says unconstrained `text` where PostgreSQL 17.11's \gdesc
// says `character varying(4)` with atttypmod 8. It is the half #708 named when
// it shipped DECIMAL's modifier and left the string family.
//
// CHAR(n) truncates and does NOT pad, which is a decision. PostgreSQL's bpchar
// pads the stored value but strips trailing blanks for `length()`, for `||`
// and for every comparison — all three verified live — and this engine has one
// TypeString and no bpchar. Padding would leak blanks into GROUP BY keys, join
// keys and equality where PostgreSQL strips them: a WRONG ROW SET in exchange
// for a right rendering. The residual is the rendered value of a SHORT CHAR(n)
// and ADR-0012's list records it.
func TestStringCastEnforcesItsLengthAndStillDropsTheDeclaration(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name, sql string
		// want is now PostgreSQL 17.11's own answer, measured live: the value
		// half agrees. pgSays is kept beside it so a cell that ever diverges
		// again names what the server said.
		want, pgSays string
	}{
		// A FOLDED literal, and the same question over a real STRING VECTOR:
		// the length was dropped in the compiler and in the kernel, so a fix
		// to either alone would leave the other cell failing.
		{"literal_varchar", `SELECT CAST('abcdef' AS VARCHAR(4)) AS v FROM decdecl WHERE id = 1`,
			"abcd", "abcd"},
		{"literal_char", `SELECT CAST('abcdef' AS CHAR(4)) AS v FROM decdecl WHERE id = 1`,
			"abcd", "abcd"},
		{"column_varchar", `SELECT CAST(s AS VARCHAR(4)) AS v FROM decdecl WHERE id = 2`,
			"12.7", "12.7"},
		// The multi-byte cell, which is the one that tells CHARACTERS from
		// BYTES: three characters, six octets. Truncating bytes would cut a
		// rune in half and put invalid UTF-8 on the wire.
		{"multibyte_counts_characters",
			`SELECT CAST('éàüxyz' AS VARCHAR(3)) AS v FROM decdecl WHERE id = 1`,
			"éàü", "éàü"},
		// A non-string operand: PostgreSQL truncates the RENDERING.
		{"number_renders_then_truncates",
			`SELECT CAST(12345 AS VARCHAR(3)) AS v FROM decdecl WHERE id = 1`, "123", "123"},
		// Two controls that the fix must NOT change: a value already within n,
		// and the unparameterized destination. They fail if a repair truncates
		// everything rather than truncating to n.
		{"ctl_within_length", `SELECT CAST(s AS VARCHAR(4)) AS v FROM decdecl WHERE id = 3`,
			"abc", "abc"},
		{"ctl_unparameterized", `SELECT CAST('abcdef' AS VARCHAR) AS v FROM decdecl WHERE id = 1`,
			"abcdef", "abcdef"},
		// The bpchar RESIDUAL, pinned: PostgreSQL pads a short CHAR(n) to n on
		// the wire and this does not. Its three consumers agree with the
		// server BECAUSE it does not pad — the cells below assert that.
		{"residual_short_char_is_not_padded",
			`SELECT CAST('ab' AS CHAR(4)) AS v FROM decdecl WHERE id = 1`, "ab", "ab  "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1", len(res.Rows))
			}
			got, _ := res.Rows[0]["v"].(string)
			if got != tc.want {
				t.Errorf("value %q, want %q (PostgreSQL 17.11 says %q)", got, tc.want, tc.pgSays)
			}
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d column metas, want 1", len(res.ColumnMetas))
			}
			// The DECLARATION half, still pinned: every string destination is
			// one unparameterized STRING here, and PostgreSQL describes
			// `CAST(x AS VARCHAR(4))` as character varying(4), atttypmod 8,
			// OID 1043. The day this carries the length, delete the pin and
			// move the \gdesc assertion into the wire corpus.
			if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeString || m.Precision != 0 {
				t.Errorf("declared %s(%d) — this pin records an unconstrained STRING. If the "+
					"length is now carried, #838's METADATA half has moved: check that "+
					"pgwire.TypeMod sends n+4 and that the OID moved to 1043/1042 with it, "+
					"then delete this pin", m.TypeID, m.Precision)
			}
		})
	}
	// The three bpchar consumers, which agree with PostgreSQL BECAUSE CHAR(n)
	// is not padded. They are the reason the padding residual above is a
	// residual and not a bug: padding would move all three away from the
	// server. Every expectation measured live on postgres:17.11.
	for _, tc := range []struct {
		name, sql string
		want      any
	}{
		{"length_of_a_short_char", `SELECT LENGTH(CAST('ab' AS CHAR(4))) AS v FROM decdecl WHERE id = 1`,
			int32(2)},
		{"concat_of_a_short_char", `SELECT CAST('ab' AS CHAR(4)) || 'x' AS v FROM decdecl WHERE id = 1`,
			"abx"},
		{"equality_of_a_short_char", `SELECT CAST('ab' AS CHAR(4)) = 'ab' AS v FROM decdecl WHERE id = 1`,
			true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 || res.Rows[0]["v"] != tc.want {
				t.Errorf("%v, want %#v (live PostgreSQL 17.11) — bpchar strips trailing blanks "+
					"for length, || and comparison, so PADDING CHAR(n) would move this cell "+
					"away from the server", res.Rows, tc.want)
			}
		})
	}
	// The destination PostgreSQL refuses outright.
	t.Run("zero_length_is_22023", func(t *testing.T) {
		_, err := db.Query(context.Background(),
			`SELECT CAST('abcdef' AS VARCHAR(0)) AS v FROM decdecl WHERE id = 1`)
		if err == nil {
			t.Fatal("answered; PostgreSQL 17.11 raises 22023")
		}
		if got, want := sqlerr.StateOf(err), "22023"; got != want {
			t.Errorf("SQLSTATE %s, want %s: %v", got, want, err)
		}
		if !strings.Contains(err.Error(), "length for type varchar must be at least 1") {
			t.Errorf("%v does not carry PostgreSQL's message", err)
		}
	})
}

// TestDecimalCastRefusesWhatItCannotCarry is ADR-0024 items 4 and 6: a cast
// that cannot produce the value REFUSES, and the SQLSTATE says which
// condition it hit.
//
// The NaN entry is the recorded divergence. PostgreSQL's numeric holds NaN and
// an Int128 has no bit pattern for it, so a NaN reaching a VALUE-producing
// site is 22003 with a message naming the record — never a zero, and never the
// saturated end of the range that the COMPARISON path legitimately answers
// with (#462).
func TestDecimalCastRefusesWhatItCannotCarry(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name  string
		sql   string
		state string
	}{
		{"past the declared precision",
			"SELECT CAST(a AS DECIMAL(3,2)) AS v FROM decdecl WHERE id = 1", "22003"},
		{"non-numeric text", "SELECT CAST('abc' AS DECIMAL(9,2)) AS v FROM decdecl", "22P02"},
		{"NaN has no DECIMAL value",
			"SELECT CAST('NaN' AS DECIMAL(9,2)) AS v FROM decdecl", "22003"},
		{"Infinity has no DECIMAL value",
			"SELECT CAST('Infinity' AS DECIMAL(9,2)) AS v FROM decdecl", "22003"},
		{"a width past the carrier",
			"SELECT CAST(a AS DECIMAL(50,2)) AS v FROM decdecl", "22003"},
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

// TestDecimalCastIsAChoiceAndSetOpArm is #555's ORIGINAL repro, from the issue
// text: `CAST(e2 AS DECIMAL) AS v ... UNION ALL SELECT e4` answered FLOAT64 on
// the stage DAG where PostgreSQL answers numeric, and COALESCE over the same
// pair failed the store outright on both paths.
//
// Both work now because the cast declares a real DECIMAL that the common-type
// fold (ADR-0024 item 2) can reconcile with a column's.
func TestDecimalCastIsAChoiceAndSetOpArm(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{"set-operation arm",
			"SELECT CAST(a AS DECIMAL(10,2)) AS v FROM decdecl WHERE id = 1 " +
				"UNION ALL SELECT b FROM decdecl WHERE id = 1", "12.7500"},
		{"coalesce argument",
			"SELECT COALESCE(CAST(a AS DECIMAL(10,2)), b) AS v FROM decdecl WHERE id = 1", "12.7500"},
		{"case branch",
			"SELECT CASE WHEN id = 1 THEN CAST(a AS DECIMAL(10,2)) ELSE b END AS v " +
				"FROM decdecl WHERE id = 1", "12.7500"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) == 0 {
				t.Fatalf("%s returned no rows", tc.sql)
			}
			got, ok := res.Rows[0]["v"].(string)
			if !ok {
				t.Fatalf("%s: v = %#v (%T), want the DECIMAL text", tc.sql, res.Rows[0]["v"], res.Rows[0]["v"])
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}
