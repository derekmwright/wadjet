package wadjet

import (
	"context"
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

// TestStringCastDropsItsLengthParameter pins the half of #708 that was NOT
// fixed, in the direction that matters.
//
// #708 made a cast that names a (p,s) carry it on the wire, and the entry
// above is that gate. It covered the DECIMAL family only, because DECIMAL is
// the only type whose modifier reaches pgwire.TypeMod. The string family gets
// the destination's length parsed and then dropped in BOTH halves, and the
// value half is a wrong answer rather than wrong metadata. Measured live on
// postgres:17.11:
//
//	SELECT CAST('abcdef' AS VARCHAR(4))   PG: abcd    wadjet: abcdef
//	SELECT CAST('abcdef' AS CHAR(4))      PG: abcd    wadjet: abcdef
//	SELECT CAST('12.7500' AS VARCHAR(4))  PG: 12.7    wadjet: 12.7500
//	\gdesc of the first                   PG: character varying(4)
//	                                      wadjet: unconstrained STRING
//
// `expr.castTargetType` maps CHAR / VARCHAR / TEXT / STRING to one
// batch.TypeString, so nothing truncates; `physical.declaredTypmod` returns -1
// for every non-DECIMAL destination, so RowDescription says unconstrained.
//
// This is a PIN and not an acceptance. It asserts today's answer so that the
// day either half is closed it FAILS and names itself — the same contract the
// oracle's knownBug entries carry. The divergence is filed as **#838**; the pin
// lives here rather than in the PostgreSQL corpus because it was written in
// review before the issue existed, and moving it into the corpus as a
// `knownBug` entry naming #838 is now the tidier home for it.
// The ORDER is value-first and ADR-0012 item 5 records why:
// declaring `character varying(4)` while returning six characters is a worse
// lie than declaring nothing, so the length is enforced before it is declared.
func TestStringCastDropsItsLengthParameter(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name, sql string
		// want is wadjet's answer, which is the divergence; pgSays is
		// PostgreSQL 17.11's, measured live on the oracle server.
		want, pgSays string
	}{
		// A FOLDED literal, and the same question over a real STRING VECTOR:
		// the length is dropped in the compiler and in the kernel, so a fix
		// to either alone leaves the other cell failing.
		{"literal_varchar", `SELECT CAST('abcdef' AS VARCHAR(4)) AS v FROM decdecl WHERE id = 1`,
			"abcdef", "abcd"},
		{"literal_char", `SELECT CAST('abcdef' AS CHAR(4)) AS v FROM decdecl WHERE id = 1`,
			"abcdef", "abcd"},
		{"column_varchar", `SELECT CAST(s AS VARCHAR(4)) AS v FROM decdecl WHERE id = 2`,
			"12.7500", "12.7"},
		// Two controls that a fix must NOT change: a value already within n,
		// and the unparameterized destination. They fail if a repair
		// truncates everything rather than truncating to n.
		{"ctl_within_length", `SELECT CAST(s AS VARCHAR(4)) AS v FROM decdecl WHERE id = 3`,
			"abc", "abc"},
		{"ctl_unparameterized", `SELECT CAST('abcdef' AS VARCHAR) AS v FROM decdecl WHERE id = 1`,
			"abcdef", "abcdef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1", len(res.Rows))
			}
			got, _ := res.Rows[0]["v"].(string)
			if got != tc.want {
				t.Errorf("value %q, want %q (PostgreSQL 17.11 says %q).\n"+
					"  If this now equals PostgreSQL, the length parameter is being "+
					"enforced: DELETE this cell, and check that the wire declaration "+
					"moved with it — ADR-0012 item 5 records that the value half comes "+
					"first and the declaration follows it.",
					got, tc.want, tc.pgSays)
			}
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d column metas, want 1", len(res.ColumnMetas))
			}
			if m := res.ColumnMetas[0]; m.TypeID != parquet.TypeString {
				t.Errorf("declared %s, want STRING — every string destination is one "+
					"unparameterized STRING here, which is the metadata half of the "+
					"same gap", m.TypeID)
			}
		})
	}
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
