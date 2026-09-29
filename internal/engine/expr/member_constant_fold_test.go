// SPDX-License-Identifier: MIT

package expr

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A CONSTANT-VALUED OUTER OPERAND IS FOLDED, NEVER A DOUBLE (#1372). Every
// form below is computed from numeric constants that this engine
// evaluates as a double — a choice, a unary minus of an expression, a bare
// NUMERIC CAST of anything but a quoted literal — and 14.0000000000000000001
// read as 14 matched the member 14. MemberProbe folds the operand exactly and
// types the result by the literal's one rule, NUMERIC(38, its own scale); a
// choice whose conditions read a column has each constant result typed at
// one NUMERIC(38,S); a division keeps PostgreSQL's select_div_scale digits;
// a form the fold does not read and that evaluates to a double while a
// numeric constant feeds it is 0A000; PostgreSQL's own float (an explicit
// float CAST, a float-only function) keeps its reading.
func TestMemberConstantOuterIsFolded(t *testing.T) {
	const m = "14.0000000000000000001"
	typedM := "cast('" + m + "' as NUMERIC(38,19))"
	for _, tc := range []struct {
		outer string
		set   parquet.TypeID
		want  string // "" = left as written
		state string
	}{
		{"CASE WHEN true THEN " + m + " END", parquet.TypeDecimal, typedM, ""},
		{"COALESCE(" + m + ", 0)", parquet.TypeDecimal, typedM, ""},
		{"COALESCE(NULL, " + m + ")", parquet.TypeDecimal, typedM, ""},
		{"NULLIF(" + m + ", 0)", parquet.TypeDecimal, typedM, ""},
		{"NULLIF(" + m + ", 14)", parquet.TypeDecimal, typedM, ""},
		{"NULLIF(" + m + ", " + m + ")", parquet.TypeDecimal, "", ""}, // NULL
		{"GREATEST(" + m + ", 1)", parquet.TypeDecimal, typedM, ""},
		{"LEAST(" + m + ", 20)", parquet.TypeDecimal, typedM, ""},
		{"-(-" + m + ")", parquet.TypeDecimal, typedM, ""},
		{"'" + m + "'::numeric::numeric", parquet.TypeDecimal, typedM, ""},
		{"CAST(CAST('" + m + "' AS TEXT) AS NUMERIC)", parquet.TypeDecimal, typedM, ""},
		{"CAST(CAST(" + m + " AS TEXT) AS NUMERIC)", parquet.TypeDecimal, typedM, ""},
		{"CAST(COALESCE(" + m + ", 0) AS NUMERIC(38,19))", parquet.TypeDecimal, typedM, ""},
		{"CAST(COALESCE(" + m + ", 0) AS NUMERIC(18,4))", parquet.TypeDecimal, "cast('14' as NUMERIC(38,0))", ""},
		{"COALESCE(" + m + ", 0) + 0", parquet.TypeDecimal, typedM, ""},
		{"CASE WHEN " + m + " > 14 THEN 14 ELSE 13.25 END", parquet.TypeDecimal, "cast('14' as NUMERIC(38,0))", ""},
		{"CASE 1 WHEN 1 THEN " + m + " END", parquet.TypeDecimal, typedM, ""},
		{"COALESCE('" + m + "', 0.0)", parquet.TypeDecimal, typedM, ""},
		{"CAST(16777216 AS NUMERIC)", parquet.TypeDecimal, "cast('16777216' as NUMERIC(38,0))", ""},
		{"CAST(16777216 AS NUMERIC)", parquet.TypeInt64, "16777216", ""},
		{"COALESCE(" + m + ", 0)", parquet.TypeInt64, typedM, ""},
		{"-(-9007199254740993)", parquet.TypeInt64, "9007199254740993", ""},
		// A choice whose conditions (or other results) read a column: each
		// constant result is typed, one scale for all.
		{"CASE WHEN a.id > 0 THEN " + m + " END", parquet.TypeDecimal,
			"case when a.id > 0 then " + typedM + " end", ""},
		{"CASE WHEN a.id > 2 THEN " + m + " ELSE 12.5 END", parquet.TypeDecimal,
			"case when a.id > 2 then " + typedM + " else cast('12.5000000000000000000' as NUMERIC(38,19)) end", ""},
		{"COALESCE(CASE WHEN a.id > 0 THEN " + m + " END, 0)", parquet.TypeDecimal,
			"coalesce(case when a.id > 0 then " + typedM + " end, cast('0.0000000000000000000' as NUMERIC(38,19)))", ""},
		{"COALESCE(" + m + ", a.v_dec)", parquet.TypeDecimal, "coalesce(" + typedM + ", a.v_dec)", ""},
		// PostgreSQL's own double, an integer choice, and exact forms: as written.
		{"CAST(" + m + " AS DOUBLE PRECISION)", parquet.TypeDecimal, "", ""},
		{"GREATEST(" + m + ", 1.5::float8)", parquet.TypeDecimal, "", ""},
		{"CASE WHEN a.id > 0 THEN 14 END", parquet.TypeDecimal, "", ""},
		{"abs(" + m + ")", parquet.TypeDecimal, "", ""},
		{"a.v_f64 + 0", parquet.TypeDecimal, "", ""},
		// A division at PostgreSQL's select_div_scale: (M / 7) * 7 rounds
		// M / 7 at scale 19 and is 14 again; 14 / 3.0 keeps 16 digits.
		{m + " / 1", parquet.TypeDecimal, typedM, ""},
		{"(" + m + " / 7) * 7", parquet.TypeInt64, "14", ""},
		{"(14 / 3.0) * 3", parquet.TypeDecimal, "cast('14.0000000000000001' as NUMERIC(38,16))", ""},
		{"(1 / 3.0) * 42", parquet.TypeDecimal, "cast('13.99999999999999999986' as NUMERIC(38,20))", ""},
		{"(1 / 30000.0) * 420000", parquet.TypeDecimal, "cast('13.99999999999999999986' as NUMERIC(38,20))", ""},
		{"CAST(CAST(14.50 AS TEXT) AS NUMERIC) / 1.0", parquet.TypeDecimal, "cast('14.5' as NUMERIC(38,1))", ""},
		{"7 / 2", parquet.TypeInt64, "3", ""},
		{"CASE WHEN a.id > 0 THEN " + m + " / 1 END", parquet.TypeDecimal, "case when a.id > 0 then " + typedM + " end", ""},
		// Not folded, evaluated as a double while a numeric constant feeds
		// it: refused. PostgreSQL's float-only functions and integer
		// arguments are its own double: left as written.
		{"sqrt(12.5 * 12.5)", parquet.TypeDecimal, "", "0A000"},
		{"sqrt(" + m + ")", parquet.TypeInt64, "", "0A000"},
		{"CASE WHEN a.id > 0 THEN sqrt(" + m + ") END", parquet.TypeDecimal, "", "0A000"},
		{"sqrt(14 * 14)", parquet.TypeDecimal, "", ""},
		{"sin(" + m + ") * 0 + 14", parquet.TypeDecimal, "", ""},
		// A division by zero is PostgreSQL's 22012 and the engine's own
		// run-time error: left as written.
		{m + " / 0", parquet.TypeDecimal, "", ""},
		// A folded value no DECIMAL(38,s) holds: the literal's 22003.
		{"-(-1e-40)", parquet.TypeDecimal, "", "22003"},
	} {
		left, err := plansql.ParseExpression(tc.outer)
		if err != nil {
			t.Fatalf("%s: %v", tc.outer, err)
		}
		got, ok, err := MemberProbe(left, tc.set)
		if tc.state != "" {
			if st := sqlerr.StateOf(err); st != tc.state {
				t.Errorf("%s against %v: got %v (ok=%v, %v), want %s", tc.outer, tc.set, st, ok, err, tc.state)
			}
			if cerr := CheckMemberProbe(left, tc.set); sqlerr.StateOf(cerr) != tc.state {
				t.Errorf("%s against %v: CheckMemberProbe %v, want %s", tc.outer, tc.set, cerr, tc.state)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s against %v: %v", tc.outer, tc.set, err)
			continue
		}
		if tc.want == "" {
			if ok {
				t.Errorf("%s against %v: typed as %s, want it left as written", tc.outer, tc.set, got)
			}
			continue
		}
		if !ok || got.String() != tc.want {
			t.Errorf("%s against %v: got %s (ok=%v), want %s", tc.outer, tc.set, got, ok, tc.want)
			continue
		}
		// Idempotent: the plan's typed operand is not typed again.
		if again, ok2, _ := MemberProbe(got, tc.set); ok2 {
			t.Errorf("%s: typed twice: %s → %s", tc.outer, got, again)
		}
		if !MemberProbeCandidate(left) {
			t.Errorf("%s: MemberProbeCandidate false for an operand MemberProbe types", tc.outer)
		}
	}
}
