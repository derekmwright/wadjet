package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #849: the integer DOMAIN of an expression is a property of its TYPE, not of
// the syntax that produced its operands.
//
// Every expectation is live PostgreSQL 17.11 over the same values, measured on
// the oracle server before the fix was written (ROUND0.md):
//
//	CREATE TEMP TABLE e1_i (a bigint);  INSERT ... VALUES (4000000000), (2);
//	SELECT CAST(a AS BIGINT)             * 4000000000 FROM e1_i;  -- 22003
//	SELECT ABS(a)                        * 4000000000 FROM e1_i;  -- 22003
//	SELECT (CASE WHEN true THEN a ELSE 1 END) * 4000000000 ...    -- 22003
//	SELECT COALESCE(a, 1)                * 4000000000 ...         -- 22003
//	SELECT GREATEST(a, 1)                * 4000000000 ...         -- 22003
//	SELECT NULLIF(a, 1)                  * 4000000000 ...         -- 22003
//	SELECT LEAST(a, 5000000000)          * 4000000000 ...         -- 22003
//	SELECT SUM(CAST(a AS BIGINT) * 4000000000) ...                -- 22003
//	SELECT CAST(a AS BIGINT) * 2 FROM e1_i;                       -- 8000000000, bigint
//
// This file is the per-node half; the five-arm census over a real plan is
// coordinator.TestNumericArc2ShapesMatchPostgres and the wire OID is
// pgwire.TestIntegerDomainDeclaresBigintOnTheWire.

func intDomainBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "i", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt32},
		{Name: "f", Type: parquet.TypeFloat64},
		{Name: "r", Type: parquet.TypeFloat32},
		{Name: "s", Type: parquet.TypeString},
	}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, int64(4000000000))
	b.Columns[1].SetValue(0, int32(7))
	b.Columns[2].SetValue(0, 2.5)
	b.Columns[3].SetValue(0, float32(2.5))
	b.Columns[4].SetValue(0, "abc")
	return b
}

func intDomainCompile(t *testing.T, sql string) Expr {
	t.Helper()
	node, err := plansql.ParseExpressionComplete(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatalf("compile %q: %v", sql, err)
	}
	return e
}

// The OPERAND KINDS, one row per producer #849 names. `4000000000 *
// 4000000000` is 1.6e19, which has no int64 and which PostgreSQL refuses with
// `bigint out of range` for every one of these spellings.
var intDomainOperands = []struct{ name, over, under string }{
	{"cast", "CAST(i AS BIGINT) * 4000000000", "CAST(i AS BIGINT) * 2"},
	// A NARROWER destination is a range check, not a narrower carrier: every
	// integer expression computes in int64 here, so `int4 * bigint` overflows
	// at int8's edge exactly as PostgreSQL's does. 7 × 4e9 fits and is not a
	// refusal on either engine, which is why this row needs the wider constant.
	{"cast_to_int32_dest", "CAST(n AS INTEGER) * 9223372036854775807",
		"CAST(n AS INTEGER) * 2"},
	{"function_abs", "ABS(i) * 4000000000", "ABS(i) * 2"},
	{"function_mod", "MOD(i, 4000000001) * 4000000000", "MOD(i, 4000000001) * 2"},
	{"case", "(CASE WHEN i > 0 THEN i ELSE 1 END) * 4000000000",
		"(CASE WHEN i > 0 THEN i ELSE 1 END) * 2"},
	{"case_no_else", "(CASE WHEN i > 0 THEN i END) * 4000000000",
		"(CASE WHEN i > 0 THEN i END) * 2"},
	{"coalesce", "COALESCE(i, 1) * 4000000000", "COALESCE(i, 1) * 2"},
	{"nullif", "NULLIF(i, 1) * 4000000000", "NULLIF(i, 1) * 2"},
	{"greatest", "GREATEST(i, 1) * 4000000000", "GREATEST(i, 1) * 2"},
	{"least", "LEAST(i, 5000000000) * 4000000000", "LEAST(i, 5000000000) * 2"},
	{"unary_minus_over_a_cast", "-CAST(i AS BIGINT) * 4000000000", "-CAST(i AS BIGINT) * 2"},
	{"nested_in_a_choice", "COALESCE(ABS(i), 1) * 4000000000", "COALESCE(ABS(i), 1) * 2"},
	// The producer on the RIGHT of the operator, so a fix that only looked at
	// one side fails here.
	{"cast_on_the_right", "4000000000 * CAST(i AS BIGINT)", "2 * CAST(i AS BIGINT)"},
	// Addition and subtraction take the same arm, and `-` is the one whose
	// overflow test is not symmetric in its operands.
	{"cast_plus", "CAST(i AS BIGINT) + 9223372036854775807",
		"CAST(i AS BIGINT) + 2"},
	{"cast_minus", "-9223372036854775807 - CAST(i AS BIGINT)",
		"CAST(i AS BIGINT) - 2"},
}

func TestIntegerDomainSurvivesEveryOperandProducer(t *testing.T) {
	b := intDomainBatch(t)
	for _, c := range intDomainOperands {
		t.Run(c.name+"/refuses", func(t *testing.T) {
			e := intDomainCompile(t, c.over)
			state, msg := recoverFatalEvalForTest(t, func() { e.Eval(b, 0) })
			if state != "22003" || msg != "bigint out of range" {
				t.Errorf("%s raised [%s] %s, want [22003] bigint out of range — "+
					"PostgreSQL 17.11 refuses this expression whatever produced its "+
					"operand (#849)", c.over, state, msg)
			}
		})
	}
	// The CONTROL per operand kind: the same producer, a constant that does
	// NOT overflow. It must answer the EXACT integer in an int64 box — a fix
	// that raised for everything, or one that kept the float64 box, fails
	// here. The box is asserted because it is the half only the wire sees:
	// before #849 these answered the right number under OID 701.
	want := map[string]int64{
		"cast": 8000000000, "cast_to_int32_dest": 14, "function_abs": 8000000000,
		"function_mod": 8000000000, "case": 8000000000, "case_no_else": 8000000000,
		"coalesce": 8000000000, "nullif": 8000000000, "greatest": 8000000000,
		"least": 8000000000, "unary_minus_over_a_cast": -8000000000,
		"nested_in_a_choice": 8000000000, "cast_on_the_right": 8000000000,
		"cast_plus": 4000000002, "cast_minus": 3999999998,
	}
	for _, c := range intDomainOperands {
		t.Run(c.name+"/control_answers_the_exact_integer", func(t *testing.T) {
			got := intDomainCompile(t, c.under).Eval(b, 0)
			iv, ok := got.(int64)
			if !ok {
				t.Fatalf("%s answered %T(%v); PostgreSQL 17.11 says bigint, so the box "+
					"must be an int64 — a float64 here is the right number under "+
					"OID 701 (#849)", c.under, got, got)
			}
			if iv != want[c.name] {
				t.Errorf("%s = %d, want %d (live PostgreSQL 17.11)", c.under, iv, want[c.name])
			}
		})
	}
}

// The BOUNDARY, attempted from the outside (protocol rule 11). Every shape
// here is one the integer domain must NOT claim, and each would be a wrong
// ANSWER — not merely a wrong type — if it did: an integer claim over a
// fractional value truncates it, and over a DECIMAL it drops the scale.
// PostgreSQL 17.11 measured for each.
func TestIntegerDomainDeclinesWhatIsNotAnIntegerExpression(t *testing.T) {
	b := intDomainBatch(t)
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		// double precision on the server: abs(float8) is float8.
		{"abs_of_a_float", "ABS(f) * 2", 5.0},
		{"coalesce_of_a_float", "COALESCE(f, 1) * 2", 5.0},
		{"case_over_a_float", "(CASE WHEN f > 0 THEN f ELSE 1 END) * 2", 5.0},
		{"greatest_over_a_float", "GREATEST(f, 1) * 2", 5.0},
		// A choice with ONE non-integer arm is not an integer expression:
		// PostgreSQL folds {bigint, double} to double precision.
		{"case_with_one_float_arm", "(CASE WHEN i > 0 THEN i ELSE f END) * 2", 8000000000.0},
		{"coalesce_with_one_float_arm", "COALESCE(f, i) * 2", 5.0},
		// FLOOR/CEIL/ROUND/SIGN are double precision over an integer on the
		// server — they are NOT domain-preserving (numeric_domain_fn.go's
		// table), so arithmetic over them stays float.
		{"floor_of_an_integer", "FLOOR(i) * 2", 8000000000.0},
		{"round_of_an_integer", "ROUND(i) * 2", 8000000000.0},
		{"sign_of_an_integer", "SIGN(i) * 2", 2.0},
		// A cast to a NON-integer destination, including the bare numeric
		// spelling whose type is the operand's.
		{"cast_to_double", "CAST(i AS DOUBLE PRECISION) * 2", 8000000000.0},
		{"cast_to_real", "CAST(r AS REAL) * 2", 5.0},
		// A NON-INTEGER literal makes the pair numeric in PostgreSQL, which
		// never refuses: `SELECT CAST(a AS BIGINT) * 2.0` answers
		// 8000000000 on the server rather than raising.
		{"cast_times_a_non_integer_literal", "CAST(i AS BIGINT) * 2.0", 8000000000.0},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intDomainCompile(t, c.sql).Eval(b, 0)
			if got != c.want {
				t.Errorf("%s = %T(%v), want %T(%v) (live PostgreSQL 17.11). An integer "+
					"claim here would truncate the value, not merely mislabel it (#849)",
					c.sql, got, got, c.want, c.want)
			}
		})
	}
	// A boundary that is a REFUSAL rather than a value, and it must keep
	// PostgreSQL's own code: an integer cast of a value the destination
	// cannot hold is 22003 whatever sits above it.
	t.Run("cast_to_integer_out_of_range_still_refuses", func(t *testing.T) {
		e := intDomainCompile(t, "CAST(i AS INTEGER) * 2")
		state, msg := recoverFatalEvalForTest(t, func() { e.Eval(b, 0) })
		if state != "22003" || msg != "integer out of range" {
			t.Errorf("raised [%s] %s, want [22003] integer out of range", state, msg)
		}
	})
	// The kill switch: with integer arithmetic disabled the node takes its
	// float delegate, so the claim must go with it. Semantics never ride a
	// kill switch — see BinOpNumeric.divTrunc — and neither may this.
	t.Run("the_kill_switch_disarms_the_claim", func(t *testing.T) {
		prev := intArithToggle.Set(false)
		defer intArithToggle.Set(prev)
		bb := intDomainBatch(t)
		if got := intDomainCompile(t, "CAST(i AS BIGINT) * 4000000000").Eval(bb, 0); got == nil {
			t.Fatalf("answered NULL with the switch off")
		} else if _, isFloat := got.(float64); !isFloat {
			t.Errorf("answered %T with WADJET_INT_ARITH=0, want float64", got)
		}
	})
}

// MinInt64 is the one value whose magnitude has no int64, and every arm has to
// agree about it: the multiply, the negation and ABS. PostgreSQL 17.11 raises
// `bigint out of range` for all three.
func TestIntegerDomainAtTheEdgeOfTheRange(t *testing.T) {
	b := intDomainBatch(t)
	for _, c := range []struct{ name, sql string }{
		{"cast_of_min_int64_negated", "-CAST(-9223372036854775808 AS BIGINT) * 1"},
		{"abs_of_min_int64_times_one", "ABS(CAST(-9223372036854775808 AS BIGINT)) * 1"},
		{"min_int64_minus_one_through_a_choice", "COALESCE(CAST(-9223372036854775808 AS BIGINT), 0) - 1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := intDomainCompile(t, c.sql)
			state, _ := recoverFatalEvalForTest(t, func() { e.Eval(b, 0) })
			if state != "22003" {
				t.Errorf("raised [%s], want [22003] — |MinInt64| has no int64 and "+
					"PostgreSQL raises `bigint out of range`", state)
			}
		})
	}
	// And the value one step inside it still answers, so the guard is a
	// boundary rather than a blanket refusal.
	if got := intDomainCompile(t, "COALESCE(CAST(-9223372036854775807 AS BIGINT), 0) - 1").
		Eval(b, 0); got != int64(math.MinInt64) {
		t.Errorf("= %T(%v), want int64 %d", got, got, int64(math.MinInt64))
	}
}
