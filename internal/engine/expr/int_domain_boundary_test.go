package expr

import (
	"testing"
	"unicode/utf8"
)

// The shapes an adversarial reading of #849 and #856 asks for that the main
// gates do not reach: a producer NESTED inside another producer, a DECIMAL
// operand under the same producers, the int4 SUPERSET this engine keeps, and
// what the character-counting family does with bytes that are not UTF-8 at all.
//
// Every PostgreSQL answer here was measured live on 17.11.

// A choice inside a CAST and a CAST inside a choice. `CAST(COALESCE(a, 1) AS
// BIGINT)` is bigint on the server and raises 22003 for the overflowing
// product, and so is the other nesting order — the domain is a property of the
// TYPE, so it survives being wrapped twice.
func TestIntegerDomainSurvivesNestedProducers(t *testing.T) {
	b := intDomainBatch(t)
	for _, c := range []struct{ name, over, under string }{
		{"choice_inside_a_cast",
			"CAST(COALESCE(i, 1) AS BIGINT) * 4000000000", "CAST(COALESCE(i, 1) AS BIGINT) * 2"},
		{"cast_inside_a_choice",
			"COALESCE(CAST(i AS BIGINT), 1) * 4000000000", "COALESCE(CAST(i AS BIGINT), 1) * 2"},
		{"function_inside_a_choice",
			"COALESCE(ABS(i), 1) * 4000000000", "COALESCE(ABS(i), 1) * 2"},
		{"choice_inside_a_choice",
			"GREATEST(COALESCE(i, 1), 1) * 4000000000", "GREATEST(COALESCE(i, 1), 1) * 2"},
		{"case_inside_a_cast",
			"CAST((CASE WHEN i > 0 THEN i ELSE 1 END) AS BIGINT) * 4000000000",
			"CAST((CASE WHEN i > 0 THEN i ELSE 1 END) AS BIGINT) * 2"},
	} {
		t.Run(c.name+"/refuses", func(t *testing.T) {
			e := intDomainCompile(t, c.over)
			state, msg := recoverFatalEvalForTest(t, func() { e.Eval(b, 0) })
			if state != "22003" || msg != "bigint out of range" {
				t.Errorf("%s raised [%s] %s, want [22003] bigint out of range", c.over, state, msg)
			}
		})
		t.Run(c.name+"/control", func(t *testing.T) {
			got := intDomainCompile(t, c.under).Eval(b, 0)
			if got != int64(8000000000) {
				t.Errorf("%s = %T(%v), want int64 8000000000", c.under, got, got)
			}
		})
	}
}

// A DECIMAL operand under the same producers is NUMERIC on the server, never
// integer: `pg_typeof(COALESCE(1.5::numeric, 1) * 2)` is numeric and the value
// is 3.0. An integer claim here would drop the scale, which is a wrong VALUE.
//
// The cells live at the expression layer with a DECIMAL literal because the
// batch has no DECIMAL column; wadjet.TestConstArithLift… and the type-matrix
// sweeps carry the column form.
func TestIntegerDomainDeclinesADecimalOperand(t *testing.T) {
	b := intDomainBatch(t)
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"coalesce_of_a_decimal_literal", "COALESCE(1.5, 1) * 2", 3.0},
		{"case_over_a_decimal_literal", "(CASE WHEN i > 0 THEN 1.5 ELSE 1 END) * 2", 3.0},
		{"greatest_over_a_decimal_literal", "GREATEST(1.5, 1) * 2", 3.0},
		// A fractional literal on the OTHER side of the operator, which is the
		// #841 boundary reached from here.
		{"cast_times_a_fractional_literal", "CAST(i AS BIGINT) * 2.5", 1.0e10},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intDomainCompile(t, c.sql).Eval(b, 0)
			if got != c.want {
				t.Errorf("%s = %T(%v), want %T(%v) — an integer claim over a fractional value "+
					"TRUNCATES it", c.sql, got, got, c.want, c.want)
			}
		})
	}
}

// The int4 SUPERSET, pinned so it cannot move silently.
//
// PostgreSQL types `int4 * int4` as int4 and raises `integer out of range` for
// 2147483647 * 2 — it does NOT promote. This engine reads every integer column
// as an int64 and declares integer arithmetic INT64, so it ANSWERS 4294967294.
// That is the widening divergence `raiseBigintOutOfRange`'s own doc records and
// ADR-0012 item 5's list keeps; #849 does not change it in either direction,
// and this cell is what says so.
func TestIntegerDomainKeepsTheInt4Superset(t *testing.T) {
	b := intDomainBatch(t)
	// n is an int32 column holding 7; the literal is int4's maximum.
	if got := intDomainCompile(t, "n * 2147483647").Eval(b, 0); got != int64(15032385529) {
		t.Errorf("= %T(%v), want int64 15032385529 — PostgreSQL raises `integer out of range` "+
			"here and this engine answers the exact value in int64, which is the recorded "+
			"superset (ADR-0012 item 5)", got, got)
	}
	// And the same expression under a producer keeps that superset rather than
	// acquiring a narrower refusal from the cast's destination.
	if got := intDomainCompile(t, "COALESCE(n, 1) * 2147483647").Eval(b, 0); got != int64(15032385529) {
		t.Errorf("= %T(%v), want int64 15032385529", got, got)
	}
	// A cast that NAMES int4 is different, and must stay different: the
	// destination's range is checked at the cast (`integer out of range`),
	// which is PostgreSQL's answer for that spelling.
	state, msg := recoverFatalEvalForTest(t, func() {
		intDomainCompile(t, "CAST(i AS INTEGER) * 2").Eval(b, 0)
	})
	if state != "22003" || msg != "integer out of range" {
		t.Errorf("CAST(i AS INTEGER) raised [%s] %s, want [22003] integer out of range", state, msg)
	}
}

// #856's accept-set at the edge: bytes that are not UTF-8 at all.
//
// PostgreSQL has no answer to compare against — a UTF-8 database REFUSES
// invalid input, so a text column there cannot hold these bytes and
// `length()` never sees them. wadjet's String column can, so the behaviour is
// pinned rather than derived: Go's rune decoder yields one RuneError per
// invalid byte, so `LENGTH` counts each of them once and `OCTET_LENGTH`
// counts the bytes. The two therefore still differ for a multi-byte value and
// agree for pure ASCII, which is the property that matters — and no function
// in the family may PANIC or truncate mid-sequence on such an input.
func TestCharSemanticsOnBytesThatAreNotUTF8(t *testing.T) {
	b := charSemanticsBatch(t)
	const bad = "a\xffb" // one invalid byte between two ASCII characters
	if utf8.ValidString(bad) {
		t.Fatal("fixture is valid UTF-8; it must not be")
	}
	for _, c := range []struct {
		name string
		expr Expr
		want any
	}{
		{"length", &FuncCall{Name: "length", Args: []Expr{&Lit{Val: bad}}}, int32(3)},
		{"char_length", &FuncCall{Name: "char_length", Args: []Expr{&Lit{Val: bad}}}, int32(3)},
		{"octet_length", &FuncCall{Name: "octet_length", Args: []Expr{&Lit{Val: bad}}}, int32(3)},
		// The invalid byte becomes U+FFFD when it is re-encoded, which is Go's
		// decoder and not a choice this arc made. What matters is that the
		// result is VALID UTF-8 — the family may not emit a broken sequence.
		{"substr", &FuncCall{Name: "substr",
			Args: []Expr{&Lit{Val: bad}, &Lit{Val: int64(1)}, &Lit{Val: int64(2)}}}, "a�"},
		{"reverse", &FuncCall{Name: "reverse", Args: []Expr{&Lit{Val: bad}}}, "b�a"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := c.expr.Eval(b, 0)
			if got != c.want {
				t.Errorf("= %#v, want %#v", got, c.want)
			}
			if s, ok := got.(string); ok && !utf8.ValidString(s) {
				t.Errorf("produced invalid UTF-8 (% x)", s)
			}
		})
	}
}
