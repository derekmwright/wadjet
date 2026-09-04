package expr

import (
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A choice construct with a FRACTIONAL literal arm is numeric, not integer
// (round-1 review, B3).
//
// PostgreSQL 17.11 types `LEAST(bigint, 1.5)` as `numeric` and answers 1.5.
// This engine typed it INT64 — the integer rung, reached because a constant
// CONTRIBUTES to the fold and does not TRIGGER it (ADR-0024) — and then built
// an int64 vector for it. The 1.5 the evaluator produced was TRUNCATED into
// that vector, and arithmetic over the choice made it worse:
//
//	                                       PostgreSQL 17.11   was
//	LEAST(c_i64, 1.5)                      1.5                1
//	LEAST(c_i64, 1.5) + 1                  2.5                2
//	LEAST(c_i64, 1.5) * 3                  4.5                4
//	(CASE … ELSE 1.5 END) * <int8 max>     an exact numeric   MinInt64
//
// The exception is narrow and it is decided in ONE place for both sides:
// expr.CommonDeclType's fractionalLitTriggersFold over the DECLARED arms and
// expr.fracLitArmTriggersFold over the COMPILED ones. A literal with a
// non-zero scale, beside at least one arm that is not a constant, puts the
// choice on the DECIMAL rung; a whole-number literal does not, and neither
// does an all-constant choice.
//
// These cells are the RULE at the node; the five-arm census over a real plan
// with the wire OID is coordinator.TestArcE1ExprTypingOnEveryArm and
// pgwire.TestChoiceWithAFractionalLiteralIsNumericOnTheWire.

func fracLitBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "i", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt32},
		{Name: "f", Type: parquet.TypeFloat64},
	}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, int64(1000003))
	b.Columns[1].SetValue(0, int32(7))
	b.Columns[2].SetValue(0, 2.5)
	return b
}

// fracLitNumber renders either box a numeric choice can produce — an exact
// DECIMAL's rendered text or a float64 — as one canonical decimal spelling, so
// these cells assert the VALUE and not the box.
//
// The box is the PLAN's business and is asserted where a plan exists:
// wadjet.TestChoiceWithAFractionalLiteralAnswersNumeric and
// pgwire.TestChoiceWithAFractionalLiteralIsNumericOnTheWire. At this layer
// there is no output vector, so the runtime's choice fold has nothing to
// resolve against and may hand back either — what must not happen, and what
// used to, is the value arriving as the integer 1.
func fracLitNumber(v any) string {
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return "?"
	}
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	return s
}

func TestChoiceWithAFractionalLiteralIsNotInteger(t *testing.T) {
	b := fracLitBatch(t)
	for _, c := range []struct {
		name, sql string
		want      string
	}{
		// The choice itself. Its value is the fractional arm, and an integer
		// claim TRUNCATES it.
		{"least", "LEAST(i, 1.5)", "1.5"},
		{"greatest", "GREATEST(0 - i, 1.5)", "1.5"},
		{"coalesce", "COALESCE(NULLIF(i, 1000003), 2.5)", "2.5"},
		{"case_else", "(CASE WHEN i > 9000000 THEN i ELSE 1.5 END)", "1.5"},
		{"case_then", "(CASE WHEN i > 0 THEN 1.5 ELSE i END)", "1.5"},
		// Arithmetic OVER the choice, which is where the truncation compounded.
		{"least_plus_one", "LEAST(i, 1.5) + 1", "2.5"},
		{"least_times_three", "LEAST(i, 1.5) * 3", "4.5"},
		{"case_else_plus_one", "(CASE WHEN i > 9000000 THEN i ELSE 1.5 END) + 1", "2.5"},
		{"coalesce_plus_one", "COALESCE(NULLIF(i, 1000003), 2.5) + 1", "3.5"},
		// An INT32 column, which folds at a narrower precision and must still
		// carry the scale.
		{"int32_column", "COALESCE(n, 1.5)", "7"},
		{"int32_column_plus", "COALESCE(n, 1.5) + 1", "8"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intDomainCompile(t, c.sql).Eval(b, 0)
			if fracLitNumber(got) != c.want {
				t.Errorf("%s = %T(%v), want %s. A choice with a fractional literal arm is "+
					"numeric on the server; an integer claim TRUNCATES the value it produces "+
					"(round-1 review, B3)", c.sql, got, got, c.want)
			}
		})
	}
}

// The BOUNDARY, from both sides. The exception is a fractional literal beside a
// NON-CONSTANT arm and nothing else; everything here must keep the type it had.
func TestChoiceFractionalLiteralExceptionIsNarrow(t *testing.T) {
	b := fracLitBatch(t)
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		// A WHOLE-number literal: `COALESCE(i, 2)` is integer in PostgreSQL
		// too, and the integer domain must survive it (#849).
		{"whole_literal_coalesce", "COALESCE(i, 2)", int64(1000003)},
		{"whole_literal_least", "LEAST(i, 2)", int64(2)},
		{"whole_literal_arithmetic", "COALESCE(i, 2) * 2", int64(2000006)},
		// ALL-CONSTANT arms: nothing to resolve the literal against, so
		// ADR-0024's literal deferral stands and a bare numeric literal keeps
		// the FLOAT64 it declares alone.
		{"all_constants", "GREATEST(-2.5, -7.5)", -2.5},
		{"all_constants_arithmetic", "GREATEST(-2.5, -7.5) * 2", -5.0},
		// A FLOAT column beside the literal folds to double precision on the
		// ladder, not to the decimal rung.
		{"float_column", "COALESCE(f, 1.5)", 2.5},
		{"float_column_arithmetic", "COALESCE(f, 1.5) * 2", 5.0},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := intDomainCompile(t, c.sql).Eval(b, 0)
			if got != c.want {
				t.Errorf("%s = %T(%v), want %T(%v) — the exception is a FRACTIONAL literal "+
					"beside a NON-CONSTANT arm and nothing else",
					c.sql, got, got, c.want, c.want)
			}
		})
	}
	// And the two sides of the fold agree about every shape above: the
	// DECLARED walk (CommonDeclType) and the COMPILED walk (decimalArmFold)
	// make one decision, which is the property that failed. A disagreement is
	// not a wrong type, it is a value stored into a vector of the other kind.
	for _, c := range []struct {
		name string
		arms []DeclType
		want bool
	}{
		{"fractional_beside_a_column", []DeclType{Decl(batch.TypeInt64),
			DeclNumericLit(batch.TypeFloat64, "1.5")}, true},
		{"whole_beside_a_column", []DeclType{Decl(batch.TypeInt64),
			DeclNumericLit(batch.TypeInt64, "2")}, false},
		{"all_constants", []DeclType{DeclNumericLit(batch.TypeFloat64, "-2.5"),
			DeclNumericLit(batch.TypeFloat64, "-7.5")}, false},
		{"no_literal_at_all", []DeclType{Decl(batch.TypeInt64), Decl(batch.TypeInt32)}, false},
	} {
		t.Run("fold/"+c.name, func(t *testing.T) {
			if got := fractionalLitTriggersFold(c.arms); got != c.want {
				t.Errorf("fractionalLitTriggersFold = %v, want %v", got, c.want)
			}
		})
	}
}

// The SEAM itself: the planner's stamp is what the runtime uses, and it is
// load-bearing in both directions.
//
// #849 introduced two hand-maintained walks over two representations of one
// expression — physical.intArithAllInt picks the output VECTOR and
// expr.operandIsInt picks the KERNEL — and B3 is what a disagreement costs: a
// float computed under an INT64 declaration is TRUNCATED into the vector, and
// at the edge it WRAPS. StampArithMode makes the planner's answer the one the
// runtime uses, so the two cannot disagree even where they would.
//
// The cells below FORCE a disagreement, which no query produces today
// precisely because the fold above was fixed — and that is the point: a gate
// that can only fire once the other fix is already broken is not a gate.
func TestStampedArithModeIsWhatTheRuntimeUses(t *testing.T) {
	b := fracLitBatch(t)
	// The runtime would say INTEGER here (two integer operands). Stamped
	// false — the planner declaring anything but INT64 — the float arm answers,
	// so no int64 reaches a vector of another kind.
	notInt := intDomainCompile(t, "CAST(i AS BIGINT) * 2")
	StampArithMode(notInt, false)
	if got := notInt.Eval(b, 0); got != 2000006.0 {
		t.Errorf("stamped NOT-integer: = %T(%v), want float64 2000006 — the runtime must use "+
			"the planner's answer, not its own (round-1 B3)", got, got)
	}
	// And stamped TRUE it takes the checked integer kernel, refusal included.
	over := intDomainCompile(t, "CAST(i AS BIGINT) * 9223372036854775807")
	StampArithMode(over, true)
	state, _ := recoverFatalEvalForTest(t, func() { over.Eval(b, 0) })
	if state != "22003" {
		t.Errorf("stamped integer: raised [%s], want [22003]", state)
	}
	// A stamp of TRUE cannot manufacture an integer out of a value that is not
	// one: intArith still reads both boxes, so a float operand falls through to
	// the float arm exactly as an unstamped node would. That is what makes the
	// stamp safe to apply from the declaration alone.
	notReally := intDomainCompile(t, "COALESCE(f, 1.5) * 2")
	StampArithMode(notReally, true)
	if got := notReally.Eval(b, 0); got != 5.0 {
		t.Errorf("stamped integer over a float: = %T(%v), want float64 5 — the box test is "+
			"the guard the stamp does not remove", got, got)
	}
}
