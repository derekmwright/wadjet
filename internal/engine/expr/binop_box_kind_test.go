package expr

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The GENERIC arithmetic node's BOX KIND (#849 round-3 residual, #555).
//
// `expr.BinOp` is the node every arithmetic expression whose operands have no
// typed protocol compiles to — a negated column, a CAST, a scalar function,
// and a CHOOSING construct. Its exact fixed-point arm boxes the result the way
// a DECIMAL COLUMN boxes one, as the value's rendered TEXT, and
// `classifyOperand` had no arm for it: so every comparison ABOVE such a node
// fell to `compare()`, which reads two strings by BYTES.
//
// `"1.00" > "1"` is true as bytes and false as a number, which is the whole
// defect. The values below are live PostgreSQL 17.11's over the same nine
// rows, measured on the oracle server before the fix was written:
//
//	CREATE TABLE f9_decpair (id bigint, a numeric(9,2), b numeric(18,4),
//	                         f float8);
//	-- a: 12.75 12.75 12.75 -0.01 2.00 0.00 NULL 12.75 NULL
//	SELECT count(*) FROM f9_decpair WHERE (COALESCE(a,0) + 1) > 1;   -- 5, was 8
//	SELECT count(*) FROM f9_decpair WHERE (-a + 1) > 1;              -- 1, was 2
//	SELECT count(*) FROM f9_decpair WHERE (ABS(a) + 1) = 1;          -- 1, was 0
//	SELECT greatest(COALESCE(a,0) + 1, 2) FROM f9_decpair;           -- 13.75, was 2
//
// This file is the per-node half. The five-arm census over a real plan is
// coordinator.TestGenericBinOpBoxKindMatchesPostgres and the wire declaration
// is pgwire.TestArithmeticOverAChoiceIsNumericOnTheWire.

// bbkBatch is the decpair fixture as one batch: a DECIMAL(9,2), a
// DECIMAL(18,4), the BIGINT key and a FLOAT8, with the NULLs the choosing
// constructs exist to answer for.
func bbkBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
	}, 9)
	b.Len = 9
	rows := []struct {
		id         int64
		a, bb      int64 // unscaled, at each column's own scale
		aNil, bNil bool
		f          float64
		fNil       bool
	}{
		{id: 1, a: 1275, bb: 127500, f: 1.5},
		{id: 2, a: 1275, bb: 127501, f: 100},
		{id: 3, a: 1275, bb: 127499, f: -3.5},
		{id: 4, a: -1, bb: -100, f: 0.5},
		{id: 5, a: 200, bb: 100000, f: 9.5},
		{id: 6, a: 0, bb: 0, f: 20},
		{id: 7, aNil: true, bb: 10000, f: 7.25},
		{id: 8, a: 1275, bNil: true, fNil: true},
		{id: 9, aNil: true, bNil: true, f: 3.5},
	}
	for i, r := range rows {
		b.Columns[0].SetValue(i, r.id)
		if r.aNil {
			b.Columns[1].Nulls.SetNull(i)
		} else {
			b.Columns[1].DecimalData.Data[i] = batch.Int128From(r.a)
		}
		if r.bNil {
			b.Columns[2].Nulls.SetNull(i)
		} else {
			b.Columns[2].DecimalData.Data[i] = batch.Int128From(r.bb)
		}
		if r.fNil {
			b.Columns[3].Nulls.SetNull(i)
		} else {
			b.Columns[3].SetValue(i, r.f)
		}
	}
	return b
}

// isDecimalTextBox is what an exact fixed-point result looks like in a box:
// the value's rendered text, exactly as Vector.GetValue hands over a DECIMAL
// column's — which is why nothing downstream can tell the two apart from the
// box, and why the KIND has to say.
func isDecimalTextBox(v any) bool {
	s, ok := v.(string)
	return ok && kernel.NewDecimalLiteral(s).Numeric()
}

func bbkCompile(t *testing.T, sql string) Expr {
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

// bbkProducers is every operand shape that compiles to the GENERIC BinOp
// rather than to the typed BinOpNumeric, one per producer, each with a
// DECIMAL operand so the node's exact arm is the one that runs. The choosing
// constructs are the ones `0214d48b` gave the exact kernel; the other four
// have had it since #555 and were wrong at every boxed site for as long.
var bbkProducers = []struct{ name, expr string }{
	{"coalesce", "COALESCE(a, 0) + 1"},
	{"case", "CASE WHEN id < 5 THEN a ELSE 0 END + 1"},
	{"nullif", "NULLIF(a, b) + 1"},
	{"greatest", "GREATEST(a, 0) + 1"},
	{"least", "LEAST(a, b) + 1"},
	{"coalesce_wide_scale", "COALESCE(b, 0) + 1"},
	{"unary_minus", "-a + 1"},
	{"cast", "CAST(a AS DECIMAL(9,2)) + 1"},
	{"scalar_fn", "ABS(a) + 1"},
	{"nested_choice", "COALESCE(ABS(a), 0) + 1"},
	// The operator set: every one of them boxes its result as decimal text.
	{"minus", "COALESCE(a, 0) - 1"},
	{"times", "COALESCE(a, 0) * 2"},
	{"divide", "COALESCE(a, 0) / 2"},
}

// TestGenericBinOpInDecimalModeIsBoxedAsADecimal is the mechanism itself: the
// node's resolved arithmetic mode must reach the comparison layer.
func TestGenericBinOpInDecimalModeIsBoxedAsADecimal(t *testing.T) {
	b := bbkBatch(t)
	for _, p := range bbkProducers {
		t.Run(p.name, func(t *testing.T) {
			e := bbkCompile(t, p.expr)
			bo, ok := e.(*BinOp)
			if !ok {
				t.Fatalf("%s compiled to %T, not the generic *BinOp — this gate's "+
					"premise is that these producers do not satisfy Float64Expr, so a "+
					"change to compileBinOp must move this test with it", p.expr, e)
			}
			// The box IS decimal text, which is what makes the kind
			// load-bearing: a Go string that another string can be ordered
			// against by bytes. Row 4 is a is 2.00 and b is 10.0000, the one
			// row on which every producer above answers a non-NULL value.
			if v := bo.Eval(b, 4); !isDecimalTextBox(v) {
				t.Fatalf("%s row 4 boxed %#v (%T), not a decimal's rendered text; "+
					"the cells below assume the exact arm ran", p.expr, v, v)
			}
			k, settled := classifyOperand(bo, b)
			if !settled {
				t.Fatalf("classifyOperand(%s) is unsettled on a batch that resolves it", p.expr)
			}
			if k != boxDecimal {
				t.Errorf("classifyOperand(%s) = kind %d, want boxDecimal (%d) — the node "+
					"boxes its result as a DECIMAL column's rendered text, so a comparison "+
					"above it reads the bytes of \"1.00\" without this", p.expr, k, boxDecimal)
			}
			if kf, _ := classifyOperandFold(bo, b); kf != boxDecimal {
				t.Errorf("classifyOperandFold(%s) = kind %d, want boxDecimal (%d)", p.expr, kf, boxDecimal)
			}
		})
	}
}

// TestGenericBinOpComparesNumericallyAgainstEveryLiteralSpelling is the
// ANSWER: the same nine rows through a bound comparison, against PostgreSQL.
//
// `> 1` is the headline — the rows whose value is exactly 1.00 answered TRUE
// because "1.00" sorts above "1" — and `= 1`, `BETWEEN` and the DECIMAL
// spelling of the same literal are the neighbours a byte order gets right or
// wrong for unrelated reasons.
func TestGenericBinOpComparesNumericallyAgainstEveryLiteralSpelling(t *testing.T) {
	b := bbkBatch(t)
	// PostgreSQL 17.11 over the nine rows, per predicate. A NULL row is
	// UNKNOWN, which a WHERE drops and EvalBool answers false for.
	for _, tc := range []struct {
		name, expr string
		want       [9]bool
	}{
		// COALESCE(a,0)+1 = 13.75 13.75 13.75 0.99 3.00 1.00 1.00 13.75 1.00
		{"coalesce_gt_1", "(COALESCE(a, 0) + 1) > 1",
			[9]bool{true, true, true, false, true, false, false, true, false}},
		{"coalesce_gt_1_decimal_literal", "(COALESCE(a, 0) + 1) > 1.0",
			[9]bool{true, true, true, false, true, false, false, true, false}},
		{"coalesce_ge_1", "(COALESCE(a, 0) + 1) >= 1",
			[9]bool{true, true, true, false, true, true, true, true, true}},
		{"coalesce_eq_1", "(COALESCE(a, 0) + 1) = 1",
			[9]bool{false, false, false, false, false, true, true, false, true}},
		{"coalesce_lt_1", "(COALESCE(a, 0) + 1) < 1",
			[9]bool{false, false, false, true, false, false, false, false, false}},
		{"coalesce_between", "(COALESCE(a, 0) + 1) BETWEEN 1 AND 2",
			[9]bool{false, false, false, false, false, true, true, false, true}},
		{"coalesce_in_list", "(COALESCE(a, 0) + 1) IN (1, 3)",
			[9]bool{false, false, false, false, true, true, true, false, true}},
		{"coalesce_distinct_from", "(COALESCE(a, 0) + 1) IS DISTINCT FROM 1",
			[9]bool{true, true, true, true, true, false, false, true, false}},
		// -a + 1 = -11.75 -11.75 -11.75 1.01 -1.00 1.00 NULL -11.75 NULL
		{"unary_minus_gt_1", "(-a + 1) > 1",
			[9]bool{false, false, false, true, false, false, false, false, false}},
		// ABS(a) + 1 = 13.75 13.75 13.75 1.01 3.00 1.00 NULL 13.75 NULL
		{"abs_eq_1", "(ABS(a) + 1) = 1",
			[9]bool{false, false, false, false, false, true, false, false, false}},
		// CAST(a AS DECIMAL(9,2)) + 1, same values as COALESCE on the non-NULL rows
		{"cast_lt_2", "(CAST(a AS DECIMAL(9,2)) + 1) < 2",
			[9]bool{false, false, false, true, false, true, false, false, false}},
		// Against a DECIMAL COLUMN rather than a literal: b = 12.7500 12.7501
		// 12.7499 -0.0100 10.0000 0.0000 1.0000 NULL NULL
		{"coalesce_gt_column", "(COALESCE(a, 0) + 1) > b",
			[9]bool{true, true, true, true, false, true, false, false, false}},
		// And against a FLOAT column, which folds to double precision.
		{"coalesce_gt_float", "(COALESCE(a, 0) + 1) > f",
			[9]bool{true, false, true, true, false, false, false, false, false}},
		// The CONTROL that must not move: an all-INTEGER choice keeps the
		// integer domain, so its box is an int64 and nothing here applies.
		{"ctl_integer_choice", "(COALESCE(id, 0) + 1) > 1",
			[9]bool{true, true, true, true, true, true, true, true, true}},
		// And a FLOAT arm folds to double precision, never to the decimal rung.
		{"ctl_float_choice", "(COALESCE(f, 0) + 1) > 1",
			[9]bool{true, true, false, true, true, true, true, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := bbkCompile(t, tc.expr)
			bp, ok := e.(BoolExpr)
			if !ok {
				t.Fatalf("%s compiled to %T, which is no predicate", tc.expr, e)
			}
			var got [9]bool
			for row := 0; row < 9; row++ {
				got[row] = bp.EvalBool(b, row)
			}
			if got != tc.want {
				t.Errorf("%s\n  got  %v\n  want %v (live PostgreSQL 17.11)",
					tc.expr, got, tc.want)
			}
		})
	}
}

// TestGenericBinOpReachesTheBoxedSitesThatAreNotAComparison is the other half
// of the boundary: `Cmp` is one consumer of a box, and GREATEST/LEAST and a
// simple CASE are three more that read the SAME pair rule. `GREATEST(dv, 2)`
// answered 2 for 13.75 because "13.75" sorts below "2".
func TestGenericBinOpReachesTheBoxedSitesThatAreNotAComparison(t *testing.T) {
	b := bbkBatch(t)
	for _, tc := range []struct {
		name, expr string
		want       [9]string
	}{
		{"greatest_over_the_node", "GREATEST(COALESCE(a, 0) + 1, 2)",
			[9]string{"13.75", "13.75", "13.75", "2", "3.00", "2", "2", "13.75", "2"}},
		{"least_over_the_node", "LEAST(COALESCE(a, 0) + 1, 2)",
			[9]string{"2", "2", "2", "0.99", "2", "1.00", "1.00", "2", "1.00"}},
		{"case_when_over_the_node", "CASE WHEN (COALESCE(a, 0) + 1) > 1 THEN 'y' ELSE 'n' END",
			[9]string{"y", "y", "y", "n", "y", "n", "n", "y", "n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := bbkCompile(t, tc.expr)
			var got [9]string
			for row := 0; row < 9; row++ {
				got[row] = fmt.Sprintf("%v", e.Eval(b, row))
			}
			if got != tc.want {
				t.Errorf("%s\n  got  %v\n  want %v (live PostgreSQL 17.11)",
					tc.expr, got, tc.want)
			}
		})
	}
}

// TestGenericBinOpInIntegerModeIsBoxedAsANumber is the OTHER mode of the same
// arm, and the boundary the decimal one is not allowed to swallow: an
// all-integer producer boxes a real int64, so its kind is boxNumber — which is
// what lets an unknown-typed literal beside it take the INTEGER input
// function, exactly as it does beside the typed node (#646).
func TestGenericBinOpInIntegerModeIsBoxedAsANumber(t *testing.T) {
	b := bbkBatch(t)
	for _, expr := range []string{
		"COALESCE(id, 0) + 1",
		"CAST(id AS BIGINT) * 2",
		"ABS(id) - 1",
		"-id + 1",
	} {
		t.Run(expr, func(t *testing.T) {
			e := bbkCompile(t, expr)
			bo, ok := e.(*BinOp)
			if !ok {
				t.Fatalf("%s compiled to %T, not the generic *BinOp", expr, e)
			}
			if v := bo.Eval(b, 0); !isNumeric(v) {
				t.Fatalf("%s boxed %#v, which is not a number", expr, v)
			}
			k, settled := classifyOperand(bo, b)
			if !settled || k != boxNumber {
				t.Errorf("classifyOperand(%s) = kind %d settled=%v, want boxNumber (%d)",
					expr, k, settled, boxNumber)
			}
		})
	}
}

// TestGenericBinOpOverATemporalOperandStaysUnclassified is the third arm's
// boundary. This node also evaluates date/interval arithmetic, whose result is
// not a number at all, and claiming a numeric kind for it would be a WRONG
// declaration rather than a missing one — the failure mode ADR-0012 item 8
// exists to prevent.
func TestGenericBinOpOverATemporalOperandStaysUnclassified(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
	}, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, int32(19675)) // 2023-11-14
	e := bbkCompile(t, "d + INTERVAL '1 day'")
	bo, ok := e.(*BinOp)
	if !ok {
		t.Fatalf("date + interval compiled to %T, not the generic *BinOp", e)
	}
	if k, _ := classifyOperand(bo, b); k != boxUnknown {
		t.Errorf("classifyOperand(d + INTERVAL '1 day') = kind %d, want boxUnknown (%d) — "+
			"a shifted date is not a number and must not be declared one", k, boxUnknown)
	}
}
