package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Issue #332 regression: a temporal COLUMN plus or minus an INTERVAL.
//
// `date ± INTERVAL` was handled only when the left operand was a Go string. A
// date LITERAL is one (it lowers to a CAST, which evaluates to text), so that
// form worked; a DATE or TIMESTAMP column is a bare number, so the expression
// fell through to numeric arithmetic where ToFloat64(IntervalValue) is 0 — the
// interval was discarded silently and the projection carried the column's raw
// epoch number, or NULL where the declared output type disagreed. TPC-H writes
// every interval against a literal, which is why 22 queries never reach it.
//
// Two things had to change, and this file pins both:
//
//   - compileBinOp routes an interval operand to the generic BinOp. An
//     interval Lit satisfies Float64Expr like any other literal, so the typed
//     arithmetic nodes took the expression before BinOp.Eval ever saw it.
//   - BinOp.Eval resolves the date operand through temporalOperand, the
//     binary-operator counterpart of FuncCall.resolveTemporalArgs (#322):
//     while the operand is still a column reference its vector knows the
//     declared type, which is the only place the epoch unit is recoverable.
//
// The answers below are the same ones date_add / date_sub give for the same
// inputs (date_arith_units_test.go) because both families now run the same
// intervalShift. Rendering follows from that: a whole day is a calendar date,
// a resolved instant goes through batch.FormatTimestamp, and TEXT keeps the
// string path's RFC3339 — the two renderers that already existed, no third.

// intervalOpCase is one `date ± interval` expression, built from the AST so
// the compile-time routing is under test alongside the evaluation.
type intervalOpCase struct {
	label string
	// col names a column of dateArithBatch; empty means the DATE-literal
	// control, which is the form that always worked and must not change.
	col  string
	op   string // "+" or "-"
	unit string
	n    int
	// flip puts the INTERVAL on the LEFT (`INTERVAL '1' DAY + col`). Only
	// meaningful for "+": subtraction is not commutative.
	flip bool
	// want is the answer for each row of the fixture: arithHi (1996-03-13
	// 14:25:36) then arithLo (1961-04-12 06:07:00).
	want [2]string
}

// build compiles the case to an Expr through the ordinary AST path.
func (c intervalOpCase) build(t *testing.T) Expr {
	t.Helper()
	var date plansql.Node
	if c.col == "" {
		// DATE '1996-03-13', which the parser lowers to a CAST.
		date = &plansql.CastNode{
			Inner:    &plansql.Lit{Value: "1996-03-13", Kind: plansql.LitString},
			TypeName: "date",
		}
	} else {
		date = &plansql.ColRef{Column: c.col}
	}
	iv := &plansql.IntervalLit{Value: c.n, Unit: c.unit}
	node := &plansql.BinaryOp{Left: date, Op: c.op, Right: iv}
	if c.flip {
		node = &plansql.BinaryOp{Left: iv, Op: c.op, Right: date}
	}
	e, err := Compile(node)
	if err != nil {
		t.Fatalf("compiling %s: %v", c.label, err)
	}
	return e
}

// intervalOperatorCases covers every operand kind the operator has to resolve
// — DATE column, TIMESTAMP column, whole-day text, text with a clock, and the
// date literal that already worked — across DAY / MONTH / YEAR (a month and a
// year are calendar arithmetic, not a fixed number of days) and HOUR, on both
// operators and both operand orders.
func intervalOperatorCases() []intervalOpCase {
	return []intervalOpCase{
		// A DATE column shifts by whole days and stays a calendar date.
		{label: "date_minus_90_day", col: "d", op: "-", unit: "day", n: 90,
			want: [2]string{"1995-12-14", "1961-01-12"}},
		{label: "date_plus_90_day", col: "d", op: "+", unit: "day", n: 90,
			want: [2]string{"1996-06-11", "1961-07-11"}},
		// MONTH and YEAR are calendar arithmetic: adding a month lands on the
		// same day number of the next month, not 30 days later.
		{label: "date_minus_1_month", col: "d", op: "-", unit: "month", n: 1,
			want: [2]string{"1996-02-13", "1961-03-12"}},
		{label: "date_plus_1_month", col: "d", op: "+", unit: "month", n: 1,
			want: [2]string{"1996-04-13", "1961-05-12"}},
		{label: "date_minus_1_year", col: "d", op: "-", unit: "year", n: 1,
			want: [2]string{"1995-03-13", "1960-04-12"}},
		{label: "date_plus_1_year", col: "d", op: "+", unit: "year", n: 1,
			want: [2]string{"1997-03-13", "1962-04-12"}},
		// The reversed operand order, which only addition admits.
		{label: "day_plus_date", col: "d", op: "+", unit: "day", n: 1, flip: true,
			want: [2]string{"1996-03-14", "1961-04-13"}},
		{label: "month_plus_date", col: "d", op: "+", unit: "month", n: 1, flip: true,
			want: [2]string{"1996-04-13", "1961-05-12"}},
		{label: "year_plus_date", col: "d", op: "+", unit: "year", n: 1, flip: true,
			want: [2]string{"1997-03-13", "1962-04-12"}},
		// An interval carrying a time component turns a whole day into an
		// instant — the same rule date_sub(d, INTERVAL '2' HOUR) follows.
		{label: "date_minus_2_hour", col: "d", op: "-", unit: "hour", n: 2,
			want: [2]string{"1996-03-12 22:00:00", "1961-04-11 22:00:00"}},
		{label: "date_plus_2_hour", col: "d", op: "+", unit: "hour", n: 2,
			want: [2]string{"1996-03-13 02:00:00", "1961-04-12 02:00:00"}},

		// A TIMESTAMP column keeps its time-of-day through a whole-day shift.
		{label: "ts_minus_90_day", col: "ts", op: "-", unit: "day", n: 90,
			want: [2]string{"1995-12-14 14:25:36", "1961-01-12 06:07:00"}},
		{label: "ts_plus_90_day", col: "ts", op: "+", unit: "day", n: 90,
			want: [2]string{"1996-06-11 14:25:36", "1961-07-11 06:07:00"}},
		{label: "ts_minus_1_month", col: "ts", op: "-", unit: "month", n: 1,
			want: [2]string{"1996-02-13 14:25:36", "1961-03-12 06:07:00"}},
		{label: "ts_plus_1_month", col: "ts", op: "+", unit: "month", n: 1,
			want: [2]string{"1996-04-13 14:25:36", "1961-05-12 06:07:00"}},
		{label: "ts_minus_1_year", col: "ts", op: "-", unit: "year", n: 1,
			want: [2]string{"1995-03-13 14:25:36", "1960-04-12 06:07:00"}},
		{label: "ts_plus_1_year", col: "ts", op: "+", unit: "year", n: 1,
			want: [2]string{"1997-03-13 14:25:36", "1962-04-12 06:07:00"}},
		{label: "day_plus_ts", col: "ts", op: "+", unit: "day", n: 1, flip: true,
			want: [2]string{"1996-03-14 14:25:36", "1961-04-13 06:07:00"}},
		{label: "ts_plus_2_hour", col: "ts", op: "+", unit: "hour", n: 2,
			want: [2]string{"1996-03-13 16:25:36", "1961-04-12 08:07:00"}},
		{label: "ts_minus_2_hour", col: "ts", op: "-", unit: "hour", n: 2,
			want: [2]string{"1996-03-13 12:25:36", "1961-04-12 04:07:00"}},

		// Whole-day TEXT answers exactly what the DATE column answers.
		{label: "text_day_minus_90_day", col: "sd", op: "-", unit: "day", n: 90,
			want: [2]string{"1995-12-14", "1961-01-12"}},
		{label: "text_day_plus_1_month", col: "sd", op: "+", unit: "month", n: 1,
			want: [2]string{"1996-04-13", "1961-05-12"}},
		{label: "text_day_plus_1_year", col: "sd", op: "+", unit: "year", n: 1,
			want: [2]string{"1997-03-13", "1962-04-12"}},
		// TEXT carrying a clock keeps the string path's RFC3339 rendering.
		// That is the second of the two renderers, and it is pinned here so a
		// future change has to notice it rather than quietly make it a third.
		{label: "text_instant_minus_90_day", col: "s", op: "-", unit: "day", n: 90,
			want: [2]string{"1995-12-14T14:25:36Z", "1961-01-12T06:07:00Z"}},

		// The control: a date LITERAL, the form that was already correct. Its
		// answer does not depend on the row, and must not change.
		{label: "literal_minus_90_day", op: "-", unit: "day", n: 90,
			want: [2]string{"1995-12-14", "1995-12-14"}},
		{label: "literal_plus_1_month", op: "+", unit: "month", n: 1,
			want: [2]string{"1996-04-13", "1996-04-13"}},
		{label: "literal_plus_1_year", op: "+", unit: "year", n: 1,
			want: [2]string{"1997-03-13", "1997-03-13"}},
		{label: "literal_day_plus_date", op: "+", unit: "day", n: 1, flip: true,
			want: [2]string{"1996-03-14", "1996-03-14"}},
	}
}

// TestDateIntervalOperator is the core regression: every operand kind, unit,
// operator and operand order, on the 1996 row and the pre-1970 row.
func TestDateIntervalOperator(t *testing.T) {
	b := dateArithBatch(t)
	for _, c := range intervalOperatorCases() {
		for row := 0; row < 2; row++ {
			t.Run(c.label, func(t *testing.T) {
				got := c.build(t).Eval(b, row)
				if got != c.want[row] {
					t.Errorf("row %d: %s = %v (%T), want %q",
						row, c.label, got, got, c.want[row])
				}
			})
		}
	}
}

// TestDateIntervalOperatorMatchesDateAdd states the invariant behind sharing
// intervalShift: the operator and the function family are the same operation
// spelled two ways, so they must answer identically — including the rendering,
// which is what decides whether `ts - INTERVAL '1' DAY` reads like the column
// it came from.
func TestDateIntervalOperatorMatchesDateAdd(t *testing.T) {
	b := dateArithBatch(t)
	for _, col := range []string{"d", "ts", "sd", "s"} {
		for _, u := range []struct {
			unit string
			n    int
			iv   IntervalValue
		}{
			{"day", 90, IntervalValue{Days: 90}},
			{"month", 1, IntervalValue{Months: 1}},
			{"year", 1, IntervalValue{Years: 1}},
			{"hour", 2, IntervalValue{Hours: 2}},
		} {
			for _, op := range []struct {
				sym string
				fn  string
			}{{"+", "date_add"}, {"-", "date_sub"}} {
				name := col + "_" + op.sym + "_" + u.unit
				t.Run(name, func(t *testing.T) {
					opExpr := intervalOpCase{col: col, op: op.sym, unit: u.unit, n: u.n, label: name}.build(t)
					fnExpr := &FuncCall{Name: op.fn, Args: []Expr{
						&ColRef{Name: col}, &Lit{Val: u.iv},
					}}
					for row := 0; row < 2; row++ {
						gotOp := opExpr.Eval(b, row)
						gotFn := fnExpr.Eval(b, row)
						if gotOp != gotFn {
							t.Errorf("row %d: %s %s INTERVAL = %v, %s(%s, INTERVAL) = %v",
								row, col, op.sym, gotOp, op.fn, col, gotFn)
						}
					}
				})
			}
		}
	}
}

// TestIntervalOperandCompilesGeneric pins the compile-time half of the fix. An
// interval Lit answers ToFloat64 = 0 like any other non-numeric literal, so
// without this routing the typed arithmetic nodes claim the expression and
// BinOp.Eval — where all the date handling lives — never runs.
func TestIntervalOperandCompilesGeneric(t *testing.T) {
	col := func() Expr { return &ColRef{Name: "d"} }
	iv := func() Expr { return &Lit{Val: IntervalValue{Days: 1}} }
	for _, tc := range []struct {
		label       string
		left, right Expr
		op          string
	}{
		{"col_minus_interval", col(), iv(), "-"},
		{"col_plus_interval", col(), iv(), "+"},
		{"interval_plus_col", iv(), col(), "+"},
		{"literal_minus_interval", &Lit{Val: "1996-03-13"}, iv(), "-"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			got := compileBinOp(tc.left, tc.right, tc.op)
			if _, ok := got.(*BinOp); !ok {
				t.Errorf("compiled to %T, want *BinOp — a typed arithmetic node "+
					"evaluates the interval as 0 and drops it", got)
			}
		})
	}
	// The guard must not broaden: ordinary arithmetic keeps its typed node.
	if got := compileBinOp(&ColRef{Name: "n"}, &Lit{Val: int64(1)}, "+"); func() bool {
		_, isGeneric := got.(*BinOp)
		return isGeneric
	}() {
		t.Errorf("column + integer compiled to the generic *BinOp; the typed path was lost")
	}
}

// TestIntervalOperandNonTemporalUnchanged: the operator resolves only what it
// can prove is a date. A bare integer column has no unit to recover — reading
// one as epoch days here would be a guess — so it stays numeric arithmetic,
// exactly as before, rather than turning into a date string.
func TestIntervalOperandNonTemporalUnchanged(t *testing.T) {
	b := dateArithBatch(t)
	e := compileBinOp(&ColRef{Name: "n"}, &Lit{Val: IntervalValue{Days: 90}}, "-")
	got := e.Eval(b, 0)
	if _, isText := got.(string); isText {
		t.Errorf("int_col - INTERVAL '90' DAY = %v (%T), want the numeric answer: "+
			"a bare number is not a date", got, got)
	}
	if f, ok := got.(float64); !ok || f != float64(epochDays(arithHi)) {
		t.Errorf("int_col - INTERVAL '90' DAY = %v (%T), want %v",
			got, got, float64(epochDays(arithHi)))
	}
}

// TestDateIntervalOperatorNullRow: a null temporal cell stays null rather than
// resolving to the epoch.
func TestDateIntervalOperatorNullRow(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}, 2)
	b.Columns[0].Int32Data[0] = epochDays(arithHi)
	b.Columns[0].Nulls.SetNull(1)
	b.Columns[1].Int64Data[0] = arithHi.UnixMilli()
	b.Columns[1].Nulls.SetNull(1)

	for _, tc := range []struct {
		col  string
		want string
	}{
		{"d", "1996-03-12"},
		{"ts", "1996-03-12 14:25:36"},
	} {
		e := compileBinOp(&ColRef{Name: tc.col}, &Lit{Val: IntervalValue{Days: 1}}, "-")
		if got := e.Eval(b, 0); got != tc.want {
			t.Errorf("%s - INTERVAL '1' DAY row 0: got %v, want %q", tc.col, got, tc.want)
		}
		if got := e.Eval(b, 1); got != nil {
			t.Errorf("%s - INTERVAL '1' DAY row 1 (null): got %v, want nil", tc.col, got)
		}
	}
}
