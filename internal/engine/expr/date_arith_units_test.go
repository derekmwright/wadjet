package expr

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Issue #322 regression: the date-ARITHMETIC family over a temporal COLUMN.
//
// parseDateValue read a boxed int64 as days since the epoch. That is the unit
// a DATE column boxes, and it is off by a factor of ~86.4 million for a
// TIMESTAMP column, which boxes epoch MILLISECONDS: date_add(ts, 1) landed
// two million years out, silently. #319 fixed the date-PART family the same
// way — resolve the column to a real instant while its declared type is still
// in hand — and deliberately left this family for its own decision, because
// these functions also render a result and so must know whether the argument
// was a whole day or an instant.
//
// The settled semantics, pinned by the table below:
//
//   - a numeric second argument counts DAYS (as a DATE argument always has,
//     and as Spark/Hive date_add does); an INTERVAL keeps its own unit;
//   - the result preserves the input's time-of-day — a DATE renders
//     YYYY-MM-DD, a TIMESTAMP renders as an instant;
//   - date_diff answers whole days, truncating toward the PAST so a partial
//     day counts alike on both sides of the epoch;
//   - a bare untyped integer keeps meaning days since the epoch: after
//     resolution it is genuinely a bare number, not a column that lost a unit.
//
// Both evaluation paths are asserted on the same fixture, as #319 asked: the
// two diverging is what hid that defect, and neither function has a vectorized
// kernel today, so the vec path here is EvalVec's per-row fallback writing
// into an output vector of the DECLARED return type — which is also what makes
// this test notice if that declaration stops matching what the function
// returns (the failure mode of #310 / cd92b79).

var (
	// A 1996 instant with a non-zero time-of-day, and a pre-1970 one.
	arithHi = time.Date(1996, 3, 13, 14, 25, 36, 0, time.UTC)
	arithLo = time.Date(1961, 4, 12, 6, 7, 0, 0, time.UTC)
)

// epochDays returns the calendar day of t as days since the Unix epoch, the
// unit a DATE vector stores.
func epochDays(t time.Time) int32 {
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return int32(d.Unix() / 86400)
}

// dateArithBatch holds the same two instants in every input shape the family
// has to handle: a DATE column, a TIMESTAMP column, ISO text with a clock,
// ISO text without one, and a bare INT64 column of day numbers.
func dateArithBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "s", Type: parquet.TypeString},
		{Name: "sd", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
	}, 2)
	for i, inst := range []time.Time{arithHi, arithLo} {
		b.Columns[0].Int32Data[i] = epochDays(inst)
		b.Columns[1].Int64Data[i] = inst.UnixMilli()
		b.Columns[2].SetValue(i, inst.Format(time.RFC3339))
		b.Columns[3].SetValue(i, inst.Format("2006-01-02"))
		b.Columns[4].Int64Data[i] = int64(epochDays(inst))
	}
	return b
}

// dateArithCase is one call shape: the function, its full argument list, and
// the answer for each fixture row.
type dateArithCase struct {
	label string
	fn    string
	args  []Expr
	want  [2]any
}

func (c dateArithCase) build() *FuncCall {
	return &FuncCall{Name: c.fn, Args: c.args}
}

// dateArithFamily is every registry function that reads an argument as an
// instant AND renders a result from it. Keep it in step with dateArithFuncs:
// TestTemporalInputFuncsCoverage reads this table, so a member missing here is
// a member nobody is checking.
func dateArithFamily() []dateArithCase {
	col := func(n string) Expr { return &ColRef{Name: n} }
	return []dateArithCase{
		// A DATE argument keeps rendering as a calendar date.
		{"date_add_date", "date_add", []Expr{col("d"), &Lit{Val: int64(1)}},
			[2]any{"1996-03-14", "1961-04-13"}},
		{"date_sub_date", "date_sub", []Expr{col("d"), &Lit{Val: int64(14)}},
			[2]any{"1996-02-28", "1961-03-29"}},

		// A TIMESTAMP argument shifts by whole days and keeps its clock.
		{"date_add_timestamp", "date_add", []Expr{col("ts"), &Lit{Val: int64(1)}},
			[2]any{"1996-03-14 14:25:36", "1961-04-13 06:07:00"}},
		{"date_sub_timestamp", "date_sub", []Expr{col("ts"), &Lit{Val: int64(14)}},
			[2]any{"1996-02-28 14:25:36", "1961-03-29 06:07:00"}},
		// The zero shift is the clearest statement of "time-of-day survives":
		// before the fix this answered a date two million years out.
		{"date_add_timestamp_zero", "date_add", []Expr{col("ts"), &Lit{Val: int64(0)}},
			[2]any{"1996-03-13 14:25:36", "1961-04-12 06:07:00"}},

		// Text: a clock in the literal makes it an instant, its absence a day.
		{"date_add_text_instant", "date_add", []Expr{col("s"), &Lit{Val: int64(1)}},
			[2]any{"1996-03-14 14:25:36", "1961-04-13 06:07:00"}},
		{"date_add_text_date", "date_add", []Expr{col("sd"), &Lit{Val: int64(1)}},
			[2]any{"1996-03-14", "1961-04-13"}},

		// A bare untyped integer still means days since the epoch.
		{"date_add_bare_int", "date_add", []Expr{col("n"), &Lit{Val: int64(1)}},
			[2]any{"1996-03-14", "1961-04-13"}},
		{"date_sub_bare_int", "date_sub", []Expr{col("n"), &Lit{Val: int64(14)}},
			[2]any{"1996-02-28", "1961-03-29"}},

		// An INTERVAL keeps its own unit. Two hours added to a whole day
		// makes an instant; a month added to one does not.
		{"date_add_interval_hours", "date_add",
			[]Expr{col("ts"), &Lit{Val: IntervalValue{Hours: 2}}},
			[2]any{"1996-03-13 16:25:36", "1961-04-12 08:07:00"}},
		{"date_sub_interval_hours", "date_sub",
			[]Expr{col("d"), &Lit{Val: IntervalValue{Hours: 2}}},
			[2]any{"1996-03-12 22:00:00", "1961-04-11 22:00:00"}},
		{"date_add_interval_month", "date_add",
			[]Expr{col("d"), &Lit{Val: IntervalValue{Months: 1}}},
			[2]any{"1996-04-13", "1961-05-12"}},

		// date_diff: whole days, truncating toward the past. The DATE column
		// is midnight of the same day the TIMESTAMP column falls in, so the
		// pair is 14h25m apart in one direction — 0 days forward, one whole
		// day back — identically for the 1996 row and the pre-1970 row.
		{"date_diff_ts_minus_date", "date_diff", []Expr{col("ts"), col("d")},
			[2]any{0.0, 0.0}},
		{"date_diff_date_minus_ts", "date_diff", []Expr{col("d"), col("ts")},
			[2]any{-1.0, -1.0}},
		{"date_diff_text_pair", "date_diff", []Expr{col("sd"), col("s")},
			[2]any{-1.0, -1.0}},
		// Against a fixed epoch, the day number of each row — negative for
		// the pre-1970 one.
		{"date_diff_date_epoch", "date_diff", []Expr{col("d"), &Lit{Val: "1970-01-01"}},
			[2]any{9568.0, -3186.0}},
		// The pre-1970 row is the one that catches truncation toward zero:
		// it sits 3185.75 days BEFORE the epoch, so flooring answers -3186
		// — its calendar day number, the same as the DATE column above —
		// while truncating toward zero would answer -3185.
		{"date_diff_ts_epoch", "date_diff", []Expr{col("ts"), &Lit{Val: "1970-01-01"}},
			[2]any{9568.0, -3186.0}},
		{"date_diff_bare_int", "date_diff", []Expr{col("n"), &Lit{Val: "1970-01-01"}},
			[2]any{9568.0, -3186.0}},

		// to_date reads the same instant and always answers a calendar date.
		{"to_date_date", "to_date", []Expr{col("d")},
			[2]any{"1996-03-13", "1961-04-12"}},
		{"to_date_timestamp", "to_date", []Expr{col("ts")},
			[2]any{"1996-03-13", "1961-04-12"}},
		{"to_date_bare_int", "to_date", []Expr{col("n")},
			[2]any{"1996-03-13", "1961-04-12"}},
	}
}

// TestDateArithOverTemporalColumns is the core regression: every call shape,
// over a DATE column, a TIMESTAMP column, text and a bare integer, on the 1996
// row and the pre-1970 row.
func TestDateArithOverTemporalColumns(t *testing.T) {
	b := dateArithBatch(t)
	for _, c := range dateArithFamily() {
		for row := 0; row < 2; row++ {
			t.Run(c.label, func(t *testing.T) {
				got := c.build().Eval(b, row)
				if got != c.want[row] {
					t.Errorf("row %d: %s = %v (%T), want %v",
						row, c.label, got, got, c.want[row])
				}
			})
		}
	}
}

// TestDateArithScalarVecAgree pins the invariant that the two paths answer the
// same question, and that they answer it into the vector type the registry
// DECLARES for the function — a declaration that stops matching what the
// function returns is how #310 killed the server process.
func TestDateArithScalarVecAgree(t *testing.T) {
	b := dateArithBatch(t)
	for _, c := range dateArithFamily() {
		t.Run(c.label, func(t *testing.T) {
			declared, conf := DefaultRegistry.ReturnType(c.fn).Resolve(0, nil)
			if conf == Undecided {
				t.Fatalf("%s has no declared return type", c.fn)
			}
			// A fresh FuncCall per path: EvalVec and Eval must not share
			// per-instance state that makes them agree by accident.
			out := batch.NewVector(declared.ID, 2)
			c.build().EvalVec(b, out, 2)
			for row := 0; row < 2; row++ {
				scalar := c.build().Eval(b, row)
				if vec := out.GetValue(row); vec != scalar {
					t.Errorf("row %d: vec = %v (%T), scalar = %v (%T)",
						row, vec, vec, scalar, scalar)
				}
				// The declared type has to be the type the value is
				// actually stored as, or the output vector above could
				// not have held it.
				switch declared.ID {
				case batch.TypeString:
					if _, isText := scalar.(string); !isText {
						t.Errorf("%s declares String but returned %T", c.fn, scalar)
					}
				case batch.TypeFloat64:
					if _, isNum := scalar.(float64); !isNum {
						t.Errorf("%s declares Float64 but returned %T", c.fn, scalar)
					}
				}
			}
		})
	}
}

// TestDateArithNullRows: a null temporal cell stays null on both paths rather
// than resolving to the epoch.
func TestDateArithNullRows(t *testing.T) {
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
		want any
	}{
		{"d", "1996-03-14"},
		{"ts", "1996-03-14 14:25:36"},
	} {
		args := []Expr{&ColRef{Name: tc.col}, &Lit{Val: int64(1)}}
		if got := (&FuncCall{Name: "date_add", Args: args}).Eval(b, 0); got != tc.want {
			t.Errorf("scalar date_add(%s) row 0: got %v want %v", tc.col, got, tc.want)
		}
		if got := (&FuncCall{Name: "date_add", Args: args}).Eval(b, 1); got != nil {
			t.Errorf("scalar date_add(%s) row 1 (null): got %v want nil", tc.col, got)
		}
		out := batch.NewVector(batch.TypeString, 2)
		(&FuncCall{Name: "date_add", Args: args}).EvalVec(b, out, 2)
		if got := out.GetValue(0); got != tc.want {
			t.Errorf("vec date_add(%s) row 0: got %v want %v", tc.col, got, tc.want)
		}
		if got := out.GetValue(1); got != nil {
			t.Errorf("vec date_add(%s) row 1 (null): got %v want nil", tc.col, got)
		}
	}
}

// TestDateArithMillisecondsAreNotDays is the bug in one line, for the reader
// who arrives from the issue: the DATE and the TIMESTAMP column hold the same
// day, so date_add over them must land on the same day. Before the fix the
// timestamp answered year 2,196,240.
func TestDateArithMillisecondsAreNotDays(t *testing.T) {
	b := dateArithBatch(t)
	fromDate := (&FuncCall{
		Name: "date_add",
		Args: []Expr{&ColRef{Name: "d"}, &Lit{Val: int64(1)}},
	}).Eval(b, 0)
	fromTS := (&FuncCall{
		Name: "date_add",
		Args: []Expr{&ColRef{Name: "ts"}, &Lit{Val: int64(1)}},
	}).Eval(b, 0)
	ts, isText := fromTS.(string)
	if !isText || len(ts) < 10 || ts[:10] != fromDate {
		t.Errorf("date_add(ts) = %v, want the same day as date_add(d) = %v", fromTS, fromDate)
	}
}
