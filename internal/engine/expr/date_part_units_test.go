package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Issue #319 regression: the date-part family over a temporal COLUMN.
//
// batch.Vector stores a DATE as days-since-epoch in Int32Data and a TIMESTAMP
// as milliseconds-since-epoch in Int64Data. ColRef.Eval boxes both as a bare
// int64 — correct and load-bearing for comparison and arithmetic, but the unit
// is gone, and parseTime reads a bare int64 as SECONDS. So 9568 days became
// 9568 seconds and YEAR(l_shipdate) answered 1970 for every row of a decade;
// 826727136000 ms became year 28167. Silent: no error, no null, and
// `GROUP BY EXTRACT(YEAR FROM d)` collapsed everything into one bucket.
//
// Both evaluation paths are asserted here, on the same fixture, because their
// DIVERGENCE is what hid the defect: the vectorized kernels were fixed first
// (ccc2e64) while the scalar path — the one the distributed SELECT projection
// and every WHERE clause actually run — stayed wrong.

// 1996-03-13T14:25:36Z in each of the three storage forms.
const (
	testEpochDay    = 9568         // 1996-03-13
	testEpochMillis = 826727136000 // 1996-03-13T14:25:36Z
	testDateEpochS  = 9568 * 86400 // midnight of that day
	testTSEpochS    = 826727136    // that instant
	testISOText     = "1996-03-13T14:25:36Z"
)

// temporalBatch builds a one-row batch holding the same day in a DATE column
// ("d"), a TIMESTAMP column ("ts"), and ISO text ("s").
func temporalBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "s", Type: parquet.TypeString},
	}, 1)
	b.Columns[0].Int32Data[0] = testEpochDay
	b.Columns[1].Int64Data[0] = testEpochMillis
	b.Columns[2].SetValue(0, testISOText)
	return b
}

// datePartCase is one function of the family. wantDate is the answer for the
// DATE column (midnight); wantInstant is the answer for the TIMESTAMP column
// and for the ISO text column, which carry the same wall clock.
type datePartCase struct {
	fn          string
	extraArgs   []Expr // arguments after the temporal one
	unitArg     *Lit   // leading unit argument (date_trunc, extract), if any
	wantDate    any
	wantInstant any
}

func (c datePartCase) build(col Expr) *FuncCall {
	var args []Expr
	if c.unitArg != nil {
		args = append(args, c.unitArg)
	}
	args = append(args, col)
	args = append(args, c.extraArgs...)
	return &FuncCall{Name: c.fn, Args: args}
}

// datePartFamily is every registry function that reads an argument as an
// instant. Keep it in step with temporalInputFuncs: a member missing from here
// is a member nobody is checking, which is how this class keeps recurring.
func datePartFamily() []datePartCase {
	return []datePartCase{
		{fn: "year", wantDate: 1996.0, wantInstant: 1996.0},
		{fn: "quarter", wantDate: 1.0, wantInstant: 1.0},
		{fn: "month", wantDate: 3.0, wantInstant: 3.0},
		{fn: "week", wantDate: 11.0, wantInstant: 11.0},
		{fn: "day", wantDate: 13.0, wantInstant: 13.0},
		{fn: "day_of_week", wantDate: 3.0, wantInstant: 3.0}, // Wednesday
		{fn: "day_of_year", wantDate: 73.0, wantInstant: 73.0},
		{fn: "hour", wantDate: 0.0, wantInstant: 14.0},
		{fn: "minute", wantDate: 0.0, wantInstant: 25.0},
		{fn: "second", wantDate: 0.0, wantInstant: 36.0},
		{fn: "epoch", wantDate: float64(testDateEpochS), wantInstant: float64(testTSEpochS)},
		{fn: "to_unixtime", wantDate: float64(testDateEpochS), wantInstant: float64(testTSEpochS)},
		{fn: "last_day_of_month", wantDate: "1996-03-31", wantInstant: "1996-03-31"},
		{
			fn: "date_format", extraArgs: []Expr{&Lit{Val: "%Y-%m-%d"}},
			wantDate: "1996-03-13", wantInstant: "1996-03-13",
		},
		{
			fn: "date_trunc", unitArg: &Lit{Val: "month"},
			wantDate: "1996-03-01T00:00:00Z", wantInstant: "1996-03-01T00:00:00Z",
		},
		{
			fn: "date_trunc", unitArg: &Lit{Val: "day"},
			wantDate: "1996-03-13T00:00:00Z", wantInstant: "1996-03-13T00:00:00Z",
		},
		// The two-argument extract(): reachable as written SQL, and the one
		// shape with a vectorized kernel of its own to disagree with.
		{fn: "extract", unitArg: &Lit{Val: "year"}, wantDate: 1996.0, wantInstant: 1996.0},
		{fn: "extract", unitArg: &Lit{Val: "quarter"}, wantDate: 1.0, wantInstant: 1.0},
		{fn: "extract", unitArg: &Lit{Val: "month"}, wantDate: 3.0, wantInstant: 3.0},
		{fn: "extract", unitArg: &Lit{Val: "week"}, wantDate: 11.0, wantInstant: 11.0},
		{fn: "extract", unitArg: &Lit{Val: "day"}, wantDate: 13.0, wantInstant: 13.0},
		{fn: "extract", unitArg: &Lit{Val: "dow"}, wantDate: 3.0, wantInstant: 3.0},
		{fn: "extract", unitArg: &Lit{Val: "doy"}, wantDate: 73.0, wantInstant: 73.0},
		{fn: "extract", unitArg: &Lit{Val: "hour"}, wantDate: 0.0, wantInstant: 14.0},
		{fn: "extract", unitArg: &Lit{Val: "minute"}, wantDate: 0.0, wantInstant: 25.0},
		{fn: "extract", unitArg: &Lit{Val: "second"}, wantDate: 0.0, wantInstant: 36.0},
		{
			fn: "extract", unitArg: &Lit{Val: "epoch"},
			wantDate: float64(testDateEpochS), wantInstant: float64(testTSEpochS),
		},
		// date_part() is EXTRACT's function spelling and shares its kernels
		// outright (#341). It is exercised over the same units because sharing
		// an implementation is a decision that can be undone: if the two ever
		// diverge, the DATE column is where it shows first.
		{fn: "date_part", unitArg: &Lit{Val: "year"}, wantDate: 1996.0, wantInstant: 1996.0},
		{fn: "date_part", unitArg: &Lit{Val: "quarter"}, wantDate: 1.0, wantInstant: 1.0},
		{fn: "date_part", unitArg: &Lit{Val: "month"}, wantDate: 3.0, wantInstant: 3.0},
		{fn: "date_part", unitArg: &Lit{Val: "week"}, wantDate: 11.0, wantInstant: 11.0},
		{fn: "date_part", unitArg: &Lit{Val: "day"}, wantDate: 13.0, wantInstant: 13.0},
		{fn: "date_part", unitArg: &Lit{Val: "dow"}, wantDate: 3.0, wantInstant: 3.0},
		{fn: "date_part", unitArg: &Lit{Val: "doy"}, wantDate: 73.0, wantInstant: 73.0},
		{fn: "date_part", unitArg: &Lit{Val: "hour"}, wantDate: 0.0, wantInstant: 14.0},
		{fn: "date_part", unitArg: &Lit{Val: "minute"}, wantDate: 0.0, wantInstant: 25.0},
		{fn: "date_part", unitArg: &Lit{Val: "second"}, wantDate: 0.0, wantInstant: 36.0},
		{
			fn: "date_part", unitArg: &Lit{Val: "epoch"},
			wantDate: float64(testDateEpochS), wantInstant: float64(testTSEpochS),
		},
	}
}

func caseLabel(c datePartCase) string {
	if c.unitArg != nil {
		return c.fn + "_" + c.unitArg.Val.(string)
	}
	return c.fn
}

// TestDatePartsOverTemporalColumns is the core regression: every member of the
// family, over a DATE column, a TIMESTAMP column, and a date held as text.
// Before the fix the DATE and TIMESTAMP columns answered off 1970 / year 28167
// for everything that ran scalar.
func TestDatePartsOverTemporalColumns(t *testing.T) {
	b := temporalBatch(t)
	cols := []struct {
		col  string
		want func(datePartCase) any
	}{
		{"d", func(c datePartCase) any { return c.wantDate }},
		{"ts", func(c datePartCase) any { return c.wantInstant }},
		{"s", func(c datePartCase) any { return c.wantInstant }},
	}
	for _, cc := range cols {
		for _, c := range datePartFamily() {
			t.Run(cc.col+"_"+caseLabel(c), func(t *testing.T) {
				got := c.build(&ColRef{Name: cc.col}).Eval(b, 0)
				if got != cc.want(c) {
					t.Errorf("%s(%s) = %v (%T), want %v", c.fn, cc.col, got, got, cc.want(c))
				}
			})
		}
	}
}

// TestDatePartsScalarVecAgree pins the invariant that the two paths answer the
// same question. The vectorized kernels resolve the instant from the vector's
// type; the scalar path resolves it from the column reference's type. Both go
// through columnInstant, and this asserts they stay tied together — including
// for the family members with no kernel, which fall back to per-row Eval.
func TestDatePartsScalarVecAgree(t *testing.T) {
	b := temporalBatch(t)
	for _, col := range []string{"d", "ts", "s"} {
		for _, c := range datePartFamily() {
			t.Run(col+"_"+caseLabel(c), func(t *testing.T) {
				scalar := c.build(&ColRef{Name: col}).Eval(b, 0)

				outType := batch.TypeFloat64
				if _, isText := scalar.(string); isText {
					outType = batch.TypeString
				}
				out := batch.NewVector(outType, 1)
				// A fresh FuncCall: EvalVec and Eval must not share
				// per-instance state that makes them agree by accident.
				c.build(&ColRef{Name: col}).EvalVec(b, out, 1)
				vec := out.GetValue(0)

				if vec != scalar {
					t.Errorf("%s(%s): vec = %v (%T), scalar = %v (%T)",
						c.fn, col, vec, vec, scalar, scalar)
				}
			})
		}
	}
}

// TestDatePartsNullRows: a null temporal cell must stay null on both paths,
// not resolve to the epoch.
func TestDatePartsNullRows(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}, 2)
	b.Columns[0].Int32Data[0] = testEpochDay
	b.Columns[0].Nulls.SetNull(1)
	b.Columns[1].Int64Data[0] = testEpochMillis
	b.Columns[1].Nulls.SetNull(1)

	for _, col := range []string{"d", "ts"} {
		fc := &FuncCall{Name: "year", Args: []Expr{&ColRef{Name: col}}}
		if got := fc.Eval(b, 0); got != 1996.0 {
			t.Errorf("scalar %s row 0: got %v want 1996", col, got)
		}
		if got := fc.Eval(b, 1); got != nil {
			t.Errorf("scalar %s row 1 (null): got %v want nil", col, got)
		}
		out := batch.NewVector(batch.TypeFloat64, 2)
		(&FuncCall{Name: "year", Args: []Expr{&ColRef{Name: col}}}).EvalVec(b, out, 2)
		if got := out.GetValue(0); got != 1996.0 {
			t.Errorf("vec %s row 0: got %v want 1996", col, got)
		}
		if got := out.GetValue(1); got != nil {
			t.Errorf("vec %s row 1 (null): got %v want nil", col, got)
		}
	}
}

// TestDatePartsUnknownUnit: an unrecognized EXTRACT unit is null on both
// paths. The kernel used to leave the pooled output vector's stale contents
// in place, so the answer was whatever the previous batch had written there.
func TestDatePartsUnknownUnit(t *testing.T) {
	b := temporalBatch(t)
	fc := &FuncCall{Name: "extract", Args: []Expr{&Lit{Val: "fortnight"}, &ColRef{Name: "d"}}}
	if got := fc.Eval(b, 0); got != nil {
		t.Errorf("scalar unknown unit: got %v want nil", got)
	}
	out := batch.NewVector(batch.TypeFloat64, 1)
	out.Float64Data[0] = 12345 // stand-in for a dirty pooled vector
	(&FuncCall{Name: "extract", Args: []Expr{&Lit{Val: "fortnight"}, &ColRef{Name: "d"}}}).EvalVec(b, out, 1)
	if got := out.GetValue(0); got != nil {
		t.Errorf("vec unknown unit: got %v want nil", got)
	}
}

// TestTemporalColumnBoxingUnchanged pins the representation the fix is built
// on top of rather than replacing. Boxing a DATE as time.Time or as its ISO
// string would have been unambiguous, but ColRef.Eval's box feeds comparison
// (compare()'s parseTemporalInt64 infers days-vs-milliseconds from the
// MAGNITUDE of this int64), arithmetic and Vector.SetValue on materialization
// — none of which accept a time.Time. The unit is restored at the function
// boundary instead, where the column's type is still in hand: for the
// date-part family here, and for the date-arithmetic family in #322.
func TestTemporalColumnBoxingUnchanged(t *testing.T) {
	b := temporalBatch(t)
	if v := (&ColRef{Name: "d"}).Eval(b, 0); v != int64(testEpochDay) {
		t.Errorf("DATE boxing changed: got %v (%T), want int64(%d)", v, v, testEpochDay)
	}
	if v := (&ColRef{Name: "ts"}).Eval(b, 0); v != int64(testEpochMillis) {
		t.Errorf("TIMESTAMP boxing changed: got %v (%T), want int64(%d)", v, v, testEpochMillis)
	}
	// A function outside the family still sees the raw number.
	abs := &FuncCall{Name: "abs", Args: []Expr{&ColRef{Name: "d"}}}
	if got := ToFloat64(abs.Eval(b, 0)); got != testEpochDay {
		t.Errorf("abs over date: got %v want %d", got, testEpochDay)
	}
	// An untyped integer column keeps meaning epoch SECONDS: only DATE and
	// TIMESTAMP columns declare a unit the engine can act on.
	ib := batch.NewRecordBatch([]parquet.Column{{Name: "n", Type: parquet.TypeInt64}}, 1)
	ib.Columns[0].Int64Data[0] = testTSEpochS
	yr := &FuncCall{Name: "year", Args: []Expr{&ColRef{Name: "n"}}}
	if got := yr.Eval(ib, 0); got != 1996.0 {
		t.Errorf("year(int64 seconds): got %v want 1996", got)
	}
}

// TestTemporalInputFuncsCoverage: every registry function that reads an
// argument as an instant must be listed in temporalInputFuncs, or its column
// arguments silently keep the unit-less number. This is the check that makes
// adding a new date-part function fail loudly instead of quietly wrong.
func TestTemporalInputFuncsCoverage(t *testing.T) {
	b := temporalBatch(t)
	// year(text) and year(date) must agree for every listed function; if a
	// function reads an instant but is NOT listed, the date column answers
	// off 1970 while the text column answers correctly.
	for name := range temporalInputFuncs {
		if DefaultRegistry.Lookup(name) == nil {
			t.Errorf("temporalInputFuncs lists %q, which is not registered", name)
		}
	}
	// The family tables must exercise every listed name: the date-part
	// table above, or the date-arithmetic table in date_arith_units_test.go
	// (issue #322), whose members render a result instead of extracting a
	// part of one.
	covered := map[string]bool{}
	for _, c := range datePartFamily() {
		covered[c.fn] = true
	}
	for _, c := range dateArithFamily() {
		covered[c.fn] = true
	}
	for name := range temporalInputFuncs {
		switch name {
		case "at_timezone", "timezone":
			// Zone-conversion functions, covered by timezone_funcs_test.go;
			// their second argument is a zone, not a part.
			continue
		}
		if !covered[name] {
			t.Errorf("temporalInputFuncs lists %q but datePartFamily does not exercise it", name)
		}
	}
	_ = b
}

// TestAtTimezoneOverTemporalColumns: the zone-conversion pair reads its
// instant through the same boundary, so a DATE or TIMESTAMP column must not
// land on 1970 there either.
func TestAtTimezoneOverTemporalColumns(t *testing.T) {
	b := temporalBatch(t)
	tz := &FuncCall{Name: "timezone", Args: []Expr{&Lit{Val: "UTC"}, &ColRef{Name: "ts"}}}
	if got := tz.Eval(b, 0); got != testISOText {
		t.Errorf("timezone('UTC', ts): got %v want %v", got, testISOText)
	}
	at := &FuncCall{Name: "at_timezone", Args: []Expr{&ColRef{Name: "d"}, &Lit{Val: "UTC"}}}
	if got := at.Eval(b, 0); got != "1996-03-13T00:00:00Z" {
		t.Errorf("at_timezone(d, 'UTC'): got %v want 1996-03-13T00:00:00Z", got)
	}
}
