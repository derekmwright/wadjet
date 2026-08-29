package expr

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The expression-layer contract for CAST to a temporal type (#340).
//
// CAST(x AS DATE) used to return x unchanged, so `CAST('1996-01-10' AS DATE) -
// 1` subtracted 1 from the number ToFloat64 read out of the TEXT and answered
// 1995. It now produces the DATE column representation — epoch days as int64,
// exactly what ColRef.Eval hands out for a batch.TypeDate column — and
// CAST(x AS TIMESTAMP) produces epoch milliseconds for the same reason.

// dateCastBatch holds one row of each source form a cast can be applied to.
// The DATE and TIMESTAMP columns are the interesting ones: both box as a bare
// int64 whose unit is recoverable only from the declared column type.
func dateCastBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "s", Type: parquet.TypeString},
		{Name: "junk", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
	}, 1)
	b.Len = 1
	// 1996-01-10 is 9505 days after the epoch.
	b.Columns[0].SetValue(0, int32(9505))
	b.Columns[1].SetValue(0, time.Date(1996, 1, 10, 13, 45, 30, 0, time.UTC).UnixMilli())
	b.Columns[2].SetValue(0, "1996-01-10")
	b.Columns[3].SetValue(0, "not-a-date")
	b.Columns[4].SetValue(0, int64(9505))
	return b
}

func TestCastToDateProducesEpochDays(t *testing.T) {
	b := dateCastBatch(t)
	const want = int64(9505) // 1996-01-10

	cases := []struct {
		name    string
		operand Expr
		want    any
	}{
		{"date column", &ColRef{Name: "d"}, want},
		// The clock is floored away: a DATE is a whole day.
		{"timestamp column", &ColRef{Name: "ts"}, want},
		{"text column", &ColRef{Name: "s"}, want},
		{"text literal", &Lit{Val: "1996-01-10"}, want},
		{"timestamp text literal", &Lit{Val: "1996-01-10T13:45:30Z"}, want},
		// A bare number keeps the days-since-epoch reading the whole
		// date-arithmetic family gives it (parseDateArg).
		{"int column", &ColRef{Name: "n"}, want},
		// No instant, no date. A pass-through of the original value is the
		// defect and is the one answer ruled out.
		{"unparseable text", &ColRef{Name: "junk"}, nil},
		{"unparseable literal", &Lit{Val: "31/12/1996"}, nil},
		{"empty string", &Lit{Val: ""}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := (&Cast{Operand: c.operand, DestType: "date"}).Eval(b, 0)
			if got != c.want {
				t.Errorf("CAST(%s AS DATE) = %v (%T), want %v", c.name, got, got, c.want)
			}
		})
	}
}

func TestCastToTimestampProducesEpochMillis(t *testing.T) {
	b := dateCastBatch(t)
	midnight := time.Date(1996, 1, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
	withClock := time.Date(1996, 1, 10, 13, 45, 30, 0, time.UTC).UnixMilli()

	cases := []struct {
		name    string
		operand Expr
		want    any
	}{
		{"date column is midnight of its day", &ColRef{Name: "d"}, midnight},
		{"timestamp column keeps its clock", &ColRef{Name: "ts"}, withClock},
		{"text column", &ColRef{Name: "s"}, midnight},
		{"text literal with a clock", &Lit{Val: "1996-01-10 13:45:30"}, withClock},
		{"unparseable text", &ColRef{Name: "junk"}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := (&Cast{Operand: c.operand, DestType: "timestamp"}).Eval(b, 0)
			if got != c.want {
				t.Errorf("CAST(%s AS TIMESTAMP) = %v (%T), want %v", c.name, got, got, c.want)
			}
		})
	}

	// A NULL operand stays NULL rather than becoming the epoch.
	b.Columns[2].Nulls.SetNull(0)
	if got := (&Cast{Operand: &ColRef{Name: "s"}, DestType: "timestamp"}).Eval(b, 0); got != nil {
		t.Errorf("CAST(NULL AS TIMESTAMP) = %v, want nil", got)
	}
}

// TestCastTemporalSpellings pins which destination names are temporal. TIME is
// deliberately not: there is no time-of-day column type to hold one, so it
// keeps the pass-through it always had.
func TestCastTemporalSpellings(t *testing.T) {
	b := dateCastBatch(t)
	for _, dest := range []string{"date", "DATE", " Date "} {
		if got := (&Cast{Operand: &Lit{Val: "1996-01-10"}, DestType: dest}).Eval(b, 0); got != int64(9505) {
			t.Errorf("CAST(... AS %q) = %v, want 9505", dest, got)
		}
	}
	for _, dest := range []string{"timestamp", "datetime", "TIMESTAMPTZ"} {
		want := time.Date(1996, 1, 10, 0, 0, 0, 0, time.UTC).UnixMilli()
		if got := (&Cast{Operand: &Lit{Val: "1996-01-10"}, DestType: dest}).Eval(b, 0); got != want {
			t.Errorf("CAST(... AS %q) = %v, want %v", dest, got, want)
		}
	}
	if got := (&Cast{Operand: &Lit{Val: "10:00:00"}, DestType: "time"}).Eval(b, 0); got != "10:00:00" {
		t.Errorf("CAST('10:00:00' AS TIME) = %v, want the text unchanged", got)
	}
}

// TestEpochDaysOfFloorsBeforeTheEpoch: a truncating division rounds toward
// zero, which is the wrong way on the negative side — 1969-12-31 would come
// back as day 0 and collide with 1970-01-01.
func TestEpochDaysOfFloorsBeforeTheEpoch(t *testing.T) {
	cases := []struct {
		in   time.Time
		want int64
	}{
		{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), 0},
		{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), -1},
		{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), -1},
		{time.Date(1969, 12, 30, 12, 0, 0, 0, time.UTC), -2},
		{time.Date(1996, 1, 10, 0, 0, 0, 0, time.UTC), 9505},
	}
	for _, c := range cases {
		if got := epochDaysOf(c.in); got != c.want {
			t.Errorf("epochDaysOf(%s) = %d, want %d", c.in.Format(time.RFC3339), got, c.want)
		}
	}
}

// TestDateArithThroughBinOp is the operator half: what a caller can do with a
// cast's result once it is a date.
func TestDateArithThroughBinOp(t *testing.T) {
	b := dateCastBatch(t)
	castDate := func(e Expr) Expr { return &Cast{Operand: e, DestType: "date"} }
	lit := func(s string) Expr { return &Lit{Val: s} }

	cases := []struct {
		name  string
		expr  Expr
		want  any
		about string
	}{
		{"date - date is a day count",
			&BinOp{Left: castDate(lit("1996-01-10")), Right: castDate(lit("1996-01-01")), Op: "-"},
			int64(9), "0 means both operands were still text"},
		{"date - date backwards is negative",
			&BinOp{Left: castDate(lit("1996-01-01")), Right: castDate(lit("1996-01-10")), Op: "-"},
			int64(-9), ""},
		{"date - n is a date",
			&BinOp{Left: castDate(lit("1996-01-10")), Right: &Lit{Val: int64(1)}, Op: "-"},
			int64(9504), "1995 means the operand was still the string"},
		{"date + n is a date",
			&BinOp{Left: castDate(lit("1996-01-10")), Right: &Lit{Val: int64(5)}, Op: "+"},
			int64(9510), ""},
		{"n + date is a date",
			&BinOp{Left: &Lit{Val: int64(5)}, Right: castDate(lit("1996-01-10")), Op: "+"},
			int64(9510), "the one reversed spelling that means anything"},
		{"date column - date column",
			&BinOp{Left: &ColRef{Name: "d"}, Right: castDate(lit("1996-01-01")), Op: "-"},
			int64(9), ""},
		{"text date column - date",
			&BinOp{Left: &ColRef{Name: "s"}, Right: castDate(lit("1996-01-01")), Op: "-"},
			int64(9), "a VARCHAR column of date strings is how most catalogs spell a date"},
		// Declines, so the numeric path answers exactly as before.
		{"non-date text - n stays numeric",
			&BinOp{Left: &ColRef{Name: "junk"}, Right: &Lit{Val: int64(1)}, Op: "-"},
			float64(-1), "'not-a-date' reads as 0 through ToFloat64, as it always did"},
		{"date * n is not date arithmetic",
			&BinOp{Left: castDate(lit("1996-01-10")), Right: &Lit{Val: int64(2)}, Op: "*"},
			float64(19010), ""},
		{"date + date is not date arithmetic",
			&BinOp{Left: castDate(lit("1996-01-10")), Right: castDate(lit("1996-01-01")), Op: "+"},
			float64(19001), "adding two dates means nothing in SQL"},
		// An instant difference is an INTERVAL in SQL and this engine has no
		// interval column to answer with, so it stays on the numeric path
		// (a difference in milliseconds) rather than inventing a unit.
		{"timestamp - timestamp stays numeric",
			&BinOp{Left: &Cast{Operand: lit("1996-01-10 00:00:01"), DestType: "timestamp"},
				Right: &Cast{Operand: lit("1996-01-10 00:00:00"), DestType: "timestamp"}, Op: "-"},
			float64(1000), ""},
		// A fractional shift declines rather than silently rounding a date.
		{"date - 1.5 declines",
			&BinOp{Left: castDate(lit("1996-01-10")), Right: &Lit{Val: 1.5}, Op: "-"},
			float64(9503.5), ""},
		// NULL in, NULL out.
		{"NULL operand",
			&BinOp{Left: castDate(&ColRef{Name: "junk"}), Right: &Lit{Val: int64(1)}, Op: "-"},
			nil, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.expr.Eval(b, 0)
			if got != c.want {
				t.Errorf("= %v (%T), want %v (%T) %s", got, got, c.want, c.want, c.about)
			}
		})
	}
}

// TestDateArithOverColumnPair covers the shape the typed nodes take:
// `col - col` compiles to BinOpNumeric, which resolves its mode against the
// first batch. Two date columns there must produce a day count, not the
// NULL a float-only reading answers for a text column.
func TestDateArithOverColumnPair(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "ship", Type: parquet.TypeString},
		{Name: "recv", Type: parquet.TypeString},
		{Name: "d1", Type: parquet.TypeDate},
		{Name: "d2", Type: parquet.TypeDate},
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "c", Type: parquet.TypeInt64},
	}, 3)
	b.Len = 3
	ships := []string{"1996-03-13", "1996-04-12", "1996-01-29"}
	recvs := []string{"1996-03-22", "1996-04-20", "1996-01-31"}
	for i := 0; i < 3; i++ {
		b.Columns[0].SetValue(i, ships[i])
		b.Columns[1].SetValue(i, recvs[i])
		b.Columns[2].SetValue(i, int32(9505+i))
		b.Columns[3].SetValue(i, int32(9500))
		b.Columns[4].SetValue(i, int64(10+i))
		b.Columns[5].SetValue(i, int64(4))
	}

	lag := compileBinOp(&ColRef{Name: "recv"}, &ColRef{Name: "ship"}, "-", nil)
	want := []int64{9, 8, 2}
	for i := range want {
		if got := lag.Eval(b, i); got != want[i] {
			t.Errorf("row %d: recv - ship = %v (%T), want int64 %d "+
				"(one repeated value across the rows is the defect)", i, got, got, want[i])
		}
	}

	dd := compileBinOp(&ColRef{Name: "d1"}, &ColRef{Name: "d2"}, "-", nil)
	for i, w := range []int64{5, 6, 7} {
		if got := dd.Eval(b, i); got != w {
			t.Errorf("row %d: d1 - d2 = %v (%T), want int64 %d", i, got, got, w)
		}
	}

	// The control: integer columns keep integer arithmetic, untouched by
	// the temporal branch.
	ii := compileBinOp(&ColRef{Name: "a"}, &ColRef{Name: "c"}, "-", nil)
	for i, w := range []int64{6, 7, 8} {
		if got := ii.Eval(b, i); got != w {
			t.Errorf("row %d: a - c = %v (%T), want int64 %d", i, got, got, w)
		}
	}
}

// TestTemporalOperandResolvesACast: the operator path recovers a cast's unit
// from its destination type, the same way it recovers a column's from the
// declared column type (#332). Without it `DATE '1998-12-01' - INTERVAL '90'
// DAY` — TPC-H Q01's filter — would have fallen through to numeric arithmetic
// the moment the cast stopped passing its text along.
func TestTemporalOperandResolvesACast(t *testing.T) {
	b := dateCastBatch(t)

	dc := &Cast{Operand: &Lit{Val: "1996-01-10"}, DestType: "date"}
	got, ok := temporalOperand(b, 0, dc, dc.Eval(b, 0))
	if !ok {
		t.Fatal("a CAST to DATE is not recognized as a date operand")
	}
	cd, isCivil := got.(civilDate)
	if !isCivil || !cd.t.Equal(time.Date(1996, 1, 10, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("resolved to %#v, want civilDate 1996-01-10", got)
	}

	tc := &Cast{Operand: &Lit{Val: "1996-01-10 13:45:30"}, DestType: "timestamp"}
	got, ok = temporalOperand(b, 0, tc, tc.Eval(b, 0))
	if !ok {
		t.Fatal("a CAST to TIMESTAMP is not recognized as a date operand")
	}
	inst, isTime := got.(time.Time)
	if !isTime || !inst.Equal(time.Date(1996, 1, 10, 13, 45, 30, 0, time.UTC)) {
		t.Errorf("resolved to %#v, want the instant 1996-01-10T13:45:30Z", got)
	}

	// A non-temporal cast is not a date operand.
	ic := &Cast{Operand: &Lit{Val: "1996"}, DestType: "bigint"}
	if _, ok := temporalOperand(b, 0, ic, ic.Eval(b, 0)); ok {
		t.Error("CAST(... AS BIGINT) was taken for a date operand")
	}

	// And the INTERVAL shift over a cast still renders the way #322 pinned.
	shift := &BinOp{Left: dc, Right: &Lit{Val: IntervalValue{Days: 90}}, Op: "-"}
	if got := shift.Eval(b, 0); got != "1995-10-12" {
		t.Errorf("CAST('1996-01-10' AS DATE) - INTERVAL '90' DAY = %v (%T), want \"1995-10-12\"", got, got)
	}
}

// TestCastArgumentsReachTemporalFunctions: a cast's result loses its unit at
// exactly the point a column's does, so the two families that repair a column
// argument — resolveTemporalArgs (#319/#322) and formatTemporalArgs (#273) —
// have to repair a cast argument too. Without it YEAR(CAST(d AS DATE)) reads
// 9505 days as 9505 seconds and answers 1970.
func TestCastArgumentsReachTemporalFunctions(t *testing.T) {
	b := dateCastBatch(t)
	dc := &Cast{Operand: &ColRef{Name: "d"}, DestType: "date"}
	tc := &Cast{Operand: &ColRef{Name: "ts"}, DestType: "timestamp"}

	if got := (&FuncCall{Name: "year", Args: []Expr{dc}}).Eval(b, 0); got != float64(1996) {
		t.Errorf("YEAR(CAST(d AS DATE)) = %v (%T), want 1996 (1970 means the days were read as seconds)", got, got)
	}
	if got := (&FuncCall{Name: "year", Args: []Expr{tc}}).Eval(b, 0); got != float64(1996) {
		t.Errorf("YEAR(CAST(ts AS TIMESTAMP)) = %v (%T), want 1996", got, got)
	}
	if got := (&FuncCall{Name: "substr", Args: []Expr{dc, &Lit{Val: int64(1)}, &Lit{Val: int64(4)}}}).Eval(b, 0); got != "1996" {
		t.Errorf("SUBSTR(CAST(d AS DATE), 1, 4) = %v, want \"1996\" (the digits of the day NUMBER is the defect)", got)
	}
	// date_add renders its result, and a whole day must render as a
	// calendar date (#322).
	if got := (&FuncCall{Name: "date_add", Args: []Expr{dc, &Lit{Val: int64(1)}}}).Eval(b, 0); got != "1996-01-11" {
		t.Errorf("date_add(CAST(d AS DATE), 1) = %v (%T), want \"1996-01-11\"", got, got)
	}
	if got := (&FuncCall{Name: "date_diff", Args: []Expr{tc, dc}}).Eval(b, 0); got != float64(0) {
		t.Errorf("date_diff(CAST(ts AS TIMESTAMP), CAST(d AS DATE)) = %v, want 0", got)
	}
}
