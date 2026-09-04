package expr

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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

// TestCastToDateRaisesForTextThatNamesNoDay is #840's DATE half: the NULL
// these three used to answer is now the SQLSTATE PostgreSQL raises.
//
// The three cells were pinned as `nil` here until this change; they are the
// proof, so they moved rather than being added beside the old ones. Every
// expectation is live postgres:17.11, and the pair of codes is the whole
// point — a client branches on 22007 to report a malformed literal and on
// 22008 to report a date that does not exist.
func TestCastToDateRaisesForTextThatNamesNoDay(t *testing.T) {
	b := dateCastBatch(t)
	for _, c := range []struct {
		name       string
		operand    Expr
		state, msg string
	}{
		{"unparseable column text", &ColRef{Name: "junk"}, "22007",
			`invalid input syntax for type date: "not-a-date"`},
		{"an impossible day", &Lit{Val: "1996-02-30"}, "22008",
			`date/time field value out of range: "1996-02-30"`},
		{"an impossible month", &Lit{Val: "1996-13-01"}, "22008",
			`date/time field value out of range: "1996-13-01"`},
		{"empty string", &Lit{Val: ""}, "22007",
			`invalid input syntax for type date: ""`},
		// A DMY spelling: both engines REFUSE it, and they disagree about the
		// CLASS. PostgreSQL's DateStyle ISO, MDY reads the leading field as a
		// month and calls month 31 a field-range failure (22008); wadjet's
		// accept-set refuses every spelling whose field ORDER DateStyle would
		// decide (#639), so it is not a date at all here and the code is
		// 22007. That classification is `parquet.ParseDateDays`'s and it is
		// the SAME answer at the ingest boundary, the writer and the filter
		// kernel — so this is one accept-set decision showing through a new
		// door, not a divergence this CAST introduced. Pinned, in ADR-0012's
		// list, against #639's accept-set rather than against this cast.
		{"a DMY spelling wadjet's accept-set does not read as a date",
			&Lit{Val: "31/12/1996"}, "22007",
			`invalid input syntax for type date: "31/12/1996"`},
		// A zero MONTH and a zero DAY, which the calendar has no more than it
		// has a 30th of February.
		{"month zero", &Lit{Val: "2024-00-01"}, "22008",
			`date/time field value out of range: "2024-00-01"`},
		{"day zero", &Lit{Val: "2024-01-00"}, "22008",
			`date/time field value out of range: "2024-01-00"`},
		// A NEGATIVE year: PostgreSQL spells BC dates `0001-01-01 BC`, not
		// with a leading minus, so this is not a date at all in either engine.
		{"a negative year", &Lit{Val: "-0001-01-01"}, "22007",
			`invalid input syntax for type date: "-0001-01-01"`},
		// YEAR ZERO and a THREE-DIGIT MONTH, promoted here from the pin this
		// file used to carry. Both were ACCEPTED — `0000-01-01` answered a
		// day and `2024-001-01` answered 2024-01-01 — and both were left
		// alone on purpose: the reading is `parquet.ParseDateDays`'s, shared
		// by the ingest boundary, the writers, the row→batch builder and the
		// filter kernel, and a second reading inside the CAST would have been
		// the very two-answers-for-one-question defect this file is about.
		// The storage arc closed it at the accept-set (#641) and this door
		// inherited the refusal with no change here — which is exactly what
		// the pin predicted, and why deleting it is the proof. Codes and
		// messages measured on live 17.11 and identical on both engines.
		{"year zero", &Lit{Val: "0000-01-01"}, "22008",
			`date/time field value out of range: "0000-01-01"`},
		{"the other end of year zero", &Lit{Val: "0000-12-31"}, "22008",
			`date/time field value out of range: "0000-12-31"`},
		{"a three-digit month", &Lit{Val: "2024-001-01"}, "22007",
			`invalid input syntax for type date: "2024-001-01"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			state, msg := recoverFatalEvalForTest(t, func() {
				(&Cast{Operand: c.operand, DestType: "date"}).Eval(b, 0)
			})
			if state != c.state || msg != c.msg {
				t.Errorf("CAST(%s AS DATE) raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
					c.name, state, msg, c.state, c.msg)
			}
		})
	}
}

// TestCastToDateReadsTheEngineAcceptSet is the other half of the same wiring:
// the CAST door takes its VALUE from `parquet.ParseDateDays` too, not only its
// error code.
//
// #836's first pass took only the CODE from there and left `parseDateArg` as
// the value source, which made the two accept-sets show as a DIVERGENCE
// instead of as one rule — `CAST('20240101' AS DATE)` is 2024-01-01 on the
// live server and in the classifier, and the mismatch turned it into a
// refusal. A cast that REFUSES what the ingest boundary STORES is the same
// two-answers-for-one-question defect this arc is about, pointing the other
// way.
func TestCastToDateReadsTheEngineAcceptSet(t *testing.T) {
	b := dateCastBatch(t)
	const jan10 = int64(9505) // 1996-01-10
	for _, c := range []struct {
		in   string
		want any
	}{
		// The compact 8-digit form, which ParseDateDays documents and
		// PostgreSQL accepts. It RAISED between #836 and this change.
		{"19960110", jan10},
		{"1996-01-10", jan10},
		{"1996/01/10", jan10},
		{"1996.1.10", jan10},
		{"  1996-01-10  ", jan10},
		{"1996-01-10 13:45:30", jan10},
		{"1996-01-10T13:45:30Z", jan10},
	} {
		if got := (&Cast{Operand: &Lit{Val: c.in}, DestType: "date"}).Eval(b, 0); got != c.want {
			t.Errorf("CAST(%q AS DATE) = %v, want %v — the value comes from the same "+
				"accept-set the ingest boundary and the filter kernel read", c.in, got, c.want)
		}
	}
}

// TestCastToDateStillReadsTheYearsAroundTheRefusal is the BOUNDARY of the
// refusal the two cells above assert, and it is the half a census of failures
// cannot carry.
//
// The pin this replaces recorded a residual: `0000-01-01` and `2024-001-01`
// were ACCEPTED here because `parquet.ParseDateDays` accepted them, and the
// fix belonged in that one shared accept-set rather than in a second reading
// inside the CAST. #641 landed it there, the CAST inherited it, and the pin
// fired — which is what a pin that starts agreeing is for. The two spellings
// now sit in the census above.
//
// What remains worth asserting is that the refusal did not over-fire. A rule
// written as "reject a year starting 000" would take year 1 with it, and a
// three-digit-month rule written on field WIDTH would take the compact
// 8-digit form. Both still answer, on live 17.11 and here, with the same day
// numbers.
func TestCastToDateStillReadsTheYearsAroundTheRefusal(t *testing.T) {
	b := dateCastBatch(t)
	for _, c := range []struct {
		in   string
		want int64
	}{
		// The first day PostgreSQL has: year zero is refused, year 1 is not.
		{"0001-01-01", -719162},
		// Past four digits, which PostgreSQL also accepts.
		{"10000-01-01", 2932897},
		// The compact form, whose month field is two digits with no
		// separator — the shape a width-based three-digit-month rule would
		// have swallowed.
		{"19960110", 9505},
	} {
		if got := (&Cast{Operand: &Lit{Val: c.in}, DestType: "date"}).Eval(b, 0); got != c.want {
			t.Errorf("CAST(%q AS DATE) = %v, want %d — #641's year-zero and three-digit-month "+
				"refusals must not reach this spelling (live PostgreSQL 17.11 answers it)",
				c.in, got, c.want)
		}
	}
}

// recoverFatalEvalForTest runs f and reports the SQLSTATE and message of the
// per-row refusal it raised. It fails the test when f produces a value: "this
// raises" is the assertion, so an answer must not read as an empty string.
func recoverFatalEvalForTest(t *testing.T, f func()) (state, msg string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("no refusal was raised")
		}
		fe, ok := r.(fatalEval)
		if !ok {
			panic(r)
		}
		state, msg = sqlerr.StateOf(fe.err), fe.err.Error()
	}()
	f()
	return "", ""
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

// TestCastToTimestampRaisesForTextThatNamesNoInstant is #836 and #840's
// TIMESTAMP half — the cell the DML census pinned as `rows=[~]` and ADR-0012
// described as unreachable because "the CAST path has no per-row error
// channel for a temporal conversion".
func TestCastToTimestampRaisesForTextThatNamesNoInstant(t *testing.T) {
	b := dateCastBatch(t)
	for _, c := range []struct {
		name       string
		operand    Expr
		state, msg string
	}{
		{"unparseable column text", &ColRef{Name: "junk"}, "22007",
			`invalid input syntax for type timestamp: "not-a-date"`},
		// #836's own headline shape: a well-formed timestamp naming a day
		// February does not have.
		{"an impossible day", &Lit{Val: "2020-02-30 12:00:00"}, "22008",
			`date/time field value out of range: "2020-02-30 12:00:00"`},
		{"an impossible month", &Lit{Val: "2020-13-01T00:00:00"}, "22008",
			`date/time field value out of range: "2020-13-01T00:00:00"`},
		{"an impossible hour", &Lit{Val: "2020-01-01T25:00:00"}, "22008",
			`date/time field value out of range: "2020-01-01T25:00:00"`},
		{"text that is not a timestamp", &Lit{Val: "not-a-timestamp"}, "22007",
			`invalid input syntax for type timestamp: "not-a-timestamp"`},
		{"empty string", &Lit{Val: ""}, "22007",
			`invalid input syntax for type timestamp: ""`},
	} {
		t.Run(c.name, func(t *testing.T) {
			state, msg := recoverFatalEvalForTest(t, func() {
				(&Cast{Operand: c.operand, DestType: "timestamp"}).Eval(b, 0)
			})
			if state != c.state || msg != c.msg {
				t.Errorf("CAST(%s AS TIMESTAMP) raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
					c.name, state, msg, c.state, c.msg)
			}
		})
	}
}

// TestCastTemporalRefusalStopsAtText is the BOUNDARY of the refusal above,
// attempted from the outside (protocol rule 11).
//
// Only TEXT raises. A box with no temporal reading at all is a TYPE-PAIR
// failure — `CAST(true AS date)` is 42846 `cannot cast type boolean to date`
// in PostgreSQL, a parse-time refusal and not a data exception — so minting
// 22007 for it would put a data-exception code on a type error. Those keep
// the NULL they have; ADR-0012's divergence list records it. This cell fails
// if a later pass widens the raise past text without deciding that question.
func TestCastTemporalRefusalStopsAtText(t *testing.T) {
	b := dateCastBatch(t)
	for _, dest := range []string{"date", "timestamp"} {
		got := func() (v any) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("CAST(<boolean> AS %s) raised %v; PostgreSQL answers 42846 for this "+
						"TYPE PAIR at parse time, not a data exception. Widening the refusal past "+
						"text needs that question decided first (ADR-0012 item 1).", dest, r)
				}
			}()
			return (&Cast{Operand: &Lit{Val: true}, DestType: dest}).Eval(b, 0)
		}()
		if got != nil {
			t.Errorf("CAST(<boolean> AS %s) = %v, want nil", dest, got)
		}
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
		// NULL in, NULL out. The operand is a genuine NULL and not the `junk`
		// column: text that names no date is a REFUSAL now (#840), and a cell
		// that stood in for NULL with unparseable text was asserting the
		// defect — CAST(<not a date> AS DATE) - 1 is 22007 in PostgreSQL, not
		// a NULL row.
		{"NULL operand",
			&BinOp{Left: castDate(&Lit{Val: nil}), Right: &Lit{Val: int64(1)}, Op: "-"},
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
