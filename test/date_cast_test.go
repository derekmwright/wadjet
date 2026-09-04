package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// CAST(x AS DATE) used to return its argument unchanged, so nothing below it
// knew a date had been asked for and every date rule reasoned about the text
// (#340). The two shapes that name the defect:
//
//	SELECT DATE '1996-01-10' - DATE '1996-01-01'   -- 0,    DuckDB 9
//	SELECT CAST('1996-01-10' AS DATE) - 1          -- 1995, DuckDB 1996-01-09
//
// 1995 is 1996 - 1: the operand was still the string, and ToFloat64 read a
// number out of its leading digits. Per row the same shape answered a constant
// for every row of a table — a shipping-lag column that is the same number on
// every line, with no error anywhere.
//
// The cast now produces the DATE column representation (days since the epoch)
// and the projection declares the column DATE, so it renders as a calendar
// date; CAST(x AS TIMESTAMP) produces the TIMESTAMP column representation
// (epoch milliseconds) for the same reason. See castTemporal in
// internal/engine/expr.
func dateCastDB(t *testing.T) (context.Context, *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "sd", Type: parquet.TypeString},
		{Name: "bad", Type: parquet.TypeString},
		{Name: "ship", Type: parquet.TypeString},
		{Name: "recv", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "evt", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Lags 0, 1, 3, 7 and 30 days: a constant answer is the bug, so the
	// per-row answers have to be able to differ from each other.
	rows := []map[string]any{
		{"id": int64(1), "d": "1996-01-10", "ts": time.Date(1996, 1, 10, 13, 45, 30, 0, time.UTC),
			"sd": "1996-01-10", "bad": "not-a-date", "ship": "1996-03-13", "recv": "1996-03-13"},
		{"id": int64(2), "d": "1996-01-11", "ts": time.Date(1996, 1, 11, 0, 0, 0, 0, time.UTC),
			"sd": "1996-01-11", "bad": "31/12/1996", "ship": "1996-04-12", "recv": "1996-04-13"},
		{"id": int64(3), "d": "1996-01-12", "ts": time.Date(1996, 1, 12, 23, 59, 59, 0, time.UTC),
			"sd": "1996-01-12", "bad": "", "ship": "1996-01-29", "recv": "1996-02-01"},
		{"id": int64(4), "d": "1996-01-13", "ts": time.Date(1996, 1, 13, 6, 0, 0, 0, time.UTC),
			"sd": "1996-01-13", "bad": "1996-13-45", "ship": "1996-04-21", "recv": "1996-04-28"},
		{"id": int64(5), "d": "1996-01-14", "ts": time.Date(1996, 1, 14, 12, 0, 0, 0, time.UTC),
			"sd": "1996-01-14", "bad": "abc", "ship": "1996-03-30", "recv": "1996-04-29"},
	}
	ing := db.NewIngester("evt", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 3})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

func dateCastRows(t *testing.T, ctx context.Context, db *wadjet.DB, sql string) []map[string]any {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %s: %v", sql, err)
	}
	return res.Rows
}

// TestDateCastIssueRepros is the pair of statements the issue reports, run
// verbatim.
func TestDateCastIssueRepros(t *testing.T) {
	ctx, db := dateCastDB(t)

	rows := dateCastRows(t, ctx, db, "SELECT DATE '1996-01-10' - DATE '1996-01-01' AS gap")
	if len(rows) != 1 {
		t.Fatalf("%d rows, want 1", len(rows))
	}
	// A day COUNT, the way DuckDB types date-minus-date BIGINT. 0 is the
	// bug: both operands were still text and ToFloat64 read 1996 out of each.
	if got := rows[0]["gap"]; got != int64(9) {
		t.Errorf("DATE '1996-01-10' - DATE '1996-01-01' = %v (%T), want int64 9 "+
			"(0 means both operands were still strings)", got, got)
	}

	rows = dateCastRows(t, ctx, db, "SELECT CAST('1996-01-10' AS DATE) - 1 AS prev")
	if len(rows) != 1 {
		t.Fatalf("%d rows, want 1", len(rows))
	}
	// A DATE, the way DuckDB types date-minus-integer. 1995 is the bug:
	// 1996 - 1, the year read out of the text's leading digits.
	if got := rows[0]["prev"]; got != "1996-01-09" {
		t.Errorf("CAST('1996-01-10' AS DATE) - 1 = %v (%T), want \"1996-01-09\" "+
			"(1995 means the operand was still the string '1996-01-10')", got, got)
	}
}

// TestDateCastOverEverySourceForm casts each form an operand can arrive in.
// The DATE and TIMESTAMP columns are the ones that carry no unit in their box
// — epoch days and epoch milliseconds are both bare int64 — so a cast that
// guessed instead of reading the declared type would answer 1970 for one of
// them.
func TestDateCastOverEverySourceForm(t *testing.T) {
	ctx, db := dateCastDB(t)

	cases := []struct {
		name string
		sql  string
		want any
	}{
		{"date column", "SELECT CAST(d AS DATE) AS x FROM evt WHERE id = 1", "1996-01-10"},
		{"timestamp column floors to its day", "SELECT CAST(ts AS DATE) AS x FROM evt WHERE id = 1", "1996-01-10"},
		{"text column", "SELECT CAST(sd AS DATE) AS x FROM evt WHERE id = 1", "1996-01-10"},
		{"text literal", "SELECT CAST('1996-01-10' AS DATE) AS x", "1996-01-10"},
		{"typed literal is the same cast", "SELECT DATE '1996-01-10' AS x", "1996-01-10"},
		// A cast through the other temporal type still lands on the day.
		{"timestamp then date", "SELECT CAST(CAST(sd AS TIMESTAMP) AS DATE) AS x FROM evt WHERE id = 1", "1996-01-10"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows := dateCastRows(t, ctx, db, c.sql)
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			if got := rows[0]["x"]; got != c.want {
				t.Errorf("%s = %v (%T), want %v", c.sql, got, got, c.want)
			}
		})
	}
}

// TestDateCastRefusesTextThatNamesNoDay is the other half of the same door,
// and it is the assertion the four cells deleted from the table above used to
// make BACKWARDS.
//
// They pinned "an operand that names no instant is NULL — the TRY_CAST rule",
// which is what #340 chose when the expression layer had no per-row error
// channel. It has one (`expr.fatalEval`), the numeric casts have used it since
// #367, and #836/#840 make the temporal casts use it too: PostgreSQL 17.11
// raises 22007 for text that is not a date and 22008 for a well-formed date
// naming no day, and a NULL is indistinguishable at every consumer from a cast
// over a NULL input — `WHERE CAST(s AS DATE) IS NULL` counted every
// unparseable row as if the column had been empty.
func TestDateCastRefusesTextThatNamesNoDay(t *testing.T) {
	ctx, db := dateCastDB(t)
	for _, c := range []struct {
		name, sql  string
		state, msg string
	}{
		{"unparseable text column", "SELECT CAST(bad AS DATE) AS x FROM evt WHERE id = 1",
			"22007", `invalid input syntax for type date: "not-a-date"`},
		{"unparseable literal", "SELECT CAST('not-a-date' AS DATE) AS x",
			"22007", `invalid input syntax for type date: "not-a-date"`},
		{"empty string", "SELECT CAST(bad AS DATE) AS x FROM evt WHERE id = 3",
			"22007", `invalid input syntax for type date: ""`},
		{"a date the calendar does not have", "SELECT CAST(bad AS DATE) AS x FROM evt WHERE id = 4",
			"22008", `date/time field value out of range: "1996-13-45"`},
		{"text that is not a timestamp", "SELECT CAST('not-a-date' AS TIMESTAMP) AS x",
			"22007", `invalid input syntax for type timestamp: "not-a-date"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := db.Query(ctx, c.sql)
			if err == nil {
				t.Fatalf("%s ANSWERED; PostgreSQL 17.11 raises [%s] %s", c.sql, c.state, c.msg)
			}
			if got := sqlerr.StateOf(err); got != c.state {
				t.Errorf("%s raised SQLSTATE %s, want %s: %v", c.sql, got, c.state, err)
			}
			if !strings.Contains(err.Error(), c.msg) {
				t.Errorf("%s: %v does not carry PostgreSQL's message %q", c.sql, err, c.msg)
			}
		})
	}
}

// TestTimestampCastIsTheTimestampColumnRepresentation pins the other half of
// the pair. Leaving CAST(x AS TIMESTAMP) inert while DATE worked would be a
// new asymmetry, so it produces what a TIMESTAMP COLUMN produces: epoch
// milliseconds, boxed as a bare int64 by every compute path and rendered by
// batch.FormatTimestamp at the renderers that still hold the declared type
// (pgwire, internal/format). That is the engine-wide split for timestamps and
// this rides it rather than inventing a third form.
func TestTimestampCastIsTheTimestampColumnRepresentation(t *testing.T) {
	ctx, db := dateCastDB(t)

	// The clock survives: a TIMESTAMP is not floored to its day.
	rows := dateCastRows(t, ctx, db, "SELECT CAST(ts AS TIMESTAMP) AS x FROM evt WHERE id = 1")
	if len(rows) != 1 {
		t.Fatalf("%d rows, want 1", len(rows))
	}
	ms, ok := rows[0]["x"].(int64)
	if !ok {
		t.Fatalf("CAST(ts AS TIMESTAMP) = %v (%T), want an int64 of epoch milliseconds", rows[0]["x"], rows[0]["x"])
	}
	want := time.Date(1996, 1, 10, 13, 45, 30, 0, time.UTC).UnixMilli()
	if ms != want {
		t.Errorf("CAST(ts AS TIMESTAMP) = %d, want %d (%s)", ms, want, batch.FormatTimestamp(want))
	}
	if got := batch.FormatTimestamp(ms); got != "1996-01-10 13:45:30" {
		t.Errorf("rendered = %q, want %q", got, "1996-01-10 13:45:30")
	}

	// A whole-day source becomes midnight, matching DuckDB's
	// `1996-01-10 00:00:00`.
	rows = dateCastRows(t, ctx, db, "SELECT CAST('1996-01-10' AS TIMESTAMP) AS x")
	ms, ok = rows[0]["x"].(int64)
	if !ok {
		t.Fatalf("CAST('1996-01-10' AS TIMESTAMP) = %v (%T), want int64", rows[0]["x"], rows[0]["x"])
	}
	if got := batch.FormatTimestamp(ms); got != "1996-01-10 00:00:00" {
		t.Errorf("CAST('1996-01-10' AS TIMESTAMP) renders %q, want %q", got, "1996-01-10 00:00:00")
	}

	// And the same REFUSAL as DATE — this cell pinned a NULL until #836/#840;
	// TestDateCastRefusesTextThatNamesNoDay carries it with the code and the
	// message, beside its DATE twin.
}

// TestDateCastArithmetic exercises what a caller does with the result: it is a
// date only if dates can be subtracted from it, shifted, compared and grouped.
func TestDateCastArithmetic(t *testing.T) {
	ctx, db := dateCastDB(t)

	t.Run("minus a date is a day count", func(t *testing.T) {
		rows := dateCastRows(t, ctx, db,
			"SELECT CAST(sd AS DATE) - DATE '1996-01-10' AS n FROM evt ORDER BY id")
		want := []int64{0, 1, 2, 3, 4}
		if len(rows) != len(want) {
			t.Fatalf("%d rows, want %d", len(rows), len(want))
		}
		for i, r := range rows {
			if got := r["n"]; got != want[i] {
				t.Errorf("row %d: %v (%T), want int64 %d", i, got, got, want[i])
			}
		}
	})

	t.Run("plus an integer is a date", func(t *testing.T) {
		rows := dateCastRows(t, ctx, db,
			"SELECT CAST(sd AS DATE) + 7 AS d, 7 + CAST(sd AS DATE) AS d2 FROM evt WHERE id = 1")
		if got := rows[0]["d"]; got != "1996-01-17" {
			t.Errorf("CAST(sd AS DATE) + 7 = %v (%T), want \"1996-01-17\"", got, got)
		}
		// `n + date` is the one reversed spelling that means anything.
		if got := rows[0]["d2"]; got != "1996-01-17" {
			t.Errorf("7 + CAST(sd AS DATE) = %v (%T), want \"1996-01-17\"", got, got)
		}
	})

	t.Run("month and year boundaries", func(t *testing.T) {
		rows := dateCastRows(t, ctx, db, "SELECT CAST('1996-03-01' AS DATE) - 1 AS leap, "+
			"CAST('1997-01-01' AS DATE) - 1 AS newyear, CAST('1969-12-31' AS DATE) + 1 AS preepoch")
		// 1996 is a leap year, so the day before March 1st is the 29th.
		if got := rows[0]["leap"]; got != "1996-02-29" {
			t.Errorf("CAST('1996-03-01' AS DATE) - 1 = %v, want \"1996-02-29\"", got)
		}
		if got := rows[0]["newyear"]; got != "1996-12-31" {
			t.Errorf("CAST('1997-01-01' AS DATE) - 1 = %v, want \"1996-12-31\"", got)
		}
		// Before the epoch the day number is negative, where a truncating
		// division would round the wrong way.
		if got := rows[0]["preepoch"]; got != "1970-01-01" {
			t.Errorf("CAST('1969-12-31' AS DATE) + 1 = %v, want \"1970-01-01\"", got)
		}
	})

	t.Run("compared against another date", func(t *testing.T) {
		rows := dateCastRows(t, ctx, db,
			"SELECT COUNT(*) AS c FROM evt WHERE CAST(sd AS DATE) < DATE '1996-01-13'")
		if got := rows[0]["c"]; got != int64(3) {
			t.Errorf("count = %v, want 3", got)
		}
		rows = dateCastRows(t, ctx, db,
			"SELECT COUNT(*) AS c FROM evt WHERE CAST(sd AS DATE) = DATE '1996-01-11'")
		if got := rows[0]["c"]; got != int64(1) {
			t.Errorf("count = %v, want 1", got)
		}
		// The DATE column and the text column must answer the same question
		// the same way.
		rows = dateCastRows(t, ctx, db,
			"SELECT COUNT(*) AS c FROM evt WHERE CAST(d AS DATE) >= CAST(ts AS DATE)")
		if got := rows[0]["c"]; got != int64(5) {
			t.Errorf("count = %v, want 5", got)
		}
	})

	t.Run("used as a GROUP BY key", func(t *testing.T) {
		rows := dateCastRows(t, ctx, db,
			"SELECT CAST(ts AS DATE) AS k, COUNT(*) AS c FROM evt GROUP BY CAST(ts AS DATE) ORDER BY k")
		want := []string{"1996-01-10", "1996-01-11", "1996-01-12", "1996-01-13", "1996-01-14"}
		if len(rows) != len(want) {
			t.Fatalf("%d groups, want %d: %v", len(rows), len(want), rows)
		}
		for i, r := range rows {
			if got := r["k"]; got != want[i] {
				t.Errorf("group %d key = %v (%T), want %q", i, got, got, want[i])
			}
			if got := r["c"]; got != int64(1) {
				t.Errorf("group %d count = %v, want 1", i, got)
			}
		}
	})
}

// TestDateCastShippingLagVariesPerRow is the per-row half of the report: the
// lag between two date columns is different on different rows, and a column of
// one repeated number is the defect. Both spellings are checked — through an
// explicit CAST, which is what DuckDB requires over VARCHAR columns and what
// answered a constant 0, and bare, which answered NULL.
func TestDateCastShippingLagVariesPerRow(t *testing.T) {
	ctx, db := dateCastDB(t)

	want := []int64{0, 1, 3, 7, 30}
	for _, sql := range []string{
		"SELECT CAST(recv AS DATE) - CAST(ship AS DATE) AS lag FROM evt ORDER BY id",
		"SELECT recv - ship AS lag FROM evt ORDER BY id",
	} {
		rows := dateCastRows(t, ctx, db, sql)
		if len(rows) != len(want) {
			t.Fatalf("%s: %d rows, want %d", sql, len(rows), len(want))
		}
		distinct := map[any]bool{}
		for i, r := range rows {
			distinct[r["lag"]] = true
			if got := r["lag"]; got != want[i] {
				t.Errorf("%s row %d: lag = %v (%T), want int64 %d", sql, i, got, got, want[i])
			}
		}
		if len(distinct) == 1 {
			t.Errorf("%s: every row answered %v — a constant lag is the defect, not an answer",
				sql, rows[0]["lag"])
		}
	}
}

// TestDateCastLeavesNonTemporalArithmeticAlone is the control. The date branch
// is reached by asking whether an operand denotes an instant, so everything
// that does not has to come out exactly as before: text that is not a date, a
// number that only looks like a year, and the CASTs that were never temporal.
func TestDateCastLeavesNonTemporalArithmeticAlone(t *testing.T) {
	ctx, db := dateCastDB(t)

	cases := []struct {
		sql  string
		want any
	}{
		// An INTEGER cast is an integer operand, so the arithmetic over it is
		// integer: `SELECT pg_typeof(CAST('1996' AS INT) - 1)` is `integer` on
		// PostgreSQL 17.11 and the value is 1995, measured. This cell wanted a
		// float64 because `expr.operandIsInt` did not look through a CAST —
		// the same blind spot that made `ABS(i) * <int8 max>` answer MinInt64
		// (#849). The DOMAIN is what moved; the width is still this engine's
		// standing int4/int8 divergence.
		{"SELECT CAST('1996' AS INT) - 1 AS x", int64(1995)},
		{"SELECT CAST('12.5' AS DOUBLE) + 1 AS x", 13.5},
		{"SELECT CAST(1996 AS VARCHAR) AS x", "1996"},
		// A time-of-day literal has no column type to become, so it keeps
		// its text.
		{"SELECT TIME '10:00:00' AS x", "10:00:00"},
		// Integer arithmetic, so an integer: `SELECT pg_typeof(5 - 2)` is
		// `integer` on PostgreSQL 17. The width still differs — this engine
		// computes integer arithmetic in int64 and declares it INT64, which
		// is what `SELECT 5 - 2 FROM t` has answered since #369 — but the
		// DOMAIN is no longer float, and it no longer depends on whether the
		// expression sits at the top of a projection.
		{"SELECT 5 - 2 AS x", int64(3)},
	}
	for _, c := range cases {
		rows := dateCastRows(t, ctx, db, c.sql)
		if got := rows[0]["x"]; got != c.want {
			t.Errorf("%s = %v (%T), want %v (%T)", c.sql, got, got, c.want, c.want)
		}
	}

	// A date column feeding a string function still reads as a date STRING,
	// not as the digits of its day number (#273 through the cast).
	rows := dateCastRows(t, ctx, db, "SELECT SUBSTR(CAST(d AS DATE), 1, 4) AS y, "+
		"YEAR(CAST(d AS DATE)) AS yr, YEAR(CAST(ts AS TIMESTAMP)) AS tyr FROM evt WHERE id = 1")
	if got := rows[0]["y"]; got != "1996" {
		t.Errorf("SUBSTR(CAST(d AS DATE), 1, 4) = %v, want \"1996\"", got)
	}
	// YEAR over a cast must not read epoch days as epoch seconds (#319).
	if got := rows[0]["yr"]; got != float64(1996) {
		t.Errorf("YEAR(CAST(d AS DATE)) = %v (%T), want 1996", got, got)
	}
	if got := rows[0]["tyr"]; got != float64(1996) {
		t.Errorf("YEAR(CAST(ts AS TIMESTAMP)) = %v (%T), want 1996", got, got)
	}
}

// TestDateCastIntervalShiftUnchanged pins the neighbouring path this fix must
// not disturb. `date ± INTERVAL` keeps the rendered-string result #322/#332
// settled for it — TPC-H Q01's `DATE '1998-12-01' - INTERVAL '90' DAY` is that
// expression — and it has to keep working now that the cast beside it produces
// a number instead of the text intervalShift used to receive.
func TestDateCastIntervalShiftUnchanged(t *testing.T) {
	ctx, db := dateCastDB(t)

	rows := dateCastRows(t, ctx, db, "SELECT DATE '1998-12-01' - INTERVAL '90' DAY AS d")
	if got := rows[0]["d"]; got != "1998-09-02" {
		t.Errorf("DATE '1998-12-01' - INTERVAL '90' DAY = %v (%T), want \"1998-09-02\"", got, got)
	}
	rows = dateCastRows(t, ctx, db, "SELECT COUNT(*) AS c FROM evt WHERE sd <= DATE '1996-01-14' - INTERVAL '2' DAY")
	if got := rows[0]["c"]; got != int64(3) {
		t.Errorf("filtered count = %v, want 3", got)
	}
	rows = dateCastRows(t, ctx, db, "SELECT CAST(sd AS DATE) + INTERVAL '1' MONTH AS d FROM evt WHERE id = 1")
	if got := rows[0]["d"]; got != "1996-02-10" {
		t.Errorf("CAST(sd AS DATE) + INTERVAL '1' MONTH = %v (%T), want \"1996-02-10\"", got, got)
	}
}
