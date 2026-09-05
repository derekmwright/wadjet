package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Issue #319 end to end: date-part functions over a temporal column answered
// off 1970 wherever the SCALAR expression path ran, which is every WHERE
// clause and — on the distributed DAG — the SELECT projection too. The unit
// tests in internal/engine/expr pin the expression layer; this pins the query
// layer, where the wrong answer was silent (no error, no null) and a
// year-grouped report collapsed into a single bogus bucket.
//
// Three column shapes, because all three occur in real catalogs and they
// exercise three different storage forms: SF100 TPC-H types l_shipdate as a
// real DATE (epoch days in Int32Data), event tables use TIMESTAMP (epoch
// milliseconds in Int64Data), and the SF0.01 fixture types dates as text.

func datePartDB(t *testing.T) (context.Context, *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "d", Type: parquet.TypeDate},
			{Name: "ts", Type: parquet.TypeTimestamp},
			{Name: "s", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "shipments", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{
			"id": int64(1), "d": "1996-03-13",
			"ts": time.Date(1996, 3, 13, 14, 25, 36, 0, time.UTC),
			"s":  "1996-03-13T14:25:36Z",
		},
		{
			"id": int64(2), "d": "1996-11-02",
			"ts": time.Date(1996, 11, 2, 3, 4, 5, 0, time.UTC),
			"s":  "1996-11-02T03:04:05Z",
		},
		{
			"id": int64(3), "d": "2003-11-07",
			"ts": time.Date(2003, 11, 7, 1, 2, 3, 0, time.UTC),
			"s":  "2003-11-07T01:02:03Z",
		},
	}
	ing := db.NewIngester("shipments", schema, nil, ingest.Config{MaxBufferRows: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

func datePartQuery(t *testing.T, ctx context.Context, db *wadjet.DB, sql string) []map[string]any {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return res.Rows
}

func wantNum(t *testing.T, row map[string]any, col string, want float64) {
	t.Helper()
	got, ok := toNumber(row[col])
	if !ok || got != want {
		t.Errorf("column %q: got %v (%T), want %v", col, row[col], row[col], want)
	}
}

func toNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// TestDatePartsOverDateColumn covers the whole family over a real DATE
// column. Every one of these answered off 1970-01-01 before the fix: year
// 1970, month 1, day 1, quarter 1, week 1, day_of_week 4 (a Thursday),
// day_of_year 1.
func TestDatePartsOverDateColumn(t *testing.T) {
	ctx, db := datePartDB(t)
	rows := datePartQuery(t, ctx, db, `
		SELECT year(d) AS y, quarter(d) AS q, month(d) AS m, week(d) AS w,
		       day(d) AS dd, day_of_week(d) AS dw, day_of_year(d) AS dy,
		       hour(d) AS h, minute(d) AS mi, second(d) AS se
		FROM shipments WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	r := rows[0]
	// 1996-03-13 is a Wednesday, ISO week 11, day 73 of the year.
	wantNum(t, r, "y", 1996)
	wantNum(t, r, "q", 1)
	wantNum(t, r, "m", 3)
	wantNum(t, r, "w", 11)
	wantNum(t, r, "dd", 13)
	wantNum(t, r, "dw", 3)
	wantNum(t, r, "dy", 73)
	// A DATE has no time of day.
	wantNum(t, r, "h", 0)
	wantNum(t, r, "mi", 0)
	wantNum(t, r, "se", 0)
}

// TestDatePartsOverTimestampColumn: a TIMESTAMP column stores epoch
// MILLISECONDS, so reading its raw number as seconds put every row in year
// 28167 — wrong in a way that no null or error announced.
func TestDatePartsOverTimestampColumn(t *testing.T) {
	ctx, db := datePartDB(t)
	rows := datePartQuery(t, ctx, db, `
		SELECT year(ts) AS y, month(ts) AS m, day(ts) AS dd,
		       hour(ts) AS h, minute(ts) AS mi, second(ts) AS se
		FROM shipments WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	r := rows[0]
	wantNum(t, r, "y", 1996)
	wantNum(t, r, "m", 3)
	wantNum(t, r, "dd", 13)
	wantNum(t, r, "h", 14)
	wantNum(t, r, "mi", 25)
	wantNum(t, r, "se", 36)
}

// TestDatePartsAgreeAcrossColumnShapes: the same instant stored three ways
// must answer the same question. A text date was always right, which is
// exactly why the DATE and TIMESTAMP breakage survived — the fixtures that
// would have caught it stored dates as strings.
func TestDatePartsAgreeAcrossColumnShapes(t *testing.T) {
	ctx, db := datePartDB(t)
	for _, part := range []string{"year", "month", "day", "quarter", "week", "day_of_week", "day_of_year"} {
		t.Run(part, func(t *testing.T) {
			rows := datePartQuery(t, ctx, db,
				"SELECT "+part+"(d) AS a, "+part+"(ts) AS b, "+part+"(s) AS c FROM shipments ORDER BY id")
			if len(rows) != 3 {
				t.Fatalf("got %d rows, want 3", len(rows))
			}
			for i, r := range rows {
				a, aok := toNumber(r["a"])
				b, bok := toNumber(r["b"])
				c, cok := toNumber(r["c"])
				if !aok || !bok || !cok {
					t.Fatalf("row %d: non-numeric result %v / %v / %v", i, r["a"], r["b"], r["c"])
				}
				if a != b || a != c {
					t.Errorf("row %d: %s(date)=%v %s(timestamp)=%v %s(text)=%v — all three must agree",
						i, part, a, part, b, part, c)
				}
			}
		})
	}
}

// TestDatePartFilterOnDateColumn is the sharpest end-to-end assertion: WHERE
// is evaluated per row through the scalar path with no vectorized kernel
// anywhere in reach, so `WHERE year(d) = 1996` matched nothing at all before
// the fix — a silent empty result, not an error.
func TestDatePartFilterOnDateColumn(t *testing.T) {
	ctx, db := datePartDB(t)
	for _, tc := range []struct {
		where string
		want  int
	}{
		{"year(d) = 1996", 2},
		{"year(d) = 2003", 1},
		{"year(d) = 1970", 0},
		{"month(d) = 11", 2},
		{"quarter(d) = 4", 2},
		{"year(ts) = 1996", 2},
		{"year(s) = 1996", 2},
	} {
		t.Run(tc.where, func(t *testing.T) {
			rows := datePartQuery(t, ctx, db, "SELECT id FROM shipments WHERE "+tc.where)
			if len(rows) != tc.want {
				t.Errorf("WHERE %s: got %d rows, want %d", tc.where, len(rows), tc.want)
			}
		})
	}
}

// TestDatePartGroupByOnDateColumn: the reported symptom. Grouping by the year
// of a DATE column produced ONE bucket labelled 1970 holding every row.
func TestDatePartGroupByOnDateColumn(t *testing.T) {
	ctx, db := datePartDB(t)
	rows := datePartQuery(t, ctx, db,
		"SELECT EXTRACT(YEAR FROM d) AS y, COUNT(*) AS n FROM shipments GROUP BY 1 ORDER BY 1")
	if len(rows) != 2 {
		t.Fatalf("got %d groups, want 2 (1996 and 2003)", len(rows))
	}
	wantNum(t, rows[0], "y", 1996)
	wantNum(t, rows[0], "n", 2)
	wantNum(t, rows[1], "y", 2003)
	wantNum(t, rows[1], "n", 1)
}

// TestExtractEpochOverColumns: EXTRACT(EPOCH ...) must be real seconds since
// 1970, derived from the resolved instant. It used to hand back the column's
// raw stored number — 9568 for a 1996 date (its epoch-day count), and
// 826727136000 for the timestamp (its epoch-millisecond count).
func TestExtractEpochOverColumns(t *testing.T) {
	ctx, db := datePartDB(t)
	rows := datePartQuery(t, ctx, db, `
		SELECT EXTRACT(EPOCH FROM d) AS ed, EXTRACT(EPOCH FROM ts) AS ets,
		       EXTRACT(EPOCH FROM s) AS es
		FROM shipments WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	// 1996-03-13T00:00:00Z and 1996-03-13T14:25:36Z.
	wantNum(t, rows[0], "ed", 826675200)
	wantNum(t, rows[0], "ets", 826727136)
	wantNum(t, rows[0], "es", 826727136)
}

// TestDateTruncOverColumns: date_trunc has no vectorized kernel, so it ran
// scalar on every path and truncated 1970 for a DATE column regardless.
func TestDateTruncOverColumns(t *testing.T) {
	ctx, db := datePartDB(t)
	rows := datePartQuery(t, ctx, db, `
		SELECT date_trunc('month', d) AS md, date_trunc('year', ts) AS yts,
		       last_day_of_month(d) AS ld
		FROM shipments WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	r := rows[0]
	// date_trunc renders the instant the ONE way this engine renders one —
	// batch.FormatTimestamp, the same text the TIMESTAMP column it read
	// produces and the same text PostgreSQL produces. It answered RFC3339
	// until #544's second pass, which is what these two cells recorded:
	// `SELECT date_trunc('year', TIMESTAMP '1996-03-13 14:25:36')::text` is
	// `1996-01-01 00:00:00` on 17.11, measured.
	//
	// Since #868 date_trunc DECLARES timestamp (OID 1114) as it does on the
	// server, so the embedded door hands back the instant — the epoch
	// milliseconds a TIMESTAMP column hands back — and the rendering happens
	// at the output. dtRendered renders whichever box arrives, so these cells
	// assert the TEXT a client reads on either side of that change.
	//
	// `ld` is unchanged: last_day_of_month answers a calendar DATE, which has
	// its own rendering and always had this one.
	for col, want := range map[string]string{
		"md":  "1996-03-01 00:00:00",
		"yts": "1996-01-01 00:00:00",
		"ld":  "1996-03-31",
	} {
		if got := dtRendered(r[col]); got != want {
			t.Errorf("column %q: got %v, want %q", col, r[col], want)
		}
	}
}

// dtRendered renders whatever box a timestamp-valued expression handed back as
// the text a client reads: epoch milliseconds through batch.FormatTimestamp,
// which is what a TIMESTAMP-declared column produces through the embedded
// door, or the text itself for an expression that is text on purpose.
func dtRendered(v any) string {
	switch tv := v.(type) {
	case int64:
		return batch.FormatTimestamp(tv)
	case string:
		return tv
	}
	return fmt.Sprint(v)
}
