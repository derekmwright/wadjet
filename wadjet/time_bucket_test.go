package wadjet

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
)

// TIME_BUCKET ANSWERS WHAT date_bin ANSWERS (#965).
//
// Every expectation in this file was measured on live PostgreSQL 17.11 on
// 2026-09-08 with `date_bin(stride, source, origin)`, which is the same
// function under another name. The cases are the ones where an
// implementation can be wrong while looking right:
//
//   - a PRE-EPOCH source, where truncating division names the bucket ABOVE
//     the row and every bar before 1970 mixes two buckets;
//   - a source exactly ON a boundary, which belongs to the bucket it OPENS;
//   - an ORIGIN that is not midnight and not before the source;
//   - a DATE source, which PostgreSQL casts to timestamp;
//   - the two refusals, whose SQLSTATEs are PostgreSQL's own (0A000 for a
//     calendar stride, 22008 for a non-positive one).
//
// The DECLARED type is asserted beside every value: a bucket that answers the
// right instant under OID 25 is a bucket every client reads as text, which is
// the defect #868 closed for date_trunc and which a new timestamp-valued
// function can reintroduce for free.
func TestTimeBucketAnswersWhatDateBinAnswers(t *testing.T) {
	ctx := context.Background()
	db := tbOpen(t, ctx)

	ms := func(text string) int64 { return tbMillis(t, text) }

	cases := []struct {
		name string
		sql  string
		want int64
		// pg is the exact date_bin call and its measured answer.
		pg string
	}{
		{"quarter_hour_interior",
			`SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2001-01-01') AS b`,
			ms("2020-02-11 15:30:00"),
			`date_bin('15 min','2020-02-11 15:44:17','2001-01-01') = 2020-02-11 15:30:00`},
		{"boundary_belongs_to_the_bucket_it_opens",
			`SELECT time_bucket(INTERVAL '15' MINUTE, TIMESTAMP '2020-02-11 15:45:00', TIMESTAMP '2001-01-01') AS b`,
			ms("2020-02-11 15:45:00"),
			`date_bin('15 min','2020-02-11 15:45:00','2001-01-01') = 2020-02-11 15:45:00`},
		{"pre_epoch_floors_toward_minus_infinity",
			`SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '1969-07-20 20:17:40') AS b`,
			ms("1969-07-20 20:00:00"),
			`date_bin('1 hour','1969-07-20 20:17:40','1970-01-01') = 1969-07-20 20:00:00`},
		{"pre_epoch_day_interior",
			`SELECT time_bucket(INTERVAL '1' DAY, TIMESTAMP '1969-12-30 23:59:59') AS b`,
			ms("1969-12-30 00:00:00"),
			`date_bin('1 day','1969-12-30 23:59:59.999999',epoch) = 1969-12-30 00:00:00`},
		{"pre_epoch_day_boundary",
			`SELECT time_bucket(INTERVAL '1' DAY, TIMESTAMP '1969-12-31 00:00:00') AS b`,
			ms("1969-12-31 00:00:00"),
			`date_bin('1 day','1969-12-31 00:00:00',epoch) = 1969-12-31 00:00:00`},
		{"far_past",
			`SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '1900-01-01 00:00:01') AS b`,
			ms("1900-01-01 00:00:00"),
			`date_bin('1 hour','1900-01-01 00:00:01',epoch) = 1900-01-01 00:00:00`},
		{"origin_after_the_source_still_aligns",
			`SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2020-02-11 15:44:17', TIMESTAMP '2030-01-01 00:30:00') AS b`,
			ms("2020-02-11 15:30:00"),
			`date_bin('1 hour','2020-02-11 15:44:17','2030-01-01 00:30:00') = 2020-02-11 15:30:00`},
		{"default_origin_is_the_epoch",
			`SELECT time_bucket(INTERVAL '1' DAY, TIMESTAMP '2020-02-11 15:44:17') AS b`,
			ms("2020-02-11 00:00:00"),
			`date_bin('1 day','2020-02-11 15:44:17','1970-01-01') = 2020-02-11 00:00:00`},
		{"week_stride",
			`SELECT time_bucket(INTERVAL '1' WEEK, TIMESTAMP '2020-02-11 15:44:17') AS b`,
			ms("2020-02-06 00:00:00"),
			`date_bin('7 days','2020-02-11 15:44:17','1970-01-01') = 2020-02-06 00:00:00`},
		{"huge_stride_is_one_bucket",
			`SELECT time_bucket(INTERVAL '100000' DAY, TIMESTAMP '2020-02-11 15:44:17') AS b`,
			ms("1970-01-01 00:00:00"),
			`date_bin('100000 days','2020-02-11 15:44:17','1970-01-01') = 1970-01-01 00:00:00`},
		{"second_stride",
			`SELECT time_bucket(INTERVAL '30' SECOND, TIMESTAMP '2020-02-11 15:44:17') AS b`,
			ms("2020-02-11 15:44:00"),
			`date_bin('30 seconds','2020-02-11 15:44:17','1970-01-01') = 2020-02-11 15:44:00`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%v\n  PostgreSQL 17: %s\n  SQL: %s", err, tc.pg, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(res.Rows))
			}
			got, ok := res.Rows[0]["b"].(int64)
			if !ok {
				t.Fatalf("b = %v (%T), want an int64 epoch-millisecond TIMESTAMP\n  PostgreSQL 17: %s",
					res.Rows[0]["b"], res.Rows[0]["b"], tc.pg)
			}
			if got != tc.want {
				t.Errorf("time_bucket = %s, want %s\n  PostgreSQL 17: %s\n  SQL: %s",
					batch.FormatTimestamp(got), batch.FormatTimestamp(tc.want), tc.pg, tc.sql)
			}
			// The DECLARATION, beside the value: a right instant under the
			// wrong OID is a bucket every client reads as text.
			if len(res.ColumnMetas) != 1 || res.ColumnMetas[0].TypeName != "TIMESTAMP" {
				t.Errorf("declared %v, want TIMESTAMP (PostgreSQL's date_bin returns timestamp, OID 1114)",
					res.ColumnMetas)
			}
		})
	}
}

// A NULL in any argument is NULL, and a DATE source is cast to an instant —
// both PostgreSQL's rules for date_bin, measured.
func TestTimeBucketNullAndDateSource(t *testing.T) {
	ctx := context.Background()
	db := tbOpen(t, ctx)

	for _, tc := range []struct {
		name, sql string
	}{
		{"null_source", `SELECT time_bucket(INTERVAL '1' HOUR, NULL) AS b`},
		{"null_origin", `SELECT time_bucket(INTERVAL '1' HOUR, TIMESTAMP '2020-01-01', NULL) AS b`},
		{"null_ts_column", `SELECT time_bucket(INTERVAL '1' HOUR, ts) AS b FROM tbt WHERE id = 4`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if len(res.Rows) != 1 || res.Rows[0]["b"] != nil {
				t.Errorf("got %v, want a single NULL (PostgreSQL: date_bin is strict in every argument)",
					res.Rows)
			}
		})
	}

	// A DATE column is the day's UTC midnight, which is what PostgreSQL's
	// implicit date->timestamp cast makes it. Measured:
	//   date_bin('1 day', DATE '2020-02-11', epoch) = 2020-02-11 00:00:00
	res, err := db.Query(ctx, `SELECT time_bucket(INTERVAL '1' DAY, dt) AS b FROM tbt WHERE id = 1`)
	if err != nil {
		t.Fatalf("date source: %v", err)
	}
	want := tbMillis(t, "2020-02-11 00:00:00")
	if got, ok := res.Rows[0]["b"].(int64); !ok || got != want {
		t.Errorf("time_bucket over a DATE column = %v, want %s (PostgreSQL casts date to timestamp)",
			res.Rows[0]["b"], batch.FormatTimestamp(want))
	}
}

// The two refusals carry PostgreSQL's own SQLSTATEs and sentences, measured
// on 17.11. A calendar stride has no fixed width and a non-positive one has no
// buckets; answering either with a number is a bar nobody can check.
func TestTimeBucketRefusesWhatDateBinRefuses(t *testing.T) {
	ctx := context.Background()
	db := tbOpen(t, ctx)

	for _, tc := range []struct {
		name, sql, state, msg string
	}{
		{"month_stride", `SELECT time_bucket(INTERVAL '1' MONTH, ts) FROM tbt`, "0A000",
			"timestamps cannot be binned into intervals containing months or years"},
		{"year_stride", `SELECT time_bucket(INTERVAL '1' YEAR, ts) FROM tbt`, "0A000",
			"timestamps cannot be binned into intervals containing months or years"},
		{"zero_stride", `SELECT time_bucket(INTERVAL '0' SECOND, ts) FROM tbt`, "22008",
			"stride must be greater than zero"},
		{"negative_stride", `SELECT time_bucket(INTERVAL '-1' HOUR, ts) FROM tbt`, "22008",
			"stride must be greater than zero"},
		// Not PostgreSQL's: wadjet's stride must be an INTERVAL literal,
		// because the accepted grammar is the SQL parser's and a second
		// interval parser in the expression layer would agree with it only by
		// inspection. Refused loudly, with the spelling that works.
		{"string_stride", `SELECT time_bucket('15 minutes', ts) FROM tbt`, "42804",
			"the stride must be an INTERVAL literal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("ANSWERED; want %s %s", tc.state, tc.msg)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("%v\n  want a message containing %q", err, tc.msg)
			}
			if st := sqlerr.StateOf(err); st != tc.state {
				t.Errorf("SQLSTATE %q, want %q (PostgreSQL 17's own for this refusal)", st, tc.state)
			}
		})
	}
}

// time_bucket is a MONOTONE function of its source, so it never stands between
// a range predicate on the same column and the row-group prune. This asserts
// the property that makes it usable as a downsampling GROUP BY key at scale:
// `WHERE ts >= …` beside `GROUP BY time_bucket(…)` prunes exactly as it does
// without the projection.
func TestTimeBucketDoesNotBlockTheRowGroupPrune(t *testing.T) {
	ctx := context.Background()
	db := tbOpen(t, ctx)

	plain, err := db.Query(ctx, `SELECT COUNT(*) AS n FROM tbt WHERE ts >= TIMESTAMP '2020-02-11 00:00:00'`)
	if err != nil {
		t.Fatal(err)
	}
	bucketed, err := db.Query(ctx, `SELECT time_bucket(INTERVAL '1' DAY, ts) AS b, COUNT(*) AS n
	                                FROM tbt WHERE ts >= TIMESTAMP '2020-02-11 00:00:00' GROUP BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, r := range bucketed.Rows {
		n, _ := r["n"].(int64)
		total += n
	}
	want, _ := plain.Rows[0]["n"].(int64)
	if total != want {
		t.Errorf("bucketed rows = %d, plain predicate rows = %d — the predicate and the "+
			"bucket projection disagree about which rows survive", total, want)
	}
	if want == 0 {
		t.Fatal("fixture: the predicate matched nothing, so this proves nothing")
	}
}

func tbOpen(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "dt", Type: parquet.TypeDate, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "tbt", schema, nil); err != nil {
		t.Fatal(err)
	}
	ms := func(s string) int64 { return tbMillis(t, s) }
	day := func(s string) int32 { return int32(tbMillis(t, s+" 00:00:00") / 86_400_000) }
	rows := []map[string]any{
		{"id": int32(1), "ts": ms("2020-02-11 15:44:17"), "dt": day("2020-02-11")},
		{"id": int32(2), "ts": ms("2020-02-11 15:45:00"), "dt": day("2020-02-11")},
		{"id": int32(3), "ts": ms("1969-07-20 20:17:40"), "dt": day("1969-07-20")},
		{"id": int32(4), "ts": nil, "dt": nil},
	}
	ing := db.NewIngester("tbt", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// tbMillis is the epoch-millisecond value of a UTC wall-clock instant, so every
// expectation above reads as the timestamp PostgreSQL printed.
func tbMillis(t *testing.T, text string) int64 {
	t.Helper()
	ts, err := time.Parse("2006-01-02 15:04:05", text)
	if err != nil {
		t.Fatalf("fixture: cannot parse %q: %v", text, err)
	}
	return ts.UTC().UnixMilli()
}
