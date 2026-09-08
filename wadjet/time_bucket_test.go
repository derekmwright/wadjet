package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
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
// a range predicate on the same column and the row-group prune. That is the
// property that makes it usable as a downsampling GROUP BY key at scale, and
// this MEASURES it: the prune COUNTER, not two answers that agree.
//
// The first draft of this gate compared the row counts of a bucketed and an
// unbucketed query and called it a prune test. It was not one — those counts
// agree whether or not a single row group was skipped — and its fixture held
// ONE row group, so nothing could be pruned at all (#965 round 2, P1).
// `scan.StatsPrunedRowGroupsSnapshot` reports what the static min/max prune
// decided; the DELTA across one query is that query's prune.
//
// Over the type-matrix fixture: 5000 rows in five row groups, c_ts spanning
// 2023-11-14 22:13 .. 2023-11-18 10:55, with the threshold in the MIDDLE so
// the predicate crosses group bounds instead of matching everything. Two of
// the five row groups fall away, bare and bucketed alike.
//
// A PRE-EXISTING GAP, measured here and pinned below: the threshold has to be
// written as an epoch-millisecond literal for the prune to engage at all.
// `c_ts >= TIMESTAMP '2030-01-01'` prunes NOTHING — not even when it is above
// every row's maximum — and neither does the quoted spelling or `c_date >=
// DATE '…'`, while the same instant as a bare integer prunes all five. So a
// TYPED TEMPORAL LITERAL never reaches the row-group prune. That is not this
// arc's mechanism and it costs no rows, only reads; it is stated here because
// it is what made the original claim untestable — a timestamp predicate
// written the way anyone writes one never reached the prune for time_bucket to
// stand in front of.
func TestTimeBucketDoesNotBlockTheRowGroupPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate builds the 5000-row type-matrix fixture")
	}
	ctx := context.Background()
	db := tmOpen(t)
	prev := scan.StatsPrune.Set(true)
	t.Cleanup(func() { scan.StatsPrune.Set(prev) })

	// rowsAndPrune runs one query and reports (rows counted, row groups the
	// static prune decided to skip). The counter is process-wide and never
	// resets, so it is read as a delta — and this package's tests do not run
	// in parallel, which is what makes the delta this query's.
	rowsAndPrune := func(t *testing.T, sql string) (int64, int64) {
		t.Helper()
		before := scan.StatsPrunedRowGroupsSnapshot()
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%v\n  SQL: %s", err, sql)
		}
		pruned := scan.StatsPrunedRowGroupsSnapshot() - before
		var total int64
		for _, r := range res.Rows {
			n, _ := tmAsInt64(r["n"])
			total += n
		}
		return total, pruned
	}

	mid := time.Date(2023, 11, 16, 16, 35, 0, 0, time.UTC).UnixMilli()
	bare := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE c_ts >= %d", typematrix.Table, mid)
	bucketed := fmt.Sprintf(`SELECT SUM(n) AS n FROM (
	   SELECT time_bucket(INTERVAL '1' DAY, c_ts) AS b, COUNT(*) AS n
	   FROM %s WHERE c_ts >= %d GROUP BY 1) t`, typematrix.Table, mid)

	bareRows, barePruned := rowsAndPrune(t, bare)
	bucketRows, bucketPruned := rowsAndPrune(t, bucketed)

	// The fixture has to be able to prove something before the comparison
	// means anything: a predicate that prunes nothing makes "the bucket did
	// not block the prune" vacuously true, which is exactly how the first
	// draft passed.
	if barePruned == 0 {
		t.Fatalf("the BARE predicate pruned no row group, so this gate proves nothing.\n"+
			"  SQL: %s\nEither the threshold no longer splits the fixture's c_ts range "+
			"or the static min/max prune stopped engaging for TIMESTAMP.", bare)
	}
	if bucketPruned != barePruned {
		t.Errorf("TIME_BUCKET STOOD BETWEEN THE PREDICATE AND THE PRUNE\n"+
			"  bare predicate pruned %d row groups\n  with the bucket projection: %d\n"+
			"  SQL: %s\nThe predicate pushed to the scan should still be the bare "+
			"`c_ts >= …` structuredConjuncts extracted; a projection over the column "+
			"is not supposed to reach it.", barePruned, bucketPruned, bucketed)
	}
	if bareRows != bucketRows || bareRows == 0 {
		t.Errorf("the two spellings kept different rows: bare %d, bucketed %d",
			bareRows, bucketRows)
	}

	// The same property with the predicate on ANOTHER column, so the bucket is
	// a pure projection rather than a projection over the filtered column.
	idBare := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE id >= 2500", typematrix.Table)
	idBucketed := fmt.Sprintf(`SELECT SUM(n) AS n FROM (
	   SELECT time_bucket(INTERVAL '1' DAY, c_ts) AS b, COUNT(*) AS n
	   FROM %s WHERE id >= 2500 GROUP BY 1) t`, typematrix.Table)
	idBareRows, idBarePruned := rowsAndPrune(t, idBare)
	idBucketRows, idBucketPruned := rowsAndPrune(t, idBucketed)
	if idBarePruned == 0 {
		t.Fatalf("the id predicate pruned nothing; the fixture changed under this gate")
	}
	if idBucketPruned != idBarePruned || idBareRows != idBucketRows {
		t.Errorf("a bucket projection changed a prune on ANOTHER column: "+
			"pruned %d vs %d, rows %d vs %d",
			idBarePruned, idBucketPruned, idBareRows, idBucketRows)
	}

	// The CONTROL. A query with no predicate must prune nothing — otherwise
	// the counter is measuring something else and every number above is noise.
	if _, pruned := rowsAndPrune(t, fmt.Sprintf(
		"SELECT COUNT(*) AS n FROM %s", typematrix.Table)); pruned != 0 {
		t.Errorf("an unfiltered scan pruned %d row groups — the counter is not "+
			"measuring the static predicate prune", pruned)
	}

	// THE PINS. Each of these prunes NOTHING today; each is a read cost and
	// never a wrong row, and each has to be moved deliberately.
	for _, pin := range []struct {
		name, sql, why string
	}{
		// A TYPED TEMPORAL LITERAL does not reach the prune, even above every
		// bound. Measured: the same instant as a bare integer prunes all five
		// row groups.
		{"typed_timestamp_literal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s WHERE c_ts >= TIMESTAMP '2030-01-01 00:00:00'",
			typematrix.Table),
			"a TIMESTAMP literal above every row still reads every row group"},
		{"quoted_timestamp_literal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s WHERE c_ts >= '2030-01-01 00:00:00'",
			typematrix.Table), "the quoted spelling, same gap"},
		{"typed_date_literal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s WHERE c_date >= DATE '2030-01-01'",
			typematrix.Table), "DATE has the same gap"},
		// The bucket used as the PREDICATE's own column. structuredConjuncts
		// requires a bare column reference, so nothing is pushed. A monotone-
		// function rewrite taught to that layer would start pruning here — and
		// `TimeBucketInThePredicate` in TestTypeMatrixPruningNeverChanges
		// TheAnswer is the gate that would have to prove the answer did not
		// change with it.
		{"bucket_in_the_predicate", fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s
			 WHERE time_bucket(INTERVAL '1' DAY, c_ts) >= TIMESTAMP '2023-11-16 00:00:00'`,
			typematrix.Table), "a monotone rewrite would push this down"},
	} {
		if _, pruned := rowsAndPrune(t, pin.sql); pruned != 0 {
			t.Errorf("%s now prunes %d row groups (%s).\nThat is an improvement, not a "+
				"failure — move this pin, and check the matching cell in "+
				"TestTypeMatrixPruningNeverChangesTheAnswer still agrees across both "+
				"prune settings.", pin.name, pruned, pin.why)
		}
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
