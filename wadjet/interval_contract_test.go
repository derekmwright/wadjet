// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestIntervalAnswersNothingBaseAnsweredRight is arc VL round 5's INTERVAL
// contract (round-4 review B1, B4, P1, P3). INTERVAL is not one of the arc's
// issues, so every cell here is either base's own answer where base was
// PostgreSQL's, or PostgreSQL's where base was wrong:
//
//   - B4: a text CAST to INTERVAL of PostgreSQL's own interval output
//     (`'1 day 02:00:00'`, `'00:30:00'`, `'1 year 2 mons'`) answered that text
//     at base and 22007 at round 4 — it prints as base printed it again (the
//     single-unit IntervalValue cannot hold it: IntervalValue.text), and any
//     arithmetic over it is 0A000 rather than the +1 ms base added;
//   - P3: IntervalValue prints PostgreSQL's `postgres` style — `-1 days`
//     (plural but at exactly 1), twelve months a year;
//   - B1: an INTERVAL's clock part is summed in checked seconds, not in a
//     time.Duration that wraps past ±292 years, so `+ INTERVAL '3000000
//     hours'` is year 2368 and `'100000000000 hours'` is 22008 on every door
//     with nothing stored (PostgreSQL says 22015 at the literal);
//   - P1: an INTERVAL value assigned to a column (no INTERVAL column type
//     exists) is PostgreSQL's: its text into TEXT, 42804 into anything else —
//     not a code-less box-validation error.
//
// PostgreSQL 17.11 values (wadjet-pg-vl), measured by the round-4 review's
// iv / iv2 / p1 / r1 scripts.
func TestIntervalAnswersNothingBaseAnsweredRight(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "ivc", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO ivc (id, s, d, ts) VALUES (1, '1 day 02:00:00', '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ sql, want, state string }{
		// B4 — PostgreSQL's own output spellings, as base answered them.
		{"SELECT CAST('1 day 02:00:00' AS INTERVAL) AS v", "1 day 02:00:00", ""},
		{"SELECT CAST('00:30:00' AS INTERVAL) AS v", "00:30:00", ""},
		{"SELECT CAST('02:00:00' AS INTERVAL) AS v", "02:00:00", ""},
		{"SELECT CAST('1 year 2 mons' AS INTERVAL) AS v", "1 year 2 mons", ""},
		{"SELECT CAST('10 days 10:00:00' AS INTERVAL) AS v", "10 days 10:00:00", ""},
		{"SELECT '1 day 02:00:00'::interval AS v", "1 day 02:00:00", ""},
		{"SELECT CAST(CAST('1 day 02:00:00' AS INTERVAL) AS TEXT) AS v", "1 day 02:00:00", ""},
		{"SELECT CAST(s AS INTERVAL) AS v FROM ivc", "1 day 02:00:00", ""},
		{"SELECT id AS v FROM ivc WHERE CAST(s AS INTERVAL) IS NOT NULL", "1", ""},
		// ... and applying one is loud, never the +1 ms base added.
		{"SELECT ts + CAST('1 day 02:00:00' AS INTERVAL) AS v FROM ivc", "", "0A000"},
		{"SELECT ts + CAST(s AS INTERVAL) AS v FROM ivc", "", "0A000"},
		// Text PostgreSQL refuses is its 22007.
		{"SELECT CAST('abc' AS INTERVAL) AS v", "", "22007"},
		{"SELECT CAST('1 fortnight' AS INTERVAL) AS v", "", "22007"},
		// P3 — PostgreSQL's interval output.
		{"SELECT CAST('-1 days' AS INTERVAL) AS v", "-1 days", ""},
		{"SELECT INTERVAL '-1 day' AS v", "-1 days", ""},
		{"SELECT INTERVAL '14 months' AS v", "1 year 2 mons", ""},
		{"SELECT INTERVAL '25 hours' AS v", "25:00:00", ""},
		{"SELECT INTERVAL '90 minutes' AS v", "01:30:00", ""},
		{"SELECT CAST('1 week' AS INTERVAL) AS v", "7 days", ""},
		{"SELECT CAST('0' AS INTERVAL) AS v", "00:00:00", ""},
		// B1 — the clock part past ±292 years of nanoseconds.
		{"SELECT CAST(TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '3000000 hours' AS TEXT) AS v", "2368-05-29 00:00:00", ""},
		{"SELECT CAST(TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '10000000000 seconds' AS TEXT) AS v", "2343-01-21 17:46:40", ""},
		{"SELECT CAST(TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '200000000 minutes' AS TEXT) AS v", "2406-06-07 21:20:00", ""},
		{"SELECT CAST(TIMESTAMP '2026-03-03 00:00:00' - INTERVAL '3000000 hours' AS TEXT) AS v", "1683-12-06 00:00:00", ""},
		{"SELECT CAST(DATE '2026-03-03' + INTERVAL '3000000 hours' AS TEXT) AS v", "2368-05-29 00:00:00", ""},
		{"SELECT CAST(ts + INTERVAL '100000000000 seconds' AS TEXT) AS v FROM ivc", "5195-01-16 20:07:10", ""},
		{"SELECT ts + INTERVAL '100000000000 hours' AS v FROM ivc", "", "22008"},
		{"SELECT d + INTERVAL '100000000000 hours' AS v FROM ivc", "", "22008"},
		{"SELECT date_add(ts, INTERVAL '100000000000 hours') AS v FROM ivc", "", "22008"},
		{"SELECT id AS v FROM ivc WHERE ts + INTERVAL '100000000000 hours' > ts", "", "22008"},
	} {
		res, err := db.Query(ctx, tc.sql)
		if tc.state != "" {
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s: got %q (%v), want %s", tc.sql, got, err, tc.state)
			}
			continue
		}
		if err != nil || len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["v"]) != tc.want {
			t.Errorf("%s = %v (%v), want %s", tc.sql, res, err, tc.want)
		}
	}
	// B1 on the write doors, and P1: nothing stored.
	for _, tc := range []struct{ sql, state string }{
		{"INSERT INTO ivc (id, ts) VALUES (2, TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '100000000000 hours')", "22008"},
		{"INSERT INTO ivc (id, ts) SELECT 3, ts + INTERVAL '100000000000 hours' FROM ivc WHERE id = 1", "22008"},
		{"UPDATE ivc SET ts = ts + INTERVAL '100000000000 hours' WHERE id = 1", "22008"},
		{"CREATE TABLE ivc_ctas AS SELECT ts + INTERVAL '100000000000 hours' AS v FROM ivc", "22008"},
		{"INSERT INTO ivc (id, n) VALUES (7, INTERVAL '1 day')", "42804"},
		{"INSERT INTO ivc (id, n) VALUES (7, CAST('90 minutes' AS INTERVAL))", "42804"},
		{"UPDATE ivc SET n = INTERVAL '1 day' WHERE id = 1", "42804"},
		{"INSERT INTO ivc (id, d) VALUES (7, INTERVAL '1 day')", "42804"},
	} {
		_, err := db.Execute(ctx, tc.sql)
		if got := sqlerr.StateOf(err); got != tc.state {
			t.Errorf("%s: got %q (%v), want %s", tc.sql, got, err, tc.state)
		}
	}
	res, err := db.Query(ctx, "SELECT id, CAST(ts AS TEXT) AS t, s, n FROM ivc ORDER BY id")
	if err != nil || len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["t"]) != "2026-03-03 10:20:30" ||
		fmt.Sprint(res.Rows[0]["s"]) != "1 day 02:00:00" || res.Rows[0]["n"] != nil {
		t.Errorf("after the refused writes: %v (%v), want the one seeded row unchanged", res, err)
	}
	// An INTERVAL into TEXT is its PostgreSQL text on every door (PostgreSQL
	// 17.11: `1 day`, `01:30:00`).
	for i, st := range []string{
		"INSERT INTO ivc (id, s) VALUES (10, INTERVAL '1 day')",
		"INSERT INTO ivc (id, s) SELECT 11, INTERVAL '1 day'",
		"INSERT INTO ivc (id, s) VALUES (12, CAST('90 minutes' AS INTERVAL))",
		"INSERT INTO ivc (id, s) VALUES (13, CAST('1 year 2 mons' AS INTERVAL))",
	} {
		if _, err := db.Execute(ctx, st); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		want := []string{"1 day", "1 day", "01:30:00", "1 year 2 mons"}[i]
		res, err := db.Query(ctx, fmt.Sprintf("SELECT s FROM ivc WHERE id = %d", 10+i))
		if err != nil || len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["s"]) != want {
			t.Errorf("%s stored %v (%v), PostgreSQL 17.11 %q", st, res, err, want)
		}
	}
}
