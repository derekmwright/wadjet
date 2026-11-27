// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A TIMESTAMPTZ VALUE IS THE INSTANT ITS TEXT NAMES (#1680 line d), on the
// embedded door, where a TIMESTAMP-declared value is its epoch milliseconds.
// Every session of this engine runs at UTC (a SET TimeZone naming another
// zone is refused at pgwire, the only door with SET), so the cells are
// PostgreSQL 17.11's answers under TimeZone=UTC
// (internal/coordinator/testdata/arc_1680d_timestamptz_pg17.tsv).
//
// `TIMESTAMPTZ '2024-07-01 12:00:00+05:30'` answered 12:00 — the offset
// discarded, as TIMESTAMP input discards it — where PostgreSQL answers the
// instant 06:30 UTC; `CAST(text AS TIMESTAMPTZ)` and `::timestamptz` the
// same. extract(timezone_hour …) answered the offset of the server PROCESS's
// local zone (this test pins the process zone to New York so the old reading
// is visible on any host), and date_part('timezone_hour' / 'timezone') NULL;
// PostgreSQL under UTC answers 0.
func TestTimestamptzIsTheInstantItsTextNames(t *testing.T) {
	saved := time.Local
	time.Local = time.FixedZone("EDT", -4*3600)
	t.Cleanup(func() { time.Local = saved })
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	at := func(s string) int64 {
		v, err := time.Parse("2006-01-02 15:04:05.999", s)
		if err != nil {
			t.Fatal(err)
		}
		return v.UnixMilli()
	}
	for _, c := range []struct {
		expr string
		want any
	}{
		{"TIMESTAMPTZ '2024-07-01 12:00:00+05:30'", at("2024-07-01 06:30:00")},
		{"TIMESTAMPTZ '2024-07-01 12:00:00-04'", at("2024-07-01 16:00:00")},
		{"TIMESTAMPTZ '2024-07-01 12:00:00Z'", at("2024-07-01 12:00:00")},
		{"TIMESTAMPTZ '2024-07-01 12:00:00'", at("2024-07-01 12:00:00")},
		{"TIMESTAMPTZ '2024-03-10 02:30:00'", at("2024-03-10 02:30:00")},
		{"TIMESTAMPTZ '2024-07-01 12:00:00.123+05:30'", at("2024-07-01 06:30:00.123")},
		{"TIMESTAMPTZ '2024-07-01T12:00:00+0530'", at("2024-07-01 06:30:00")},
		{"TIMESTAMPTZ '2024-07-01 02:00:00+05:30'", at("2024-06-30 20:30:00")},
		{"CAST('2024-07-01 12:00:00+05:30' AS TIMESTAMPTZ)", at("2024-07-01 06:30:00")},
		{"'2024-07-01 12:00:00 -04:00'::timestamptz", at("2024-07-01 16:00:00")},
		{"CAST(TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS TIMESTAMP)", at("2024-07-01 06:30:00")},
		{"date_trunc('day', TIMESTAMPTZ '2024-07-01 02:00:00+05:30')", at("2024-06-30 00:00:00")},
		{"TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AT TIME ZONE 'UTC'", at("2024-07-01 06:30:00")},
		// A TIMESTAMP literal discards an offset (PostgreSQL's timestamp rule):
		// the control that the fix is the timestamptz reading only.
		{"TIMESTAMP '2024-07-01 12:00:00+05:30'", at("2024-07-01 12:00:00")},
		{"CAST('2024-07-01 12:00:00+05:30' AS TIMESTAMP)", at("2024-07-01 12:00:00")},
		{"extract(hour FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", 6.0},
		{"extract(timezone_hour FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", int64(0)},
		{"extract(timezone_minute FROM TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", int64(0)},
		{"date_part('timezone_hour', TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", 0.0},
		{"date_part('timezone', TIMESTAMPTZ '2024-07-01 12:00:00+05:30')", 0.0},
		{"TIMESTAMPTZ '2024-07-01 12:00:00+00' = TIMESTAMPTZ '2024-07-01 17:30:00+05:30'", true},
		{"CAST(TIMESTAMPTZ '2024-07-01 02:00:00+05:30' AS DATE) = DATE '2024-06-30'", true},
	} {
		sql := "SELECT " + c.expr + " AS v"
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			continue
		}
		if got := res.Rows[0]["v"]; fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("%s = %v (%T), want %v (PostgreSQL 17.11 under TimeZone=UTC)", sql, got, got, c.want)
		}
	}
	// An offset that carries the instant past PostgreSQL's range is its 22008.
	if _, err := db.Query(ctx, "SELECT TIMESTAMPTZ '294276-12-31 23:00:00-15' AS v"); sqlerr.StateOf(err) != "22008" {
		t.Errorf("TIMESTAMPTZ '294276-12-31 23:00:00-15': %v, want 22008 timestamp out of range", err)
	}
	// A comparison of a timestamptz with a quoted literal spelling a nonzero
	// offset is refused by name; a literal without one still answers.
	_, err = db.Query(ctx, "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 17:30:00+05:30' AS v")
	if sqlerr.StateOf(err) != "0A000" || sqlerr.ReasonOf(err) != "timestamptz-zoned-literal" {
		t.Errorf("tstz = zoned quoted literal: %v, want the 0A000 timestamptz-zoned-literal refusal", err)
	}
	if res, err := db.Query(ctx, "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 12:00:00' AS v"); err != nil || res.Rows[0]["v"] != true {
		t.Errorf("tstz = quoted literal without an offset: %v %v, want true", res, err)
	}
}
