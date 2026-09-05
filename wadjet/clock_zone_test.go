package wadjet

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// #870: every clock function reads ONE zone.
//
// `NOW()` and `CURRENT_TIMESTAMP` render through expr.formatInstant, which
// normalizes to UTC — a wadjet TIMESTAMP is a zoneless instant, so there is no
// offset to print. `CURRENT_DATE` formatted `time.Now()` in the MACHINE'S
// LOCAL zone, so on a host west of Greenwich the two named different DAYS for
// the hours between local midnight and UTC midnight. A landing battery run at
// 20:00 ET is where it showed up: `CURRENT_DATE` said 2026-09-04 and `NOW()`
// said 2026-09-05.
//
// PostgreSQL keeps them consistent through the session's TimeZone —
// `current_date = now()::date` is TRUE there, measured on 17.11 under
// `TimeZone = UTC` — and this engine has no session zone. UTC is the zone its
// rendering already commits to, so it is the zone the clock reads in.
//
// The clock is MOCKED here because the defect is a CONDITION, not a query
// shape: the two functions agree for most of the day and disagree only inside
// the host's UTC offset, so a gate reading the real clock passes on the
// machine that has the bug for sixteen hours out of twenty-four. Each instant
// below straddles midnight UTC in a zone this test does not have to be
// running in.
func TestEveryClockFunctionReadsOneZone(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table

	// The mocked instants are returned in a WEST-of-Greenwich location, not
	// in UTC: `time.Now()` hands back a Time in the machine's Local zone, and
	// a mock that hands back a UTC one cannot tell `.Format` from
	// `.UTC().Format` — it would pass on the engine that has the bug. The zone
	// is a fixed offset rather than a named one so the gate does not depend on
	// the host's tzdata.
	west := time.FixedZone("test-utc-minus-4", -4*60*60)
	for _, c := range []struct {
		name string
		at   time.Time
		date string
	}{
		// Half an hour PAST midnight UTC, which is 20:30 the previous day in
		// New York — the shape the battery hit.
		{"just_after_midnight_utc", time.Date(2026, 9, 5, 0, 30, 0, 0, time.UTC), "2026-09-05"},
		// Half an hour BEFORE it, the same day either way in New York and a
		// different one in Tokyo — the mirror, so a fix that simply moved the
		// offset would fail one of the two.
		{"just_before_midnight_utc", time.Date(2026, 9, 4, 23, 30, 0, 0, time.UTC), "2026-09-04"},
		{"midnight_utc_exactly", time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), "2026-09-05"},
		{"midday_utc", time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC), "2026-09-05"},
	} {
		t.Run(c.name, func(t *testing.T) {
			at := c.at.In(west)
			restore := expr.SetClockForTest(func() time.Time { return at })
			t.Cleanup(restore)

			res, err := db.Query(ctx, `SELECT CURRENT_DATE AS d, NOW() AS n, `+
				`CURRENT_TIMESTAMP AS ct FROM `+tbl+` WHERE id = 0`)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows", len(res.Rows))
			}
			r := res.Rows[0]
			if got, _ := r["d"].(string); got != c.date {
				t.Errorf("CURRENT_DATE = %v, want %q — the clock's UTC date", r["d"], c.date)
			}
			for _, col := range []string{"n", "ct"} {
				ms, ok := r[col].(int64)
				if !ok {
					t.Fatalf("%s came back as %#v, want the instant", col, r[col])
				}
				if got := batch.FormatTimestamp(ms); got[:10] != c.date {
					t.Errorf("%s renders %q, whose date is not CURRENT_DATE's %q — two clock "+
						"functions, two zones", col, got, c.date)
				}
			}
		})
	}

	// The claim as a QUERY, which is the form a user meets it in and the form
	// PostgreSQL answers TRUE for.
	restore := expr.SetClockForTest(func() time.Time {
		return time.Date(2026, 9, 5, 0, 30, 0, 0, time.UTC).In(west)
	})
	t.Cleanup(restore)
	for _, sql := range []string{
		`SELECT COUNT(*) AS n FROM ` + tbl + ` WHERE CURRENT_DATE = CAST(NOW() AS DATE)`,
		`SELECT COUNT(*) AS n FROM ` + tbl + ` WHERE CURRENT_DATE = CAST(CURRENT_TIMESTAMP AS DATE)`,
	} {
		if got := tmScalarInt(t, ctx, db, sql); got != int64(typematrix.Rows) {
			t.Errorf("%s matched %d of %d rows; `current_date = now()::date` is TRUE on "+
				"PostgreSQL 17.11", sql, got, typematrix.Rows)
		}
	}
}
