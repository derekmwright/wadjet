// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Arc CW round 6 (B1, a closure-review regression): arrays of INTERVAL on the
// embedded engine. INTERVAL declares no column type anywhere else in this
// engine (physical.binOpTemporalType's note), so an ARRAY[INTERVAL …] literal
// declared Undecided as a WHOLE — the projection took the STRING guess and a
// container box into STRING/BYTES is refused (ADR-0045 §2), and the
// comparator's own last-resort shape reader defaulted an unmatched
// expr.IntervalValue to INT64 — both #361 silent-write guard panics on a
// shape base 08109be6 answered PostgreSQL's value for: a projection nothing
// reads, `=`, `<`, GROUP BY, DISTINCT and UNION.
//
// The fix declares an INTERVAL element DURATION (physical.arrayLitDeclaredType,
// batch.DurationNanoser, expr.IntervalValue.DurationNanos): PostgreSQL's own
// interval_cmp metric (a month is 30 days), so "2 days" orders before
// "10 days" where the two intervals' RENDERED TEXT does not, and "1 month"
// equals "30 days" as PostgreSQL's array_cmp does. Rendering an interval
// element to text still refuses 0A000 (#1268) — DURATION's own renderer
// prints a plain nanosecond count and nothing here asks it to.
func TestArcCW6IntervalArraysAnswerAsPostgreSQLOnTheEmbeddedEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{"CREATE TABLE cw6 (id BIGINT)", "INSERT INTO cw6 VALUES (1), (2), (3)"} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	cells := []struct{ name, sql, want string }{
		{"count-unread-projection", "SELECT count(*) FROM (SELECT ARRAY[INTERVAL '1 day'] AS c FROM cw6 WHERE id < 3) z", "2"},
		{"where-unread-projection", "SELECT count(*) FROM (SELECT ARRAY[INTERVAL '1 day'] AS c FROM cw6 WHERE id > 1) z", "2"},
		{"eq", "SELECT ARRAY[INTERVAL '1 day'] = ARRAY[INTERVAL '1 day']", "true"},
		// The discriminating pair: PostgreSQL orders by VALUE (2 days < 10
		// days); the two intervals' rendered TEXT orders the other way
		// ("2 days" > "10 days" lexically) — the comparator's old
		// last-resort fallback.
		{"lt-value-not-text", "SELECT ARRAY[INTERVAL '2 days'] < ARRAY[INTERVAL '10 days']", "true"},
		// A month is 30 days in PostgreSQL's own interval comparison — a
		// per-field struct equality (Months=1,Days=0 vs Months=0,Days=30)
		// would say false.
		{"eq-month-equals-days", "SELECT ARRAY[INTERVAL '1 month'] = ARRAY[INTERVAL '30 days']", "true"},
		{"group", "SELECT count(*) FROM (SELECT c FROM (SELECT ARRAY[INTERVAL '1 day'] AS c FROM cw6) s GROUP BY c) z", "1"},
		{"distinct", "SELECT count(*) FROM (SELECT DISTINCT c FROM (SELECT ARRAY[INTERVAL '1 day'] AS c FROM cw6) s) z", "1"},
		{"union", "SELECT count(*) FROM (SELECT ARRAY[INTERVAL '1 day'] AS c UNION SELECT ARRAY[INTERVAL '2 days']) z", "2"},
		{"case", "SELECT count(*) FROM (SELECT CASE WHEN id = 1 THEN ARRAY[INTERVAL '1 day'] ELSE ARRAY[INTERVAL '2 days'] END AS c FROM cw6) z", "3"},
		// Controls: unaffected by the fix, still PostgreSQL's value.
		{"control/cardinality", "SELECT cardinality(ARRAY[INTERVAL '1 day'])", "1"},
		{"control/is-not-null", "SELECT ARRAY[INTERVAL '1 day'] IS NOT NULL", "true"},
		{"control/element-equality", "SELECT (ARRAY[INTERVAL '1 day'])[1] = INTERVAL '1 day'", "true"},
	}
	for _, c := range cells {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s refused: %v", c.name, c.sql, err)
			continue
		}
		if len(res.Rows) != 1 {
			t.Errorf("%s: %s answered %d rows", c.name, c.sql, len(res.Rows))
			continue
		}
		if got := fmt.Sprint(res.Cells(0)[0]); got != c.want {
			t.Errorf("%s: %s\n  got %s, want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
	// CAST(… AS TEXT) still refuses (#1268, unchanged by this round): the
	// refusal reads the runtime box directly (expr.refuseUnrenderable), which
	// this fix never converts.
	if _, err := db.Query(ctx, "SELECT CAST(ARRAY[INTERVAL '1 day'] AS TEXT)"); err == nil ||
		!strings.Contains(err.Error(), "has no text form here") {
		t.Errorf("cast-to-text still refuses: got %v, want the 0A000 refusal", err)
	}
	// A BARE projection of the array is not the CAST door: it renders through
	// DURATION's own carrier now (an interval element's nanosecond count,
	// PostgreSQL's own convention for none of it — docs/postgres-differences.md
	// #125 records DURATION's rendering already), where it used to refuse
	// 42000. Not PostgreSQL's value; not a crash either. This is the one
	// sentence #1268's close text changes (N10).
	res, err := db.Query(ctx, "SELECT ARRAY[INTERVAL '1 hour']")
	if err != nil {
		t.Fatalf("bare projection: %v", err)
	}
	if got := fmt.Sprint(res.Cells(0)[0]); got != "[3600000000000]" {
		t.Errorf("bare projection: got %s, want [3600000000000] (DURATION nanoseconds)", got)
	}
}
