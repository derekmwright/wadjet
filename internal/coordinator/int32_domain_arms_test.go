package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// A number with no int32 is REFUSED on every arm, never wrapped into a
// plausible-looking value.
//
// `<bigint>::DATE` is the one shape in the grammar that reaches
// batch.Vector.SetValue's DATE arm, and at de5bc970 it WRAPPED:
// `SELECT 3000000000::DATE` answered -3543531-12-19, a date rendered like any
// other, with no error on any path (CodeQL go/incorrect-integer-conversion
// #34; #32 and #33 are the PORT/PROTOCOL twins, which no SQL cast reaches
// today and which internal/engine/batch's seam gate covers instead).
//
// PostgreSQL has no int-to-date cast at all: `SELECT 3000000000::date` is
// 42846, `cannot cast type bigint to date`. Wadjet's cast is a deliberate
// SUPERSET (ADR-0012 item 5), and inside a superset the rule is that a value
// it cannot represent is LOUD — 22003, the class PostgreSQL uses for the same
// magnitude reaching an int4 — and never a different number.
//
// Four arms, because a per-row refusal has to SURVIVE the trip: a worker's
// panic boundary turning it into a query error is what makes the DAG arms say
// anything at all, and a refusal that is swallowed there reads as an empty
// result rather than an error.
func TestAnInt32DomainRefusalHoldsOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
	}{
		{"single", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
		{"dag-shuffled", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, sql) }},
	}

	tbl := typematrix.Table
	for _, tc := range []struct {
		name    string
		sql     string
		refused bool
	}{
		// 3000000000 has no int32. Built from the column so the value reaches
		// the store as a computed per-row box, which is the shape #841's guard
		// exists for.
		{"date_above_the_range", "SELECT (c_i64 + 3000000000)::DATE AS v FROM " + tbl, true},
		{"date_below_the_range", "SELECT (c_i64 - 3000000000)::DATE AS v FROM " + tbl, true},
		// The boundary from the other side: a day count an int32 holds is an
		// ANSWER, so the refusals above are about the VALUE and not about the
		// cast existing. c_i32 rather than c_i64 because c_i64 runs to
		// i*1000003 and leaves the int32 range on its own.
		{"date_inside_the_range", "SELECT (c_i32 + 1)::DATE AS v FROM " + tbl, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				_, err := arm.run(tc.sql)
				switch {
				case tc.refused && err == nil:
					t.Errorf("%s arm: %s answered instead of refusing; no int32 holds that day count",
						arm.name, tc.sql)
				case tc.refused && !strings.Contains(err.Error(), "integer out of range"):
					t.Errorf("%s arm: %s refused with %v, want PostgreSQL's \"integer out of range\"",
						arm.name, tc.sql, err)
				case !tc.refused && err != nil:
					t.Errorf("%s arm: %s refused a day count an int32 holds: %v",
						arm.name, tc.sql, err)
				}
			}
		})
	}
}
