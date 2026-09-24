// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestArcVLTemporalSeamsOnEveryArm is the DAG half of arc VL round 4's two
// expression seams, on the single, dag and dag-shuffled arms:
//
//   - the RANGE rule (round-3 review B1): a DATE or TIMESTAMP constructed past
//     PostgreSQL's range is 22008 — `date out of range` / `timestamp out of
//     range` — wherever the expression is evaluated, a projection, a filter,
//     an aggregate's argument; base answered `-5877585-08-24` for `d - n`;
//   - the OPERATOR rule (round-3 review B3): the date/timestamp `+`/`-` pairs
//     PostgreSQL has no operator for are 42883 in every clause.
//
// The embedded doors (VALUES, INSERT … SELECT, UPDATE, DELETE, MERGE, CTAS)
// hold the same two rules in wadjet.TestTemporalConstructionRefusesPastPostgreSQLRange
// and wadjet.TestTemporalArithmeticRefusedInEveryStatement.
func TestArcVLTemporalSeamsOnEveryArm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)
	for _, tc := range []struct{ sql, state string }{
		{`SELECT c_date + 2147483647 AS v FROM typemx`, "22008"},
		{`SELECT c_date - 5000000 AS v FROM typemx`, "22008"},
		{`SELECT c_date - CAST(2147483647 AS INTEGER) AS v FROM typemx`, "22008"},
		{`SELECT c_ts + INTERVAL '300000000 years' AS v FROM typemx`, "22008"},
		{`SELECT id FROM typemx WHERE c_date + 2147483647 > c_date`, "22008"},
		{`SELECT MAX(c_date - 5000000) AS v FROM typemx`, "22008"},
		{`SELECT CAST(CAST('5874898-01-01' AS DATE) AS TEXT) AS v`, "22008"},
		{`SELECT c_ts + 0 AS v FROM typemx`, "42883"},
		{`SELECT c_date + 1.5 AS v FROM typemx`, "42883"},
		{`SELECT 1 - c_date AS v FROM typemx`, "42883"},
		{`SELECT id FROM typemx WHERE c_date + 1.5 > c_date`, "42883"},
		{`SELECT id FROM typemx ORDER BY c_ts + 1`, "42883"},
		{`SELECT MAX(c_ts) + 0 AS v FROM typemx`, "42883"},
		{`SELECT id, SUM(c_i32) OVER (ORDER BY c_ts + 1) AS v FROM typemx`, "42883"},
		{`SELECT COUNT(*) AS n FROM typemx GROUP BY id HAVING MAX(c_date) + 1.5 > MAX(c_date)`, "42883"},
	} {
		for _, arm := range arms {
			_, err := arm.run(tc.sql)
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s [%s]: got %q (%v), want %s", tc.sql, arm.name, got, err, tc.state)
			}
		}
	}
}
