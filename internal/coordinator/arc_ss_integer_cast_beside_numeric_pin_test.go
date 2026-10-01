// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestArcSSPinIntegerCastBesideNumericIsDouble pins, on five arms, three
// answers that DIFFER from PostgreSQL 17.11's, each the same at v0.25.3: an
// integer CAST the query writes as an operand beside a `numeric` is declared
// and computed double precision (N-10's float rung), so past 2^53 the value is
// the double's. A correlated subquery's re-run spells an outer `integer` /
// `bigint` column as `CAST(v AS BIGINT)`, so the outer value in the
// subquery's own WHERE / EXISTS / NOT EXISTS predicate is that double too —
// the SELECT list of the re-run is typed by the declaration walk and is exact.
//
// Fail-on-agree: an arm that answers PostgreSQL's value fails here, and a
// build that answers it deletes this pin (and the recorded gap in
// docs/sql-reference.md's consumer paragraph with it). Any other change fails
// too and is re-measured.
func TestArcSSPinIntegerCastBesideNumericIsDouble(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	cells := []struct {
		name, sql, pg, here string
	}{
		{
			"notExistsBin",
			"SELECT t.id FROM ss_t t WHERE NOT EXISTS (SELECT 1 FROM ss_t u WHERE u.id = t.id AND u.b * 10000000 * u.n - 3 = t.b * 10000000 * t.n) ORDER BY t.id",
			"{int} rows=6 1 | 2 | 3 | 4 | 5 | 6",
			"{int} rows=5 1 | 2 | 4 | 5 | 6",
		},
		{
			"corrWhereBin",
			"SELECT t.id, (SELECT count(*) FROM ss_i q WHERE q.id = 1 AND t.b * 10000000 * t.n - 3 = 900000000000000000) FROM ss_t t WHERE t.id = 3",
			"{int,int} rows=1 3,0",
			"{int,int} rows=1 3,1",
		},
		{
			"constCastSelect",
			"SELECT CAST(9000000000 AS BIGINT) * 10000000 * CAST(10.00 AS NUMERIC(10,2)) - 3",
			"{numeric} rows=1 899999999999999997.00",
			"{float} rows=1 9e+17",
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := arm.run(tc.sql, true)
					if err != nil {
						res = "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
					}
					got[i] = res
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				switch {
				case ssMatches(got[i], tc.pg):
					t.Errorf("%s\n  arm  %s\n  got  %s = PostgreSQL 17.11's answer — a build that answers it deletes this pin", tc.sql, arm.name, got[i])
				case !ssMatches(got[i], tc.here):
					t.Errorf("%s\n  arm  %s\n  got  %s\n  pinned %s (PostgreSQL 17.11: %s) — the answer changed: re-measure", tc.sql, arm.name, got[i], tc.here, tc.pg)
				}
			}
		})
	}
}
