// SPDX-License-Identifier: MIT

package physical

import "testing"

// An INTERVAL value as an aggregate's or a window function's argument is
// refused 0A000 at binding (#1713c round 3): no interval accumulator and no
// interval column type exist, so the argument was declared float8 (0 / NULL,
// or the rendered text's order for min / max of a CAST) and, once interval
// arithmetic answered its interval, met the store guard. PostgreSQL 17.11
// answers every refused statement here (the coordinator's interval table,
// agg/* and win/*). The controls are the statements that only USE an
// interval — a timestamp shift, a comparison, an EXTRACT — or aggregate a
// DDL INTERVAL column (DURATION, temporal r15), which keep their answers.
func TestIntervalAggregateArgumentIsRefused(t *testing.T) {
	const msg = "over an interval argument is not supported"
	runBRCells(t, []brCell{
		{"SELECT count(INTERVAL '1 day' * 2) AS v FROM lat_ord", "0A000", "count " + msg},
		{"SELECT count(-INTERVAL '1 day') AS v FROM lat_ord", "0A000", msg},
		{"SELECT count(DISTINCT INTERVAL '1 day' + INTERVAL '1 hour') AS v FROM lat_ord", "0A000", msg},
		{"SELECT count(INTERVAL '1 day') AS v FROM lat_ord", "0A000", msg},
		{"SELECT count(CAST(customer AS INTERVAL)) AS v FROM lat_ord", "0A000", msg},
		{"SELECT max(CAST(customer AS INTERVAL)) AS v FROM lat_ord", "0A000", "max " + msg},
		{"SELECT min(INTERVAL '1 day' / 2) AS v FROM lat_ord", "0A000", msg},
		{"SELECT avg(c_i64 * INTERVAL '1 day') AS v FROM tm", "0A000", "avg " + msg},
		{"SELECT max(CASE WHEN id = 1 THEN INTERVAL '1 day' END) AS v FROM lat_ord", "0A000", msg},
		{"SELECT max(CASE WHEN id = 1 THEN INTERVAL '1 day' ELSE '2 days' END) AS v FROM lat_ord", "0A000", msg},
		{"SELECT sum(COALESCE(INTERVAL '1 day', NULL)) AS v FROM lat_ord", "0A000", msg},
		{"SELECT max(NULLIF(INTERVAL '1 day', INTERVAL '2 days')) AS v FROM lat_ord", "0A000", msg},
		{"SELECT count(*) AS n FROM lat_ord HAVING max(CAST(customer AS INTERVAL)) > INTERVAL '1 day'", "0A000", msg},
		{"SELECT max(INTERVAL '1 day') OVER () AS v FROM lat_ord", "0A000", msg},
		{"SELECT lag(CAST(customer AS INTERVAL)) OVER (ORDER BY id) AS v FROM lat_ord", "0A000", "lag " + msg},
		{"SELECT first_value(INTERVAL '1 day' * 2) OVER () AS v FROM lat_ord", "0A000", msg},
		{"SELECT (SELECT max(INTERVAL '1 day') FROM lat_ord) AS v", "0A000", msg},
		// Controls.
		{"SELECT count(*) AS v FROM lat_ord", "", ""},
		{"SELECT max(c_ts + INTERVAL '1 day') AS v FROM tm", "", ""},
		{"SELECT min_by(id, c_ts + INTERVAL '1 second') AS v FROM tm", "", ""},
		{"SELECT count(CAST(customer AS INTERVAL) = INTERVAL '1 day') AS v FROM lat_ord", "", ""},
		{"SELECT sum(date_part('day', CAST(customer AS INTERVAL))) AS v FROM lat_ord", "", ""},
		{"SELECT sum(c_dur) AS v FROM tm", "", ""},
		{"SELECT max(CASE WHEN id = 1 THEN CAST(INTERVAL '1 day' AS TEXT) END) AS v FROM lat_ord", "", ""},
		{"SELECT CAST(CAST(customer AS INTERVAL) * 2 AS TEXT) AS v FROM lat_ord", "", ""},
		// #1713d: the interval rule refuses these too (a key, EXTRACT's
		// numeric/float8 type gap, a published interval); they were
		// controls under #1713c.
		{"SELECT count(*) OVER (PARTITION BY CAST(customer AS INTERVAL)) AS v FROM lat_ord", "0A000", "PARTITION BY key"},
		{"SELECT sum(extract(day FROM CAST(customer AS INTERVAL))) AS v FROM lat_ord", "0A000", "EXTRACT over an interval"},
		{"SELECT CAST(customer AS INTERVAL) * 2 AS v FROM lat_ord", "0A000", "query result column"},
	})
}
