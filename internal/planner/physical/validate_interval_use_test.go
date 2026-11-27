// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The INTERVAL rule (#1713d, validate_interval_use.go) at the binder, every
// position it names: an interval is consumed inline or refused, and a block
// never publishes one. PostgreSQL 17.11 answers every 0A000 cell here (the
// coordinator's interval table measures each family on every arm); where it
// refuses, its SQLSTATE is the cell's. The controls are statements whose
// interval is consumed by an allowlisted operation, or that never had one (a
// TEXT column whose content looks like an interval, a DURATION column).
func TestIntervalRuleAtTheBinder(t *testing.T) {
	const pub = "query result column"
	runBRCells(t, []brCell{
		// Publication, through every block boundary.
		{"SELECT INTERVAL '1 day' AS a", "0A000", pub},
		{"SELECT CAST(customer AS INTERVAL) * 2 AS a FROM lat_ord", "0A000", pub},
		{"SELECT a FROM (SELECT INTERVAL '1 day' AS a) t", "0A000", pub},
		{"SELECT count(a) AS n FROM (SELECT CAST(customer AS INTERVAL) * 2 AS a FROM lat_ord) t", "0A000", pub},
		{"WITH x AS (SELECT INTERVAL '1 day' AS a) SELECT 1 AS one", "0A000", pub},
		{"SELECT 1 AS one UNION ALL SELECT INTERVAL '1 day'", "0A000", pub},
		{"SELECT x FROM (VALUES (INTERVAL '1 day')) v(x)", "0A000", pub},
		{"SELECT id FROM lat_ord WHERE EXISTS (SELECT INTERVAL '1 day')", "0A000", pub},
		{"SELECT CASE WHEN count(*) > 0 THEN INTERVAL '1 day' * 2 END AS a FROM lat_ord", "0A000", pub},
		{"SELECT max(id) AS m, count(*) * INTERVAL '1 hour' AS h FROM lat_ord", "0A000", pub},
		{"SELECT COALESCE(INTERVAL '1 day', NULL) AS a", "0A000", pub},
		{"SELECT NULLIF(INTERVAL '1 day', INTERVAL '2 days') AS a", "0A000", pub},
		{"SELECT INTERVAL '1 day' + '1 hour' AS a", "0A000", pub},
		// Keys.
		{"SELECT count(*) AS n FROM lat_ord GROUP BY CAST(customer AS INTERVAL)", "0A000", "GROUP BY key"},
		{"SELECT id FROM lat_ord ORDER BY CAST(customer AS INTERVAL) * 2, id", "0A000", "ORDER BY key"},
		{"SELECT id, row_number() OVER (ORDER BY CAST(customer AS INTERVAL)) AS r FROM lat_ord", "0A000", "window ORDER BY key"},
		{"SELECT id, count(*) OVER (PARTITION BY -INTERVAL '1 day') AS r FROM lat_ord", "0A000", "PARTITION BY key"},
		// Consumers.
		{"SELECT max(INTERVAL '1 day') AS a FROM lat_ord", "0A000", "max over an interval argument"},
		{"SELECT lag(id, 1, INTERVAL '1 day') OVER (ORDER BY id) AS a FROM lat_ord", "0A000", "lag over an interval argument"},
		{"SELECT ARRAY[INTERVAL '1 day'] = ARRAY[INTERVAL '24 hours'] AS a", "0A000", "array element"},
		{"SELECT (INTERVAL '1 day', 1) = (INTERVAL '24 hours', 1) AS a", "0A000", "row element"},
		{"SELECT id FROM lat_ord WHERE CAST(customer AS INTERVAL) IN (SELECT customer FROM lat_ord)", "0A000", "subquery membership"},
		{"SELECT id FROM lat_ord WHERE CAST(customer AS INTERVAL) = ANY (SELECT customer FROM lat_ord)", "0A000", "subquery membership"},
		{"SELECT 'x' || INTERVAL '1 day' AS a", "0A000", "||"},
		{"SELECT extract(day FROM INTERVAL '1 day') AS a", "0A000", "EXTRACT over an interval"},
		{"SELECT upper(INTERVAL '1 day') AS a", "0A000", "upper over an interval argument"},
		{"SELECT * FROM generate_series(1, 3, INTERVAL '1 day')", "0A000", "table function"},
		// PostgreSQL's own refusals keep their codes.
		{"SELECT CASE WHEN true THEN INTERVAL '1 day' * 2 ELSE 1 END AS a", "42804", "CASE types integer and interval cannot be matched"},
		{"SELECT CASE WHEN id = 1 THEN -INTERVAL '1 day' ELSE id END AS a FROM lat_ord", "42804", "cannot be matched"},
		{"SELECT COALESCE(INTERVAL '1 day' * 2, 1) AS a", "42804", "COALESCE types interval and integer cannot be matched"},
		{"SELECT greatest(INTERVAL '1 day', id) AS a FROM lat_ord", "42804", "GREATEST types"},
		{"SELECT ifnull(INTERVAL '1 day', NULL) * 2 AS a", "42883", "function ifnull(interval, unknown) does not exist"},
		{"SELECT INTERVAL '1 day' = 1 AS a", "42883", "operator does not exist: interval = integer"},
		{"SELECT id FROM lat_ord WHERE CAST(customer AS INTERVAL) = customer", "42883", "interval = text"},
		{"SELECT INTERVAL '1 day' * INTERVAL '1 day' AS a", "42883", "operator does not exist"},
		{"SELECT CAST(customer AS INTERVAL) * customer AS a FROM lat_ord", "42883", "interval * text"},
		{"SELECT CAST(INTERVAL '1 day' AS INTEGER) AS a", "42846", "cannot cast type interval to integer"},
		{"SELECT CASE WHEN false THEN CAST('x' AS INTERVAL) END IS NULL AS a", "22007", "invalid input syntax for type interval"},
		// arc VL's text-date reading, unchanged (temporal r19's family).
		{"SELECT '2024-01-01' + INTERVAL '1 day' AS a", "", ""},
		{"SELECT customer - INTERVAL '1 day' AS a FROM lat_ord", "", ""},
		{"SELECT CASE '1 day' WHEN INTERVAL '24 hours' THEN 1 END AS a", "42883", "text = interval"},
		{"SELECT id FROM lat_ord WHERE INTERVAL '1 day'", "42804", "argument of WHERE must be type boolean, not type interval"},
		{"SELECT INTERVAL '2147483648 days' AS a", "22015", "out of range"},
		{"SELECT INTERVAL '1 day' / 0 AS a", "22012", "division by zero"},
		{"SELECT c_dur + INTERVAL '1 day' AS a FROM tm", "42883", "operator does not exist: bigint + interval"},
		// Controls.
		{"SELECT CAST(INTERVAL '1 day' AS TEXT) AS a", "", ""},
		{"SELECT CAST(CAST(customer AS INTERVAL) * 2 AS TEXT) AS a, count(*) AS n FROM lat_ord GROUP BY CAST(CAST(customer AS INTERVAL) * 2 AS TEXT)", "", ""},
		{"SELECT id FROM lat_ord WHERE CAST(customer AS INTERVAL) BETWEEN INTERVAL '1 day' AND '2 days'", "", ""},
		{"SELECT id FROM lat_ord WHERE CAST(customer AS INTERVAL) IN (INTERVAL '1 day', '2 days', NULL)", "", ""},
		{"SELECT id FROM lat_ord WHERE CAST(customer AS INTERVAL) IS NOT DISTINCT FROM INTERVAL '1 day'", "", ""},
		{"SELECT CASE CAST(customer AS INTERVAL) WHEN INTERVAL '1 day' THEN 'd' END AS k FROM lat_ord", "", ""},
		{"SELECT count(*) FILTER (WHERE CAST(customer AS INTERVAL) > INTERVAL '1 day') AS n FROM lat_ord", "", ""},
		{"SELECT count(*) AS n FROM lat_ord HAVING max(id) > 0 AND NULLIF(INTERVAL '1 day', '2 days') > INTERVAL '0 days'", "", ""},
		{"SELECT a.id FROM lat_ord a JOIN lat_ord b ON CAST(a.customer AS INTERVAL) = CAST(b.customer AS INTERVAL) * 2", "", ""},
		{"SELECT c_ts + INTERVAL '1 month' AS a, c_ts - INTERVAL '1 day' * 2 AS b FROM tm ORDER BY c_ts + INTERVAL '1 day'", "", ""},
		{"SELECT max(c_ts + INTERVAL '1 day') AS a FROM tm GROUP BY c_ts - INTERVAL '1 hour'", "", ""},
		{"SELECT id FROM tm WHERE c_ts > now() - INTERVAL '7 days'", "", ""},
		{"SELECT time_bucket(INTERVAL '1 hour', c_ts) AS b FROM tm", "", ""},
		{"SELECT date_part('day', INTERVAL '30 days') AS a", "", ""},
		{"SELECT customer, count(*) AS n FROM lat_ord GROUP BY customer ORDER BY customer", "", ""},
		{"SELECT c_dur, sum(c_dur) AS s FROM tm GROUP BY c_dur ORDER BY c_dur", "", ""},
		{"SELECT CAST(NULL AS INTERVAL) IS NULL AS a", "", ""},
		// A FROM-less scalar subquery IS its expression here (the parser's
		// unfoldFromlessScalars, #1044): the interval never leaves the
		// comparison or the cast that consumes it, and PostgreSQL's answer
		// is the same value (the coordinator's pos/scalarSubquery and
		// pos/castTextOfSubquery). One with a FROM keeps its block, which
		// publishes the interval and is refused.
		{"SELECT (SELECT INTERVAL '1 day') = INTERVAL '24 hours' AS a", "", ""},
		{"SELECT CAST((SELECT INTERVAL '1 day') AS TEXT) AS a", "", ""},
		{"SELECT (SELECT INTERVAL '1 day' FROM lat_ord LIMIT 1) = INTERVAL '24 hours' AS a", "0A000", pub},
	})
}

// A timestamp difference is an INTERVAL-valued producer (#1713d round 6, the
// maintainer's ruling on ADR-0012 temporal r16): PostgreSQL 17.11 types
// `timestamp - timestamp` interval and this engine computes a double of
// milliseconds, so no statement may observe that number. Each use is decided
// as PostgreSQL decides it for an interval (1713d_r6.md's measured table, the
// coordinator's td/* and tsub/* cells): a publication, key, aggregate,
// function argument or comparison is refused 0A000 naming the difference; a
// number beside it is PostgreSQL's own 42883 / 42804 / 42846; IS [NOT] NULL
// answers. `date - date` is an integer on both sides and is not a difference.
func TestTimestampDifferenceIsAnIntervalValuedProducer(t *testing.T) {
	const why = "a timestamp difference (timestamp - timestamp) is an interval and cannot yet be used as "
	const how = ": compare the timestamps themselves instead, for example ts > TIMESTAMP '2024-01-01 00:00:00' + " +
		"INTERVAL '30 days' or a.ts > b.ts + INTERVAL '30 days', or take a number of seconds as " +
		"extract(epoch FROM ts2) - extract(epoch FROM ts1)"
	df := "c_ts - TIMESTAMP '2024-01-01 00:00:00'"
	cells := []brCell{
		// Published, keyed, ordered, aggregated: PostgreSQL answers each.
		{"SELECT " + df + " AS d FROM tm", "0A000", why + "a query result column" + how},
		{"SELECT a.c_ts - b.c_ts AS d FROM tm a, tm b", "0A000", why + "a query result column"},
		{"SELECT count(*) AS n FROM tm GROUP BY " + df, "0A000", why + "a GROUP BY key"},
		{"SELECT id FROM tm ORDER BY " + df, "0A000", why + "an ORDER BY key"},
		{"SELECT sum(" + df + ") AS s FROM tm", "0A000", why + "an argument of sum"},
		{"SELECT count(" + df + ") AS s FROM tm", "0A000", why + "an argument of count"},
		{"SELECT max(" + df + ") AS s FROM tm", "0A000", why + "an argument of max"},
		{"SELECT id, lag(" + df + ") OVER (ORDER BY id) AS v FROM tm", "0A000", why + "an argument of lag"},
		{"SELECT id, sum(id) OVER (ORDER BY " + df + ") AS v FROM tm", "0A000", why + "a window ORDER BY key"},
		{"SELECT id FROM tm WHERE extract(epoch FROM " + df + ") > 1000", "0A000", why + "an argument of extract"},
		{"SELECT date_part('epoch', " + df + ") AS e FROM tm", "0A000", why + "an argument of date_part"},
		{"SELECT CAST(" + df + " AS TEXT) AS v FROM tm", "0A000", why + "the operand of a cast to text"},
		{"SELECT abs(" + df + ") AS v FROM tm", "0A000", why + "an argument of abs"},
		{"SELECT ARRAY[" + df + "] AS v FROM tm", "0A000", why + "an array element"},
		{"SELECT count(*) AS n FROM (SELECT " + df + " AS d FROM tm) x", "0A000", why + "a query result column"},
		{"WITH c AS (SELECT " + df + " AS d FROM tm) SELECT 1 AS one", "0A000", why + "a query result column"},
		{"SELECT id, (" + df + ") / 1000 AS v FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, CASE WHEN id = 1 THEN " + df + " END AS v FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - '2024-01-01' AS v FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, '2024-01-01' - c_ts AS v FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - NULL AS v FROM tm", "0A000", why + "a query result column"},
		// Beside an interval (round 3's cells): compared, combined, matched.
		{"SELECT id FROM tm WHERE " + df + " > INTERVAL '30 days'", "0A000", why + "an operand of a comparison (>)" + how},
		{"SELECT id FROM tm WHERE INTERVAL '30 days' <= " + df, "0A000", "an operand of a comparison (<=)"},
		{"SELECT id, " + df + " = INTERVAL '1 day' AS b FROM tm", "0A000", "an operand of a comparison (=)"},
		{"SELECT a.id FROM tm a JOIN tm b ON a.c_ts - b.c_ts <> INTERVAL '1 day'", "0A000", "an operand of a comparison (!=)"},
		{"SELECT a.id FROM tm a JOIN tm b ON (a.c_ts - b.c_ts) = (b.c_ts - a.c_ts)", "0A000", "an operand of a comparison (=)"},
		{"SELECT id FROM tm WHERE " + df + " BETWEEN INTERVAL '1 day' AND INTERVAL '2 days'", "0A000", "an operand of a comparison (between)"},
		{"SELECT id FROM tm WHERE " + df + " IN (INTERVAL '1 day', INTERVAL '2 days')", "0A000", "an operand of a comparison (in)"},
		{"SELECT id FROM tm WHERE " + df + " > '1 day'", "0A000", "an operand of a comparison (>)"},
		{"SELECT id FROM tm WHERE NULLIF(" + df + ", INTERVAL '1 day') IS NULL", "0A000", "an operand of a comparison (=)"},
		{"SELECT id FROM tm WHERE (" + df + ") + INTERVAL '1 day' > INTERVAL '2 days'", "0A000", "an operand of a comparison (>)"},
		{"SELECT id FROM tm WHERE COALESCE(" + df + ", INTERVAL '0 days') > INTERVAL '2 days'", "0A000", "an operand of a comparison (>)"},
		{"SELECT id FROM tm WHERE GREATEST(" + df + ", INTERVAL '1 day') > INTERVAL '2 days'", "0A000", "an operand of a comparison (>)"},
		{"SELECT id FROM tm WHERE CASE WHEN id > 1 THEN " + df + " ELSE INTERVAL '1 day' END > INTERVAL '2 days'", "0A000", "an operand of a comparison (>)"},
		// A number beside it: PostgreSQL's own refusal for an interval.
		{"SELECT id FROM tm WHERE " + df + " > 1000", "42883", "operator does not exist: interval > integer"},
		{"SELECT id, (" + df + ") + 1 AS v FROM tm", "42883", "operator does not exist: interval + integer"},
		{"SELECT id, 1 + (" + df + ") AS v FROM tm", "42883", "operator does not exist: integer + interval"},
		{"SELECT id FROM tm WHERE (" + df + ") / 1000 > 50", "42883", "operator does not exist: interval > integer"},
		{"SELECT CAST(" + df + " AS BIGINT) AS v FROM tm", "42846", "cannot cast type interval to bigint"},
		{"SELECT CASE WHEN id = 1 THEN " + df + " ELSE 0 END AS v FROM tm", "42804", "CASE types integer and interval cannot be matched"},
		{"SELECT COALESCE(" + df + ", 0) AS v FROM tm", "42804", "COALESCE types interval and integer cannot be matched"},
		{"SELECT NULLIF(" + df + ", 0) AS v FROM tm", "42883", "operator does not exist: interval = integer"},
		{"SELECT id FROM tm WHERE " + df, "42804", "argument of WHERE must be type boolean, not type interval"},
		// date - date is an integer there too: PostgreSQL's own 42883.
		{"SELECT id FROM tm WHERE c_date - DATE '2024-01-01' > INTERVAL '30 days'", "42883", "operator does not exist: bigint > interval"},
		// Round 7: an operand whose TIMESTAMP the declared-type walk cannot
		// type — an aggregate, a window function, a scalar subquery, and an
		// expression over one — is a timestamp all the same.
		{"SELECT max(c_ts) - min(c_ts) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT extract(epoch FROM max(c_ts) - min(c_ts)) AS e FROM tm", "0A000", why + "an argument of extract"},
		{"SELECT CAST(max(c_ts) - min(c_ts) AS TEXT) AS v FROM tm", "0A000", why + "the operand of a cast to text"},
		{"SELECT count(*) AS n FROM tm HAVING max(c_ts) - min(c_ts) > 1000", "42883", "operator does not exist: interval > integer"},
		{"SELECT count(*) AS n FROM tm HAVING max(c_ts) - min(c_ts) > INTERVAL '1 day'", "0A000", why + "an operand of a comparison (>)"},
		{"SELECT id FROM tm GROUP BY id ORDER BY max(c_ts) - min(c_ts)", "0A000", why + "an ORDER BY key"},
		{"SELECT min_by(c_ts, id) - max_by(c_ts, id) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT max(c_ts) - '2024-01-01' AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT COALESCE(max(c_ts), c_ts) - c_ts AS d FROM tm GROUP BY c_ts", "0A000", why + "a query result column"},
		{"SELECT date_trunc('day', max(c_ts)) - min(c_ts) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT (max(c_ts) + INTERVAL '1 day') - min(c_ts) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - lag(c_ts) OVER (ORDER BY id) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, lead(c_ts) OVER (ORDER BY id) - c_ts AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - first_value(c_ts) OVER (ORDER BY id) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - last_value(c_ts) OVER (ORDER BY id) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - nth_value(c_ts, 2) OVER (ORDER BY id) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - max(c_ts) OVER () AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - lag(c_ts) OVER (ORDER BY id) > 1000 AS b FROM tm", "42883", "operator does not exist: interval > integer"},
		{"SELECT id, max(c_ts) - lag(max(c_ts)) OVER (ORDER BY id) AS d FROM tm GROUP BY id", "0A000", why + "a query result column"},
		{"SELECT id, (SELECT max(c_ts) FROM tm) - c_ts AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id FROM tm WHERE c_ts - (SELECT min(c_ts) FROM tm) > 1000", "42883", "operator does not exist: interval > integer"},
		{"SELECT (SELECT max(c_ts) FROM tm) - (SELECT min(c_ts) FROM tm) AS d", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - (SELECT max(c_ts) FROM tm UNION SELECT NULL) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - (SELECT lag(c_ts) OVER (ORDER BY id) FROM tm LIMIT 1) AS d FROM tm", "0A000", why + "a query result column"},
		// Round 8 (the round-7 review): an outer column a body with its own
		// FROM publishes, a star body, and lag / lead whose TIMESTAMP is the
		// default argument. Each was left to execution through round 7.
		{"SELECT id, (SELECT x.c_ts FROM tm y LIMIT 1) - c_ts AS d FROM tm x", "0A000", why + "a query result column"},
		{"SELECT id, CAST((SELECT x.c_ts FROM tm y LIMIT 1) - c_ts AS TEXT) AS d FROM tm x", "0A000", why + "the operand of a cast to text"},
		{"SELECT id, c_ts - (SELECT * FROM (SELECT max(c_ts) FROM tm) z) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - (SELECT * FROM (SELECT c_ts FROM tm LIMIT 1) z) AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, lag(NULL, 1, c_ts) OVER (ORDER BY id) - c_ts AS d FROM tm", "0A000", why + "a query result column"},
		{"SELECT id, c_ts - lead(NULL, 1, c_ts) OVER (ORDER BY id) > 1000 AS b FROM tm", "42883", "operator does not exist: interval > integer"},
		// Their controls: no TIMESTAMP minus a TIMESTAMP.
		{"SELECT id, lag(NULL, 1, c_date) OVER (ORDER BY id) - c_date AS d FROM tm", "", ""},
		{"SELECT id, (SELECT x.id FROM tm y LIMIT 1) - id AS d FROM tm x", "", ""},
		{"SELECT id, c_ts > (SELECT * FROM (SELECT max(c_ts) FROM tm) z) AS b FROM tm", "", ""},
		// Their neighbours are unchanged: only the producer reads the new
		// TIMESTAMP typing (PostgreSQL: 42846 and 42883; both refused here).
		{"SELECT CAST(max(c_ts) AS INTERVAL) AS v FROM tm", "0A000", "interval values cannot yet be used as a query result column"},
		{"SELECT INTERVAL '1 day' * max(c_ts) AS v FROM tm", "0A000", "interval values cannot yet be used as a query result column"},
		// Controls: the same producers with no timestamp subtraction.
		{"SELECT max(c_ts) > min(c_ts) AS b FROM tm", "", ""},
		{"SELECT max(c_ts) - INTERVAL '1 day' AS v FROM tm", "", ""},
		{"SELECT (max(c_ts) - min(c_ts)) IS NULL AS b FROM tm", "", ""},
		{"SELECT max(c_date) - min(c_date) AS v FROM tm", "", ""},
		{"SELECT max(id) - min(id) AS v FROM tm", "", ""},
		{"SELECT id, c_ts > lag(c_ts) OVER (ORDER BY id) AS b FROM tm", "", ""},
		{"SELECT id, lag(c_ts) OVER (ORDER BY id) + INTERVAL '1 day' AS v FROM tm", "", ""},
		{"SELECT id, c_date - lag(c_date) OVER (ORDER BY id) AS v FROM tm", "", ""},
		{"SELECT id, c_ts > (SELECT min(c_ts) FROM tm) AS b FROM tm", "", ""},
		{"SELECT extract(epoch FROM max(c_ts)) - extract(epoch FROM min(c_ts)) AS s FROM tm", "", ""},
		// Controls: the workarounds the message names, date - date, IS NULL,
		// a single timestamp's epoch, a quoted interval beside a timestamp
		// (not a difference), and a timestamp shift as a key.
		{"SELECT id FROM tm WHERE c_ts > TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '30 days'", "", ""},
		{"SELECT a.id FROM tm a JOIN tm b ON a.c_ts > b.c_ts + INTERVAL '30 days'", "", ""},
		{"SELECT extract(epoch FROM c_ts) - extract(epoch FROM TIMESTAMP '2024-01-01 00:00:00') AS s FROM tm", "", ""},
		{"SELECT c_date - DATE '2024-01-01' AS d FROM tm ORDER BY c_date - DATE '2024-01-01'", "", ""},
		{"SELECT id, (" + df + ") IS NULL AS b FROM tm", "", ""},
		{"SELECT id FROM tm WHERE (" + df + ") IS NOT NULL", "", ""},
		{"SELECT c_ts - '1 day' AS a FROM tm", "22007", "invalid input syntax for type timestamp"},
		{"SELECT id FROM tm ORDER BY c_ts + INTERVAL '1 day'", "", ""},
	}
	runBRCells(t, cells)
}

// A DML door's expression is judged as on SELECT (RefuseIntervalWrite); the
// written value itself is the assignment's (wadjet's assignSource.check).
func TestIntervalRuleOnTheWriteDoors(t *testing.T) {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}, {Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "d", Type: parquet.TypeDuration}}
	for _, c := range []struct{ sql, state string }{
		{"INTERVAL '1 day' * 3", ""},
		{"ARRAY[INTERVAL '1 day']", "0A000"},
		{"ts + INTERVAL '1 day'", ""},
		{"CAST(INTERVAL '1 day' * 2 AS TEXT)", ""},
		{"CAST(s AS INTERVAL) > INTERVAL '1 day'", ""},
		{"CAST(s AS INTERVAL) = s", "42883"},
		{"s || INTERVAL '1 day'", "0A000"},
		{"d", ""},
	} {
		node, err := plansql.ParseExpressionComplete(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := sqlerr.StateOf(RefuseIntervalWrite(node, "", schema))
		if c.state == "" && RefuseIntervalWrite(node, "", schema) != nil {
			t.Errorf("%s: refused %v", c.sql, RefuseIntervalWrite(node, "", schema))
		} else if got != c.state {
			t.Errorf("%s: SQLSTATE %q, want %q", c.sql, got, c.state)
		}
	}
	// A scalar subquery answering a TIMESTAMP, typed for the producer by
	// the door's resolver (RefuseIntervalWriteWith, #1713d round 8): the
	// DML WHERE / MERGE condition gets PostgreSQL's 42883 at plan time.
	tsSub := func(sql string) bool { return strings.Contains(sql, "max(ts)") }
	for _, c := range []struct{ sql, state string }{
		{"ts - (SELECT max(ts) FROM t) > 1000", "42883"},
		{"(SELECT max(ts) FROM t) - ts > INTERVAL '1 day'", "0A000"},
		{"CAST((SELECT max(ts) FROM t) - ts AS TEXT) = s", "0A000"},
		{"ts > (SELECT max(ts) FROM t)", ""},
		{"ts - (SELECT max(n) FROM t) > 1000", ""},
	} {
		node, err := plansql.ParseExpressionComplete(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if got := sqlerr.StateOf(RefuseIntervalWriteWith(node, "", schema, tsSub)); got != c.state {
			t.Errorf("%s: SQLSTATE %q, want %q", c.sql, got, c.state)
		}
		if c.state == "42883" {
			// Without the resolver the rule cannot type the subquery and the
			// statement is left to the expression guard (as through round 7).
			if got := sqlerr.StateOf(RefuseIntervalWrite(node, "", schema)); got != "" {
				t.Errorf("%s without the resolver: SQLSTATE %q, want none", c.sql, got)
			}
		}
	}
}
