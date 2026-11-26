// SPDX-License-Identifier: MIT

package physical

import (
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

// A timestamp difference beside an interval (#1713d round 3): the
// difference is a double of milliseconds here (ADR-0012 temporal r16) and an
// interval on PostgreSQL 17.11, which answers every refused statement below
// (the coordinator's tsub/* cells). The refusal is 0A000 with the operation
// named and the workaround that answers today; `date - date` is an integer
// on both sides, and PostgreSQL's own 42883 for `integer > interval` stays.
func TestTimestampDifferenceBesideAnIntervalIsRefused(t *testing.T) {
	const ms = "it is a number of milliseconds here, not an interval; compare the timestamps themselves instead, " +
		"for example ts > TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '30 days' or a.ts > b.ts + INTERVAL '30 days'"
	df := "c_ts - TIMESTAMP '2024-01-01 00:00:00'"
	cells := []brCell{
		{"SELECT id FROM tm WHERE " + df + " > INTERVAL '30 days'", "0A000", "a timestamp difference (timestamp - timestamp) cannot yet be compared with an interval (>): " + ms},
		{"SELECT id FROM tm WHERE INTERVAL '30 days' <= " + df, "0A000", "compared with an interval (<=)"},
		{"SELECT id, " + df + " = INTERVAL '1 day' AS b FROM tm", "0A000", "compared with an interval (=)"},
		{"SELECT a.id FROM tm a JOIN tm b ON a.c_ts - b.c_ts <> INTERVAL '1 day'", "0A000", "compared with an interval (!=)"},
		{"SELECT id FROM tm WHERE (" + df + ") < INTERVAL '1 day'", "0A000", "compared with an interval (<)"},
		{"SELECT id FROM tm WHERE " + df + " >= INTERVAL '1 day'", "0A000", "compared with an interval (>=)"},
		{"SELECT id FROM tm WHERE " + df + " BETWEEN INTERVAL '1 day' AND INTERVAL '2 days'", "0A000", "compared with an interval (between)"},
		{"SELECT id FROM tm WHERE " + df + " IN (INTERVAL '1 day', INTERVAL '2 days')", "0A000", "compared with an interval (in)"},
		{"SELECT id FROM tm WHERE NULLIF(" + df + ", INTERVAL '1 day') IS NULL", "0A000", "compared with an interval (=)"},
		{"SELECT id FROM tm WHERE (" + df + ") + INTERVAL '1 day' > INTERVAL '2 days'", "0A000", "combined with an interval (+)"},
		{"SELECT id FROM tm WHERE INTERVAL '1 day' - (" + df + ") > INTERVAL '2 days'", "0A000", "combined with an interval (-)"},
		{"SELECT id FROM tm WHERE COALESCE(" + df + ", INTERVAL '0 days') > INTERVAL '2 days'", "0A000", "matched with an interval in COALESCE"},
		{"SELECT id FROM tm WHERE GREATEST(" + df + ", INTERVAL '1 day') > INTERVAL '2 days'", "0A000", "matched with an interval in GREATEST"},
		{"SELECT id FROM tm WHERE CASE WHEN id > 1 THEN " + df + " ELSE INTERVAL '1 day' END > INTERVAL '2 days'", "0A000", "matched with an interval in CASE"},
		// PostgreSQL's own refusal: date - date is an integer there too.
		{"SELECT id FROM tm WHERE c_date - DATE '2024-01-01' > INTERVAL '30 days'", "42883", "operator does not exist: bigint > interval"},
		// Controls: the workaround, and the difference used as a value
		// (temporal r16's millisecond double, unchanged).
		{"SELECT id FROM tm WHERE c_ts > TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '30 days'", "", ""},
		{"SELECT a.id FROM tm a JOIN tm b ON a.c_ts > b.c_ts + INTERVAL '30 days'", "", ""},
		{"SELECT " + df + " AS d FROM tm", "", ""},
		{"SELECT id FROM tm WHERE " + df + " > 1000", "", ""},
		{"SELECT id FROM tm WHERE extract(epoch FROM " + df + ") > 1000", "", ""},
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
}
