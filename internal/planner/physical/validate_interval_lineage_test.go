// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// lineageCell is one statement and the refusal identity it must earn
// ("" for a statement that binds).
type lineageCell struct {
	why, sql, reason string
}

func runLineageCells(t *testing.T, cells []lineageCell) {
	t.Helper()
	cat := brCatalog()
	for _, c := range cells {
		err := validateColumns(context.Background(), cat, mustExtract(t, c.sql))
		got := sqlerr.ReasonOf(err)
		switch {
		case c.reason == "" && err != nil:
			t.Errorf("%s\n  %s\n  refused %v {%s}, want it to bind", c.why, c.sql, err, got)
		case c.reason != "" && (err == nil || got != c.reason || sqlerr.StateOf(err) != "0A000"):
			t.Errorf("%s\n  %s\n  got %v {%s}, want 0A000 {%s}", c.why, c.sql, err, got, c.reason)
		}
	}
}

// The boundaries an interval slot is forwarded across (#1680c, ADR-0052 §8,
// the record §9.1), each a FROM item handing the column v (beside id).
var lineageBoundaries = []struct{ name, seam, from string }{
	{"projection", expr.LineageDerived, "(SELECT id, w AS v FROM (SELECT id, CAST(customer AS INTERVAL) AS w FROM lat_ord) p0) p"},
	{"derived", expr.LineageDerived, "(SELECT id, CAST(customer AS INTERVAL) AS v FROM lat_ord) p"},
	{"cte", expr.LineageCTE, "p"},
	{"subquery", expr.LineageSubquery, "(SELECT a.id, (SELECT CAST(customer AS INTERVAL) FROM lat_ord b WHERE b.id = a.id) AS v FROM lat_ord a) p"},
	{"setop", expr.LineageSetOp, "(SELECT id, CAST(customer AS INTERVAL) AS v FROM lat_ord UNION ALL SELECT id, INTERVAL '1 day' FROM lat_ord) p"},
	{"join", expr.LineageDerived, "(SELECT a.id, b.v FROM lat_ord a JOIN (SELECT id, CAST(customer AS INTERVAL) AS v FROM lat_ord) b ON a.id = b.id) p"},
	{"star", expr.LineageStar, "(SELECT * FROM (SELECT id, CAST(customer AS INTERVAL) AS v FROM lat_ord) q) p"},
}

const lineageCTE = "WITH p AS (SELECT id, CAST(customer AS INTERVAL) AS v FROM lat_ord) "

// lineageConsumers are unsupported consumers of v, each with the refusal it
// earns: an item of its own, a container, a DISTINCT aggregate, a TIME cast.
var lineageConsumers = []struct{ list, reason string }{
	{"SELECT id, to_char(v, 'HH24:MI') AS x", "function-to_char"},
	{"SELECT id, CAST(date_trunc('hour', v) AS TEXT) AS x", "function-date_trunc"},
	{"SELECT id, CAST(ARRAY[v] AS TEXT) AS x", "container"},
	{"SELECT CAST(sum(DISTINCT v) AS TEXT) AS x", "distinct-aggregate"},
	{"SELECT id, CAST(CAST(v AS TIME) AS TEXT) AS x", "time-type"},
}

func lineageSQL(from, list string) string {
	if from == "p" {
		return lineageCTE + list + " FROM p"
	}
	return list + " FROM " + from
}

// TestIntervalLineageNamesTheConsumer: an interval column forwarded across
// every boundary reaches its consumer typed, so an unsupported consumer is
// refused under its own name; the slot is still not admitted (an
// allowlisted consumer of the forwarded column, a key, a bare read are
// refused exactly as the interim refused them), and statements without an
// interval bind.
func TestIntervalLineageNamesTheConsumer(t *testing.T) {
	var cells []lineageCell
	for _, b := range lineageBoundaries {
		for _, c := range lineageConsumers {
			cells = append(cells, lineageCell{b.name + " → " + c.reason, lineageSQL(b.from, c.list), c.reason})
		}
		// Not admitted: an allowlisted consumer, a key, a bare read of the
		// forwarded slot keep the interim's refusal of the published column.
		cells = append(cells,
			lineageCell{b.name + ": a display cast of the forwarded slot (stage D)", lineageSQL(b.from, "SELECT id, CAST(v AS TEXT) AS x"), "bare-interval-result"},
			lineageCell{b.name + ": a key over the forwarded slot (stage D)", lineageSQL(b.from, "SELECT count(*) AS n") + " GROUP BY v", "bare-interval-result"},
			lineageCell{b.name + ": the bare slot", lineageSQL(b.from, "SELECT id, v"), "bare-interval-result"},
		)
	}
	cells = append(cells,
		// Controls: no interval crosses the boundary.
		lineageCell{"control: a text column across a derived table", "SELECT id, upper(c) AS x FROM (SELECT id, customer AS c FROM lat_ord) p", ""},
		lineageCell{"control: an interval consumed inside the block", "SELECT id, x FROM (SELECT id, CAST(CAST(customer AS INTERVAL) AS TEXT) AS x FROM lat_ord) p", ""},
		lineageCell{"control: an inner column of the same name shadows the outer slot", "SELECT p.id, (SELECT upper(v) FROM (SELECT customer AS v FROM lat_ord) q LIMIT 1) AS x FROM (SELECT id FROM lat_ord) p", ""},
		// An unused CTE that publishes an interval is still refused.
		lineageCell{"an unused CTE publishing an interval", "WITH p AS (SELECT INTERVAL '1 day' AS v) SELECT 1 AS one", "bare-interval-result"},
		// A star at the top reads the slot as a result column.
		lineageCell{"a top-level star over a forwarded slot", "SELECT * FROM (SELECT id, CAST(customer AS INTERVAL) AS v FROM lat_ord) p", "bare-interval-result"},
	)
	runLineageCells(t, cells)
}

// TestIntervalLineageUnresolvedIsRefused: a star over a body whose columns
// the binder cannot enumerate, below which an interval was published, is
// refused loudly (interval-lineage-unresolved), never read without its type.
func TestIntervalLineageUnresolvedIsRefused(t *testing.T) {
	runLineageCells(t, []lineageCell{
		{"a star over an unenumerable body that published an interval",
			"SELECT * FROM (SELECT * FROM read_csv('/nonexistent/iv80.csv') r, (SELECT INTERVAL '1 day' AS v) q) z",
			"interval-lineage-unresolved"},
		{"control: the same star with no interval below it binds",
			"SELECT * FROM (SELECT * FROM read_csv('/nonexistent/iv80.csv') r, (SELECT 1 AS v) q) z", ""},
	})
}

// TestIntervalLineageNegativeControl: with forwarding disabled at one
// boundary, that boundary's unsupported consumers lose their names (the
// interim's published-column refusal answers instead), and the other
// boundaries keep theirs. The test that the lineage is what names them.
func TestIntervalLineageNegativeControl(t *testing.T) {
	cat := brCatalog()
	for _, off := range lineageBoundaries {
		func() {
			restore := expr.DisableIntervalLineageForTest(off.seam)
			defer restore()
			for _, b := range lineageBoundaries {
				err := validateColumns(context.Background(), cat, mustExtract(t, lineageSQL(b.from, lineageConsumers[0].list)))
				named := sqlerr.ReasonOf(err) == lineageConsumers[0].reason
				// Every boundary but the CTE's hands its column out through a
				// derived table as well, so forwarding off at the derived
				// table silences them all.
				dependsOnSeam := b.seam == off.seam || (off.seam == expr.LineageDerived && b.name != "cte")
				if dependsOnSeam && named {
					t.Errorf("forwarding off at %s: %s still names %s", off.seam, b.name, lineageConsumers[0].reason)
				}
				if !dependsOnSeam && !named {
					t.Errorf("forwarding off at %s: %s lost its name (%v)", off.seam, b.name, err)
				}
			}
		}()
	}
}

// TestIntervalIntersectionsAreRefused: the wrong-today intersections of an
// interval with the TIME and timestamptz paths, a precision typmod, a CHAR
// display and a sub-millisecond literal are refused under their names
// (ADR-0052 §5; the record §11); the statements beside them that answer
// right on main still bind.
func TestIntervalIntersectionsAreRefused(t *testing.T) {
	runLineageCells(t, []lineageCell{
		{"TIME + interval answered NULL", "SELECT CAST(TIME '23:00:00' + INTERVAL '2 hours' AS TEXT) AS a", "time-type"},
		{"interval + TIME answered NULL", "SELECT CAST(INTERVAL '2 hours' + TIME '23:00:00' AS TEXT) AS a", "time-type"},
		{"TIME - interval answered NULL", "SELECT CAST(TIME '01:00:00' - INTERVAL '2 hours' AS TEXT) AS a", "time-type"},
		{"TIME - TIME answered a number of hours", "SELECT CAST(TIME '01:00:00' - TIME '23:00:00' AS TEXT) AS a", "time-type"},
		{"DATE + TIME answered a day count", "SELECT DATE '2024-01-31' + TIME '01:02:03' AS a", "time-type"},
		{"a cast of TIME to interval", "SELECT CAST(CAST(TIME '01:02:03' AS INTERVAL) AS TEXT) AS a", "time-type"},
		{"timestamptz + interval dropped the zone", "SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + INTERVAL '1 day' AS a", "timestamptz"},
		{"a timestamptz difference", "SELECT CAST(TIMESTAMPTZ '2024-01-02 00:00:00+00' - TIMESTAMPTZ '2024-01-01 00:00:00+00' AS TEXT) AS a", "timestamptz"},
		{"age over dates resolves to timestamptz", "SELECT CAST(age(DATE '2024-03-01', DATE '2024-01-31') AS TEXT) AS a", "timestamptz"},
		{"an interval precision", "SELECT CAST(CAST(INTERVAL '1 second' AS INTERVAL(3)) AS TEXT) AS a", "typmod"},
		{"an interval to CHAR(n)", "SELECT CAST(INTERVAL '1 day' AS CHAR(10)) AS a", "char-display"},
		{"a sub-millisecond timestamp in a difference", "SELECT CAST(TIMESTAMP '2024-01-01 00:00:00.000001' - TIMESTAMP '2024-01-01 00:00:00' AS TEXT) AS a", "timestamp-millisecond-carrier"},
		// Controls: right on main, not interval operations.
		{"control: a TIME literal alone", "SELECT TIME '01:02:03' AS a", ""},
		{"control: a TIME cast to text", "SELECT CAST(TIME '01:02:03' AS TEXT) AS a", ""},
		{"control: a timestamptz literal alone", "SELECT TIMESTAMPTZ '2024-01-01 00:00:00+00' AS a", ""},
		{"control: timestamp + interval", "SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 day' AS a", ""},
		{"control: a qualified literal under the display cast", "SELECT CAST(INTERVAL '5' YEAR AS TEXT) AS a", ""},
		{"control: a millisecond timestamp in a shift", "SELECT TIMESTAMP '2024-01-01 00:00:00.123' + INTERVAL '1 day' AS a", ""},
		{"control: an interval to VARCHAR", "SELECT CAST(INTERVAL '1 day' AS VARCHAR) AS a", ""},
	})
}
