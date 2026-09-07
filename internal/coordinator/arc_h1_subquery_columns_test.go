package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A SUBQUERY USED WHERE ONE COLUMN IS REQUIRED RETURNS ONE COLUMN — the other
// half of #875's site.
//
// `expr.ScalarSubqueryValue` reduced a subquery's row to one value with
// `for _, v := range rows[0] { return v }`, and its own comment recorded the
// multi-column case as "deliberately not decided here". A Go map's iteration
// order is randomized per range statement, so `SELECT (SELECT id, c_i64 FROM
// t ORDER BY id LIMIT 1)` answered a DIFFERENT COLUMN on different runs of the
// same query — the same defect the hidden ORDER BY key produced, over a shape
// no trim can fix, because the SELECT list really does have two items.
//
// PostgreSQL 17 refuses both spellings at analysis time, measured:
//
//	SELECT (SELECT id, c_i64 FROM h1_typemx ORDER BY id LIMIT 1)
//	  ERROR: subquery must return only one column          (42601)
//	SELECT COUNT(*) FROM h1_decpair d WHERE d.id IN (SELECT id, c_i64 FROM h1_typemx)
//	  ERROR: subquery has too many columns                 (42601)
//
// Loud beats plausible: an arbitrary column is not a smaller answer than a
// refusal, it is a wrong one.
func TestArcH1AMultiColumnSubqueryIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	for _, tc := range []struct {
		name, sql, want string
	}{
		{"scalar-two-columns",
			`SELECT d.id AS did, (SELECT t.id, t.c_i64 FROM typemx t ORDER BY t.id LIMIT 1) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-star",
			`SELECT d.id AS did, (SELECT * FROM decpair x WHERE x.id = 1) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-two-aggregates",
			`SELECT d.id AS did, (SELECT MIN(t.id), MAX(t.id) FROM typemx t) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"in-two-columns",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN (SELECT t.id, t.c_i64 FROM typemx t)`,
			"subquery has too many columns"},
		{"not-in-two-columns",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id NOT IN (SELECT t.id, t.c_i64 FROM typemx t)`,
			"subquery has too many columns"},
		{"correlated-in-two-columns",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
				`(SELECT t.id, t.c_i64 FROM typemx t WHERE t.id = d.id)`,
			"subquery has too many columns"},

		// TWO COLUMNS UNDER ONE NAME. The row a subquery hands its reducer is
		// a Go MAP keyed by column name, and PostgreSQL lets two output
		// columns share one — `SELECT ABS(a), ABS(b)` is two columns both
		// called `abs` — so the map held ONE entry for TWO columns and the
		// count walked straight through this refusal. Measured before the
		// fix: `12.7500` on every arm and every door where PostgreSQL raises,
		// and `n | 0` for the IN twin. The count now comes from the SCHEMA,
		// which is positional and cannot collapse.
		{"scalar-two-columns-one-name",
			`SELECT d.id AS did, (SELECT ABS(x.a), ABS(x.b) FROM decpair x WHERE x.id = 1) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-two-columns-one-alias",
			`SELECT d.id AS did, (SELECT x.id AS q, x.c_i64 AS q FROM typemx x WHERE x.id = 3) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-the-same-column-twice",
			`SELECT d.id AS did, (SELECT x.id, x.id FROM typemx x WHERE x.id = 3) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-star-over-a-two-column-relation",
			`SELECT d.id AS did, (SELECT * FROM typemx_dim x WHERE x.k = 1) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-a-union-of-two-column-arms",
			`SELECT d.id AS did, (SELECT x.id, x.c_i64 FROM typemx x WHERE x.id = 3 ` +
				`UNION ALL SELECT y.id, y.c_i64 FROM typemx y WHERE y.id = 4) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"in-two-columns-one-name",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
				`(SELECT x.id AS q, x.c_i64 AS q FROM typemx x WHERE x.id < 5)`,
			"subquery has too many columns"},
		{"in-star-over-a-two-column-relation",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN (SELECT * FROM typemx_dim)`,
			"subquery has too many columns"},

		// ZERO ROWS. PostgreSQL raises this during PARSE ANALYSIS, so it does
		// not depend on what the subquery would have returned: an empty
		// multi-column subquery is 42601 there too. Counting the returned
		// rows cannot reach that case — there is nothing to count — and the
		// first cut answered SQL NULL for the scalar and a row count for the
		// IN. The arity comes from the subquery's own PLAN now, asked before
		// it runs.
		{"scalar-two-columns-no-rows",
			`SELECT d.id AS did, (SELECT x.id, x.c_i64 FROM typemx x WHERE x.id < 0) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"scalar-two-columns-one-name-no-rows",
			`SELECT d.id AS did, (SELECT ABS(x.a), ABS(x.b) FROM decpair x WHERE x.id < 0) AS v ` +
				`FROM decpair d WHERE d.id < 2`,
			"subquery must return only one column"},
		{"in-two-columns-no-rows",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
				`(SELECT x.id, x.c_i64 FROM typemx x WHERE x.id < 0)`,
			"subquery has too many columns"},
		{"correlated-in-two-columns-no-rows",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
				`(SELECT t.id, t.c_i64 FROM typemx t WHERE t.id = d.id AND t.id < 0)`,
			"subquery has too many columns"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				_, rows, err := arm.run(tc.sql)
				if err == nil {
					t.Errorf("%s arm ANSWERED a shape PostgreSQL 17 refuses with 42601, by "+
						"picking one column out of a Go map: %d rows\n  SQL: %s",
						arm.name, len(rows), tc.sql)
					continue
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Errorf("%s arm refused with %q\n  want a refusal carrying PostgreSQL's "+
						"own sentence %q\n  SQL: %s", arm.name, err.Error(), tc.want, tc.sql)
				}
			}
		})
	}

	// THE CONTROLS: one column is one column, however it is spelled, and an
	// EXISTS never reads a value at all — so a multi-column EXISTS keeps
	// answering, which is PostgreSQL's rule too (`EXISTS (SELECT 1, 2)` is
	// legal there).
	t.Run("ctl-one-column-and-exists", func(t *testing.T) {
		for _, tc := range []struct{ name, sql, want string }{
			{"one-column-scalar",
				`SELECT d.id AS did, (SELECT MAX(t.id) FROM typemx t) AS v ` +
					`FROM decpair d WHERE d.id < 2`,
				`did,v | 1,4999`},
			{"one-column-in",
				`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN (SELECT t.id FROM typemx t)`,
				`n | 9`},
			// A ONE-column subquery over an EMPTY input still answers, which
			// is the other side of the zero-row rule: the refusal is about
			// the SELECT list's arity and about nothing else.
			{"one-column-scalar-no-rows",
				`SELECT d.id AS did, (SELECT MAX(t.id) FROM typemx t WHERE t.id < 0) AS v ` +
					`FROM decpair d WHERE d.id < 2`,
				`did,v | 1,NULL`},
			{"one-column-in-no-rows",
				`SELECT COUNT(*) AS n FROM decpair d WHERE d.id IN ` +
					`(SELECT t.id FROM typemx t WHERE t.id < 0)`,
				`n | 0`},
			{"exists-over-two-columns",
				`SELECT COUNT(*) AS n FROM decpair d WHERE EXISTS ` +
					`(SELECT t.id, t.c_i64 FROM typemx t WHERE t.id = d.id)`,
				`n | 9`},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				for _, arm := range arms {
					cols, rows, err := arm.run(tc.sql)
					if err != nil {
						t.Errorf("%s arm refused a one-column subquery: %v\n  SQL: %s",
							arm.name, err, tc.sql)
						continue
					}
					if got := e3Render(cols, rows); got != tc.want {
						t.Errorf("%s arm: %s, want %s\n  SQL: %s", arm.name, got, tc.want, tc.sql)
					}
				}
			})
		}
	})
}
