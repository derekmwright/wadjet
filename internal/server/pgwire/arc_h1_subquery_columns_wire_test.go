package pgwire

// A MULTI-COLUMN SUBQUERY IS 42601 ON THE WIRE — #875's second face, and the
// spelling that defeated the first cut of it.
//
// The row a subquery hands its reducer is a Go MAP keyed by column NAME, and
// PostgreSQL lets two output columns share one: `SELECT ABS(a), ABS(b)` is two
// columns both called `abs`. So the map held ONE entry for TWO columns, the
// guard counted the map, and `SELECT (SELECT ABS(a), ABS(b) FROM t)` ANSWERED
// `12.7500` on every door where PostgreSQL 17 raises
// `subquery must return only one column`, SQLSTATE 42601.
//
// The SQLSTATE is the half a value oracle cannot see, which is why it is
// asserted here: a client branches on the code.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestArcH1AMultiColumnSubqueryIs42601OnTheWire(t *testing.T) {
	srv := setupH1ScalarDB(t)
	for _, c := range []struct{ name, sql, msg string }{
		// TWO COLUMNS UNDER ONE NAME — the spelling the map could not count.
		{"two_columns_one_name",
			`SELECT (SELECT ABS(x.c_dec), ABS(x.c_f64) FROM h1scal x WHERE x.id = 1) AS v ` +
				`FROM h1scal WHERE id = 1`,
			"subquery must return only one column"},
		{"two_columns_one_alias",
			`SELECT (SELECT x.id AS q, x.c_i64 AS q FROM h1scal x WHERE x.id = 2) AS v ` +
				`FROM h1scal WHERE id = 1`,
			"subquery must return only one column"},
		{"the_same_column_twice",
			`SELECT (SELECT x.id, x.id FROM h1scal x WHERE x.id = 2) AS v FROM h1scal WHERE id = 1`,
			"subquery must return only one column"},
		{"in_two_columns_one_name",
			`SELECT COUNT(*) AS n FROM h1scal d WHERE d.id IN ` +
				`(SELECT x.id AS q, x.c_i64 AS q FROM h1scal x)`,
			"subquery has too many columns"},
		// DISTINCT names, which the map could count — kept so the pair cannot
		// drift apart.
		{"two_columns_distinct_names",
			`SELECT (SELECT x.id, x.c_i64 FROM h1scal x WHERE x.id = 2) AS v ` +
				`FROM h1scal WHERE id = 1`,
			"subquery must return only one column"},
		// ZERO ROWS. PostgreSQL raises this in parse analysis, so it does not
		// depend on what the subquery would have returned; counting the rows
		// cannot reach the case and the first cut answered NULL here.
		{"two_columns_no_rows",
			`SELECT (SELECT x.id, x.c_i64 FROM h1scal x WHERE x.id < 0) AS v ` +
				`FROM h1scal WHERE id = 1`,
			"subquery must return only one column"},
		{"two_columns_one_name_no_rows",
			`SELECT (SELECT ABS(x.c_dec), ABS(x.c_f64) FROM h1scal x WHERE x.id < 0) AS v ` +
				`FROM h1scal WHERE id = 1`,
			"subquery must return only one column"},
		{"in_two_columns_no_rows",
			`SELECT COUNT(*) AS n FROM h1scal d WHERE d.id IN ` +
				`(SELECT x.id, x.c_i64 FROM h1scal x WHERE x.id < 0)`,
			"subquery has too many columns"},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err == nil {
				t.Fatalf("the wire answered a shape PostgreSQL 17 refuses: %v\n  SQL: %s",
					res.Rows, c.sql)
			}
			var pge *pgconn.PgError
			if !errors.As(res.Err, &pge) {
				t.Fatalf("not a PgError: %v", res.Err)
			}
			if pge.Code != "42601" {
				t.Errorf("SQLSTATE %s, want 42601 (PostgreSQL 17)\n  SQL: %s\n  %v",
					pge.Code, c.sql, res.Err)
			}
			if !strings.Contains(pge.Message, c.msg) {
				t.Errorf("message %q does not carry PostgreSQL's own sentence %q\n  SQL: %s",
					pge.Message, c.msg, c.sql)
			}
		})
	}

	// THE CONTROLS: one column is one column, and it still answers — over an
	// EMPTY input too, which is what says the refusal is about the SELECT
	// list's arity and about nothing else.
	t.Run("ctl_one_column", func(t *testing.T) {
		conn := connectPgconn(t, srv.Addr())
		res := conn.ExecParams(context.Background(),
			`SELECT (SELECT MAX(c_i64) FROM h1scal) AS v FROM h1scal WHERE id = 1`,
			nil, nil, nil, []int16{0}).Read()
		if res.Err != nil {
			t.Fatalf("a one-column subquery was refused: %v", res.Err)
		}
		if got := string(res.Rows[0][0]); got != "4999014997" {
			t.Errorf("wire sent %q, want 4999014997", got)
		}
	})
	t.Run("ctl_one_column_no_rows", func(t *testing.T) {
		conn := connectPgconn(t, srv.Addr())
		res := conn.ExecParams(context.Background(),
			`SELECT (SELECT MAX(c_i64) FROM h1scal WHERE id < 0) AS v FROM h1scal WHERE id = 1`,
			nil, nil, nil, []int16{0}).Read()
		if res.Err != nil {
			t.Fatalf("a one-column subquery over an empty input was refused: %v", res.Err)
		}
		if res.Rows[0][0] != nil {
			t.Errorf("wire sent %q, want NULL", string(res.Rows[0][0]))
		}
	})
}
