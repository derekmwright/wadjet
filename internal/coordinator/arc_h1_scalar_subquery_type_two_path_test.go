package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A SCALAR SUBQUERY'S VALUE IS THE SAME VALUE AT THE SAME TYPE AS THE PLAIN
// SPELLING — #874 (and #714's third box), on FOUR ARMS.
//
//	SELECT id, (SELECT MAX(c_i64) FROM typemx) AS mx …   mx = "4999014997" (string)
//	SELECT MAX(c_i64) AS mx FROM typemx                  mx = 4999014997  (int64)
//	SELECT id, (SELECT MAX(c_i64) FROM typemx) + 1 AS mx mx = 4.999014998e+09
//
// `nodeDeclaredType` had no `*plansql.SubqueryNode` arm, so a SELECT-list
// scalar subquery fell through to Undecided and the projection allocated its
// STRING output vector; the const-arith fold saw the same Undecided operand
// and took the FLOAT rung. PostgreSQL 17 declares bigint for both.
//
// The gate compares the SUBQUERY spelling to the PLAIN spelling of the same
// value, cell by cell, rather than to a literal — that is the invariant, it
// holds for every type this engine has whatever the aggregate's own width
// rules say, and it cannot be satisfied by a fix that moves one path only.
// The declared OID is asserted separately on the WIRE
// (internal/server/pgwire/arc_h1_scalar_subquery_wire_test.go), which is the
// half a value oracle cannot see.
func TestArcH1AScalarSubqueryAnswersItsOwnType(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	// Each pair is the SAME value spelled twice: once read through a
	// SELECT-list scalar subquery, once directly. The Go TYPE of the cell is
	// compared as well as its rendering, because a bigint arriving as a Go
	// string is exactly what #874 is.
	for _, tc := range []struct{ name, sub, plain string }{
		{"bigint", `SELECT (SELECT MAX(c_i64) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_i64) AS v FROM typemx`},
		{"int32", `SELECT (SELECT MAX(c_i32) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_i32) AS v FROM typemx`},
		{"float64", `SELECT (SELECT MAX(c_f64) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_f64) AS v FROM typemx`},
		{"string", `SELECT (SELECT MAX(c_str) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_str) AS v FROM typemx`},
		{"timestamp", `SELECT (SELECT MAX(c_ts) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_ts) AS v FROM typemx`},
		{"bool", `SELECT (SELECT MAX(c_bool) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_bool) AS v FROM typemx`},
		{"decimal", `SELECT (SELECT MAX(a) FROM decpair) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(a) AS v FROM decpair`},
		{"date", `SELECT (SELECT MAX(c_date) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_date) AS v FROM typemx`},
		{"uuid", `SELECT (SELECT MAX(c_uuid) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_uuid) AS v FROM typemx`},
		{"ipv4", `SELECT (SELECT MAX(c_ipv4) FROM typemx) AS v FROM decpair WHERE id = 1`,
			`SELECT MAX(c_ipv4) AS v FROM typemx`},
		// A BARE COLUMN subquery, not an aggregate: the same question with no
		// aggregate result type in the way.
		{"bare-column", `SELECT (SELECT c_i64 FROM typemx WHERE id = 3) AS v FROM decpair WHERE id = 1`,
			`SELECT c_i64 AS v FROM typemx WHERE id = 3`},
		{"bare-string-column", `SELECT (SELECT c_str FROM typemx WHERE id = 3) AS v FROM decpair WHERE id = 1`,
			`SELECT c_str AS v FROM typemx WHERE id = 3`},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				subCols, subRows, err := arm.run(tc.sub)
				if err != nil {
					t.Fatalf("%s arm refused the subquery spelling: %v\n  SQL: %s",
						arm.name, err, tc.sub)
				}
				plainCols, plainRows, err := arm.run(tc.plain)
				if err != nil {
					t.Fatalf("%s arm refused the plain spelling: %v\n  SQL: %s",
						arm.name, err, tc.plain)
				}
				if got, want := e3Render(subCols, subRows), e3Render(plainCols, plainRows); got != want {
					t.Errorf("%s arm: the subquery spelling answers %s and the plain one %s\n"+
						"  %s\n  %s", arm.name, got, want, tc.sub, tc.plain)
				}
				if got, want := h1CellType(subRows), h1CellType(plainRows); got != want {
					t.Errorf("%s arm: the subquery spelling boxes its value as %s and the plain "+
						"one as %s — a scalar subquery is one value with ONE type\n  %s\n  %s",
						arm.name, got, want, tc.sub, tc.plain)
				}
			}
		})
	}

	// #714's THIRD BOX, closed by the same declaration: with the subquery
	// Undecided the const-arith fold took the FLOAT rung, so `+ 1` over a
	// bigint answered 4.999014998e+09. PostgreSQL 17 answers 4999014998 and
	// declares bigint.
	t.Run("arithmetic-over-a-scalar-subquery-is-exact", func(t *testing.T) {
		for _, tc := range []struct{ name, sql, want, typ string }{
			{"plus-one", `SELECT (SELECT MAX(c_i64) FROM typemx) + 1 AS v FROM decpair WHERE id = 1`,
				`v | 4999014998`, "int64"},
			{"times-two", `SELECT (SELECT MAX(c_i64) FROM typemx) * 2 AS v FROM decpair WHERE id = 1`,
				`v | 9998029994`, "int64"},
			{"minus-a-column", `SELECT (SELECT MAX(c_i64) FROM typemx) - id AS v FROM decpair WHERE id = 1`,
				`v | 4999014996`, "int64"},
		} {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				for _, arm := range arms {
					cols, rows, err := arm.run(tc.sql)
					if err != nil {
						t.Fatalf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					}
					if got := e3Render(cols, rows); got != tc.want {
						t.Errorf("%s arm: %s, want %s (live PostgreSQL 17)\n  SQL: %s",
							arm.name, got, tc.want, tc.sql)
					}
					if got := h1CellType(rows); got != tc.typ {
						t.Errorf("%s arm: boxed as %s, want %s — the const-arith fold reads the "+
							"subquery's declaration\n  SQL: %s", arm.name, got, tc.typ, tc.sql)
					}
				}
			})
		}
	})
}

// h1CellType is the Go type of the first row's LAST cell, which is the one
// every pair above puts the value under.
func h1CellType(rows [][]any) string {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "-"
	}
	return fmt.Sprintf("%T", rows[0][len(rows[0])-1])
}
