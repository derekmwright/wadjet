// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"strings"
	"testing"
)

// The WIRE answer to a query-sourced write: the command tag, over both
// protocols, and no result set under it (#1024).
//
// The tag is not decoration — for a write it IS the statement's whole answer,
// and #816 is the precedent: over the extended protocol, the protocol pgx,
// JDBC, psycopg and every ORM use, wadjet reported `SELECT 1` for every INSERT,
// UPDATE, DELETE and MERGE, because those statements fell through to the query
// path and the tag described the one row the embedded door boxes a write's
// answer into. A CTAS routed the same way would answer `SELECT 1` for a
// statement PostgreSQL answers `SELECT 3`.
//
// Both protocols are run because the routing predicate is shared and the bug it
// exists to prevent lived on only one of them. Each tag was measured on
// PostgreSQL 17.11.
func TestAQuerySourcedWriteSendsPostgresCommandTag(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	cases := []struct {
		name string
		sql  string
		tag  string
	}{
		// `CREATE TABLE t AS SELECT …` with rows → `SELECT <n>`.
		{"CreateWithData", `CREATE TABLE w_a AS SELECT id, name FROM users`, "SELECT 3"},
		{"CreateZeroRows", `CREATE TABLE w_b AS SELECT id FROM users WHERE id < 0`, "SELECT 0"},
		// … and without running the query → the bare `CREATE TABLE AS`.
		{"CreateWithNoData", `CREATE TABLE w_c AS SELECT id FROM users WITH NO DATA`, "CREATE TABLE AS"},
		{"CreateIfNotExistsSkips", `CREATE TABLE IF NOT EXISTS w_a AS SELECT id FROM users`, "CREATE TABLE AS"},
		// The append → `INSERT 0 <n>`, oid field and all.
		{"InsertSelect", `INSERT INTO w_a SELECT id, name FROM users`, "INSERT 0 3"},
		{"InsertSelectZeroRows", `INSERT INTO w_a SELECT id, name FROM users WHERE id < 0`, "INSERT 0 0"},
		{"InsertSelectColumnList", `INSERT INTO w_a (name, id) SELECT name, id FROM users`, "INSERT 0 3"},
	}

	t.Run("SimpleProtocol", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				mr := conn.Exec(ctx, tc.sql)
				results, err := mr.ReadAll()
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if len(results) != 1 {
					t.Fatalf("%d results, want 1", len(results))
				}
				if got := results[0].CommandTag.String(); got != tc.tag {
					t.Errorf("CommandComplete %q, want %q", got, tc.tag)
				}
				if n := len(results[0].Rows); n != 0 {
					t.Errorf("%d rows under a write's tag, want none", n)
				}
			})
		}
	})

	// A second server, so the extended protocol runs the same statements from
	// the same starting state rather than over the first run's tables.
	_, srv2 := setupRealDB(t)
	conn2 := connectPgconn(t, srv2.Addr())
	t.Run("ExtendedProtocol", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				// Describe FIRST, the way a driver does: a write has no result
				// shape, and describing it must not EXECUTE it (#816's other
				// half — Describe ran the write and Execute ran it again).
				if _, err := conn2.Prepare(ctx, "", tc.sql, nil); err != nil {
					t.Fatalf("Prepare: %v", err)
				}
				res := conn2.ExecParams(ctx, tc.sql, nil, nil, nil, nil).Read()
				if res.Err != nil {
					t.Fatalf("%s: %v", tc.sql, res.Err)
				}
				if got := res.CommandTag.String(); got != tc.tag {
					t.Errorf("CommandComplete %q, want %q", got, tc.tag)
				}
				if n := len(res.Rows); n != 0 {
					t.Errorf("%d rows under a write's tag, want none", n)
				}
			})
		}
	})

	// Describe on the extended protocol must not have EXECUTED the write.
	// That was #816's other half, and it is invisible to a tag assertion: a
	// Describe that runs the statement and an Execute that runs it again both
	// report the right tag over a table that holds twice the rows.
	//
	// w_a on the second server: 3 from the create, then +3, +0, +3 from the
	// three appends = 9. Any doubling is a Describe that executed.
	res := conn2.ExecParams(ctx, `SELECT COUNT(*) FROM w_a`, nil, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if got := string(res.Rows[0][0]); got != "9" {
		t.Errorf("w_a holds %s rows, want 9 — a Describe that EXECUTED the write "+
			"would have run each of these statements twice", got)
	}
}

// A declared CREATE TABLE still takes the query path and keeps its long-
// standing tag: the write predicate's boundary, attempted from the other side.
func TestADeclaredCreateTableIsNotRoutedAsAWrite(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	res := conn.ExecParams(ctx, `CREATE TABLE declared (a INT64, b STRING)`, nil, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatalf("declared CREATE TABLE: %v", res.Err)
	}
	if got := res.CommandTag.String(); !strings.HasPrefix(got, "SELECT") {
		t.Errorf("CommandComplete %q; the declared form keeps the query path's tag "+
			"(a pre-existing divergence from PostgreSQL's `CREATE TABLE`, recorded in ADR-0012 "+
			"and untouched by #1024)", got)
	}
}
