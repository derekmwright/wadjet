package pgwire

// #711 — a simple-protocol string carrying MORE THAN ONE statement.
//
// The census (dml_census_test.go) carries the values and the table states.
// What it cannot see is the PROTOCOL: a client reads message types, and "two
// statements ran" means TWO CommandCompletes, while "the connection is still
// in step" means exactly ONE ReadyForQuery — a second one is invisible to the
// client that was waiting for the first and is consumed as the answer to its
// NEXT statement, which is the desync `dispatch`'s own comment describes.
//
// These are therefore raw-socket tests over message TYPES, not values.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

func multiStmtDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Query(ctx, "CREATE TABLE m711 (id INT64, n INT64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO m711 (id, n) VALUES (1, 10), (2, 20), (3, 30)"); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestSimpleProtocolRunsEachStatementAndEndsWithOneReadyForQuery is the
// protocol half of #711: N CommandCompletes, one Z, and the RowDescription /
// DataRow messages of every SELECT in the sequence.
func TestSimpleProtocolRunsEachStatementAndEndsWithOneReadyForQuery(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		// want is the exact message-type sequence. C = CommandComplete,
		// T = RowDescription, D = DataRow, E = ErrorResponse, I = EmptyQuery,
		// Z = ReadyForQuery.
		want string
	}{
		{name: "one statement is unchanged",
			sql:  "DELETE FROM m711 WHERE id = 1",
			want: "CZ"},
		{name: "two DML statements",
			sql:  "DELETE FROM m711 WHERE id = 1; DELETE FROM m711 WHERE id = 2",
			want: "CCZ"},
		{name: "three DML statements",
			sql: "DELETE FROM m711 WHERE id = 1; DELETE FROM m711 WHERE id = 2; " +
				"DELETE FROM m711 WHERE id = 3",
			want: "CCCZ"},
		{name: "DML then SELECT",
			sql:  "DELETE FROM m711 WHERE id = 1; SELECT id FROM m711 ORDER BY id",
			want: "CTDDCZ"},
		{name: "SELECT then DML",
			sql:  "SELECT id FROM m711 ORDER BY id; DELETE FROM m711 WHERE id = 1",
			want: "TDDDCCZ"},
		{name: "two SELECTs",
			sql:  "SELECT id FROM m711 WHERE id = 1; SELECT id FROM m711 WHERE id = 2",
			want: "TDCTDCZ"},
		// AN ERROR STOPS THE SEQUENCE. The first statement ran, the second
		// reported, and the third never happened.
		{name: "an error stops the sequence",
			sql: "DELETE FROM m711 WHERE id = 1; DELETE FROM m711 WHERE nosuchcol = 1; " +
				"DELETE FROM m711 WHERE id = 3",
			want: "CEZ"},
		// THE WHOLE STRING IS PARSED FIRST, so a syntax error anywhere means
		// NO statement runs: one ErrorResponse and no CommandComplete at all.
		{name: "a syntax error anywhere runs nothing",
			sql:  "DELETE FROM m711 WHERE id = 1; ZZZ NOT SQL",
			want: "EZ"},
		{name: "a syntax error FIRST runs nothing",
			sql:  "ZZZ NOT SQL; DELETE FROM m711 WHERE id = 1",
			want: "EZ"},
		// The BI-tool prefixes this door answers without the parser have to
		// survive the pre-parse pass, or every session-setup script breaks.
		{name: "BEGIN, a statement, COMMIT",
			sql:  "BEGIN; DELETE FROM m711 WHERE id = 1; COMMIT",
			want: "CCCZ"},
		{name: "SET then a statement",
			sql:  "SET application_name = 'x'; SELECT id FROM m711 WHERE id = 1",
			want: "CTDCZ"},
		// Semicolons that are not separators.
		{name: "trailing semicolon",
			sql:  "DELETE FROM m711 WHERE id = 1;",
			want: "CZ"},
		{name: "doubled semicolons",
			sql:  "DELETE FROM m711 WHERE id = 1;; DELETE FROM m711 WHERE id = 2;;",
			want: "CCZ"},
		{name: "an empty string is one EmptyQueryResponse",
			sql:  "",
			want: "IZ"},
		{name: "only semicolons is one EmptyQueryResponse",
			sql:  " ; ; ",
			want: "IZ"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startTestServer(t, multiStmtDB(t))
			c := newPGClient(t, srv.Addr())
			c.startup("wadjet", "wadjet")
			c.writeMsg('Q', append([]byte(tc.sql), 0))
			got, errText := readUntilReady(t, c, 10*time.Second)
			if got != tc.want {
				t.Errorf("%q\n  message types %q, want %q (error text: %q)",
					tc.sql, got, tc.want, errText)
			}
			// The assertion a Z COUNT cannot make: a second, stale Z sits in
			// the socket and is read as the answer to the NEXT statement.
			assertNothingPending(t, c, tc.name)
		})
	}
}

// TestSimpleProtocolMultiStatementTagsAreOnePerStatement: the tags themselves,
// in order. The message-type gate above cannot see a CommandComplete that
// names the wrong statement or the wrong count.
func TestSimpleProtocolMultiStatementTagsAreOnePerStatement(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want []string
	}{
		{name: "two DELETEs", want: []string{"DELETE 1", "DELETE 1"},
			sql: "DELETE FROM m711 WHERE id = 1; DELETE FROM m711 WHERE id = 2"},
		{name: "two INSERTs", want: []string{"INSERT 0 1", "INSERT 0 1"},
			sql: "INSERT INTO m711 (id, n) VALUES (7, 70); INSERT INTO m711 (id, n) VALUES (8, 80)"},
		{name: "UPDATE then DELETE", want: []string{"UPDATE 1", "DELETE 1"},
			sql: "UPDATE m711 SET n = 99 WHERE id = 1; DELETE FROM m711 WHERE id = 2"},
		{name: "DELETE then SELECT", want: []string{"DELETE 1", "SELECT 2"},
			sql: "DELETE FROM m711 WHERE id = 1; SELECT id FROM m711"},
		{name: "a DELETE that matches nothing still reports", want: []string{"DELETE 0", "DELETE 1"},
			sql: "DELETE FROM m711 WHERE id = 99; DELETE FROM m711 WHERE id = 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startTestServer(t, multiStmtDB(t))
			c := newPGClient(t, srv.Addr())
			c.startup("wadjet", "wadjet")
			c.writeMsg('Q', append([]byte(tc.sql), 0))

			var tags []string
			deadline := time.Now().Add(10 * time.Second)
			for {
				if err := c.conn.SetReadDeadline(deadline); err != nil {
					t.Fatal(err)
				}
				typ, data, err := c.readMsg()
				if err != nil {
					t.Fatalf("reading: %v", err)
				}
				switch typ {
				case 'C':
					tags = append(tags, strings.TrimRight(string(data), "\x00"))
				case 'E':
					t.Fatalf("%q: unexpected error %q", tc.sql, c.parseError(data))
				}
				if typ == 'Z' {
					_ = c.conn.SetReadDeadline(time.Time{})
					break
				}
			}
			if len(tags) != len(tc.want) {
				t.Fatalf("%q\n  %d command tags %v, want %d %v", tc.sql, len(tags), tags, len(tc.want), tc.want)
			}
			for i := range tags {
				if tags[i] != tc.want[i] {
					t.Errorf("%q\n  tag %d is %q, want %q (all: %v)", tc.sql, i, tags[i], tc.want[i], tc.want)
				}
			}
		})
	}
}

// TestExtendedProtocolRefusesMultipleCommands: PostgreSQL answers a
// multi-statement string 42601 `cannot insert multiple commands into a
// prepared statement` on every entry point that is not the simple query
// protocol, measured against 17.11 through pgx in QueryExecModeExec.
//
// The REFUSAL'S PROTOCOL SHAPE matters as much as its class. Parse reports and
// enters the error state; every message up to Sync is discarded; Sync — the
// only message that owes a Z in this sub-protocol — delivers exactly one. A
// ReadyForQuery from the Parse handler would be the spurious-Z desync.
func TestExtendedProtocolRefusesMultipleCommands(t *testing.T) {
	srv := startTestServer(t, multiStmtDB(t))

	for _, tc := range []struct {
		name, sql string
	}{
		{"two INSERTs", "INSERT INTO m711 (id, n) VALUES (7, 70); INSERT INTO m711 (id, n) VALUES (8, 80)"},
		{"two DELETEs", "DELETE FROM m711 WHERE id = 1; DELETE FROM m711 WHERE id = 2"},
		{"two SELECTs", "SELECT id FROM m711; SELECT n FROM m711"},
		{"DELETE then SELECT", "DELETE FROM m711 WHERE id = 1; SELECT id FROM m711"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newPGClient(t, srv.Addr())
			c.startup("wadjet", "wadjet")

			// Parse, Bind, Describe, Execute, Sync — the sequence pgx sends.
			var parse []byte
			parse = append(parse, 0) // unnamed statement
			parse = append(parse, tc.sql...)
			parse = append(parse, 0)
			parse = append(parse, 0, 0) // no parameter types
			c.writeMsg('P', parse)
			c.writeMsg('B', []byte{0, 0, 0, 0, 0, 0, 0, 0})
			c.writeMsg('D', append([]byte{'P'}, 0))
			c.writeMsg('E', append(append([]byte{0}, 0, 0, 0, 0), 0))
			c.writeMsg('S', nil)

			got, errText := readUntilReady(t, c, 10*time.Second)
			// One ErrorResponse from Parse, then everything up to Sync
			// discarded, then Sync's single Z. Nothing in between: no
			// ParseComplete, no BindComplete, no CommandComplete.
			if got != "EZ" {
				t.Errorf("%q\n  message types %q, want \"EZ\" (error: %q)", tc.sql, got, errText)
			}
			if !strings.Contains(errText, "cannot insert multiple commands into a prepared statement") {
				t.Errorf("%q\n  error %q\n  want PostgreSQL's message: cannot insert multiple commands into a prepared statement",
					tc.sql, errText)
			}
			assertNothingPending(t, c, tc.name)

			// AND THE CONNECTION IS STILL USABLE. A refusal that desynced the
			// stream would show up here as the wrong answer to a good query.
			cols, rows, tag := c.simpleQuery("SELECT id FROM m711 WHERE id = 1")
			if len(cols) != 1 || len(rows) != 1 || tag != "SELECT 1" {
				t.Errorf("after the refusal the connection answered cols=%v rows=%v tag=%q",
					cols, rows, tag)
			}
		})
	}
}

// TestExtendedProtocolStillAcceptsOneStatement is the boundary from the other
// side: the refusal must not catch a single statement with a semicolon in it.
func TestExtendedProtocolStillAcceptsOneStatement(t *testing.T) {
	ctx := context.Background()
	srv := startTestServer(t, multiStmtDB(t))
	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, tc := range []struct{ sql, tag string }{
		{"DELETE FROM m711 WHERE id = 99", "DELETE 0"},
		{"DELETE FROM m711 WHERE id = 99;", "DELETE 0"},
		{"DELETE FROM m711 WHERE id = 99;;", "DELETE 0"},
		{"UPDATE m711 SET n = 1 WHERE id = 99", "UPDATE 0"},
	} {
		tag, err := censusWireExec(ctx, conn, tc.sql, pgx.QueryExecModeExec)
		if err != nil {
			t.Errorf("%q: %v", tc.sql, err)
			continue
		}
		if tag != tc.tag {
			t.Errorf("%q reported %q, want %q", tc.sql, tag, tc.tag)
		}
	}
}
