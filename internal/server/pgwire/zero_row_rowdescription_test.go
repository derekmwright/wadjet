package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// #846 at the door.
//
// `SELECT *` returning zero rows sent no RowDescription at all: the simple
// protocol answered EmptyQueryResponse ('I') and the extended protocol
// answered NoData ('n'). Both are protocol statements that there is NO RESULT
// SET, not that the result set is empty — psql prints nothing for them and
// pgJDBC's executeQuery throws "No results were returned by the query" — and
// PostgreSQL sends neither for a SELECT. 'I' is its reply to an empty query
// STRING, and 'n' its reply to a statement with no tuple result (a command, or
// DML without RETURNING).
//
// The fix has two halves and this gate covers the door's: a statement that ran
// as a QUERY always gets a RowDescription, whatever it declared. The other
// half is physical.starOnlyDeclaredOutputSchema, which is what makes that
// RowDescription carry the table's columns rather than none
// (wadjet.TestStarEmptyResultDeclaresSameColumnsAsNonEmpty).

func zrSetup(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt32},
		{Name: "c1", Type: parquet.TypeString},
		{Name: "c2", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
	}}
	for _, tbl := range []string{"zrempty", "zrfull"} {
		if err := db.CreateTable(ctx, tbl, sch, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Query(ctx, "INSERT INTO zrfull VALUES (1,'a',1.5), (2,'b',2.5)"); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

// zrShapes pairs a zero-row statement with the statement that returns rows for
// the same shape, so the declaration can be compared against a live reference
// rather than a transcribed one. want is the reference's own answer.
//
// The reference for a query over the permanently-empty table is the same query
// over the populated one: `SELECT * FROM zrempty` and `SELECT * FROM zrfull`
// describe the same schema, which is exactly the claim.
func zrShapes() []struct{ name, empty, full string } {
	return []struct{ name, empty, full string }{
		{"star_empty_table", "SELECT * FROM zrempty", "SELECT * FROM zrfull"},
		{"star_where_no_match", "SELECT * FROM zrfull WHERE c0 = 999", "SELECT * FROM zrfull"},
		{"star_where_false", "SELECT * FROM zrfull WHERE false", "SELECT * FROM zrfull"},
		{"star_ordered", "SELECT * FROM zrempty ORDER BY c0", "SELECT * FROM zrfull ORDER BY c0"},
		{"star_limit", "SELECT * FROM zrempty LIMIT 10", "SELECT * FROM zrfull LIMIT 10"},
		{"star_distinct", "SELECT DISTINCT * FROM zrempty", "SELECT DISTINCT * FROM zrfull"},
		{"star_group_by_all", "SELECT * FROM zrempty GROUP BY c0, c1, c2",
			"SELECT * FROM zrfull GROUP BY c0, c1, c2"},
		{"star_derived", "SELECT * FROM (SELECT c0, c1 FROM zrfull WHERE c0 = 999) s",
			"SELECT * FROM (SELECT c0, c1 FROM zrfull) s"},
		{"star_cte", "WITH q AS (SELECT c0, c1 FROM zrfull WHERE c0 = 999) SELECT * FROM q",
			"WITH q AS (SELECT c0, c1 FROM zrfull) SELECT * FROM q"},
		{"star_union_all_empties", "SELECT * FROM zrempty UNION ALL SELECT * FROM zrempty",
			"SELECT * FROM zrfull UNION ALL SELECT * FROM zrfull"},
		// The controls: written-out select lists, which #416 already covered.
		// They are here so a regression that took the declaration away from
		// EVERY zero-row result is not read as a star-only one.
		{"named_where_no_match", "SELECT c0, c2 FROM zrfull WHERE c0 = 999", "SELECT c0, c2 FROM zrfull"},
		{"literal_where_false", "SELECT 1 AS x WHERE false", "SELECT 1 AS x"},
	}
}

// zrDescribe runs one statement through the raw wire and returns the
// RowDescription's fields, rendered as "name:oid:typmod", plus the whole
// backend message trace.
func zrDescribe(t *testing.T, srv *Server, sql string) (fields []string, trace string) {
	t.Helper()
	c := newPGClient(t, srv.Addr())
	c.startup("wadjet", "wadjet")
	defer c.terminate()

	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, sql...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	c.writeMsg('P', parseBuf)
	c.writeMsg('D', []byte{'S', 0}) // Describe STATEMENT — the pgJDBC path
	c.writeMsg('S', nil)

	var parts []string
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			t.Fatalf("reading describe response for %q: %v", sql, err)
		}
		parts = append(parts, string(typ))
		if typ == 'T' {
			fields = zrParseRowDescFull(data)
		}
		if typ == 'E' {
			t.Fatalf("describe of %q failed: %s", sql, c.parseError(data))
		}
		if typ == 'Z' {
			return fields, strings.Join(parts, " ")
		}
	}
}

// zrParseRowDescFull renders every RowDescription field as name:oid:typmod —
// the three a client picks its type from. Format code is deliberately not
// included: a statement Describe is unconditionally text (#362) and the
// portal-format question is portal_format_test.go's.
func zrParseRowDescFull(data []byte) []string {
	if len(data) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := readCString(data)
		rest := data[len(name)+1:]
		oid := binary.BigEndian.Uint32(rest[6:10])
		typmod := int32(binary.BigEndian.Uint32(rest[12:16]))
		out = append(out, fmt.Sprintf("%s:%d:%d", name, oid, typmod))
		data = rest[18:]
	}
	return out
}

// TestZeroRowSelectDescribesLikeItsNonEmptyTwin is the declaration half: the
// same statement with a predicate that matches and one that matches nothing
// must describe the same fields, names, OIDs and typmods alike.
func TestZeroRowSelectDescribesLikeItsNonEmptyTwin(t *testing.T) {
	srv := zrSetup(t)
	for _, tc := range zrShapes() {
		t.Run(tc.name, func(t *testing.T) {
			want, wantTrace := zrDescribe(t, srv, tc.full)
			if len(want) == 0 {
				t.Fatalf("the REFERENCE statement %q described no fields (%s); "+
					"the expectation would be meaningless", tc.full, wantTrace)
			}
			got, gotTrace := zrDescribe(t, srv, tc.empty)
			if len(got) == 0 {
				t.Fatalf("%q described NO fields (%s).\n"+
					"PostgreSQL always sends a RowDescription for a SELECT; without one psql "+
					"prints nothing and pgJDBC's executeQuery throws \"No results were "+
					"returned by the query\" (#846).\nReference %q describes: %v",
					tc.empty, gotTrace, tc.full, want)
			}
			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Errorf("zero-row and non-empty arms describe differently:\n"+
					" zero-row %q\n   %v\n non-empty %q\n   %v",
					tc.empty, got, tc.full, want)
			}
		})
	}
}

// TestZeroRowSelectAlwaysSendsARowDescription is the DOOR's own claim, and it
// is the one that has to hold for the shape whose schema the planner cannot
// declare — `SELECT *` over a JOIN, deferred in #846. Whatever the
// declaration, a statement that ran as a query answers with a result set:
// 'T' then 'C', never 'I' (EmptyQueryResponse) on the simple protocol and
// never 'n' (NoData) on the extended one.
func TestZeroRowSelectAlwaysSendsARowDescription(t *testing.T) {
	srv := zrSetup(t)

	sqls := make([]string, 0, len(zrShapes())+1)
	for _, tc := range zrShapes() {
		sqls = append(sqls, tc.empty)
	}
	// The deferred cell: its RowDescription carries zero fields, and that is
	// precisely why the door's guarantee is separate from the declaration's.
	sqls = append(sqls, "SELECT * FROM zrfull a JOIN zrempty b ON a.c0 = b.c0")

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			c := newPGClient(t, srv.Addr())
			c.startup("wadjet", "wadjet")
			defer c.terminate()

			_, _, tag := c.simpleQuery(sql)
			if tag == "EMPTY" {
				t.Errorf("simple protocol answered EmptyQueryResponse ('I') for %q.\n"+
					"'I' is PostgreSQL's reply to an empty query STRING, never to a SELECT: "+
					"the client is told there is no result set at all (#846).", sql)
			}
			if !strings.HasPrefix(tag, "SELECT") {
				t.Errorf("simple protocol command tag for %q is %q, want a SELECT tag", sql, tag)
			}

			trace := traceString(c.extendedTrace(sql))
			if strings.Contains(trace, " n ") || strings.HasPrefix(trace, "n ") {
				t.Errorf("extended protocol answered NoData ('n') for %q: %s\n"+
					"NoData says the statement has no tuple result, which is a command's or a "+
					"DML statement's answer, not a SELECT's (#846).", sql, trace)
			}
			if !strings.Contains(trace, "T(") {
				t.Errorf("extended protocol sent no RowDescription for %q: %s", sql, trace)
			}
		})
	}
}

// TestZeroRowStarDescribesTheSameBeforeAndAfterExecute is pgJDBC's actual
// shape: Parse, Describe(statement), Bind, Execute — and then the SAME
// prepared statement described again, because a driver caches the statement
// and re-uses it. The description must not depend on whether the portal has
// run, and the DML in between must not disturb it either: a transaction that
// wrote rows and then asked for the shape of an empty read is where a
// describe cache goes stale.
func TestZeroRowStarDescribesTheSameBeforeAndAfterExecute(t *testing.T) {
	srv := zrSetup(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet@127.0.0.1:%s/wadjet?sslmode=disable",
		srv.Addr()[len("127.0.0.1:"):]))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	const sql = "SELECT * FROM zrempty"
	render := func(sd *pgconn.StatementDescription) string {
		parts := make([]string, len(sd.Fields))
		for i, f := range sd.Fields {
			parts[i] = fmt.Sprintf("%s:%d:%d", f.Name, f.DataTypeOID, f.TypeModifier)
		}
		return strings.Join(parts, " ")
	}

	sd, err := conn.Prepare(ctx, "zrstar", sql)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	before := render(sd)
	if before == "" {
		t.Fatalf("Describe of a prepared %q returned no fields — pgJDBC ties its ResultSet to "+
			"this Describe and throws \"No results were returned by the query\" (#846)", sql)
	}

	rows, err := conn.Query(ctx, "zrstar")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	sd2, err := conn.Prepare(ctx, "zrstar2", sql)
	if err != nil {
		t.Fatalf("re-prepare: %v", err)
	}
	if after := render(sd2); after != before {
		t.Errorf("the same statement describes differently after Execute:\n before %s\n after  %s",
			before, after)
	}

	// The same read, in a transaction, after a write to the OTHER table.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO zrfull VALUES (9, 'z', 9.5)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	sd3, err := conn.Prepare(ctx, "zrstar3", sql)
	if err != nil {
		t.Fatalf("prepare after DML: %v", err)
	}
	if after := render(sd3); after != before {
		t.Errorf("the same statement describes differently after a DML in a transaction:\n"+
			" before %s\n after  %s", before, after)
	}
	tx.Rollback(ctx)
}

// TestCommandAndDMLStillDescribeAsNoData is the boundary from the other side
// (rule 11): the two statement classes that DO describe as NoData must keep
// doing so. Turning every NoData into a RowDescription would tell a driver
// that an UPDATE returns tuples, which is #816's defect wearing the other
// sign.
func TestCommandAndDMLStillDescribeAsNoData(t *testing.T) {
	srv := zrSetup(t)
	for _, sql := range []string{
		"UPDATE zrfull SET c0 = c0 WHERE c0 = 999",
		"DELETE FROM zrfull WHERE c0 = 999",
		"INSERT INTO zrempty VALUES (9, 'z', 9.5)",
		"SET application_name = 'x'",
	} {
		t.Run(sql, func(t *testing.T) {
			_, trace := zrDescribe(t, srv, sql)
			if !strings.Contains(trace, "n") {
				t.Errorf("describe of %q did not answer NoData: %s", sql, trace)
			}
			if strings.Contains(trace, "T") {
				t.Errorf("describe of %q answered a RowDescription: %s\n"+
					"A command and a DML statement without RETURNING have no tuple result; "+
					"describing one is how every INSERT reported `SELECT 1` (#816).", sql, trace)
			}
		})
	}
}
