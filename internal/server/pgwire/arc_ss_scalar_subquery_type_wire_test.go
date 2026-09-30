// SPDX-License-Identifier: MIT

package pgwire

// A SCALAR SUBQUERY'S ANSWER IS DECLARED AND SENT AS THE TYPE ITS SELECT LIST
// DECLARES (#1428 #1431 #1427 #1422), over the wire.
//
// One cell per result type, uncorrelated and correlated over the OUTER
// column itself, plus the issues' operand shapes: the OID in RowDescription
// and the DataRow text, over both the simple and the extended (text format)
// protocols. Every want is PostgreSQL 17.11's, measured live over the same
// DDL. At v0.25.3 the correlated form declared TEXT for every outer column
// (the subquery was planned with its outer names unresolved), `c.f + x.v`
// declared integer and sent 6 for 6.5, `x.g + x.v` declared integer even
// uncorrelated and with zero rows, a TIMESTAMP subquery under CAST sent its
// epoch milliseconds, `+ INTERVAL` dropped the hour, and a scalar-subquery
// GROUP BY key declared text.

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupSSWireDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2), s VARCHAR, o BOOLEAN, " +
			"d DATE, ts TIMESTAMP, u UUID, a ARRAY(INT))",
		"INSERT INTO ss_t VALUES " +
			"(1, 3, 30, 1.5, 2.25, 'abc', true, DATE '2024-03-04', TIMESTAMP '2024-03-04 12:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000001' AS UUID), ARRAY[1,2]), " +
			"(3, 5, 9000000000, 0.25, 10.00, 'zz', true, DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59.999', " +
			"CAST('00000000-0000-4000-8000-000000000003' AS UUID), ARRAY[4,5,6]), " +
			"(5, 1, 1, 100.125, 0.01, 'x', true, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999', " +
			"CAST('00000000-0000-4000-8000-000000000005' AS UUID), ARRAY[8])",
		"CREATE TABLE ss_i (id BIGINT, v INT, g DOUBLE, m NUMERIC(10,2))",
		"INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func TestArcSSScalarSubqueryTypedOnTheWire(t *testing.T) {
	srv := setupSSWireDB(t)
	for _, c := range []struct {
		name, sql string
		oid       uint32
		value     string // "<none>" for a zero-row result
	}{
		{"int", `SELECT (SELECT i FROM ss_t WHERE id = 1) AS v`, 23, "3"},
		{"bigint", `SELECT (SELECT b FROM ss_t WHERE id = 3) AS v`, 20, "9000000000"},
		{"double", `SELECT (SELECT f FROM ss_t WHERE id = 1) AS v`, 701, "1.5"},
		{"numeric", `SELECT (SELECT n FROM ss_t WHERE id = 1) AS v`, 1700, "2.25"},
		{"text", `SELECT (SELECT s FROM ss_t WHERE id = 1) AS v`, 25, "abc"},
		{"bool", `SELECT (SELECT o FROM ss_t WHERE id = 1) AS v`, 16, "t"},
		{"date", `SELECT (SELECT d FROM ss_t WHERE id = 3) AS v`, 1082, "9999-12-31"},
		{"timestamp", `SELECT (SELECT ts FROM ss_t WHERE id = 5) AS v`, 1114, "1969-12-31 23:59:59.999"},
		{"uuid", `SELECT (SELECT u FROM ss_t WHERE id = 1) AS v`, 2950, "00000000-0000-4000-8000-000000000001"},
		{"array", `SELECT (SELECT a FROM ss_t WHERE id = 1) AS v`, 1007, "{1,2}"},
		{"tsCastVarchar", `SELECT CAST((SELECT ts FROM ss_t WHERE id = 5) AS VARCHAR) AS v`, 1043, "1969-12-31 23:59:59.999"},
		{"tsPlusHour", `SELECT (SELECT max(ts) FROM ss_t WHERE id <> 3) + INTERVAL '1 hour' AS v`, 1114, "2024-03-04 13:00:00"},
		{"dateTrunc", `SELECT date_trunc('day', (SELECT ts FROM ss_t WHERE id = 5)) AS v`, 1114, "1969-12-31 00:00:00"},
		{"corrInt", `SELECT (SELECT o.i FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "3"},
		{"corrBigint", `SELECT (SELECT o.b FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 3`, 20, "9000000000"},
		{"corrDouble", `SELECT (SELECT o.f FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 701, "1.5"},
		{"corrNumeric", `SELECT (SELECT o.n FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "2.25"},
		{"corrText", `SELECT (SELECT o.s FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 25, "abc"},
		{"corrBool", `SELECT (SELECT o.o FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 16, "t"},
		{"corrDate", `SELECT (SELECT o.d FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 3`, 1082, "9999-12-31"},
		{"corrTimestamp", `SELECT (SELECT o.ts FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 5`, 1114, "1969-12-31 23:59:59.999"},
		{"corrUuid", `SELECT (SELECT o.u FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 2950, "00000000-0000-4000-8000-000000000001"},
		// PostgreSQL declares integer[] (1007). The re-run spells the outer
		// array as CAST('{1,2}' AS INT[]), and every integer CAST spelling
		// declares bigint here (numeric-decimal#r1), so the element is int8:
		// 1016, the re-run's own type. v0.25.3 declared text (25).
		{"corrArray", `SELECT (SELECT o.a FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1016, "{1,2}"},
		{"corrFPlusV", `SELECT (SELECT o.f + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 701, "6.5"},
		{"corrNPlusV", `SELECT (SELECT o.n + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "7.25"},
		{"corrIPlusG", `SELECT (SELECT o.i + x.g FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 701, "3.5"},
		// int4 arithmetic inside a scalar subquery is int4, correlated over
		// an int4 outer column or not; v0.25.3 declared both 23 as well.
		{"corrIPlusV", `SELECT (SELECT o.i + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "8"},
		{"innerVPlusV", `SELECT (SELECT x.v + x.v FROM ss_i x WHERE x.id = 1) AS v`, 23, "10"},
		// A DATE answer past 9999-12-31 is read back as the date it is.
		{"datePast9999", `SELECT (SELECT d + 1 FROM ss_t WHERE id = 3) AS v`, 1082, "10000-01-01"},
		{"innerGPlusV", `SELECT (SELECT x.g + x.v FROM ss_i x WHERE x.id = 1) AS v`, 701, "5.5"},
		{"innerGPlusVZero", `SELECT x.g + x.v AS v FROM ss_i x WHERE x.id = 99`, 701, "<none>"},
		{"groupByKey", `SELECT (SELECT i FROM ss_t WHERE id = 1) AS v FROM ss_t GROUP BY 1`, 23, "3"},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			ctx := context.Background()
			ext := ssWireRead(t, conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}), c.sql)
			mrr := conn.Exec(ctx, c.sql)
			if !mrr.NextResult() {
				t.Fatalf("simple: no result\n  SQL: %s", c.sql)
			}
			simple := ssWireRead(t, mrr.ResultReader(), c.sql)
			if err := mrr.Close(); err != nil {
				t.Fatalf("simple: %v\n  SQL: %s", err, c.sql)
			}
			for proto, r := range map[string]ssWireResult{"extended": ext, "simple": simple} {
				if r.oid != c.oid {
					t.Errorf("%s: %s\n  OID %d, PostgreSQL 17.11 declares %d", proto, c.sql, r.oid, c.oid)
				}
				got := "<none>"
				if len(r.rows) == 1 {
					got = string(r.rows[0])
				} else if len(r.rows) > 1 {
					got = "<many>"
				}
				if got != c.value {
					t.Errorf("%s: %s\n  sent %q, PostgreSQL 17.11 sends %q", proto, c.sql, got, c.value)
				}
			}
		})
	}
}

type ssWireResult struct {
	oid  uint32
	rows [][]byte
}

// ssWireRead reads one result: the OID from the RowDescription itself
// (ResultReader.FieldDescriptions, so a ZERO-ROW result still reports what
// the wire declared — n1WireExec's reason) and the first column of each row.
func ssWireRead(t *testing.T, rr *pgconn.ResultReader, sql string) ssWireResult {
	t.Helper()
	fds := rr.FieldDescriptions()
	if len(fds) != 1 {
		t.Fatalf("%d RowDescription fields, want 1\n  SQL: %s", len(fds), sql)
	}
	out := ssWireResult{oid: fds[0].DataTypeOID}
	for rr.NextRow() {
		out.rows = append(out.rows, append([]byte(nil), rr.Values()[0]...))
	}
	if _, err := rr.Close(); err != nil {
		t.Fatalf("%v\n  SQL: %s", err, sql)
	}
	return out
}
