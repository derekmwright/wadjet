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
	"errors"
	"fmt"
	"strings"
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
		// The outer array is typed as its column: integer[] (1007), not the
		// bigint[] an INT[] CAST declares. v0.25.3 declared text (25).
		{"corrArray", `SELECT (SELECT o.a FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1007, "{1,2}"},
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
		// A correlated outer value is typed as its COLUMN, not as the cast
		// that spells it in the re-run: a choice over an int4 outer column
		// is integer and over an int4[] one integer[] (v0.25.3 declared each
		// as PostgreSQL does; a CAST-typed outer value declared 20 and 1016).
		{"coalesceOuterInt", `SELECT (SELECT coalesce(o.i, x.v) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "3"},
		{"coalesceInnerFirst", `SELECT (SELECT coalesce(x.v, o.i) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "5"},
		{"coalesceOuterZero", `SELECT (SELECT coalesce(o.i, 0) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "3"},
		{"caseOuterInt", `SELECT (SELECT CASE WHEN x.v > 0 THEN o.i ELSE x.v END FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "3"},
		{"greatestOuterInt", `SELECT (SELECT greatest(o.i, x.v) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "5"},
		{"leastOuterInt", `SELECT (SELECT least(o.i, 3) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 5`, 23, "1"},
		{"nullifOuterInt", `SELECT (SELECT nullif(o.i, x.v) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "3"},
		{"absOuterInt", `SELECT (SELECT abs(o.i) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "3"},
		{"coalesceOuterArray", `SELECT (SELECT coalesce(o.a, x.a) FROM ss_t x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 5`, 1007, "{8}"},
		{"coalesceInnerArray", `SELECT (SELECT coalesce(x.a, o.a) FROM ss_t x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 5`, 1007, "{1,2}"},
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

// WHAT A CORRELATED OUTER VALUE STORES, and what an integer CAST the user
// writes still answers. CREATE TABLE AS over `(SELECT coalesce(o.i, x.v) …)`
// makes an integer column and over `(SELECT coalesce(o.a, x.a) …)` an
// integer[] one, as PostgreSQL 17.11 does; and `CAST(t.i AS INTEGER) / t.n`
// — no subquery — stores 1.33333333333333330000 into a numeric(30,20) and
// finds rows 1 and 5 above 1.333333333331, PostgreSQL's (and v0.25.3's)
// answers, where an integer cast read as a fixed-point operand truncated the
// quotient at eleven digits (1.33333333333000000000; row 5 only).
func TestArcSSOuterValueStoresAsItsColumn(t *testing.T) {
	srv := setupSSWireDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE ss_ctas AS SELECT o.id, (SELECT coalesce(o.i, x.v) FROM ss_i x WHERE x.id = 1) AS k, " +
			"(SELECT coalesce(o.a, x.a) FROM ss_t x WHERE x.id = 1) AS ka, " +
			"(SELECT CASE WHEN x.v > 0 THEN o.i ELSE x.v END FROM ss_i x WHERE x.id = 1) AS kc FROM ss_t o",
		"CREATE TABLE ss_q (id BIGINT, x NUMERIC(30,20))",
		"INSERT INTO ss_q SELECT t.id, CAST(t.i AS INTEGER) / t.n FROM ss_t t",
	} {
		if _, err := conn.Exec(ctx, sql).ReadAll(); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, c := range []struct {
		sql  string
		oids []uint32
		rows string
	}{
		{"SELECT k, ka, kc FROM ss_ctas WHERE id = 5", []uint32{23, 1007, 23}, "1;{8};1"},
		{"SELECT x FROM ss_q ORDER BY id", []uint32{1700},
			"1.33333333333333330000|0.50000000000000000000|100.00000000000000000000"},
		{"SELECT id FROM ss_t t WHERE CAST(t.i AS INTEGER) / t.n > 1.333333333331 ORDER BY id", []uint32{20}, "1|5"},
	} {
		rr := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil)
		var oids []uint32
		for _, fd := range rr.FieldDescriptions() {
			oids = append(oids, fd.DataTypeOID)
		}
		var rows []string
		for rr.NextRow() {
			var cells []string
			for _, v := range rr.Values() {
				cells = append(cells, string(v))
			}
			rows = append(rows, strings.Join(cells, ";"))
		}
		if _, err := rr.Close(); err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if fmt.Sprint(oids) != fmt.Sprint(c.oids) {
			t.Errorf("%s\n  OIDs %v, PostgreSQL 17.11 declares %v", c.sql, oids, c.oids)
		}
		if got := strings.Join(rows, "|"); got != c.rows {
			t.Errorf("%s\n  sent %q, PostgreSQL 17.11 sends %q", c.sql, got, c.rows)
		}
	}
}

// A SCALAR SUBQUERY IS DECLARED BY THE SELECT-LIST WALK: its type is the one
// the planner declares for any SELECT-list expression, at the integer width
// the subquery's plan publishes for it — the pair a derived table's column is
// read as. A subscript of an int4[] is int4, a day count and ascii() are
// int4, a bigint beside an int4 is int8, and SUM over a derived or CTE int4
// column is bigint, as on PostgreSQL 17.11. v0.25.3 answered each of these
// right only where its declaration looked an expression up by the last column
// name in its text; `__column_value(…)` — the re-run's own spelling of an
// outer value — is 42883 from a client, as on PostgreSQL and v0.25.3.
func TestArcSSDeclaredByTheSelectListWalk(t *testing.T) {
	srv := setupSSWireDB(t)
	setup := connectPgconn(t, srv.Addr())
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE ss_r4a AS SELECT o.id, (SELECT (o.d - DATE '2024-01-01') + x.v FROM ss_i x WHERE x.id = 1) AS k, " +
			"(SELECT ascii(o.s) + x.v FROM ss_i x WHERE x.id = 1) AS ka, " +
			"(SELECT o.a[1] + x.v FROM ss_i x WHERE x.id = 1) AS ki FROM ss_t o",
		"CREATE TABLE ss_r4b AS SELECT sum(s.k) AS tot FROM (SELECT t.i + t.i AS k FROM ss_t t) s",
	} {
		if _, err := setup.Exec(ctx, sql).ReadAll(); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, c := range []struct {
		name, sql string
		oid       uint32
		value     string // "<none>" for a zero-row result; "ERR <SQLSTATE>" for a refusal
	}{
		{"idxPlusV", `SELECT (SELECT o.a[1] + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "6"},
		{"vPlusIdx", `SELECT (SELECT x.v + o.a[2] FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "7"},
		{"idxOvf", `SELECT (SELECT o.a[1] + 2147483647 FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 0, "ERR 22003"},
		{"uncIdxPlus", `SELECT (SELECT x.a[1] + 1 FROM ss_t x WHERE x.id = 1) AS v`, 23, "2"},
		{"uncIdxOvf", `SELECT (SELECT x.a[1] + 2147483647 FROM ss_t x WHERE x.id = 1) AS v`, 0, "ERR 22003"},
		{"dDiffPlusV", `SELECT (SELECT (o.d - DATE '2024-01-01') + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "68"},
		{"dDiffDivV", `SELECT (SELECT (o.d - DATE '2024-01-01') / x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "12"},
		{"ddPlusV", `SELECT (SELECT (o.d - o.d) + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "5"},
		{"asciiPlusV", `SELECT (SELECT ascii(o.s) + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 23, "102"},
		{"uncAscii", `SELECT (SELECT ascii(x.s) FROM ss_t x WHERE x.id = 1) AS v`, 23, "97"},
		{"plainAscii", `SELECT ascii(s) AS v FROM ss_t WHERE id = 1`, 23, "97"},
		{"plainAsciiZero", `SELECT ascii(s) AS v FROM ss_t WHERE id = 99`, 23, "<none>"},
		{"sumDerived", `SELECT sum(s.k) AS v FROM (SELECT t.i + t.i AS k FROM ss_t t) s`, 20, "18"},
		{"sumCte", `WITH q AS (SELECT t.i + t.i AS k FROM ss_t t) SELECT sum(q.k) AS v FROM q`, 20, "18"},
		{"sumDerivedTimes2", `SELECT sum(s.k) AS v FROM (SELECT t.i * 2 AS k FROM ss_t t) s`, 20, "18"},
		{"sumDerivedBMinusI", `SELECT sum(s.k) AS v FROM (SELECT t.b - t.i AS k FROM ss_t t) s`, 1700, "9000000022"},
		{"keepBMinusI", `SELECT (SELECT x.b - x.i FROM ss_t x WHERE x.id = 3) AS v`, 20, "8999999995"},
		{"ctasDayCount", `SELECT k FROM ss_r4a WHERE id = 1`, 23, "68"},
		{"ctasAscii", `SELECT ka FROM ss_r4a WHERE id = 1`, 23, "102"},
		{"ctasSubscript", `SELECT ki FROM ss_r4a WHERE id = 1`, 23, "6"},
		{"ctasSum", `SELECT tot FROM ss_r4b`, 20, "18"},
		{"spelling", `SELECT __column_value(cast(5 as integer)) AS v`, 0, "ERR 42883"},
		{"spellingInSub", `SELECT (SELECT __column_value(cast(5 as integer)) + x.v FROM ss_i x WHERE x.id = 1) AS v`, 0, "ERR 42883"},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			if want, isErr := strings.CutPrefix(c.value, "ERR "); isErr {
				_, extErr := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}).Close()
				_, simpleErr := conn.Exec(ctx, c.sql).ReadAll()
				for proto, err := range map[string]error{"extended": extErr, "simple": simpleErr} {
					var pe *pgconn.PgError
					if !errors.As(err, &pe) || pe.Code != want {
						t.Errorf("%s: %s\n  got %v, PostgreSQL 17.11 raises %s", proto, c.sql, err, want)
					}
				}
				return
			}
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
