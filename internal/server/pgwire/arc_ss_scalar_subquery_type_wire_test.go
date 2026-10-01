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

// AN EXPRESSION IS READ IN ITS OWN SCOPE, AND AN INTEGER OPERAND IS AN
// INTEGER IN DECIMAL ARITHMETIC. A derived table that publishes both `t.b AS
// i` (bigint) and its own unaliased `i + i` (over ss_t's int4 `i`) does not
// make the query's `i + i` — arithmetic over the bigint `i` — the int4
// column that carries the same text: the scalar subquery answers
// 18000000000, SUM over a column derived from it is numeric and does not
// overflow, as on PostgreSQL 17.11 (and v0.25.3). A day count, ascii(),
// length(), a subscript of an int4[] and COALESCE over integers beside a
// NUMERIC are numeric arithmetic, inside a scalar subquery and outside one,
// and CREATE TABLE AS stores numeric. A zero-row result declares a correlated
// subquery over an int4[] outer column as the rows would (integer, numeric,
// integer[]). A subquery whose answer multiplies an integer CAST or an
// integral EXTRACT field by a NUMERIC is numeric, computed exactly, as v0.25.3
// and PostgreSQL declare it; the same expression in the query's own SELECT
// list keeps its recorded double precision. An integer subquery times a
// NUMERIC stays exact through the operators after it, past 2^53, and so does
// every consumer of an exact operand: unary minus, abs, round, %, a CASE or
// COALESCE arm, over a subquery, an integer call or a marked EXTRACT — and a
// window function's input over the same operands. A relation, a CTE or an alias NAMED
// __column_value answers; only the re-run's spelling `__column_value(cast(…))`
// — and any other call of that name — is 42883.
func TestArcSSScopeAndIntegerOperandsOnTheWire(t *testing.T) {
	srv := setupSSWireDB(t)
	setup := connectPgconn(t, srv.Addr())
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE ss_r5a AS SELECT (SELECT i + i FROM (SELECT t.b AS i, i + i FROM ss_t t WHERE t.id = 3) t) AS k, " +
			"(SELECT (t.d - DATE '2024-01-01') * t.n FROM ss_t t WHERE t.id = 1) AS kn, " +
			"(SELECT ascii(o.s) * x.m FROM ss_i x, ss_t o WHERE x.id = 1 AND o.id = 1) AS ka",
		"CREATE TABLE ss_r5b AS SELECT sum(q.k) AS tot FROM (SELECT i + i AS k FROM (SELECT t.b AS i, i + i FROM ss_t t WHERE t.id = 3) t) q",
		"CREATE TABLE ss_r5c AS SELECT (SELECT CAST(t.b AS INTEGER) * t.n FROM ss_t t WHERE t.id = 1) AS kc, " +
			"(SELECT extract(year FROM t.d) * t.n FROM ss_t t WHERE t.id = 1) AS ke",
		"CREATE TABLE ss_r6n AS SELECT (SELECT (SELECT z.v FROM ss_i z WHERE z.id = 1) * y.m FROM ss_i y WHERE y.id = 1) AS k",
		"CREATE TABLE ss_r7z AS SELECT (SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3 AS k FROM ss_t t WHERE t.id = 1",
		"CREATE TABLE ss_r8n AS SELECT -((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n - 3) AS k FROM ss_t t WHERE t.id = 1",
		"CREATE TABLE ss_r9w AS SELECT t.id, sum((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3) OVER (ORDER BY t.id) AS k FROM ss_t t WHERE t.id = 1",
		"CREATE TABLE ss_r10w AS SELECT (SELECT sum((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * y.m + 3) OVER () FROM ss_t o, ss_i y WHERE y.id = 1 AND o.id = 1) AS k",
		"CREATE TABLE __column_value (k integer)",
		"INSERT INTO __column_value (k) VALUES (4)",
	} {
		// A setup statement PostgreSQL 17.11 answers is a cell too: its
		// refusal is reported and the cells after it still run (the ones
		// reading its table then fail by name).
		if _, err := setup.Exec(ctx, sql).ReadAll(); err != nil {
			t.Errorf("%s: %v, PostgreSQL 17.11 answers", sql, err)
		}
	}
	for _, c := range []struct {
		name, sql string
		oid       uint32
		value     string // "<none>" for a zero-row result; "ERR <SQLSTATE>" for a refusal
		kept      string // a catalogued divergence: PostgreSQL 17.11 declares otherwise
	}{
		{"shadowSub", `SELECT (SELECT i + i FROM (SELECT t.b AS i, i + i FROM ss_t t WHERE t.id = 3) t) AS v`, 20, "18000000000", ""},
		{"shadowSubIn4", `SELECT (SELECT i + i FROM (SELECT t.b AS i, i + i FROM ss_t t WHERE t.id = 1) t) AS v`, 20, "60", ""},
		{"shadowSubQual", `SELECT (SELECT t.i + t.i FROM (SELECT t.b AS i, t.i + t.i FROM ss_t t WHERE t.id = 3) t) AS v`, 20, "18000000000", ""},
		{"shadowGroupSub", `SELECT (SELECT i + i FROM (SELECT t.b AS i, i + i FROM ss_t t WHERE t.id = 3) t GROUP BY i + i) AS v`, 20, "18000000000", ""},
		{"shadowDerivedSum", `SELECT sum(q.k) AS v FROM (SELECT i + i AS k FROM (SELECT t.b AS i, i + i FROM ss_t t WHERE t.id = 3) t) q`, 1700, "18000000000", ""},
		{"shadowSumOvf", `SELECT sum(q.k) AS v FROM (SELECT i + 0 AS k FROM (SELECT t.b * 1000000000 AS i, i + 0 FROM ss_t t, ss_i x WHERE t.id = 3) t) q`, 1700, "18000000000000000000", ""},
		{"subDayCountN", `SELECT (SELECT (t.d - DATE '2024-01-01') * t.n FROM ss_t t WHERE t.id = 1) AS v`, 1700, "141.75", ""},
		{"corrAsciiM", `SELECT (SELECT ascii(o.s) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "121.25", ""},
		{"corrIdxM", `SELECT (SELECT o.a[1] * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "1.25", ""},
		{"corrLenM", `SELECT (SELECT length(o.s) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "3.75", ""},
		{"corrCoalM", `SELECT (SELECT COALESCE(o.i, 0) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "3.75", ""},
		{"corrIdxP1M", `SELECT (SELECT (o.a[1] + 1) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "2.50", ""},
		{"plainDayCountPlus1", `SELECT (t.d - DATE '2024-01-01') + 1 AS v FROM ss_t t WHERE t.id = 1`, 20, "64", "temporal#r17: integer arithmetic over a day count declares bigint (PostgreSQL: integer)"},
		{"plainDayCountDiv7", `SELECT (t.d - DATE '2024-01-01') / 7 AS v FROM ss_t t WHERE t.id = 1`, 20, "9", "temporal#r17: integer arithmetic over a day count declares bigint (PostgreSQL: integer)"},
		{"plainDayCountN", `SELECT (t.d - DATE '2024-01-01') * t.n AS v FROM ss_t t WHERE t.id = 1`, 1700, "141.75", ""},
		{"plainAsciiN", `SELECT ascii(t.s) * t.n AS v FROM ss_t t WHERE t.id = 1`, 1700, "218.25", ""},
		{"plainIdxN", `SELECT t.a[1] * t.n AS v FROM ss_t t WHERE t.id = 1`, 1700, "2.25", ""},
		{"plainLenN", `SELECT length(t.s) * t.n AS v FROM ss_t t WHERE t.id = 1`, 1700, "6.75", ""},
		{"ctasShadow", `SELECT k FROM ss_r5a`, 20, "18000000000", ""},
		{"ctasDayCountN", `SELECT kn FROM ss_r5a`, 1700, "141.75", ""},
		{"ctasAsciiM", `SELECT ka FROM ss_r5a`, 1700, "121.25", ""},
		{"ctasShadowSum", `SELECT tot FROM ss_r5b`, 1700, "18000000000", ""},
		{"zeroIdxV", `SELECT (SELECT o.a[1] + x.v FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 99`, 23, "<none>", ""},
		{"zeroIdxM", `SELECT (SELECT o.a[1] * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 99`, 1700, "<none>", ""},
		{"zeroArr", `SELECT (SELECT o.a FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 99`, 1007, "<none>", ""},
		{"subCastM", `SELECT (SELECT CAST(o.b AS INTEGER) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "37.50", ""},
		{"subExtractM", `SELECT (SELECT extract(year FROM o.d) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "2530.00", ""},
		{"uncCastN", `SELECT (SELECT CAST(t.b AS INTEGER) * t.n FROM ss_t t WHERE t.id = 1) AS v`, 1700, "67.50", ""},
		{"subDerivedCastN", `SELECT (SELECT q.k FROM (SELECT CAST(t.b AS INTEGER) * t.n AS k FROM ss_t t WHERE t.id = 1) q) AS v`, 1700, "67.50", ""},
		{"subMDivCast", `SELECT (SELECT x.m / CAST(o.i AS INTEGER) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "0.4166666666666666666667", "numeric-decimal#r19: the quotient keeps max(6, s1 + p2 + 1) fraction digits (PostgreSQL: 0.41666666666666666667)"},
		{"zeroCastM", `SELECT (SELECT CAST(o.b AS INTEGER) * x.m FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 99`, 1700, "<none>", ""},
		{"ctasCastN", `SELECT kc FROM ss_r5c`, 1700, "67.50", ""},
		{"ctasExtractN", `SELECT ke FROM ss_r5c`, 1700, "4554.00", ""},
		{"plainCastN", `SELECT CAST(t.b AS INTEGER) * t.n AS v FROM ss_t t WHERE t.id = 1`, 701, "67.5", "N-10: an integer CAST beside a NUMERIC takes the float8 rung in a query's own SELECT list (PostgreSQL: numeric 67.50)"},
		{"plainExtractN", `SELECT extract(year FROM t.d) * t.n AS v FROM ss_t t WHERE t.id = 1`, 701, "4554", "ADR-0024 §2c: extract() declares double precision in a query's own SELECT list (PostgreSQL: numeric 4554.00)"},
		{"subVtimesM", `SELECT (SELECT (SELECT z.v FROM ss_i z WHERE z.id = 1) * y.m FROM ss_i y WHERE y.id = 1) AS v`, 1700, "6.25", ""},
		{"subMaxVtimesM", `SELECT (SELECT (SELECT max(z.v) FROM ss_i z) * y.m FROM ss_i y WHERE y.id = 1) AS v`, 1700, "7.50", ""},
		{"subOuterVtimesM", `SELECT (SELECT (SELECT o.i FROM ss_i z WHERE z.id = 1) * y.m FROM ss_i y WHERE y.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "3.75", ""},
		{"subYearTimesM", `SELECT (SELECT (SELECT extract(year FROM o.d) FROM ss_i z WHERE z.id = 1) * y.m FROM ss_i y WHERE y.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 1700, "2530.00", ""},
		{"ctasSubV", `SELECT k FROM ss_r6n`, 1700, "6.25", ""},
		{"mTimesYearDiv7", `SELECT (SELECT x.m * (extract(year FROM o.d) / 7) FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 701, "361.42857142857144", "ADR-0024 §2c: a quotient over EXTRACT is the double the kernel divides, as at v0.25.3 (PostgreSQL: numeric 361.428571428571428625)"},
		{"yearDiv7Times15", `SELECT (SELECT extract(year FROM o.d) / 7 * 1.5 FROM ss_i x WHERE x.id = 1) AS v FROM ss_t o WHERE o.id = 1`, 701, "433.7142857142858", "ADR-0024 §2c: a quotient over EXTRACT is the double the kernel divides, as at v0.25.3 (PostgreSQL: numeric 433.71428571428571435)"},
		{"bigTimesNPlus3", `SELECT (SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3 AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000003.00", ""},
		{"corrBig", `SELECT (SELECT x.b * 10000000 + t.i FROM ss_t x WHERE x.id = 3) * t.n + 3 AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000009.75", ""},
		{"subSubBig", `SELECT (SELECT (SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * y.m + 3 FROM ss_i y WHERE y.id = 1) AS v`, 1700, "112500000000000003.00", ""},
		{"ctasBig", `SELECT k FROM ss_r7z`, 1700, "202500000000000003.00", ""},
		{"negSub", `SELECT -((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n - 3) AS v FROM ss_t t WHERE t.id = 1`, 1700, "-202499999999999997.00", ""},
		{"negSubMod", `SELECT -((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n - 3) % 1000 AS v FROM ss_t t WHERE t.id = 1`, 1700, "-997.00", ""},
		{"absSub", `SELECT abs((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3) + 1 AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000004.00", ""},
		{"roundSub", `SELECT round((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3, 1) + 1 AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000004.0", ""},
		{"coalNegMod", `SELECT -COALESCE((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n - 3, 0) % 1000 AS v FROM ss_t t WHERE t.id = 1`, 1700, "-997.00", ""},
		{"ascii15", `SELECT ascii(t.s) * 10000000000000000 * 1.5 + 3 AS v FROM ss_t t WHERE t.id = 1`, 1700, "1455000000000000003.0", ""},
		{"subCaseYear", `SELECT (SELECT CASE WHEN o.o THEN extract(year FROM o.d) ELSE 0 END * y.m FROM ss_i y, ss_t o WHERE y.id = 1 AND o.id = 1) AS v`, 1700, "2530.00", ""},
		{"subNegYear", `SELECT (SELECT -(extract(year FROM o.d) * 100000000000000 * y.m - 3) FROM ss_i y, ss_t o WHERE y.id = 1 AND o.id = 1) AS v`, 1700, "-252999999999999997.00", ""},
		{"ctasNeg", `SELECT k FROM ss_r8n`, 1700, "-202499999999999997.00", ""},
		{"windowSubSum", `SELECT sum((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3) OVER (ORDER BY t.id) AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000003.00", ""},
		{"windowSubMax", `SELECT max((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3) OVER () AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000003.00", ""},
		{"windowSubLag", `SELECT lag((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * t.n + 3, 0) OVER (ORDER BY t.id) AS v FROM ss_t t WHERE t.id = 1`, 1700, "202500000000000003.00", ""},
		{"windowIdx", `SELECT sum(t.a[1] * t.n) OVER (ORDER BY t.id) AS v FROM ss_t t WHERE t.id = 1`, 1700, "2.25", ""},
		{"ctasWindow", `SELECT k FROM ss_r9w`, 1700, "202500000000000003.00", ""},
		{"bodyWindowNested", `SELECT (SELECT sum((SELECT z.v FROM ss_i z WHERE z.id = 1) * y.m) OVER () FROM ss_i y WHERE y.id = 1) AS v`, 1700, "6.25", ""},
		{"bodyWindowIdx", `SELECT (SELECT sum(o.a[1] * y.m) OVER () FROM ss_t o, ss_i y WHERE y.id = 1 AND o.id = 1) AS v`, 1700, "1.25", ""},
		{"bodyWindowCol", `SELECT (SELECT sum(o.i * y.m) OVER () FROM ss_t o, ss_i y WHERE y.id = 1 AND o.id = 1) AS v`, 1700, "3.75", ""},
		{"bodyWindowDc", `SELECT (SELECT sum((o.d - DATE '2024-01-01') * y.m) OVER () FROM ss_t o, ss_i y WHERE y.id = 1 AND o.id = 1) AS v`, 1700, "78.75", ""},
		{"bodyWindowSub", `SELECT (SELECT sum((SELECT x.b * 10000000 FROM ss_t x WHERE x.id = 3) * y.m + 3) OVER () FROM ss_t o, ss_i y WHERE y.id = 1 AND o.id = 1) AS v`, 1700, "112500000000000003.00", ""},
		{"ctasBodyWindow", `SELECT k FROM ss_r10w`, 1700, "112500000000000003.00", ""},
		{"tableNamed", `SELECT k AS v FROM __column_value`, 23, "4", ""},
		{"aliasNamed", `SELECT __column_value.k AS v FROM (SELECT 1 AS k) AS __column_value (k)`, 23, "1", ""},
		{"cteNamed", `WITH __column_value (k) AS (SELECT 1) SELECT k AS v FROM __column_value`, 23, "1", ""},
		{"spellingColon", `SELECT __column_value(5::integer) AS v`, 0, "ERR 42883", ""},
		{"spellingCast", `SELECT __column_value(cast(5 as integer)) AS v`, 0, "ERR 42883", ""},
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
				got := "<none>"
				if len(r.rows) == 1 {
					got = string(r.rows[0])
				} else if len(r.rows) > 1 {
					got = "<many>"
				}
				if r.oid != c.oid || got != c.value {
					why := "PostgreSQL 17.11 sends"
					if c.kept != "" {
						why = "kept (" + c.kept + ")"
					}
					t.Errorf("%s: %s\n  sent OID %d %q, %s OID %d %q", proto, c.sql, r.oid, got, why, c.oid, c.value)
				}
			}
		})
	}
}
