// SPDX-License-Identifier: MIT

package pgwire

// EVERY OPERAND KIND × THE CONSUMERS ONLY A CLIENT SEES (#1422's class): the
// text encoder over the simple and the extended protocol, the binary encoder
// (extended, binary results), and the four write doors — CTAS, INSERT …
// SELECT into numeric(30,2) / bigint / text / double precision columns,
// DELETE … WHERE and UPDATE … WHERE over the value. The kinds are
// coordinator.TestArcSSOperandKindTimesConsumerEveryArm's (its header names
// them), over the same six-row fixture; that test holds the five-arm
// consumers. The dv/* and wk/* cells are a derived table's column read by an
// outer expression and a window key holding a scalar subquery. The r12/*
// cells are the kinds stored by CTAS inside an ARRAY constructor and read
// through a view, and the wire forms of the choice-arm, array-element and
// recursive-seed cells.
//
// Every want is PostgreSQL 17.11's, measured over the same DDL with the same
// client (pgx, default_query_exec_mode simple_protocol / exec /
// describe_exec): the declared OIDs and the raw DataRow bytes, hex for the
// binary format (testdata/arc_ss_audit_consumers_wire_pg17.tsv: name, mode,
// ordered, sql, answer; a cell's statements are joined by " ;; " and the
// answer is the last one's). The cells in
// testdata/arc_ss_audit_consumers_wire_kept.tsv are recorded divergences,
// asserted as they stand.

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupSSAuditWireDB(t *testing.T) *Server {
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
			"(2, -7, -70, -2.5, -3.5, 'Hello', false, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000002' AS UUID), ARRAY[3]), " +
			"(3, 5, 9000000000, 0.25, 10.00, 'zz', true, DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59.999', " +
			"CAST('00000000-0000-4000-8000-000000000003' AS UUID), ARRAY[4,5,6]), " +
			"(4, 0, 0, 0.0, 0.00, '', false, DATE '1000-01-01', TIMESTAMP '1000-01-01 00:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000004' AS UUID), ARRAY[7]), " +
			"(5, 1, 1, 100.125, 0.01, 'x', true, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999', " +
			"CAST('00000000-0000-4000-8000-000000000005' AS UUID), ARRAY[8]), " +
			"(6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
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

// ssAuditWireRun answers one statement the way the measurement read
// PostgreSQL: the declared OIDs, then each row's raw DataRow values (hex
// when the result format is binary), rows sorted unless ordered.
func ssAuditWireRun(ctx context.Context, conn *pgx.Conn, sql string, ordered, binary bool) string {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return ssAuditWireErr(err)
	}
	defer rows.Close()
	var oids []string
	for _, fd := range rows.FieldDescriptions() {
		oids = append(oids, fmt.Sprint(fd.DataTypeOID))
	}
	var out []string
	for rows.Next() {
		raw := rows.RawValues()
		cells := make([]string, len(raw))
		for i, r := range raw {
			switch {
			case r == nil:
				cells[i] = "NULL"
			case binary:
				cells[i] = hex.EncodeToString(r)
			default:
				cells[i] = string(r)
			}
		}
		out = append(out, strings.Join(cells, ","))
	}
	if err := rows.Err(); err != nil {
		return ssAuditWireErr(err)
	}
	if !ordered {
		sort.Strings(out)
	}
	return fmt.Sprintf("{%s} rows=%d %s", strings.Join(oids, ","), len(out), strings.Join(out, " | "))
}

func ssAuditWireErr(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "ERR " + pe.Code + " " + strings.ReplaceAll(pe.Message, "\n", " ")
	}
	return "ERR ? " + strings.ReplaceAll(err.Error(), "\n", " ")
}

func ssAuditWireTSV(t *testing.T, path string, fields int) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out [][]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", fields)
		if len(p) != fields {
			t.Fatalf("malformed line %q", line)
		}
		out = append(out, p)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestArcSSAuditConsumersOnTheWire(t *testing.T) {
	srv := setupSSAuditWireDB(t)
	ctx := context.Background()
	modes := map[string]string{"s": "simple_protocol", "e": "exec", "b": "describe_exec"}
	conns := map[string]*pgx.Conn{}
	for m, qm := range modes {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable&default_query_exec_mode=%s", srv.Addr(), qm))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(ctx) })
		conns[m] = conn
	}
	kept := map[string][2]string{}
	for _, k := range ssAuditWireTSV(t, "testdata/arc_ss_audit_consumers_wire_kept.tsv", 4) {
		kept[k[0]+"/"+k[1]] = [2]string{k[2], k[3]}
	}
	cells := ssAuditWireTSV(t, "testdata/arc_ss_audit_consumers_wire_pg17.tsv", 5)
	seen := map[string]bool{}
	for _, c := range cells {
		seen[c[0]+"/"+c[1]] = true
	}
	for k := range kept {
		if !seen[k] {
			t.Fatalf("kept cell %s is not in the table", k)
		}
	}
	for _, c := range cells {
		name, mode, ordered, sql, pg := c[0], c[1], c[2] == "true", c[3], c[4]
		t.Run(name+"/"+mode, func(t *testing.T) {
			conn := conns[mode]
			var got, firstErr string
			for _, st := range strings.Split(sql, " ;; ") {
				r := ssAuditWireRun(ctx, conn, st, ordered, mode == "b")
				if strings.HasPrefix(r, "ERR") && firstErr == "" && !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(st)), "DROP") {
					firstErr = r
				}
				got = r
			}
			if firstErr != "" {
				got = firstErr
			}
			want, why := pg, "PostgreSQL 17.11 sends"
			if k, ok := kept[name+"/"+mode]; ok {
				want, why = k[0], "kept: "+k[1]
			}
			if got != want {
				t.Errorf("%s\n  got  %s\n  want %s (%s)", sql, got, want, why)
			}
		})
	}
}
