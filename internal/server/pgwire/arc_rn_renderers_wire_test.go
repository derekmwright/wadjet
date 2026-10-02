// SPDX-License-Identifier: MIT

package pgwire

// THE SCALAR RENDERERS' TEXT OVER THE WIRE (#1474 #1466 #1467 #1481): the
// issues' statements on the arc SS fixture, the value sent and the OID
// declared, over the simple protocol and the extended protocol in text AND
// binary result format (text's binary form is its bytes), plus a CREATE TABLE
// … AS that stores each renderer's text. Every value is PostgreSQL 17.11's,
// measured over the same DDL. format and regexp_replace declare text (25) as
// PostgreSQL does; json_build_object declares text where PostgreSQL declares
// json (114) — the catalogued declaration divergence (#1470), pinned here so a
// change is re-measured. At c39858f3 the nested object was an escaped string,
// the DATE / TIMESTAMP their day count / epoch milliseconds, format printed
// Go's `%!s(float64=6.375)|`, and regexp_replace replaced every match.

import (
	"context"
	"testing"
)

func TestArcRNRenderersOnTheWire(t *testing.T) {
	srv := setupSSWireDB(t)
	cells := []struct {
		name, sql string
		oid       uint32
		value     string
	}{
		{"1474/nested", `SELECT json_build_object('o', json_build_object('a', n, 'b', i)) AS v FROM ss_t WHERE id = 1`,
			25, `{"o" : {"a" : 2.25, "b" : 3}}`},
		{"1466/dateTs", `SELECT json_build_object('d', d, 'ts', ts) AS v FROM ss_t WHERE id = 3`,
			25, `{"d" : "9999-12-31", "ts" : "9999-12-31T23:59:59.999"}`},
		{"1466/key", `SELECT json_build_object(d, 1) AS v FROM ss_t WHERE id = 5`,
			25, `{"1969-12-31" : 1}`},
		{"1467/double", `SELECT format('%s|', f * n + 3) AS v FROM ss_t WHERE id = 1`, 25, "6.375|"},
		{"1467/null", `SELECT format('%s|', NULL) AS v`, 25, "|"},
		{"1467/kinds", `SELECT format('%s|%L|%s|%s|%I', o, d, a, ts, s) AS v FROM ss_t WHERE id = 5`,
			25, `t|'1969-12-31'|{8}|1969-12-31 23:59:59.999|x`},
		{"1481/first", `SELECT regexp_replace(CAST(a[1] * n AS TEXT), '0', 'z') AS v FROM ss_t WHERE id = 3`, 25, "4z.00"},
		{"1481/g", `SELECT regexp_replace(CAST(a[1] * n AS TEXT), '0', 'z', 'g') AS v FROM ss_t WHERE id = 3`, 25, "4z.zz"},
		{"1481/gi", `SELECT regexp_replace('aAbA', 'a', 'z', 'gi') AS v`, 25, "zzbz"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			ctx := context.Background()
			text := ssWireRead(t, conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}), c.sql)
			binary := ssWireRead(t, conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{1}), c.sql)
			mrr := conn.Exec(ctx, c.sql)
			if !mrr.NextResult() {
				t.Fatalf("simple: no result\n  SQL: %s", c.sql)
			}
			simple := ssWireRead(t, mrr.ResultReader(), c.sql)
			if err := mrr.Close(); err != nil {
				t.Fatalf("simple: %v\n  SQL: %s", err, c.sql)
			}
			for proto, r := range map[string]ssWireResult{"extended-text": text, "extended-binary": binary, "simple": simple} {
				if r.oid != c.oid {
					t.Errorf("%s: %s\n  OID %d, want %d", proto, c.sql, r.oid, c.oid)
				}
				if len(r.rows) != 1 || string(r.rows[0]) != c.value {
					t.Errorf("%s: %s\n  sent %q, PostgreSQL 17.11 sends %q", proto, c.sql, r.rows, c.value)
				}
			}
		})
	}

	// WHAT A CREATE TABLE … AS STORES: each renderer's text in a column.
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()
	const ctas = `CREATE TABLE rn_c AS SELECT id, json_build_object('d', d, 'ts', ts) AS j, ` +
		`format('%s', f) AS f, regexp_replace(s, 'z', 'Z') AS r FROM ss_t`
	if _, err := conn.Exec(ctx, ctas).ReadAll(); err != nil {
		t.Fatalf("%s: %v", ctas, err)
	}
	for _, c := range []struct{ sql, value string }{
		{`SELECT j AS v FROM rn_c WHERE id = 5`, `{"d" : "1969-12-31", "ts" : "1969-12-31T23:59:59.999"}`},
		{`SELECT f AS v FROM rn_c WHERE id = 5`, "100.125"},
		{`SELECT r AS v FROM rn_c WHERE id = 3`, "Zz"},
	} {
		r := ssWireRead(t, conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}), c.sql)
		if len(r.rows) != 1 || string(r.rows[0]) != c.value {
			t.Errorf("stored: %s\n  sent %q, PostgreSQL 17.11 sends %q", c.sql, r.rows, c.value)
		}
	}
}
