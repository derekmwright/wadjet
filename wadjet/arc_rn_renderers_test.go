// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The embedded engine (#1474 #1466 #1467 #1481): `DB.Query` over the issues'
// own statements on the arc SS fixture, unspilled and under a 512 KiB budget,
// the network types (whose text is the engine's own, CAST(… AS TEXT)'s),
// and the text an INSERT … SELECT STORES in a VARCHAR column when a renderer
// is the source. Every want is PostgreSQL 17.11's, measured over the same
// DDL. At c39858f3 the nested object was an escaped string, the DATE /
// TIMESTAMP were the day count / epoch milliseconds (19786, 1709553600000),
// format printed `%!s(float64=6.375)|` and `%!s(<nil>)|`, and regexp_replace
// without 'g' replaced every match (-1z.5z).
func TestArcRNEmbeddedRenderers(t *testing.T) {
	for _, budget := range []int64{0, 512 * 1024} {
		t.Run(fmt.Sprintf("budget=%d", budget), func(t *testing.T) {
			ctx := context.Background()
			cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
			if budget > 0 {
				cfg.MemoryBudget = budget
				cfg.SpillDir = t.TempDir()
			}
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, ddl := range []string{
				"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2), s VARCHAR, o BOOLEAN, d DATE, ts TIMESTAMP, u UUID, a ARRAY(INT))",
				"INSERT INTO ss_t VALUES (1, 3, 30, 1.5, 2.25, 'abc', true, '2024-03-04', '2024-03-04 12:00:00', '00000000-0000-4000-8000-000000000001', ARRAY[1,2])",
				"INSERT INTO ss_t VALUES (2, -7, -70, -2.5, -3.5, 'Hello', false, '1970-01-01', '1970-01-01 00:00:00', '00000000-0000-4000-8000-000000000002', ARRAY[3])",
				"INSERT INTO ss_t VALUES (3, 5, 9000000000, 0.25, 10, 'zz', true, '9999-12-31', '9999-12-31 23:59:59.999', '00000000-0000-4000-8000-000000000003', ARRAY[4,5,6])",
				"INSERT INTO ss_t VALUES (4, 0, 0, 0, 0, '', false, '1000-01-01', '1000-01-01 00:00:00', '00000000-0000-4000-8000-000000000004', ARRAY[7])",
				"INSERT INTO ss_t VALUES (5, 1, 1, 100.125, 0.01, 'x', true, '1969-12-31', '1969-12-31 23:59:59.999', '00000000-0000-4000-8000-000000000005', ARRAY[8])",
				"INSERT INTO ss_t VALUES (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
				"CREATE TABLE rn_tgt (k VARCHAR, v VARCHAR)",
				"CREATE TABLE rn_net (ip IPV4, m MAC, ips ARRAY(IPV4))",
				"INSERT INTO rn_net VALUES ('10.0.0.1', 'aa:bb:cc:dd:ee:ff', ARRAY[CAST('10.0.0.2' AS IPV4)])",
			} {
				if _, err := db.Query(ctx, ddl); err != nil {
					t.Fatalf("%s: %v", ddl, err)
				}
			}
			cases := []struct{ name, sql, want string }{
				{"1474", "SELECT json_build_object('o', json_build_object('a', t.n, 'b', t.i)) FROM ss_t t WHERE t.id = 1",
					`{"o" : {"a" : 2.25, "b" : 3}}`},
				{"1466", "SELECT t.id, json_build_object('d', t.d, 'ts', t.ts) FROM ss_t t ORDER BY t.id",
					`1,{"d" : "2024-03-04", "ts" : "2024-03-04T12:00:00"} | ` +
						`2,{"d" : "1970-01-01", "ts" : "1970-01-01T00:00:00"} | ` +
						`3,{"d" : "9999-12-31", "ts" : "9999-12-31T23:59:59.999"} | ` +
						`4,{"d" : "1000-01-01", "ts" : "1000-01-01T00:00:00"} | ` +
						`5,{"d" : "1969-12-31", "ts" : "1969-12-31T23:59:59.999"} | ` +
						`6,{"d" : null, "ts" : null}`},
				{"1467/double", "SELECT format('%s|', t.f * t.n + 3) FROM ss_t t WHERE t.id = 1", "6.375|"},
				{"1467/null", "SELECT format('%s|', NULL)", "|"},
				{"1481", "SELECT t.id, regexp_replace(CAST(t.a[1] * t.n AS TEXT), '0', 'z') FROM ss_t t ORDER BY t.id",
					"1,2.25 | 2,-1z.50 | 3,4z.00 | 4,z.00 | 5,z.08 | 6,NULL"},
				// A network type has no PostgreSQL spelling: its text is the
				// one CAST(… AS TEXT) answers, and json writes it as that
				// string (it was the encoded integer, 167772161).
				{"network/json", "SELECT json_build_object('ip', ip, 'm', m, 'ips', ips) FROM rn_net",
					`{"ip" : "10.0.0.1", "m" : "aa:bb:cc:dd:ee:ff", "ips" : ["10.0.0.2"]}`},
				{"network/jsonKey", "SELECT json_build_object(ip, 1) FROM rn_net", `{"10.0.0.1" : 1}`},
				{"network/format", "SELECT format('%s|%s|%s', ip, m, ips) FROM rn_net", "10.0.0.1|aa:bb:cc:dd:ee:ff|{10.0.0.2}"},
				{"1481/g", "SELECT t.id, regexp_replace(CAST(t.a[1] * t.n AS TEXT), '0', 'z', 'g') FROM ss_t t ORDER BY t.id",
					"1,2.25 | 2,-1z.5z | 3,4z.zz | 4,z.zz | 5,z.z8 | 6,NULL"},
			}
			for _, c := range cases {
				res, err := db.Query(ctx, c.sql)
				if err != nil {
					t.Errorf("%s: %s\n  refused: %v\n  want %s", c.name, c.sql, err, c.want)
					continue
				}
				if got := rnEmbeddedRender(res); got != c.want {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
				}
			}
			// WHAT IS STORED: each renderer as an INSERT … SELECT source into
			// a VARCHAR column.
			for _, ins := range []string{
				"INSERT INTO rn_tgt SELECT 'j', json_build_object('d', d, 'ts', ts) FROM ss_t WHERE id = 3",
				"INSERT INTO rn_tgt SELECT 'n', json_build_object('o', json_build_object('a', n, 'b', i)) FROM ss_t WHERE id = 2",
				"INSERT INTO rn_tgt SELECT 'f', format('%s|%L|%s', f * n + 3, d, NULL) FROM ss_t WHERE id = 1",
				"INSERT INTO rn_tgt SELECT 'r', regexp_replace(CAST(a[1] * n AS TEXT), '0', 'z') FROM ss_t WHERE id = 3",
			} {
				if _, err := db.Query(ctx, ins); err != nil {
					t.Errorf("%s\n  refused: %v", ins, err)
				}
			}
			const stored = "SELECT k, v FROM rn_tgt ORDER BY k"
			want := `f,6.375|'2024-03-04'| | j,{"d" : "9999-12-31", "ts" : "9999-12-31T23:59:59.999"} | ` +
				`n,{"o" : {"a" : -3.50, "b" : -7}} | r,4z.00`
			res, err := db.Query(ctx, stored)
			if err != nil {
				t.Fatalf("%s: %v", stored, err)
			}
			if got := rnEmbeddedRender(res); got != want {
				t.Errorf("stored values\n  got  %s\n  want %s (PostgreSQL 17.11)", got, want)
			}
		})
	}
}

// rnEmbeddedRender is the rows positionally, `a,b | c,d`, NULL for a NULL.
func rnEmbeddedRender(res *QueryResult) string {
	rows := make([]string, 0, len(res.Rows))
	for r := range res.Rows {
		cells := res.Cells(r)
		parts := make([]string, len(cells))
		for i, v := range cells {
			if v == nil {
				parts[i] = "NULL"
				continue
			}
			parts[i] = fmt.Sprint(v)
		}
		rows = append(rows, strings.Join(parts, ","))
	}
	return strings.Join(rows, " | ")
}
