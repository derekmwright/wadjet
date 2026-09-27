// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The embedded arm of the membership operand rule (`DB.Query`, the raw
// positional `Result.RowValues` via QueryResult.Cells), over arc ST's
// `st_pair` fixture (stEmbeddedDB), for the six issues' own statements. At
// v0.25.1 the first two families were WRONG VALUES on this arm: a quoted
// literal against a typed body answered no rows (#1372) and a DATE against a
// set-operation or literal body answered 0 (#1373), where PostgreSQL 17.11
// answers the rows below; the other four answered where PostgreSQL raises
// 42883 (#1369 #1370 #1368 #1374).
func TestArcSMEmbeddedMembershipOperands(t *testing.T) {
	db := stEmbeddedDB(t)
	ctx := context.Background()
	const n = "SELECT count(*) AS n FROM st_pair a WHERE "
	cases := []struct{ name, sql, want, state, msg string }{
		{name: "1372/in", sql: "SELECT a.id FROM st_pair a WHERE '12' IN (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3) ORDER BY a.id", want: "[[1] [2] [3] [4]]"},
		{name: "1372/eqAny", sql: "SELECT a.id FROM st_pair a WHERE '12' = ANY (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3) ORDER BY a.id", want: "[[1] [2] [3] [4]]"},
		{name: "1372/notIn", sql: "SELECT a.id FROM st_pair a WHERE '12' NOT IN (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3) ORDER BY a.id", want: "[]"},
		{name: "1372/numeric", sql: "SELECT a.id FROM st_pair a WHERE '12.5' IN (SELECT r.v_dec FROM st_pair r WHERE r.id <= 3) ORDER BY a.id", want: "[[1] [2] [3] [4]]"},
		{name: "1372/notAValue", sql: "SELECT a.id FROM st_pair a WHERE 'zz' IN (SELECT r.v_i64 FROM st_pair r WHERE r.id <= 3)",
			state: "22P02", msg: `invalid input syntax for type bigint: "zz"`},
		{name: "1373/unionAll", sql: n + "a.v_date IN (SELECT r.v_date FROM st_pair r WHERE r.id = 1 UNION ALL SELECT r.v_date FROM st_pair r WHERE r.id = 2)", want: "[[2]]"},
		{name: "1373/union", sql: n + "a.v_date IN (SELECT r.v_date FROM st_pair r WHERE r.id = 1 UNION SELECT r.v_date FROM st_pair r WHERE r.id = 2)", want: "[[2]]"},
		{name: "1373/literal", sql: n + "a.v_date IN (SELECT DATE '2024-01-02' FROM st_pair r WHERE r.id = 1)", want: "[[1]]"},
		{name: "1373/castUnion", sql: n + "a.v_date IN (SELECT CAST('2024-01-02' AS DATE) FROM st_pair r WHERE r.id = 1 UNION SELECT CAST('2024-03-04' AS DATE) FROM st_pair r WHERE r.id = 2)", want: "[[2]]"},
		{name: "1373/fromless", sql: n + "a.v_date IN (SELECT DATE '2024-01-02' UNION ALL SELECT DATE '2024-03-04')", want: "[[2]]"},
		{name: "1373/eqAny", sql: n + "a.v_date = ANY (SELECT r.v_date FROM st_pair r WHERE r.id = 1 UNION ALL SELECT r.v_date FROM st_pair r WHERE r.id = 2)", want: "[[2]]"},
		{name: "1373/notIn", sql: n + "a.v_date NOT IN (SELECT DATE '2024-01-02' FROM st_pair r WHERE r.id = 1 UNION ALL SELECT DATE '2024-03-04' FROM st_pair r WHERE r.id = 2)", want: "[[1]]"},
		{name: "1369/in", sql: n + "a.v_i64 + 0 IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)", state: "42883", msg: "operator does not exist: bigint = text"},
		{name: "1369/notIn", sql: n + "a.v_i64 + 0 NOT IN (SELECT r.s_i64 FROM st_pair r WHERE r.id <= 3)", state: "42883", msg: "operator does not exist: bigint = text"},
		{name: "1370/upper", sql: n + "a.v_i64 IN (SELECT upper(r.s_i64) FROM st_pair r WHERE r.id <= 3)", state: "42883", msg: "operator does not exist: bigint = text"},
		{name: "1370/concat", sql: n + "a.v_i64 IN (SELECT r.s_i64 || '' FROM st_pair r WHERE r.id <= 3)", state: "42883", msg: "operator does not exist: bigint = text"},
		{name: "1370/date", sql: n + "a.v_date IN (SELECT upper(r.s_date) FROM st_pair r WHERE r.id <= 2)", state: "42883", msg: "operator does not exist: date = text"},
		{name: "1370/uuidLower", sql: n + "a.v_uuid IN (SELECT lower(r.s_uuid) FROM st_pair r WHERE r.id <= 3)", state: "42883", msg: "operator does not exist: uuid = text"},
		{name: "1368/lateral", sql: "SELECT count(*) AS n FROM st_pair a, LATERAL (SELECT r.s_i64 FROM st_pair r WHERE r.s_i64 = a.v_i64) l", state: "42883", msg: "operator does not exist: text = bigint"},
		{name: "1374/in", sql: n + "a.v_dec IN (SELECT CAST(r.v_i64 AS TEXT) FROM st_pair r WHERE r.id <= 3)", state: "42883", msg: "operator does not exist: numeric = text"},
		{name: "1374/exists", sql: n + "EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND a.v_dec = CAST(r.v_i64 AS TEXT))", state: "42883", msg: "operator does not exist: numeric = text"},
		{name: "1374/notExists", sql: n + "NOT EXISTS (SELECT 1 FROM st_pair r WHERE r.id <= 3 AND a.v_dec = CAST(r.v_i64 AS TEXT))", state: "42883", msg: "operator does not exist: numeric = text"},
	}
	for _, c := range cases {
		res, err := db.Query(ctx, c.sql)
		if c.state != "" {
			if err == nil {
				t.Errorf("%s: %s\n  got  %s\n  want %s %q (PostgreSQL 17.11)", c.name, c.sql, stCells(res), c.state, c.msg)
				continue
			}
			if st := sqlerr.StateOf(err); st != c.state || !strings.Contains(err.Error(), c.msg) {
				t.Errorf("%s: %s\n  got  %s %v\n  want %s %q", c.name, c.sql, st, err, c.state, c.msg)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s", c.name, c.sql, err, c.want)
			continue
		}
		if got := stCells(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
}
