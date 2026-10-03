// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The embedded engine (#1386 #1392 #1450): `DB.Query` over the issues' own
// statements, each result read positionally (QueryResult.Cells) by its
// declared type, and the values an INSERT … SELECT STORES into INTEGER,
// NUMERIC(10,2), DOUBLE PRECISION and NUMERIC(30,20) columns. Every want is
// PostgreSQL 17.11's, measured over the same DDL. At v0.25.3 the integer CAST
// beside a numeric was a double (`CAST(i AS INTEGER) % n` 0.00999999999999998
// for 0.00, `CAST(b AS BIGINT) * 10000000 * n - 3` 9e+17), the wide constant
// in a choice / unary minus / scalar subquery was 14 and compared equal to 14,
// and the explicit integer CAST of `5 / 2.0`, `SQRT(6.25)`, `POWER(2.5, 1)`
// rounded the double half to even (2).
func TestArcNXEmbeddedNumericCarrier(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE nx_e (id BIGINT, i INT, b BIGINT, n NUMERIC(10,2))",
		"INSERT INTO nx_e VALUES (1, 3, 30, 2.25), (3, 5, 9000000000, 10.00), (5, 1, 1, 0.01), (6, NULL, NULL, NULL)",
		"CREATE TABLE nx_tgt (k VARCHAR, i INT, n NUMERIC(10,2), f DOUBLE, w NUMERIC(30,20))",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	const k = "14.0000000000000000001"
	cases := []struct{ name, sql, want string }{
		{"1450/modMulPast2^53",
			"SELECT t.id, CAST(t.i AS INTEGER) % t.n AS m, CAST(t.i AS INTEGER) * 0.1 AS p, " +
				"CAST(t.b AS BIGINT) * 10000000 * t.n - 3 AS q FROM nx_e t ORDER BY t.id",
			"{int,numeric,numeric,numeric} 1,0.75,0.3,674999997.00 | 3,5.00,0.5,899999999999999997.00 | " +
				"5,0.00,0.1,99997.00 | 6,NULL,NULL,NULL"},
		{"1386/choiceUnarySubquery",
			"SELECT COALESCE(" + k + ", 0) AS a, CASE WHEN true THEN " + k + " END = 14 AS b, LEAST(" + k + ", 20) = 14 AS c, " +
				"-(-" + k + ") = 14 AS d, (SELECT COALESCE(" + k + ", 0)) AS e",
			"{numeric,bool,bool,bool,numeric} 14.0000000000000000001,false,false,false,14.0000000000000000001"},
		{"1386/where", "SELECT count(*) AS c FROM nx_e a WHERE COALESCE(" + k + ", 0) = 14", "{int} 0"},
		{"1386/greatest", "SELECT a.id, GREATEST(a.n, " + k + ") AS g FROM nx_e a ORDER BY a.id",
			"{int,numeric} 1,14.0000000000000000001 | 3,14.0000000000000000001 | 5,14.0000000000000000001 | " +
				"6,14.0000000000000000001"},
		{"1392/six",
			"SELECT CAST(5 / 2.0 AS INTEGER) AS a, CAST(SQRT(6.25) AS INTEGER) AS b, CAST(POWER(2.5, 1) AS INTEGER) AS c, " +
				"CAST(2.5 * 1 AS INTEGER) AS d, CAST(2.5 AS INTEGER) AS e, CAST(ABS(2.5) AS INTEGER) AS f",
			"{int,int,int,int,int,int} 3,3,3,3,3,3"},
		{"1392/negSmallBig",
			"SELECT CAST(-(5 / 2.0) AS INTEGER) AS a, CAST(-SQRT(6.25) AS INTEGER) AS b, " +
				"CAST(5 / 2.0 AS SMALLINT) AS c, CAST(SQRT(6.25) AS BIGINT) AS d",
			"{int,int,int,int} -3,-3,3,3"},
		// A bare NUMERIC cast is a DECIMAL operand of a choice and of a
		// comparison, never its rendered text (round 5, B1): by byte order
		// "3" sorts above "100" and "25".
		{"b1/bareCastChoice",
			"SELECT a.id, LEAST(CAST(a.i AS NUMERIC), 100) AS l, LEAST(CAST('3' AS NUMERIC), 100) AS t, " +
				"CAST(3 AS NUMERIC) > 25 AS g, GREATEST(CAST(a.i + 20 AS NUMERIC), 100) AS h FROM nx_e a ORDER BY a.id",
			"{int,numeric,numeric,bool,numeric} 1,3,3,false,100 | 3,5,3,false,100 | 5,1,3,false,100 | 6,100,3,false,100"},
		{"b1/bareCastCoalesceWhere", "SELECT count(*) AS c FROM nx_e a WHERE COALESCE(CAST(a.i AS NUMERIC), 100) > 25", "{int} 1"},
	}
	for _, c := range cases {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s", c.name, c.sql, err, c.want)
			continue
		}
		if got := ssEmbeddedRender(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
	// WHAT IS STORED: each operand kind as an INSERT … SELECT source.
	for _, ins := range []string{
		"INSERT INTO nx_tgt (k, i) SELECT 'i', CAST(SQRT(6.25) AS INTEGER) + CAST(5 / 2.0 AS INTEGER)",
		"INSERT INTO nx_tgt (k, n) SELECT 'n', CAST(t.i AS INTEGER) * 0.105 FROM nx_e t WHERE t.id = 1",
		"INSERT INTO nx_tgt (k, f) SELECT 'f', CAST(t.i AS INTEGER) * 0.1 FROM nx_e t WHERE t.id = 1",
		"INSERT INTO nx_tgt (k, w) SELECT 'w', COALESCE(" + k + ", 0)",
		"INSERT INTO nx_tgt (k, w) SELECT 'wb', CAST(t.b AS BIGINT) * 10000000 * t.n / 100000000000 FROM nx_e t WHERE t.id = 3",
	} {
		if _, err := db.Query(ctx, ins); err != nil {
			t.Errorf("%s\n  refused: %v", ins, err)
		}
	}
	const stored = "SELECT k, i, n, f, w FROM nx_tgt ORDER BY k"
	want := "{text,int,numeric,float,numeric} f,NULL,NULL,0.3,NULL | i,6,NULL,NULL,NULL | n,NULL,0.32,NULL,NULL | " +
		"w,NULL,NULL,NULL,14.00000000000000000010 | wb,NULL,NULL,NULL,9000000.00000000000000000000"
	res, err := db.Query(ctx, stored)
	if err != nil {
		t.Fatalf("%s: %v", stored, err)
	}
	if got := ssEmbeddedRender(res); got != want {
		t.Errorf("stored values\n  got  %s\n  want %s (PostgreSQL 17.11)", got, want)
	}
}
