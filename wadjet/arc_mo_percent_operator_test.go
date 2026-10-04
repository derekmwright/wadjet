// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// THE `%` OPERATOR IS MOD (#1527), embedded: what a query answers and what
// CREATE TABLE AS and INSERT … SELECT store. `a % b` parses to mod(a, b); the
// operator's own implementation answered `t.n % '2.5'` 0 | -1 | 0 | 0 | 0
// (PostgreSQL 2.25 | -1.00 | 0.00 | 0.00 | 0.01), raised XX000 "integer
// divide by zero" for `NULL % t.n`, and summed `t.i % NULL` to 0.
//
// Every operand pair below runs in both spellings, as a query and as a
// CREATE TABLE AS: the `%` spelling answers and stores exactly what MOD does
// (rows and declared column types), and nothing answers an internal error.
// The pinned cells are PostgreSQL 17.11's answers. The five-arm table is
// coordinator.TestArcMOPercentIsModEveryArm.

func moOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2))",
		"INSERT INTO ss_t VALUES (1, 3, 30, 1.5, 2.25), (2, -7, -70, -2.5, -3.5), (3, 5, 9000000000, 0.25, 10.00), " +
			"(4, 0, 0, 0.0, 0.00), (5, 1, 1, 100.125, 0.01), (6, NULL, NULL, NULL, NULL)",
		"CREATE TABLE ss_i (id BIGINT, v INT, g DOUBLE, m NUMERIC(10,2))",
		"INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	return db
}

// moRun renders a statement's answer: "ERR <sqlstate>" or the declared
// column types and the rows.
func moRun(db *DB, sql string) string {
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		return "ERR " + sqlerr.StateOf(err) + " " + err.Error()
	}
	var types []string
	for _, m := range res.ColumnMetas {
		types = append(types, m.TypeID.String())
	}
	rows := make([]string, len(res.Rows))
	for i := range res.Rows {
		var c []string
		for _, v := range res.Cells(i) {
			if v == nil {
				c = append(c, "NULL")
				continue
			}
			c = append(c, fmt.Sprint(v))
		}
		rows[i] = strings.Join(c, ",")
	}
	return "{" + strings.Join(types, ",") + "} " + strings.Join(rows, " | ")
}

func moState(s string) string {
	if rest, ok := strings.CutPrefix(s, "ERR "); ok {
		st, _, _ := strings.Cut(rest, " ")
		return "ERR " + st
	}
	return s
}

func TestArcMOPercentIsModEmbedded(t *testing.T) {
	db := moOpen(t)
	ops := []string{"t.i", "t.b", "t.n", "t.f", "8", "7.5", "2.5", "0", "'2.5'", "'8'", "'0'", "'abc'", "NULL",
		"CAST(NULL AS INT)", "(SELECT v FROM ss_i WHERE id = 1)", "(SELECT m FROM ss_i WHERE id = 1)"}
	n := 0
	for _, a := range ops {
		for _, b := range ops {
			n++
			for _, shape := range []string{
				"SELECT t.id, %s FROM ss_t t ORDER BY t.id",
				"SELECT SUM(%s) FROM ss_t t",
				"DROP TABLE IF EXISTS mo_e ;; CREATE TABLE mo_e AS SELECT t.id, %s AS v FROM ss_t t ;; SELECT * FROM mo_e ORDER BY id",
			} {
				run := func(e string) string {
					var out string
					for _, st := range strings.Split(fmt.Sprintf(shape, e), " ;; ") {
						r := moRun(db, st)
						if strings.HasPrefix(r, "ERR") && !strings.HasPrefix(st, "DROP") {
							return r
						}
						out = r
					}
					return out
				}
				pct, mod := a+" % "+b, "MOD("+a+", "+b+")"
				gp, gm := run(pct), run(mod)
				if strings.HasPrefix(gp, "ERR XX000") || strings.HasPrefix(gm, "ERR XX000") {
					t.Errorf("%s: an internal error is never an answer: %% %s | MOD %s", fmt.Sprintf(shape, pct), gp, gm)
				}
				if moState(gp) != moState(gm) {
					t.Errorf("%s\n  %%    %s\n  MOD  %s (the operator is MOD)", fmt.Sprintf(shape, pct), gp, gm)
				}
			}
		}
	}
	if n != len(ops)*len(ops) {
		t.Fatal("operand grid incomplete")
	}
}

func TestArcMOPercentIsModStored(t *testing.T) {
	db := moOpen(t)
	for _, tc := range []struct{ sql, want string }{
		// PostgreSQL 17.11: NULL | NULL | NULL | NULL | NULL | NULL, numeric
		// (here double precision, the MOD declaration of an untyped NULL
		// operand: filing candidate MO-C2).
		{"DROP TABLE IF EXISTS mo_s1 ;; CREATE TABLE mo_s1 AS SELECT t.id, NULL % t.n AS v FROM ss_t t ;; SELECT * FROM mo_s1 ORDER BY id",
			"{INT64,FLOAT64} 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL"},
		// PostgreSQL 17.11: integer 0 | -1 | 2 | 0 | 1 | NULL.
		{"DROP TABLE IF EXISTS mo_s2 ;; CREATE TABLE mo_s2 AS SELECT t.id, t.i % 3 AS v FROM ss_t t ;; SELECT * FROM mo_s2 ORDER BY id",
			"{INT64,INT32} 1,0 | 2,-1 | 3,2 | 4,0 | 5,1 | 6,NULL"},
		// PostgreSQL 17.11: 2.25 | -1.00 | 0.00 | 0.00 | 0.01 | NULL.
		{"DROP TABLE IF EXISTS mo_s3 ;; CREATE TABLE mo_s3 (id BIGINT, v NUMERIC(10,2)) ;; INSERT INTO mo_s3 SELECT t.id, t.n % '2.5' FROM ss_t t ;; SELECT * FROM mo_s3 ORDER BY id",
			"{INT64,DECIMAL} 1,2.25 | 2,-1.00 | 3,0.00 | 4,0.00 | 5,0.01 | 6,NULL"},
		// PostgreSQL 17.11: 2 | -1 | 0 | 0 | 0 | NULL (2.25 rounds to 2).
		{"DROP TABLE IF EXISTS mo_s4 ;; CREATE TABLE mo_s4 (id BIGINT, v INTEGER) ;; INSERT INTO mo_s4 SELECT t.id, t.n % '2.5' FROM ss_t t ;; SELECT * FROM mo_s4 ORDER BY id",
			"{INT64,INT32} 1,2 | 2,-1 | 3,0 | 4,0 | 5,0 | 6,NULL"},
		// PostgreSQL 17.11: NULL; 22012; 1.26.
		{"SELECT SUM(t.i % NULL) FROM ss_t t", "{FLOAT64} NULL"},
		{"SELECT SUM(t.n % '0') FROM ss_t t", "ERR 22012"},
		{"SELECT SUM(t.n % '2.5') FROM ss_t t", "{FLOAT64} 1.26"},
	} {
		var got string
		for _, st := range strings.Split(tc.sql, " ;; ") {
			r := moRun(db, st)
			if strings.HasPrefix(r, "ERR") && !strings.HasPrefix(st, "DROP") {
				got = moState(r)
				break
			}
			got = r
		}
		if got != tc.want {
			t.Errorf("%s\n  got  %s\n  want %s", tc.sql, got, tc.want)
		}
	}
}
