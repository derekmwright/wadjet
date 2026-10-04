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

// A double that has no int64 is REFUSED where it is assigned, and the
// statement stores nothing (#1484).
//
// At 93e4804e the assignment cast's range check compared the rounded double
// against 9.223372036854776e18 — which is 2^63 — with `>`, so `INSERT INTO
// t(a BIGINT) SELECT f` with f the double 2^63 STORED -9223372036854775808
// where PostgreSQL raises 22003 and the explicit CAST of the same value is
// 22003 here too. Every origin that hands the assignment a double reached it:
// a column, a float expression, a UNION ALL arm, CASE, COALESCE, max(), an
// array element, a REAL column, POWER over numerics. The bound is now
// parquet.FloatToInt64, the one the CAST kernel shares.
//
// Each cell reads the table back after the statement, so a refusal must
// leave what it found: INSERT stores no row of a statement that fails on one,
// UPDATE and MERGE change no row. Every want is PostgreSQL 17.11's over the
// same DDL and rows (iw_author/pg_embedded.txt).
func TestArcIWEmbeddedFloatToIntegerAssignment(t *testing.T) {
	ctx := context.Background()
	const f63 = "CAST('9223372036854775808' AS DOUBLE PRECISION)"
	const m63 = "CAST('-9223372036854775808' AS DOUBLE PRECISION)"
	setup := []string{
		"CREATE TABLE e_s (id BIGINT, f DOUBLE PRECISION)",
		"INSERT INTO e_s VALUES (1, 1), (2, " + f63 + "), (3, 2.5), (4, " + m63 + ")",
		"CREATE TABLE e_r (id BIGINT, r REAL)",
		"INSERT INTO e_r VALUES (1, CAST('9223372036854775808' AS REAL))",
		"CREATE TABLE e_af (af ARRAY(DOUBLE))",
		"INSERT INTO e_af VALUES (ARRAY[" + f63 + "])",
	}
	cases := []struct {
		name  string
		stmts []string
		want  string
	}{
		{"issue", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT f FROM e_s WHERE id = 2", "SELECT a FROM e_t"}, "ERR 22003 => (0 rows)"},
		{"insertSelectAtomic", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT f FROM e_s", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"insertSelectInRange", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT f FROM e_s WHERE id <> 2", "SELECT a FROM e_t ORDER BY a"}, "-9223372036854775808; 1; 2"},
		{"insertValuesAtomic", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t VALUES (1), (" + f63 + "), (2)", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"updateAtomic", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT, f DOUBLE PRECISION)", "INSERT INTO e_t SELECT id, 7, f FROM e_s", "UPDATE e_t SET a = f", "SELECT id, a FROM e_t ORDER BY id"}, "ERR 22003 => 1,7; 2,7; 3,7; 4,7"},
		{"updateInRange", []string{"CREATE TABLE e_t (id BIGINT, a BIGINT, f DOUBLE PRECISION)", "INSERT INTO e_t SELECT id, 7, f FROM e_s", "UPDATE e_t SET a = f WHERE id <> 2", "SELECT id, a FROM e_t ORDER BY id"}, "1,1; 2,7; 3,2; 4,-9223372036854775808"},
		{"mergeUpdate", []string{"CREATE TABLE e_m (id BIGINT, a BIGINT)", "INSERT INTO e_m VALUES (1, 0), (2, 0)", "MERGE INTO e_m USING e_s ON e_m.id = e_s.id WHEN MATCHED THEN UPDATE SET a = e_s.f", "SELECT id, a FROM e_m ORDER BY id"}, "ERR 22003 => 1,0; 2,0"},
		{"unionAll", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT " + f63 + " UNION ALL SELECT 1", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"case", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT CASE WHEN id = 2 THEN f END FROM e_s WHERE id = 2", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"coalesce", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT COALESCE(f, 0) FROM e_s WHERE id = 2", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"max", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT max(f) FROM e_s", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"floatExpr", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT f * 1 FROM e_s WHERE id = 2", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"power20", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT POWER(2.0, 63)", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"powerInt", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT POWER(2, 63)", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"realColumn", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT r FROM e_r", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"arrayElement", []string{"CREATE TABLE e_t (a BIGINT)", "INSERT INTO e_t SELECT af[1] FROM e_af", "SELECT count(*) FROM e_t"}, "ERR 22003 => 0"},
		{"intColumn", []string{"CREATE TABLE e_t (a INTEGER)", "INSERT INTO e_t SELECT f FROM e_s WHERE id IN (1, 3)", "SELECT a FROM e_t ORDER BY a"}, "1; 2"},
		{"ctasCast", []string{"CREATE TABLE e_c AS SELECT CAST(f AS BIGINT) AS a FROM e_s", "SELECT count(*) FROM e_c"}, "ERR 22003 => ERR 42P01"},
		{"ctasCastInRange", []string{"CREATE TABLE e_c AS SELECT CAST(f AS BIGINT) AS a FROM e_s WHERE id <> 2", "SELECT a FROM e_c ORDER BY a"}, "-9223372036854775808; 1; 2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, s := range setup {
				if _, err := db.Query(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			var parts []string
			for _, s := range c.stmts {
				res, err := db.Query(ctx, s)
				if err != nil {
					parts = append(parts, "ERR "+sqlerr.StateOf(err))
					continue
				}
				if !strings.HasPrefix(s, "SELECT") {
					continue
				}
				var rows []string
				for i := range res.Rows {
					cells := res.Cells(i)
					f := make([]string, len(cells))
					for j, v := range cells {
						f[j] = fmt.Sprint(v)
					}
					rows = append(rows, strings.Join(f, ","))
				}
				if len(rows) == 0 {
					rows = []string{"(0 rows)"}
				}
				parts = append(parts, strings.Join(rows, "; "))
			}
			if got := strings.Join(parts, " => "); got != c.want {
				t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", strings.Join(c.stmts, " ;; "), got, c.want)
			}
		})
	}
}
