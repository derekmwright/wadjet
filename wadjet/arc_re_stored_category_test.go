// SPDX-License-Identifier: MIT

package wadjet

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A STORED COLUMN KEEPS THE CATEGORY IT WAS CREATED WITH (#381, arc RE round
// 3): read from storage, not from the session that created it.
//
// CREATE TABLE AS of an expression PostgreSQL types numeric and this engine
// computes in a double (`5 / 2.0 + id * 0`, `sqrt(6.25 + id * 0)`, `power`,
// `sqrt(n * n)`, `ln(exp(…))`) creates a DOUBLE PRECISION column marked with
// that category (parquet.Column.PGNumeric), and `round()` / the integer CAST
// over the stored column round half away from zero as PostgreSQL rounds its
// numeric column. At 1f580f7d they rounded half to even: `round(b)` over a
// stored sqrt(6.25) answered 2 where PostgreSQL 17.11 and 89cea148 answer 3.
// The mark is the catalog record's and the file footer's: the answers hold
// after the database is closed and reopened, after compaction rewrites every
// file, and after both; the rewritten files still carry it in their footer.
// Every want is PostgreSQL 17.11's answer to the same statements.
func TestArcREStoredColumnKeepsItsCategoryAcrossReopenAndCompaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	open := func() *DB {
		db, err := Open(ctx, Config{DataDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	for _, q := range []string{
		"CREATE TABLE rs_a (id BIGINT, f DOUBLE PRECISION, n NUMERIC(38,16), i INTEGER)",
		"INSERT INTO rs_a VALUES (1, 0.5, 0.5, 1), (2, 1.5, 1.5, 3), (3, 2.5, 2.5, 5), (4, 3.5, 3.5, 7), (5, -0.5, -0.5, -1), (6, -1.5, -1.5, -3), (7, -2.5, -2.5, -5), (8, NULL, NULL, NULL)",
		"CREATE TABLE rs_h AS SELECT id, 5 / 2.0 + id * 0 AS a, sqrt(6.25 + id * 0) AS b, power(2.5 + id * 0, 1) AS c, i / 2.0 AS d, sqrt(n * n) AS e, ln(exp(2.5 + id * 0)) AS g FROM rs_a WHERE id IN (1,3)",
		"INSERT INTO rs_h SELECT id + 10, 5 / 2.0, sqrt(6.25), power(2.5, 1), i / 2.0, sqrt(n * n), 2.5 FROM rs_a WHERE id = 3",
		"INSERT INTO rs_h VALUES (20, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5)",
		"UPDATE rs_h SET b = 0.5 WHERE id = 1",
		"CREATE TABLE rs_n AS SELECT id, sqrt(6.25 + id * 0) AS b FROM rs_a WITH NO DATA",
		"INSERT INTO rs_n SELECT id, sqrt(6.25 + id * 0) FROM rs_a WHERE id IN (1,3)",
		"CREATE TABLE rs_k AS SELECT id, b, b + 0 AS bn, b + CAST(0 AS DOUBLE PRECISION) AS bf FROM rs_h WHERE id IN (3, 13)",
		"CREATE TABLE rs_c AS SELECT id, i / 2.0 AS x, sqrt(i * i / 4.0) AS s, f AS g FROM rs_a WHERE id IN (1,3,5,7)",
		// The assignment doors read the stored column's category too.
		"CREATE TABLE rs_m AS SELECT id, CAST(0 AS INTEGER) AS v, sqrt(6.25 + id * 0) AS b FROM rs_a WHERE id IN (1,3)",
		"UPDATE rs_m SET v = b",
		"CREATE TABLE rs_mt (id BIGINT, v INTEGER, w INTEGER, x INTEGER)",
		"INSERT INTO rs_mt VALUES (1, 0, 0, 0), (3, 0, 0, 0)",
		"MERGE INTO rs_mt t USING rs_m s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.b, w = s.b + 0",
		"INSERT INTO rs_mt SELECT id + 100, b, b, b FROM rs_m",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// PostgreSQL 17.11 (wadjet-pg-re2), the same statements.
	want := []struct{ q, w string }{
		{"SELECT id, round(a), round(b), round(c), round(d), round(e), round(g) FROM rs_h ORDER BY 1",
			"1,3,1,3,1,1,3; 3,3,3,3,3,3,3; 13,3,3,3,3,3,3; 20,3,3,3,3,3,3"},
		{"SELECT id, CAST(a AS INTEGER), CAST(b AS INTEGER), CAST(c AS INTEGER), CAST(e AS INTEGER) FROM rs_h ORDER BY 1",
			"1,3,1,3,1; 3,3,3,3,3; 13,3,3,3,3; 20,3,3,3,3"},
		{"SELECT id, b::bigint, CAST(ARRAY[b] AS BIGINT[]), round(b, 0) FROM rs_h ORDER BY 1",
			"1,1,{1},1; 3,3,{3},3; 13,3,{3},3; 20,3,{3},3"},
		{"SELECT id FROM rs_h WHERE round(b) = 3 ORDER BY 1", "3; 13; 20"},
		{"SELECT id, round(b), CAST(b AS INTEGER) FROM rs_n ORDER BY 1", "1,3,3; 3,3,3"},
		{"SELECT id, round(b), round(bn), round(bf) FROM rs_k ORDER BY 1", "3,3,3,2; 13,3,3,2"},
		{"SELECT id, round(x), CAST(x AS INTEGER), round(s), round(g) FROM rs_c ORDER BY 1",
			"1,1,1,1,0; 3,3,3,3,2; 5,-1,-1,1,-0; 7,-3,-3,3,-2"},
		{"SELECT round((SELECT b FROM rs_h WHERE id = 3))", "3"},
		{"SELECT id, v FROM rs_m ORDER BY 1", "1,3; 3,3"},
		{"SELECT id, v, w, x FROM rs_mt ORDER BY 1", "1,3,3,0; 3,3,3,0; 101,3,3,3; 103,3,3,3"},
	}
	const marks = "rs_c.s rs_h.a rs_h.b rs_h.c rs_h.e rs_h.g rs_k.b rs_k.bn rs_m.b rs_n.b"
	check := func(db *DB, when string) {
		t.Helper()
		for _, c := range want {
			res, err := db.Query(ctx, c.q)
			if err != nil {
				t.Errorf("%s: %s: %v", when, c.q, err)
				continue
			}
			rows := make([]string, len(res.Rows))
			for i := range res.Rows {
				cells := res.Cells(i)
				f := make([]string, len(cells))
				for j, v := range cells {
					f[j] = reText(v)
				}
				rows[i] = strings.Join(f, ",")
			}
			if got := strings.Join(rows, "; "); got != c.w {
				t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", when, c.q, got, c.w)
			}
		}
		var got []string
		for _, name := range []string{"rs_a", "rs_c", "rs_h", "rs_k", "rs_m", "rs_mt", "rs_n"} {
			tb, err := db.Catalog().GetTable(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range tb.Schema.Columns {
				if c.PGNumeric {
					got = append(got, name+"."+c.Name)
				}
			}
		}
		// The declaration is ADR-0024 §2c's divergence and stays: the column
		// is double precision here (numeric on PostgreSQL), its values the
		// doubles the expression computed.
		if res, err := db.Query(ctx, "SELECT data_type FROM information_schema.columns WHERE table_name = 'rs_h' AND column_name = 'b'"); err != nil || len(res.Rows) != 1 || reText(res.Cells(0)[0]) != "double precision" {
			t.Errorf("%s: rs_h.b is not declared double precision (%v)", when, err)
		}
		sort.Strings(got)
		if g := strings.Join(got, " "); g != marks {
			t.Errorf("%s: the catalog marks %s, want %s", when, g, marks)
		}
	}
	// footers is the marked columns of every data file of a table, as the
	// footer's declared schema says.
	footers := func(db *DB, name string) string {
		t.Helper()
		m, err := db.Catalog().GetManifest(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		files := 0
		for _, part := range m.Partitions {
			for _, f := range part.Files {
				rc, _, err := db.store.Get(ctx, db.bucket, f.Path)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(rc)
				rc.Close()
				if err != nil {
					t.Fatal(err)
				}
				r, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				files++
				for _, c := range r.Schema().Columns {
					if c.PGNumeric {
						seen[c.Name] = true
					}
				}
			}
		}
		var out []string
		for k := range seen {
			out = append(out, k)
		}
		sort.Strings(out)
		if files == 0 {
			t.Fatalf("%s has no data files", name)
		}
		return strings.Join(out, " ")
	}
	check(db, "created")
	if got := footers(db, "rs_h"); got != "a b c e g" {
		t.Errorf("the rs_h files' footers mark %q, want \"a b c e g\"", got)
	}
	db.Close()
	db = open()
	check(db, "reopened")
	c := compaction.New(db.Catalog(), slog.Default(), compaction.Config{})
	for _, name := range []string{"rs_h", "rs_n", "rs_k", "rs_c"} {
		if _, err := c.RewriteTable(ctx, name); err != nil {
			t.Fatalf("compacting %s: %v", name, err)
		}
	}
	check(db, "compacted")
	if got := footers(db, "rs_h"); got != "a b c e g" {
		t.Errorf("the compacted rs_h files' footers mark %q, want \"a b c e g\"", got)
	}
	db.Close()
	db = open()
	defer db.Close()
	check(db, "compacted and reopened")
}
