// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE EMBEDDED ARM of #1524: a select item that IS a GROUP BY key spelled
// with or without the relation's qualifier is the key — its value, its order
// and its declared type — in `DB.Query`, in the column a CREATE TABLE … AS
// declares (read from the catalog), and in what an INSERT … SELECT stores.
// Every want is PostgreSQL 17.11's over the same DDL. At 33e2fb92 the item
// spelled apart from its key was declared TEXT: `ORDER BY 1` put `20.00`
// before `4.50`, the CTAS column was TEXT, and the qualified key under a bare
// item was 42803.
func TestArcGKEmbeddedGroupKeySpelling(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE gk_e (id BIGINT, i INT, n NUMERIC(10,2), d DATE)",
		"INSERT INTO gk_e VALUES (1, 3, 2.25, '2024-03-04'), (2, -7, -3.50, '1970-01-01'), (3, 5, 10.00, '9999-12-31'), " +
			"(4, 0, 0.00, '1000-01-01'), (5, 1, 0.01, '1969-12-31'), (6, NULL, NULL, NULL)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, c := range []struct{ name, sql, want string }{
		{"intItemQual", "SELECT t.i + 1, COUNT(*) FROM gk_e t GROUP BY i + 1 ORDER BY 1",
			"{int,int} -6,1 | 1,1 | 2,1 | 4,1 | 6,1 | NULL,1"},
		{"numItemQual", "SELECT 2 * t.n, COUNT(*) FROM gk_e t GROUP BY 2 * n ORDER BY 1",
			"{numeric,int} -7.00,1 | 0.00,1 | 0.02,1 | 4.50,1 | 20.00,1 | NULL,1"},
		{"absItemQual", "SELECT ABS(-1) * t.n, COUNT(*) FROM gk_e t GROUP BY ABS(-1) * n ORDER BY 1",
			"{numeric,int} -3.50,1 | 0.00,1 | 0.01,1 | 2.25,1 | 10.00,1 | NULL,1"},
		{"numKeyQual", "SELECT 2 * n AS k, COUNT(*) FROM gk_e t GROUP BY 2 * t.n ORDER BY 1",
			"{numeric,int} -7.00,1 | 0.00,1 | 0.02,1 | 4.50,1 | 20.00,1 | NULL,1"},
		{"dateItemQual", "SELECT t.d + 1 AS k, COUNT(*) FROM gk_e t GROUP BY d + 1 ORDER BY 1",
			"{date,int} 1000-01-02,1 | 1970-01-01,1 | 1970-01-02,1 | 2024-03-05,1 | 10000-01-01,1 | NULL,1"},
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s", c.name, c.sql, err, c.want)
			continue
		}
		if got := ssEmbeddedRender(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}

	// CREATE TABLE … AS stores the KEY's type: PostgreSQL's information_schema
	// says integer, numeric, numeric, date.
	for _, c := range []struct {
		table, sql string
		want       parquet.TypeID
	}{
		{"gk_c1", "CREATE TABLE gk_c1 AS SELECT t.i + 1 AS k, count(*) AS c FROM gk_e t GROUP BY i + 1", parquet.TypeInt64},
		{"gk_c2", "CREATE TABLE gk_c2 AS SELECT 2 * t.n AS k, count(*) AS c FROM gk_e t GROUP BY 2 * n", parquet.TypeDecimal},
		{"gk_c3", "CREATE TABLE gk_c3 AS SELECT 2 * n AS k, count(*) AS c FROM gk_e t GROUP BY 2 * t.n", parquet.TypeDecimal},
		{"gk_c4", "CREATE TABLE gk_c4 AS SELECT t.d + 1 AS k FROM gk_e t GROUP BY d + 1", parquet.TypeDate},
	} {
		if _, err := db.Query(ctx, c.sql); err != nil {
			t.Errorf("%s refused: %v", c.sql, err)
			continue
		}
		schema, err := db.discoverTableSchema(ctx, c.table)
		if err != nil {
			t.Errorf("%s: reading the catalog: %v", c.table, err)
			continue
		}
		var got parquet.TypeID = -1
		for _, col := range schema.Columns {
			if col.Name == "k" {
				got = col.Type
			}
		}
		// An integer key is stored as either integer width; the class is the
		// question (PostgreSQL: integer).
		if got == parquet.TypeInt32 {
			got = parquet.TypeInt64
		}
		if got != c.want {
			t.Errorf("%s: the CTAS column k is declared %v, want %v (PostgreSQL 17.11)\n  %s", c.table, got, c.want, c.sql)
		}
	}
	// The CTAS column's ORDER is the key's, which a TEXT column cannot give.
	if res, err := db.Query(ctx, "SELECT k FROM gk_c2 ORDER BY k"); err != nil {
		t.Errorf("reading gk_c2: %v", err)
	} else if got, want := ssEmbeddedRender(res), "{numeric} -7.00 | 0.00 | 0.02 | 4.50 | 20.00 | NULL"; got != want {
		t.Errorf("gk_c2 ordered\n  got  %s\n  want %s (PostgreSQL 17.11)", got, want)
	}

	// INSERT … SELECT into typed columns stores the key's value.
	for _, q := range []string{
		"CREATE TABLE gk_tgt (tag TEXT, k INT, kn NUMERIC(12,2), kd DATE)",
		"INSERT INTO gk_tgt (tag, k) SELECT 'k', t.i + 1 FROM gk_e t GROUP BY i + 1",
		"INSERT INTO gk_tgt (tag, kn) SELECT 'kn', 2 * t.n FROM gk_e t GROUP BY 2 * n",
		"INSERT INTO gk_tgt (tag, kd) SELECT 'kd', t.d + 1 FROM gk_e t GROUP BY d + 1",
		"INSERT INTO gk_tgt (tag, k) SELECT 'k2', i + 1 FROM gk_e t GROUP BY t.i + 1",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Errorf("%s\n  refused: %v", q, err)
		}
	}
	const stored = "SELECT tag, k, kn, kd FROM gk_tgt ORDER BY tag, k, kn, kd"
	want := "{text,int,numeric,date} k,-6,NULL,NULL | k,1,NULL,NULL | k,2,NULL,NULL | k,4,NULL,NULL | k,6,NULL,NULL | " +
		"k,NULL,NULL,NULL | k2,-6,NULL,NULL | k2,1,NULL,NULL | k2,2,NULL,NULL | k2,4,NULL,NULL | k2,6,NULL,NULL | " +
		"k2,NULL,NULL,NULL | kd,NULL,NULL,1000-01-02 | kd,NULL,NULL,1970-01-01 | kd,NULL,NULL,1970-01-02 | " +
		"kd,NULL,NULL,2024-03-05 | kd,NULL,NULL,10000-01-01 | kd,NULL,NULL,NULL | kn,NULL,-7.00,NULL | " +
		"kn,NULL,0.00,NULL | kn,NULL,0.02,NULL | kn,NULL,4.50,NULL | kn,NULL,20.00,NULL | kn,NULL,NULL,NULL"
	res, err := db.Query(ctx, stored)
	if err != nil {
		t.Fatalf("%s: %v", stored, err)
	}
	if got := ssEmbeddedRender(res); got != want {
		t.Errorf("stored values\n  got  %s\n  want %s (PostgreSQL 17.11)", got, want)
	}
}

// THE PLANNING-DEPTH BOUND of the re-spelling pass: a key sixteen operators
// deep, matched by its resolved identity in the SELECT list, the HAVING and
// the ORDER BY, plans and answers in milliseconds — the pass renders each
// node's identity once per key, which is quadratic in the depth of ONE
// expression and nothing worse.
func TestArcGKRespellPlanningDepth(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE gk_d (id BIGINT, i INT)",
		"INSERT INTO gk_d VALUES (1, 1), (2, 2), (3, 3)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	nest := func(col string, depth int) string {
		e := col
		for i := 0; i < depth; i++ {
			e = "(" + e + " + 1)"
		}
		return e
	}
	for _, depth := range []int{4, 8, 16} {
		item, key := nest("t.i", depth), nest("i", depth)
		q := fmt.Sprintf("SELECT %s AS k, count(*) AS c FROM gk_d t GROUP BY %s HAVING %s > 0 ORDER BY %s",
			item, key, item, item)
		start := time.Now()
		res, err := db.Query(ctx, q)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
		t.Logf("depth=%d elapsed=%s", depth, elapsed)
		want := fmt.Sprintf("{int,int} %d,1 | %d,1 | %d,1", 1+depth, 2+depth, 3+depth)
		if got := ssEmbeddedRender(res); got != want {
			t.Errorf("depth %d\n  got  %s\n  want %s", depth, got, want)
		}
		if depth == 16 && elapsed > 2*time.Second {
			t.Errorf("depth %d took %s, want < 2s: the re-spelling is not bounded in depth", depth, elapsed)
		}
	}
}
