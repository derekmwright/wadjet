// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A COLUMN CREATED FROM AN UNCONSTRAINED NUMERIC STORES WHAT IS WRITTEN TO IT
// (#1541, ADR-0024 §10). Every creation path — DDL `NUMERIC` / `DECIMAL`, the
// constrained controls `NUMERIC(10,2)` / `NUMERIC(5)`, and CREATE TABLE AS
// from a literal, a bare NUMERIC cast, a typed NULL, a constrained column,
// COALESCE / CASE / GREATEST / NULLIF / UNION ALL over two scales, SUM / AVG,
// a product, a quotient, LAG / LEAD with a typed-NULL default, ROUND and a
// bare copy of an unconstrained (and of a constrained) column —
// then every write (VALUES with more than 10 fraction digits and more than 28
// integer digits, INSERT … SELECT from a numeric(10,2) and a double column,
// UPDATE, MERGE) and every read (the rows, SUM, a predicate, GROUP BY,
// CAST(v AS TEXT), v * v, information_schema). testdata/arc_un_enum.tsv holds
// each statement with this engine's answer, PostgreSQL 17.11's and the
// disposition that relates them; `=` rows are asserted equal to PostgreSQL's
// text here too. At 8e681724 `CREATE TABLE ddl_num (id BIGINT, v NUMERIC)`
// was DECIMAL(38,0) and stored 1.25 as 1, 0.755 as 1.
func TestArcUNUnconstrainedColumnEnumeration(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	f, err := os.Open("testdata/arc_un_enum.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var gen *os.File
	if p := os.Getenv("UN_GEN"); p != "" {
		if gen, err = os.Create(p); err != nil {
			t.Fatal(err)
		}
		defer gen.Close()
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	n := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		for len(parts) < 4 {
			parts = append(parts, "")
		}
		stmt, want, pg, disp := parts[0], parts[1], parts[2], parts[3]
		got := unAnswer(ctx, t, db, stmt)
		if gen != nil {
			fmt.Fprintf(gen, "%s\t%s\n", stmt, got)
			continue
		}
		n++
		if got != want {
			t.Errorf("%s\n  got  %s\n  want %s\n  (PostgreSQL %s; %s)", stmt, got, want, pg, disp)
		}
		if disp == "=" && pg != want {
			t.Errorf("%s: the table says this agrees with PostgreSQL, but %q is not %q", stmt, want, pg)
		}
	}
	if gen == nil && n < 500 {
		t.Fatalf("%d statements read; the table is truncated", n)
	}
}

// unAnswer is one statement's answer in the table's form: the rows (cells
// `|`-joined, sorted, space-joined, NULL spelled out), a command's tag, or ERR
// with the SQLSTATE; a CREATE TABLE adds the created declaration of every
// numeric column — DECIMAL(p,s), and `u` when it is marked unconstrained.
func unAnswer(ctx context.Context, t *testing.T, db *DB, stmt string) string {
	t.Helper()
	res, err := db.Query(ctx, stmt)
	if err != nil {
		return "ERR " + sqlerr.StateOf(err)
	}
	var rows []string
	for i := range res.Rows {
		var cells []string
		for _, v := range res.Cells(i) {
			if v == nil {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, fmt.Sprint(v))
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	sort.Strings(rows) // a statement with no ORDER BY has no order
	out := strings.Join(rows, " ")
	if f := strings.Fields(stmt); len(f) > 2 && strings.EqualFold(f[0], "CREATE") && strings.EqualFold(f[1], "TABLE") {
		tb, err := db.Catalog().GetTable(ctx, f[2])
		if err != nil {
			return out + " CATALOG ERR"
		}
		out += " CATALOG " + unDecls(tb.Schema.Columns)
	}
	return out
}

func unDecls(cols []parquet.Column) string {
	var ds []string
	for _, c := range cols {
		switch c.Type {
		case parquet.TypeDecimal:
			d := fmt.Sprintf("%s:DECIMAL(%d,%d)", c.Name, c.Precision, c.Scale)
			if unColumnMarked(c) {
				d += "u"
			}
			ds = append(ds, d)
		case parquet.TypeFloat64:
			ds = append(ds, c.Name+":FLOAT64")
		}
	}
	return strings.Join(ds, ",")
}

// A store reopened, and its files rewritten by compaction, keeps the column's
// declaration, its marker and every value: the catalog record carries the
// marker and the file footer's declared schema carries it to a reader that
// decodes the file alone.
func TestArcUNUnconstrainedColumnPersists(t *testing.T) {
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
		"CREATE TABLE p1 (id BIGINT, v NUMERIC)",
		"INSERT INTO p1 VALUES (1, 1.25), (2, 0.755), (3, 1), (4, 0.00000000005), (5, 12345678901234567890.123456789012)",
		"INSERT INTO p1 VALUES (6, 2.5)",
		"CREATE TABLE p2 AS SELECT id, CAST(id AS NUMERIC) AS v FROM p1",
		"INSERT INTO p2 VALUES (9, 0.75)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	db.Close()
	want := map[string]string{
		"SELECT id, v FROM p1 ORDER BY id":               "1|1.25 2|0.755 3|1 4|0.0000000001 5|12345678901234567890.123456789 6|2.5",
		"SELECT id, CAST(v AS TEXT) FROM p1 ORDER BY id": "1|1.25 2|0.755 3|1 4|0.0000000001 5|12345678901234567890.123456789 6|2.5",
		"SELECT id, v FROM p2 ORDER BY id":               "1|1 2|2 3|3 4|4 5|5 6|6 9|0.75",
		"SELECT SUM(v) FROM p2":                          "21.7500000000",
		"SELECT numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'p1' AND column_name = 'v'": "NULL|NULL",
	}
	check := func(db *DB, when string) {
		for q, w := range want {
			if got := unAnswer(ctx, t, db, q); got != w {
				t.Errorf("%s: %s\n  got  %s\n  want %s", when, q, got, w)
			}
		}
		for _, name := range []string{"p1", "p2"} {
			tb, err := db.Catalog().GetTable(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if got := unDecls(tb.Schema.Columns); got != "v:DECIMAL(38,10)u" {
				t.Errorf("%s: %s declares %s, want v:DECIMAL(38,10)u", when, name, got)
			}
		}
	}
	db = open()
	check(db, "reopened")
	c := compaction.New(db.Catalog(), slog.Default(), compaction.Config{})
	for _, name := range []string{"p1", "p2"} {
		if _, err := c.RewriteTable(ctx, name); err != nil {
			t.Fatalf("compacting %s: %v", name, err)
		}
	}
	check(db, "compacted")
	db.Close()
	db = open()
	defer db.Close()
	check(db, "compacted and reopened")
}

// A catalog record written before the marker existed — a DDL `NUMERIC`
// column the 8e681724 door declared DECIMAL(38,0), with no `unconstrained`
// key — reads, writes and prints exactly as it did: it is a constrained
// column of scale 0 (ADR-0024 §8, §10). The record is written here as that
// door wrote it.
func TestArcUNARecordWithoutTheMarkerIsTheColumnItWas(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "legacy", old, nil); err != nil {
		t.Fatal(err)
	}
	for q, w := range map[string]string{
		"INSERT INTO legacy VALUES (1, 1.25), (2, 7), (3, 0.5)": "INSERT 0 3",
		"SELECT id, v FROM legacy ORDER BY id":                  "1|1 2|7 3|1",
		"SELECT id, CAST(v AS TEXT) FROM legacy ORDER BY id":    "1|1 2|7 3|1",
		"SELECT numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'legacy' AND column_name = 'v'": "38|0",
	} {
		if got := unAnswer(ctx, t, db, q); got != w {
			t.Errorf("%s\n  got  %s\n  want %s", q, got, w)
		}
	}
	res, err := db.Query(ctx, "SELECT v FROM legacy")
	if err != nil {
		t.Fatal(err)
	}
	if m := res.ColumnMetas[0]; m.Precision != 38 || m.Scale != 0 || m.WireUnconstrained {
		t.Errorf("legacy v declares (%d,%d) wire-unconstrained=%v, want (38,0) with its typmod", m.Precision, m.Scale, m.WireUnconstrained)
	}
}

// unColumnMarked reads the marker by name, so this file compiles against a
// tree that has none (the at-base run).
func unColumnMarked(c parquet.Column) bool {
	f := reflect.ValueOf(c).FieldByName("Unconstrained")
	return f.IsValid() && f.Bool()
}
