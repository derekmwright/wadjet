// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A CREATE TABLE AS COLUMN DECLARES WHAT POSTGRESQL'S TYPMOD RULE SAYS
// (ADR-0024 §10). The column is created unconstrained —
// DECIMAL(38, max(s,10)), marked — only where PostgreSQL creates plain
// numeric; a column whose source keeps a typmod (a column through a derived
// table, a CTE, a scalar subquery, a set operation; a CASE / COALESCE /
// GREATEST / LEAST whose every value carries the same one; NULLIF; CAST to
// NUMERIC(p,s); a CASE whose condition folds to a constant on WITH DATA)
// keeps it, and a later INSERT of 1.255 stores what PostgreSQL stores. Where
// the planner cannot tell, the column keeps the declaration c23adbbb gave it.
// testdata/arc_un_typmod.tsv holds each cell with PostgreSQL 17.11's and
// c23adbbb's stored values; the want column is one of the two for every cell.
// At 742965c1 a scalar subquery over a numeric(10,2) column and a constant
// CASE (`CASE WHEN 1 = 1 THEN n END`) were marked unconstrained and stored
// 1.255 where PostgreSQL and c23adbbb store 1.26.
func TestArcUNCreatedColumnKeepsPostgresTypmod(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	f, err := os.Open("testdata/arc_un_typmod.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var gen *os.File
	if p := os.Getenv("UN_TYPMOD_GEN"); p != "" {
		if gen, err = os.Create(p); err != nil {
			t.Fatal(err)
		}
		defer gen.Close()
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	cells := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if parts[0] == "SETUP" {
			if _, err := db.Query(ctx, parts[1]); err != nil && !strings.HasPrefix(parts[1], "CREATE VIEW") {
				t.Fatalf("%s: %v", parts[1], err)
			}
			continue
		}
		if len(parts) != 6 {
			t.Fatalf("malformed row %q", line)
		}
		cell, source, want, pg, base, disp := parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
		got := unTypmodCell(ctx, db, cell, source)
		if gen != nil {
			fmt.Fprintf(gen, "%s\t%s\n", cell, got)
			continue
		}
		cells++
		if got != want {
			t.Errorf("%s: CREATE TABLE AS %s; INSERT 1.255, 0.755\n  got  %s\n  want %s (%s; PostgreSQL %s; c23adbbb %s)",
				cell, source, got, want, disp, pg, base)
		}
	}
	if gen == nil && cells < 600 {
		t.Fatalf("%d cells read; the table is truncated", cells)
	}
}

// unTypmodCell creates the cell's table, writes 1.255 and 0.755 to it and
// answers what it reads back, ` | `-joined in id order.
func unTypmodCell(ctx context.Context, db *DB, cell, source string) string {
	stmts := []string{
		"CREATE TABLE " + cell + " AS " + source,
		"INSERT INTO " + cell + " (id, x) VALUES (99, 1.255), (98, 0.755)",
		"SELECT x FROM " + cell + " WHERE id >= 98 ORDER BY id",
	}
	var res *QueryResult
	for _, s := range stmts {
		r, err := db.Query(ctx, s)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err)
		}
		res = r
	}
	var rows []string
	for i := range res.Rows {
		for _, v := range res.Cells(i) {
			if v == nil {
				rows = append(rows, "NULL")
			} else {
				rows = append(rows, fmt.Sprint(v))
			}
		}
	}
	return strings.Join(rows, " | ")
}
