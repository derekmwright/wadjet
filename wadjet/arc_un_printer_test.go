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

// EVERY TEXT RENDERING OF A COLUMN CREATED FROM AN UNCONSTRAINED NUMERIC IS
// ONE PRINTER (ADR-0024 §10, arc UN round 3). A DECIMAL's box is its text,
// and batch.Vector.GetValueOf boxes a marked column's value as its printed
// text (`1.5`, not `1.5000000000`); the result rows and every expression that
// renders the value — CAST to TEXT, `||`, concat, concat_ws, format,
// quote_literal, quote_nullable, json_build_object, array_to_string — read
// that box, through a derived table, a CTE, a GROUP BY key, DISTINCT, a join,
// a set operation whose arms are all such columns and a LATERAL body.
// testdata/arc_un_printer.tsv holds each statement with PostgreSQL 17.11's
// answer; `=` rows are PostgreSQL's text, `r18` rows print a computed value
// at its one scale (catalog numeric-decimal r18). At 742965c1
// `SELECT COUNT(*) FROM un WHERE CAST(v AS TEXT) = concat(v)` answered 0
// where PostgreSQL answers 4: `concat(v)` printed 1.5000000000.
func TestArcUNOneTextPrinter(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	f, err := os.Open("testdata/arc_un_printer.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	n := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if parts[0] == "SETUP" {
			if _, err := db.Query(ctx, parts[1]); err != nil {
				t.Fatalf("%s: %v", parts[1], err)
			}
			continue
		}
		if len(parts) != 4 {
			t.Fatalf("malformed row %q", line)
		}
		stmt, want, pg, disp := parts[0], parts[1], parts[2], parts[3]
		n++
		if disp == "=" && want != pg {
			t.Errorf("%s: the table says this is PostgreSQL's text, but %q is not %q", stmt, want, pg)
		}
		res, err := db.Query(ctx, stmt)
		if err != nil {
			t.Errorf("%s: %s %v", stmt, sqlerr.StateOf(err), err)
			continue
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
			rows = append(rows, strings.Join(cells, ","))
		}
		if got := strings.Join(rows, " | "); got != want {
			t.Errorf("%s\n  got  %s\n  want %s (%s; PostgreSQL %s)", stmt, got, want, disp, pg)
		}
	}
	if n < 80 {
		t.Fatalf("%d statements read; the table is truncated", n)
	}
}
