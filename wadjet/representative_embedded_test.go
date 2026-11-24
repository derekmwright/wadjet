// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The embedded engine picks ONE representative among SQL-equal values,
// whatever its scan parallelism and whether its aggregate spills (#1713;
// ADR-0024 §12). The table spans eight files; the first occurrence of each
// value (file 1) is spelled bare, every later one with a trailing zero
// through a computed key, so "the first met" and "the last met" are both
// different answers from the rule's. Before the rule, four scan workers
// printed 7.00 on 29 of 40 runs of the GROUP BY and 7 on 11, and the spilled
// aggregate printed the last spelling.
func TestRepresentativeEmbeddedEveryWorkerCount(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: eight files, four worker counts, spilled and not")
	}
	const key = "CASE WHEN id <= 10000 THEN v ELSE CAST(v AS NUMERIC(10,2)) END"
	cells := []struct{ name, sql, want string }{
		{"group", "SELECT CAST(k AS TEXT), count(*) FROM (SELECT " + key + " AS k FROM zr) s GROUP BY k",
			"1.5,11669 | 100,1667 | 2,11662 | 200,1667 | 300,1666 | 7,11669"},
		{"group sum", "SELECT CAST(k AS TEXT), count(*), sum(id) FROM (SELECT " + key + " AS k, id FROM zr) s GROUP BY k",
			""},
		{"distinct", "SELECT CAST(k AS TEXT) FROM (SELECT DISTINCT " + key + " AS k FROM zr) s",
			"1.5 | 100 | 2 | 200 | 300 | 7"},
		{"distinct like", "SELECT count(*) FROM (SELECT DISTINCT " + key + " AS k FROM zr) s WHERE CAST(k AS TEXT) LIKE '%.%'",
			"1"},
		{"union", "SELECT CAST(k AS TEXT) FROM (SELECT " + key + " AS k FROM zr UNION SELECT CAST(v AS NUMERIC(10,3)) FROM zq) s",
			"1.5 | 100 | 2 | 200 | 300 | 7"},
		{"intersect", "SELECT CAST(k AS TEXT) FROM (SELECT CAST(v AS NUMERIC(10,3)) AS k FROM zq INTERSECT SELECT " + key + " FROM zr) s",
			"1.500 | 100.000 | 2.000 | 200.000 | 300.000 | 7.000"},
		{"except all", "SELECT CAST(k AS TEXT), count(*) FROM (SELECT " + key + " AS k FROM zr EXCEPT ALL SELECT v FROM zq WHERE id <= 30) s GROUP BY k",
			"1.5,11669 | 100,1657 | 2,11662 | 200,1657 | 300,1656 | 7,11669"},
	}
	for _, spilled := range []bool{false, true} {
		ctx := context.Background()
		cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if spilled {
			cfg.MemoryBudget = 1 << 18
			cfg.SpillDir = t.TempDir()
		}
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		must := func(q string) {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%.80s: %v", q, err)
			}
		}
		must("CREATE TABLE zr (id BIGINT, v NUMERIC)")
		must("CREATE TABLE zq (id BIGINT, v NUMERIC)")
		id := 0
		for f := 0; f < 8; f++ {
			set := []string{"7.00", "1.50", "2.000"}
			switch f {
			case 0:
				set = []string{"100", "200", "300"}
			case 1:
				set = []string{"7", "1.5", "2"}
			}
			var b strings.Builder
			for i := 0; i < 5000; i++ {
				id++
				if i > 0 {
					b.WriteString(",")
				}
				fmt.Fprintf(&b, "(%d,%s)", id, set[i%3])
			}
			must("INSERT INTO zr VALUES " + b.String())
			must("INSERT INTO zq VALUES " + b.String())
		}
		for _, workers := range []string{"1", "2", "4", ""} {
			t.Setenv("WADJET_SCAN_WORKERS", workers)
			for _, c := range cells {
				seen := map[string]int{}
				for run := 0; run < 5; run++ {
					res, err := db.Query(ctx, c.sql)
					if err != nil {
						t.Fatalf("%s: %v", c.name, err)
					}
					var rows []string
					for r := range res.Rows {
						cells := make([]string, 0, len(res.Cells(r)))
						for _, v := range res.Cells(r) {
							cells = append(cells, fmt.Sprint(v))
						}
						rows = append(rows, strings.Join(cells, ","))
					}
					sort.Strings(rows)
					seen[strings.Join(rows, " | ")]++
				}
				if len(seen) != 1 {
					t.Errorf("spilled=%v workers=%q %s: %d different answers over 5 runs: %v", spilled, workers, c.name, len(seen), seen)
					continue
				}
				for got := range seen {
					if c.want != "" && got != c.want {
						t.Errorf("spilled=%v workers=%q %s:\n got  %s\n want %s", spilled, workers, c.name, got, c.want)
					}
				}
			}
		}
		db.Close()
	}
}
