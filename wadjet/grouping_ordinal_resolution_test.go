// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A GROUP BY TERM IS RESOLVED THE SAME WAY WHATEVER CONSTRUCT WROTE IT (#1480).
// An ordinal or an output alias inside ROLLUP, CUBE or GROUPING SETS names the
// select-list item exactly as it does in a plain GROUP BY. The engine resolved
// the key list but left the grouping sets spelled `1` (or `k`), which named no
// key: `SELECT t.i AS k, count(*) FROM ss_t t GROUP BY ROLLUP (1)` answered
// the grand total twice where PostgreSQL 17.11 answers seven rows.
//
// The table (testdata/grouping_ordinal_pg17.tsv) is the enumeration: grouping
// construct {plain, ROLLUP, CUBE, GROUPING SETS, ROLLUP nested in GROUPING
// SETS, a plain term beside ROLLUP} x key spelling {ordinal, column, alias,
// qualified column, expression, ordinal naming an expression, ordinal naming
// an aggregate, out of range, 0, negative, 1.0, '1', ((1)), a composite} x
// {bare, ORDER BY 1, HAVING, GROUPING by column, GROUPING(1)} x {rows, zero
// rows}, with extra cells. Every want is PostgreSQL 17.11's; a cell with a
// fourth field is a RESIDUAL the engine answers differently at the base and
// at this fix alike, pinned at the engine's answer so the day it changes the
// pin is revisited. Each runs on the plain engine and with every aggregate
// forced to drain to disk.
func TestGroupByOrdinalInGroupingConstructs(t *testing.T) {
	ctx := context.Background()
	type cell struct{ name, sql, pg, pin string }
	var cells []cell
	f, err := os.Open("testdata/grouping_ordinal_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 4 {
			t.Fatalf("malformed line %q", line)
		}
		cells = append(cells, cell{p[0], p[1], p[2], p[3]})
	}
	f.Close()
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	open := func(spill bool) *DB {
		cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if spill {
			cfg.MemoryBudget = 1 << 16
			cfg.SpillDir = t.TempDir()
		}
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, q := range []string{
			"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT)",
			"INSERT INTO ss_t VALUES (1,3,30),(2,-7,-70),(3,5,9000000000),(4,0,0),(5,1,1),(6,NULL,NULL)",
			"CREATE TABLE ss_z (id BIGINT, i INT, b BIGINT)",
		} {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		return db
	}
	answer := func(db *DB, sql string) string {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err)
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
		if !strings.Contains(sql, "ORDER BY") {
			sort.Strings(rows)
		}
		if len(rows) == 0 {
			return "(none)"
		}
		return strings.Join(rows, " | ")
	}
	// same compares two answers as multisets, and on the order of the first
	// column when the statement sorts.
	same := func(sql, got, want string) bool {
		if got == want {
			return true
		}
		if !strings.Contains(sql, "ORDER BY") || strings.HasPrefix(got, "ERR") || strings.HasPrefix(want, "ERR") {
			return false
		}
		g, w := strings.Split(got, " | "), strings.Split(want, " | ")
		if len(g) != len(w) {
			return false
		}
		for i := range g {
			if strings.SplitN(g[i], ",", 2)[0] != strings.SplitN(w[i], ",", 2)[0] {
				return false
			}
		}
		sort.Strings(g)
		sort.Strings(w)
		return strings.Join(g, " | ") == strings.Join(w, " | ")
	}

	for _, arm := range []string{"plain", "spilled"} {
		t.Run(arm, func(t *testing.T) {
			db := open(arm == "spilled")
			if arm == "spilled" {
				defer exec.ForceAggDrainEvery(exec.ForceAggDrainEvery(1))
			}
			before := exec.ForcedAggDrains.Load()
			for _, c := range cells {
				got := answer(db, c.sql)
				switch {
				case c.pin == "" && !same(c.sql, got, c.pg):
					t.Errorf("%s: %s\n got  %s\n want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.pg)
				case c.pin != "" && !same(c.sql, got, c.pin):
					t.Errorf("%s: %s\n got    %s\n pinned %s (a residual; PostgreSQL 17.11 answers %s)", c.name, c.sql, got, c.pin, c.pg)
				}
			}
			if arm == "spilled" && exec.ForcedAggDrains.Load() == before {
				t.Error("no aggregate drained to disk: the spilled arm did not exercise the spill path")
			}
		})
	}
}
