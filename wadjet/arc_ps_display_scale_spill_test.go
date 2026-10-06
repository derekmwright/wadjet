// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// psSpillN is the fixture's row count: several batches, so each forced spill
// writes more than one run.
const psSpillN = 6000

// psSpillN2 is row i's NUMERIC(10,2) value, NULL every fourth row.
func psSpillN2(i int) string { return []string{"1.00", "1.50", "7.25", "NULL"}[i%4] }

func psSpillDB(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Query(ctx, "CREATE TABLE ps_big (id BIGINT, n NUMERIC(10,2))"); err != nil {
		t.Fatal(err)
	}
	for lo := 1; lo <= psSpillN; lo += 1000 {
		var b strings.Builder
		b.WriteString("INSERT INTO ps_big VALUES ")
		for i := lo; i < lo+1000; i++ {
			if i > lo {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "(%d,%s)", i, psSpillN2(i))
		}
		if _, err := db.Query(ctx, b.String()); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// psSpillRows is a result as text rows, in the result's order.
func psSpillRows(t *testing.T, db *DB, sql string) []string {
	t.Helper()
	res, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([]string, len(res.Rows))
	for i := range res.Rows {
		cells := res.Cells(i)
		parts := make([]string, len(cells))
		for j, c := range cells {
			if c == nil {
				parts[j] = "NULL"
			} else {
				parts[j] = fmt.Sprint(c)
			}
		}
		out[i] = strings.Join(parts, ",")
	}
	return out
}

// TestArcPSDisplayScaleThroughEverySpill: a column whose values carry
// different display scales (COALESCE(n, 1.5) over a NUMERIC(10,2) column:
// `1.00`, `1.50`, `7.25` and the literal's `1.5`) keeps each value's text
// through every pipeline breaker's spill — the external sort's runs, the hash
// join's evicted partitions, the aggregate's partial-state drains (a group key
// keeps its first member's text; every group here has one spelling) and the
// window's runs — with each spill's engagement counted, and answers exactly
// what the same statement answers in memory and what PostgreSQL 17.11 answers
// (the expectation is the fixture's own arithmetic, measured on PostgreSQL
// over the same rows).
func TestArcPSDisplayScaleThroughEverySpill(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: forced spills over 6,000 rows")
	}
	k := func(i int) string {
		if v := psSpillN2(i); v != "NULL" {
			return v
		}
		return "1.5"
	}
	defer exec.ForceSmallSpillRuns(4096)()
	mem := psSpillDB(t, 0)
	spill := psSpillDB(t, 1<<20)
	cells := []struct {
		name    string
		sql     string
		want    func() []string
		force   func(int64) int64
		counter *atomic.Int64
	}{
		{"sort", "SELECT id, COALESCE(n, 1.5) AS k FROM ps_big ORDER BY id DESC",
			func() []string {
				var out []string
				for i := psSpillN; i >= 1; i-- {
					out = append(out, fmt.Sprintf("%d,%s", i, k(i)))
				}
				return out
			}, exec.ForceSortSpillEvery, &exec.ForcedSortSpills},
		{"join", "SELECT b.id, s.k FROM ps_big b JOIN (SELECT id, COALESCE(n, 1.5) AS k FROM ps_big) s ON s.id = b.id ORDER BY b.id",
			func() []string {
				var out []string
				for i := 1; i <= psSpillN; i++ {
					out = append(out, fmt.Sprintf("%d,%s", i, k(i)))
				}
				return out
			}, exec.ForceJoinPartitionEvictEvery, &exec.ForcedJoinEvictions},
		{"aggregate", "SELECT k, count(*) FROM (SELECT COALESCE(n, 1.5) AS k FROM ps_big WHERE n IS NULL OR n <> 1.50) s GROUP BY k ORDER BY k",
			func() []string {
				return []string{fmt.Sprintf("1.00,%d", psSpillN/4), fmt.Sprintf("1.5,%d", psSpillN/4), fmt.Sprintf("7.25,%d", psSpillN/4)}
			}, exec.ForceAggDrainEvery, &exec.ForcedAggDrains},
		{"window", "SELECT id, lag(k) OVER (ORDER BY id) FROM (SELECT id, COALESCE(n, 1.5) AS k FROM ps_big) s ORDER BY id",
			func() []string {
				out := []string{"1,NULL"}
				for i := 2; i <= psSpillN; i++ {
					out = append(out, fmt.Sprintf("%d,%s", i, k(i-1)))
				}
				return out
			}, exec.ForceWindowSpillEvery, &exec.ForcedWindowSpills},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			want := strings.Join(c.want(), " | ")
			if got := strings.Join(psSpillRows(t, mem, c.sql), " | "); got != want {
				t.Fatalf("in memory: %.300s\n  want %.300s", got, want)
			}
			before := c.counter.Load()
			prev := c.force(1)
			got := strings.Join(psSpillRows(t, spill, c.sql), " | ")
			c.force(prev)
			if c.counter.Load() == before {
				t.Fatalf("%s: the spill did not engage — the comparison would be two in-memory runs", c.name)
			}
			if got != want {
				t.Errorf("spilled: %.300s\n  want %.300s", got, want)
			}
		})
	}
}
