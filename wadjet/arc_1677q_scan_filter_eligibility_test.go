// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A JOIN'S BUILD-KEY SCAN FILTERS NEVER PRUNE THROUGH A KEY CHANGE (#1677
// line q).
//
// A hash join hands the probe side's scan two row-group filters derived from
// its build keys: the dynamic min/max range and, for an integer key, the bloom
// scan filter. Both compare the build's key values with the STORED column's
// statistics. The planner installed them by walking down to the scan and
// ignoring every operator in between, so a key the query had changed on the
// way — a policy mask, `x+1 AS x`, `y AS x`, a swap, a cast, a nested derived
// table — was pruned by the stored column of the same name: every row group
// went, and `IN` / `EXISTS` answered 0 where PostgreSQL 17.11 answers 1. A
// filter on a scan that fills the multi-consumer scan cache truncated what
// the cache's other consumer replayed (pc/cache/unionarm2: 2 where PostgreSQL
// answers 11). The min/max range is recorded by the SERIAL key-only build
// only, so the string cells went wrong on one CPU alone; the integer and date
// cells, through the bloom scan filter, on every host.
//
// The grid (testdata/arc_1677q_cells.tsv, PostgreSQL 17.11's answers over
// testdata/arc_1677q_fixture.sql) is {INTEGER, VARCHAR, DATE, NUMERIC} × the
// probe key {stored, renamed, swapped, computed, cast, through a CTE, a UNION
// ALL arm, a nested derived table, a LEFT JOIN's nullable side, a lower join's
// build side} × {inner, left, IN, EXISTS, NOT IN, NOT EXISTS}, plus the scan
// cache cells. Every arm runs with the hash-join build forced onto its
// one-CPU routes (exec.ForceSerialJoinBuildForTest) — the routes that record
// the build keys' min/max — and on the host's own, so a many-core CI host
// exercises what `taskset -c 0` does.
func TestArc1677qScanFiltersNeverPruneThroughAKeyChange(t *testing.T) {
	ctx := context.Background()
	cells := arc1677qCells(t, "testdata/arc_1677q_cells.tsv")
	arms := []struct {
		name   string
		budget int64
		forced bool
	}{{"unbudgeted", 0, false}, {"budget256m", 256 << 20, false}, {"evict-every-batch", 4 << 20, true}}
	for _, serial := range []bool{true, false} {
		for _, arm := range arms {
			name := arm.name
			if serial {
				name += "/serial-build"
			}
			t.Run(name, func(t *testing.T) {
				db := arc1677qOpen(t, ctx, arm.budget, "testdata/arc_1677q_fixture.sql")
				if serial {
					defer exec.ForceSerialJoinBuildForTest()()
				}
				if arm.forced {
					prev := exec.ForceJoinPartitionEvictEvery(1)
					defer exec.ForceJoinPartitionEvictEvery(prev)
				}
				for _, c := range cells {
					if got := arc1677qAnswer(ctx, db, c[2]); got != c[1] {
						t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c[0], c[2], got, c[1])
					}
				}
			})
		}
	}
}

func arc1677qCells(t *testing.T, path string) [][3]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cells [][3]string
	for _, l := range strings.Split(string(raw), "\n") {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := strings.SplitN(l, "\t", 3)
		if len(f) != 3 {
			t.Fatalf("malformed cell %q", l)
		}
		cells = append(cells, [3]string{f[0], f[1], f[2]})
	}
	return cells
}

func arc1677qOpen(t *testing.T, ctx context.Context, budget int64, fixture string) *DB {
	t.Helper()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	raw, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range strings.Split(string(raw), "\n") {
		if q == "" || strings.HasPrefix(q, "--") {
			continue
		}
		if strings.HasPrefix(q, "INSERT") {
			_, err = db.Execute(ctx, q)
		} else {
			_, err = db.Query(ctx, q)
		}
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return db
}

// arc1677qAnswer renders a one-column result as PostgreSQL's `psql -At` does
// for these cells: the rows' values joined by ';'.
func arc1677qAnswer(ctx context.Context, db *DB, sql string) string {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR " + strings.SplitN(err.Error(), "\n", 2)[0]
	}
	rows := make([]string, 0, len(res.Rows))
	for i := range res.Rows {
		var cs []string
		for _, c := range res.Cells(i) {
			cs = append(cs, fmt.Sprint(c))
		}
		rows = append(rows, strings.Join(cs, "|"))
	}
	return strings.Join(rows, ";")
}
