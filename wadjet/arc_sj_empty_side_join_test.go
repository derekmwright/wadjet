// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A SPILLED RIGHT / FULL JOIN OVER AN EMPTY SIDE KEEPS THE PRESERVED SIDE'S
// VALUES (#1359).
//
// The grace hash join replays each evicted build partition through a
// temporary join at the end of the probe. When the probe side produced no
// rows at all, that replay's probe never ran Execute, so it had no probe-side
// schema and no output narrowing of its own: the unmatched build rows of a
// RIGHT or FULL join came out as a batch of a different shape from the
// in-memory partitions' (no probe half, every build column), and the consumer
// read the preserved side's values as NULL. Whether a partition was evicted at
// all depended on the budget and on what else held memory, so the same query
// answered right or wrong from one run to the next.
//
// The grid (testdata/arc_sj_empty_side_join_pg17.tsv, generated and measured
// on PostgreSQL 17.11 over testdata/arc_sj_fixture.sql by the arc's
// gen_pg_cells.py) is {RIGHT, FULL, LEFT, INNER} × the side written first
// (the build side is the right child: `ep` builds the preserved side, `pe`
// builds the empty one) × the preserved side {1 row, 2 048 rows, 20 000 rows,
// NULL keys, duplicate keys} × the empty side {an empty table, a table
// filtered to empty, an empty LIMIT 0 subquery, a VALUES list under WHERE
// false} × the key {INTEGER, BIGINT, VARCHAR, DATE}; every cell also carries
// a DECIMAL and an ARRAY column of the preserved side. Each answer is six text
// cells per row, rendered by the query itself.
//
// sjArms runs it at no budget and at 4 MiB, 512 KiB and 256 KiB, each budget
// also with every arriving build batch forcing an eviction (ADR-0027 decision
// 6): there the eviction is ASSERTED on every cell whose build is the
// non-empty preserved side, because without it the cell compares two
// in-memory runs.
func TestArcSJEmptySideJoinEveryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: 640 cells × 7 arms over a 20 000-row fixture")
	}
	ctx := context.Background()
	cells := sjReadCells(t)
	for _, arm := range sjArms {
		t.Run(arm.name, func(t *testing.T) {
			db := sjOpen(t, ctx, arm.budget)
			if arm.forced {
				prev := exec.ForceJoinPartitionEvictEvery(1)
				defer exec.ForceJoinPartitionEvictEvery(prev)
			}
			fails, refused := 0, 0
			for _, c := range cells {
				before := exec.JoinPartitionsEvicted.Load()
				got := sjAnswer(ctx, db, c.sql)
				evicted := exec.JoinPartitionsEvicted.Load() - before
				if got != c.want && sjBudgetRefusal(arm.budget, c, got) {
					refused++
					continue
				}
				if got != c.want {
					fails++
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
				}
				if arm.forced && c.buildsPreserved() && c.jt != "inner" && evicted == 0 {
					t.Errorf("%s: no build partition was evicted — the cell compared two in-memory runs", c.name)
				}
			}
			t.Logf("SJ-GRID %s: differ=%d refused=%d cells=%d", arm.name, fails, refused, len(cells))
		})
	}
}

// TestArcSJEmptySideJoinIntermittent is the cells the report measured as
// INTERMITTENT: a RIGHT or FULL join that builds the preserved side, probing
// an empty one, under a budget alone — no forcing knob, so whether a partition
// is evicted is the weather. It runs under -count=20 in the arc's gate list;
// each run counts the cells that differed and, separately, how many cells
// spilled at all, so a run where nothing spilled is visible as such.
func TestArcSJEmptySideJoinIntermittent(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: 160 cells × 3 budgets over a 20 000-row fixture")
	}
	ctx := context.Background()
	var cells []sjCell
	for _, c := range sjReadCells(t) {
		if c.buildsPreserved() && (c.jt == "right" || c.jt == "full") {
			cells = append(cells, c)
		}
	}
	for _, budget := range []int64{4 << 20, 512 << 10, 256 << 10} {
		t.Run(fmt.Sprintf("budget=%d", budget), func(t *testing.T) {
			db := sjOpen(t, ctx, budget)
			fails, spilled := 0, 0
			for _, c := range cells {
				before := exec.JoinPartitionsEvicted.Load()
				got := sjAnswer(ctx, db, c.sql)
				if exec.JoinPartitionsEvicted.Load() != before {
					spilled++
				}
				if got != c.want && sjBudgetRefusal(budget, c, got) {
					continue
				}
				if got != c.want {
					fails++
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
				}
			}
			t.Logf("SJ-INTERMITTENT budget=%d differ=%d spilled=%d cells=%d", budget, fails, spilled, len(cells))
		})
	}
}

var sjArms = []struct {
	name   string
	budget int64
	forced bool
}{
	{"unlimited", 0, false},
	{"4MiB", 4 << 20, false},
	{"4MiB_forced", 4 << 20, true},
	{"512KiB", 512 << 10, false},
	{"512KiB_forced", 512 << 10, true},
	{"256KiB", 256 << 10, false},
	{"256KiB_forced", 256 << 10, true},
}

// sjBudgetRefusal is the one outcome other than PostgreSQL's answer a cell may
// have: building the 20 000-row side at 512 KiB or less can refuse LOUDLY with
// "memory budget exceeded", because the scan feeding the build holds its
// decoded batches past the budget (the refusal names "scan decoded batch").
// That is the never-OOM contract's loud outcome (ADR-0006), it is the same at
// the arc's base, and it is not this arc's seam; a refusal anywhere else, and
// any answer other than PostgreSQL's, fails the cell.
func sjBudgetRefusal(budget int64, c sjCell, got string) bool {
	return budget > 0 && budget <= 512<<10 && c.shape == "b20000" && c.buildsPreserved() &&
		strings.HasPrefix(got, "ERR ") && strings.Contains(got, "memory budget exceeded")
}

type sjCell struct {
	name, jt, order, shape string
	preserved              bool
	sql, want              string
}

// buildsPreserved: the build side is the join's right child, so `ep` builds
// the preserved (non-empty) relation whatever the join type.
func (c sjCell) buildsPreserved() bool { return c.order == "ep" }

func sjReadCells(t *testing.T) []sjCell {
	t.Helper()
	f, err := os.Open("testdata/arc_sj_empty_side_join_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []sjCell
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 {
			t.Fatalf("malformed cell line %q", line)
		}
		parts := strings.Split(p[0], "/")
		out = append(out, sjCell{name: p[0], jt: parts[0], order: parts[1], shape: parts[2],
			preserved: p[1] == "1", sql: p[2], want: p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) != 640 {
		t.Fatalf("read %d cells, want 640", len(out))
	}
	return out
}

// sjOpen loads the fixture into a fresh engine; budget 0 = unlimited. Every
// statement writes at most 1 000 rows, which fits the smallest budget.
func sjOpen(t *testing.T, ctx context.Context, budget int64) *DB {
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
	raw, err := os.ReadFile("testdata/arc_sj_fixture.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range strings.Split(string(raw), "\n") {
		if q == "" || strings.HasPrefix(q, "--") {
			continue
		}
		var err error
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

// sjAnswer renders a result exactly as gen_pg_cells.py renders PostgreSQL's:
// each row's cells joined by '|', NULL spelled NULL, rows sorted, the MD5 of
// the lines joined by newlines.
func sjAnswer(ctx context.Context, db *DB, sql string) string {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR " + strings.ReplaceAll(err.Error(), "\n", " ")
	}
	rows := make([]string, 0, len(res.Rows))
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
		rows = append(rows, strings.Join(parts, "|"))
	}
	sort.Strings(rows)
	sum := md5.Sum([]byte(strings.Join(rows, "\n")))
	return fmt.Sprintf("rows=%d %s", len(rows), hex.EncodeToString(sum[:]))
}

// TestArcSJBareColumnsForced is the report's own shape — bare columns of both
// sides handed to the client, no text rendering in between — under a forced
// eviction, read positionally against PostgreSQL 17.11's rows:
//
//	SELECT p.id, p.k_int, p.v_dec, p.v_arr, e.k_int, e.ey FROM sj_e e RIGHT JOIN sj_p_nullk p ... ORDER BY p.id LIMIT 4
//	 1 | 1 | 1.25 | {1,2} |  |       (and 2, 3 with a NULL key, 4)
//
// At the arc's base the first refused with `cannot store int64 into ARRAY
// vector` and the FULL join over one row answered `1, 1, 1.25, {1,2}, 1,
// NULL` — the preserved key read again as the empty side's.
func TestArcSJBareColumnsForced(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: loads the 20 000-row fixture")
	}
	ctx := context.Background()
	db := sjOpen(t, ctx, 4<<20)
	prev := exec.ForceJoinPartitionEvictEvery(1)
	defer exec.ForceJoinPartitionEvictEvery(prev)
	for _, c := range []struct{ sql, want string }{
		{"SELECT p.id, p.k_int, p.v_dec, p.v_arr, e.k_int, e.ey FROM sj_e e RIGHT JOIN sj_p_nullk p ON e.k_int = p.k_int ORDER BY p.id LIMIT 4",
			"[1 1 1.25 [1 2] <nil> <nil>] [2 2 2.50 [2 3] <nil> <nil>] [3 <nil> 3.75 [3 4] <nil> <nil>] [4 4 5.00 [4 5] <nil> <nil>]"},
		{"SELECT p.id, p.k_int, p.v_dec, p.v_arr, e.k_int, e.ey FROM sj_e e FULL JOIN sj_p_one p ON e.k_int = p.k_int",
			"[1 1 1.25 [1 2] <nil> <nil>]"},
	} {
		before := exec.JoinPartitionsEvicted.Load()
		res, err := db.Query(ctx, c.sql)
		if exec.JoinPartitionsEvicted.Load() == before {
			t.Errorf("%s: no build partition was evicted", c.sql)
		}
		if err != nil {
			t.Errorf("%s: %v\n  want %s (PostgreSQL 17.11)", c.sql, err, c.want)
			continue
		}
		rows := make([]string, len(res.Rows))
		for i := range res.Rows {
			rows[i] = fmt.Sprint(res.Cells(i))
		}
		if got := strings.Join(rows, " "); got != c.want {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, got, c.want)
		}
	}
}
