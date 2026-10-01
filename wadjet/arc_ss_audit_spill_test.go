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

// EVERY OPERAND KIND × THE CONSUMERS THAT SPILL (#1422's class): the hash
// aggregate's partial-state merge and its GROUP BY key, a window's input,
// a window's PARTITION BY / ORDER BY keys and a sort's keys, each read past
// the memory budget with its spill engaged and asserted per cell. The kinds
// are coordinator.TestArcSSOperandKindTimesConsumerEveryArm's (its header
// names them), over a 20 000-row table; the correlated kind is re-run per
// row, and the bare CAST AS NUMERIC and the DECIMAL subquery are the double
// their plan declares (filing candidates N-19 and N-2), whose sums depend on
// the order the runs merge in; those three stay on the six-row five-arm
// table. Every answer is rendered as text by the query itself (a sort's ids
// as the MD5 of their order) and every want is PostgreSQL 17.11's over the
// same statements (testdata/arc_ss_audit_spill_pg17.tsv: name, knob, sql,
// answer).
func TestArcSSAuditConsumersUnderSpill(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: 75 spill cells over a 20 000-row table")
	}
	ctx := context.Background()
	const cols = "x AS id, CAST(x % 11 - 5 AS INTEGER) AS i, CAST(x AS BIGINT) * 1000 AS b, " +
		"CAST(x AS DOUBLE PRECISION) / 8 AS f, CAST((x % 100) / 4.0 - 12 AS NUMERIC(10,2)) AS n, " +
		"CAST(x % 90 + 10 AS TEXT) AS s, DATE '2000-01-01' + CAST(x % 3000 AS INTEGER) AS d, " +
		"ARRAY[CAST(x % 7 AS INTEGER)] AS a"
	open := func(budget int64) *DB {
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
		qs := []string{
			"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2), s VARCHAR, o BOOLEAN, d DATE, a ARRAY(INT))",
			"INSERT INTO ss_t VALUES (1, 3, 30, 1.5, 2.25, 'abc', true, DATE '2024-03-04', ARRAY[1,2]), " +
				"(2, -7, -70, -2.5, -3.5, 'Hello', false, DATE '1970-01-01', ARRAY[3]), " +
				"(3, 5, 9000000000, 0.25, 10.00, 'zz', true, DATE '9999-12-31', ARRAY[4,5,6])",
			"CREATE TABLE ss_i (id BIGINT, v INT, g DOUBLE, m NUMERIC(10,2))",
			"INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL)",
			// The table is written in slices of 1 000 rows, each a statement
			// whose result fits the budgeted DB's write budget.
			fmt.Sprintf("CREATE TABLE ss_w AS SELECT %s FROM generate_series(1, 1000) AS s(x)", cols),
		}
		for lo := 1001; lo <= 20000; lo += 1000 {
			qs = append(qs, fmt.Sprintf("INSERT INTO ss_w SELECT %s FROM generate_series(%d, %d) AS s(x)", cols, lo, lo+999))
		}
		for i, q := range qs {
			var err error
			if i < 4 {
				_, err = db.Query(ctx, q)
			} else {
				_, err = db.Execute(ctx, q)
			}
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		return db
	}
	plain := open(0)
	spilled := open(256 * 1024)
	defer exec.ForceSmallSpillRuns(4096)()

	read := func(path string) [][]string {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var out [][]string
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			if line := sc.Text(); line != "" && !strings.HasPrefix(line, "#") {
				out = append(out, strings.SplitN(line, "\t", 4))
			}
		}
		return out
	}
	counter := func(knob string) int64 {
		switch knob {
		case "agg":
			return exec.ForcedAggDrains.Load() + exec.AggregatePartialDrains.Load() + exec.RawRowSpillFiles.Load()
		case "sort":
			return exec.ForcedSortSpills.Load() + exec.SortRunsWritten.Load()
		case "window":
			return exec.WindowRunsWritten.Load()
		}
		t.Fatalf("unknown knob %q", knob)
		return 0
	}
	answer := func(db *DB, sql string, sortKey bool) string {
		res, err := db.Query(ctx, sql)
		if err != nil {
			return "ERR " + strings.ReplaceAll(err.Error(), "\n", " ")
		}
		var rows []string
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
			rows = append(rows, strings.Join(parts, ","))
		}
		if sortKey {
			sum := md5.Sum([]byte(strings.Join(rows, ",")))
			return "rows=1 " + hex.EncodeToString(sum[:])
		}
		sort.Strings(rows)
		return fmt.Sprintf("rows=%d %s", len(rows), strings.Join(rows, " | "))
	}
	for _, c := range read("testdata/arc_ss_audit_spill_pg17.tsv") {
		name, knob, sql, want := c[0], c[1], c[2], c[3]
		t.Run(name, func(t *testing.T) {
			sortKey := strings.HasPrefix(name, "sp/sortKey_")
			if got := answer(plain, sql, sortKey); got != want {
				t.Errorf("plain: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", sql, got, want)
			}
			var undo func()
			switch knob {
			case "agg":
				prev := exec.ForceAggDrainEvery(1)
				undo = func() { exec.ForceAggDrainEvery(prev) }
			case "sort":
				prev := exec.ForceSortSpillEvery(1)
				undo = func() { exec.ForceSortSpillEvery(prev) }
			}
			before := counter(knob)
			got := answer(spilled, sql, sortKey)
			if undo != nil {
				undo()
			}
			if counter(knob) == before {
				t.Errorf("spilled: %s\n  the %s spill was not engaged", sql, knob)
			}
			if got != want {
				t.Errorf("spilled: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", sql, got, want)
			}
		})
	}
}
