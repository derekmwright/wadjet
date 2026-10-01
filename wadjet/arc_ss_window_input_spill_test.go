// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A WINDOW FUNCTION'S INPUT OVER AN INTEGER OPERAND BESIDE A NUMERIC IS EXACT,
// AND STAYS EXACT ACROSS A WINDOW SPILL. The input is materialized by the
// window's own key projection; it was declared double for a subscript of an
// integer array (the element was not in the declaration's columns), so the
// kernel's exact product met #361's guard, and it compiled a scalar subquery
// without its declaration, so `(SELECT max(b) …) * n + 3` was an exact numeric
// computed in a double. Every want is PostgreSQL 17.11's over the same
// generate_series fixture.
func TestArcSSWindowInputExactUnderSpill(t *testing.T) {
	ctx := context.Background()
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
		// The fixture is written in slices of 1 000 rows, each a statement
		// whose result fits the budgeted DB's write budget.
		const cols = "x AS id, x % 5 AS g, CAST((x % 100) / 4.0 AS NUMERIC(10,2)) AS n, ARRAY[x % 7] AS a"
		qs := []string{fmt.Sprintf("CREATE TABLE ssw AS SELECT %s FROM generate_series(1, 1000) AS s(x)", cols)}
		for lo := 1001; lo <= 20000; lo += 1000 {
			qs = append(qs, fmt.Sprintf("INSERT INTO ssw SELECT %s FROM generate_series(%d, %d) AS s(x)", cols, lo, lo+999))
		}
		qs = append(qs, "CREATE TABLE ssb AS SELECT CAST(90000000000000000 AS BIGINT) AS b")
		for _, q := range qs {
			if _, err := db.Execute(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		return db
	}
	plain := open(0)
	spilled := open(256 * 1024)
	defer exec.ForceSmallSpillRuns(4096)()

	for _, c := range []struct {
		name, sql string
		want      []string // id|value rows PostgreSQL 17.11 answers
	}{
		{"subscriptTimesN",
			`SELECT id, sum(a[1] * n) OVER (ORDER BY id) AS v FROM ssw ORDER BY id DESC LIMIT 1`,
			[]string{"20000|742474.75"}},
		{"subqueryTimesNPlus3",
			`SELECT id, sum((SELECT max(b) FROM ssb) * n + 3) OVER (PARTITION BY g ORDER BY id) AS v ` +
				`FROM ssw ORDER BY id DESC LIMIT 2`,
			[]string{"20000|4275000000000000012000.00", "19999|4635000000000000012000.00"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for arm, db := range map[string]*DB{"plain": plain, "spilled": spilled} {
				before := exec.WindowRunsWritten.Load()
				res, err := db.Query(ctx, c.sql)
				if err != nil {
					t.Fatalf("%s: %v\n  SQL: %s", arm, err, c.sql)
				}
				var got []string
				for i := range res.Rows {
					cells := res.Cells(i)
					got = append(got, fmt.Sprintf("%v|%v", cells[0], cells[1]))
				}
				if fmt.Sprint(got) != fmt.Sprint(c.want) {
					t.Errorf("%s: %v, PostgreSQL 17.11 answers %v\n  SQL: %s", arm, got, c.want, c.sql)
				}
				if arm == "spilled" && exec.WindowRunsWritten.Load() == before {
					t.Errorf("spilled: no window run reached disk — the cell compared an in-memory window\n  SQL: %s", c.sql)
				}
			}
		})
	}
}
