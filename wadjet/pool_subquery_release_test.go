// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestPoolSubqueryRunsReleaseTheirCharges (1685e round 2). A subquery the
// planner runs by itself (a correlated scalar subquery, once per outer row)
// executed its pipeline and never closed it, so what its operators charged
// stayed charged: an aggregate's group state was released only by a Close
// that never came. On a per-statement tracker that died with the statement;
// on an engine instance's shared pool it outlives the statement, so every run
// of `SELECT k, (SELECT corr(x + po.k, y) FROM pt) FROM po` left 17,920 bytes
// of forced spill tracking for good, and after enough of them an ordinary
// statement that answers alone was refused (`memory budget exceeded ... of
// which forced=... by "spill tracking"`, found by
// TestAggregateComputedArgumentsEveryPosition/spilled). Each shape below runs
// many times on one DB: the pool returns to zero after every statement, the
// answers are PostgreSQL 17.11's (1685e_evidence/r2/pg_oracle_r2_subquery.txt),
// and a statement that needs room afterwards still answers.
func TestPoolSubqueryRunsReleaseTheirCharges(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 1 << 16, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE pt (id INT, g INT, x INT, y INT)",
		"INSERT INTO pt VALUES (1,1,10,100),(2,1,20,NULL),(3,2,NULL,300),(4,2,40,400),(5,1,50,500)",
		"CREATE TABLE po (k INT)",
		"INSERT INTO po VALUES (1),(2)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	assertDBPoolQuiescent(t, db)
	shapes := []struct{ name, sql, want string }{
		// The aggregate's argument reads the outer row: one run per row.
		{"aggregate", "SELECT k, (SELECT corr(x + po.k, y) FROM pt) AS c FROM po ORDER BY k",
			"[map[c:1 k:1] map[c:1 k:2]]"},
		{"aggregate-grouped", "SELECT k, (SELECT sum(x + po.k) FROM pt WHERE g = 1 GROUP BY g) AS s FROM po ORDER BY k",
			"[map[k:1 s:83] map[k:2 s:86]]"},
		// A sort (top-K) inside the per-row run.
		{"sort", "SELECT k, (SELECT y + po.k FROM pt WHERE y IS NOT NULL ORDER BY y DESC LIMIT 1) AS m FROM po ORDER BY k",
			"[map[k:1 m:501] map[k:2 m:502]]"},
		// A join inside the subquery.
		{"join", "SELECT k, (SELECT count(*) FROM pt a JOIN pt b ON a.id = b.id WHERE a.g = po.k) AS n FROM po ORDER BY k",
			"[map[k:1 n:3] map[k:2 n:2]]"},
	}
	tr := db.pool.Tracker()
	for _, sh := range shapes {
		for round := 0; round < 30; round++ {
			got, err := rowsText(t, db, sh.sql)
			if err != nil || got != sh.want {
				t.Fatalf("%s round %d: %s %v, PostgreSQL 17.11 %s", sh.name, round, got, err, sh.want)
			}
			if u := tr.Used(); u != 0 {
				var by []string
				for p := memory.ForcePurpose(0); p < memory.NumForcePurposes; p++ {
					if v := tr.ForcedFor(p); v != 0 {
						by = append(by, fmt.Sprintf("%s=%d", p, v))
					}
				}
				t.Fatalf("%s round %d: the statement ended and left %d bytes on the DB's pool (forced %v)", sh.name, round, u, by)
			}
		}
		assertDBPoolQuiescent(t, db)
	}
	// A statement that needs room: the build of a cross join cannot spill,
	// so stranded charges are what would refuse it. PostgreSQL 17.11: 1 for
	// both keys.
	const control = "SELECT k, corr(x, a2) AS c FROM (SELECT po.k, x, y + po.k AS a2 FROM po CROSS JOIN pt) q GROUP BY k ORDER BY k"
	if got, err := rowsText(t, db, control); err != nil || got != "[map[c:1 k:1] map[c:1 k:2]]" {
		t.Fatalf("after the subqueries: %s %v, PostgreSQL 17.11 [1 1, 2 1]", got, err)
	}
	assertDBPoolQuiescent(t, db)
}
