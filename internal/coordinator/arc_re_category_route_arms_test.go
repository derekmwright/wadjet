// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// ONLY A PLAN THAT ROUNDS TAKES THE LOCAL ROUTE FOR A CATEGORY THE STAGE
// DAG'S NAME MAP LOSES (#381, arc RE round 4), on five arms.
//
// rv_mf.f is a stored column created from `sqrt(6.25 + id * 0)` (numeric to
// PostgreSQL 17.11, FLOAT64 here with the mark) and rv_a.f a double
// precision: a plan reading both leaves f out of the map the stages key by
// name (physical.PlanPGCategories), and a stage rounding f would take the
// carrier's reading. Round 3 sent every such plan to the coordinator-local
// pipeline (errCategoryByName), including the ones nothing in which rounds —
// a semi join, a count, two sums, a bare UNION ALL, a bare join projection
// ran on one process with no category to read. Now only a plan with a
// ROUND or an integer cast routes (planHasRoundingSite); the rest run on the
// stage DAG. Every want is PostgreSQL 17.11's, except where noted.

var rcrFixture = []string{
	"CREATE TABLE rv_a (id BIGINT, f DOUBLE PRECISION, n NUMERIC(38,16), i INTEGER)",
	"INSERT INTO rv_a VALUES (1, 0.5, 0.5, 1), (2, 1.5, 1.5, 3), (3, 2.5, 2.5, 5), (4, 3.5, 3.5, 7), (5, -0.5, -0.5, -1), (6, -1.5, -1.5, -3), (7, -2.5, -2.5, -5), (8, NULL, NULL, NULL)",
	"CREATE TABLE rv_mf AS SELECT id, sqrt(6.25 + id * 0) AS f FROM rv_a WHERE id = 3",
}

var rcrCells = []struct {
	name, sql, want string
	routes          bool
}{
	// Nothing rounds: the stage DAG.
	{"D03", "SELECT a.id FROM rv_a a WHERE a.id IN (SELECT id FROM rv_mf) ORDER BY 1", "3", false},
	{"D04", "SELECT count(*) FROM rv_mf m JOIN rv_a a ON a.id = m.id", "1", false},
	// PostgreSQL prints the numeric sum 2.500000000000000 (its display
	// scale; this engine carries the value in a double, ADR-0024 §2c).
	{"D05", "SELECT sum(a.f), sum(m.f) FROM rv_a a JOIN rv_mf m ON a.id = m.id", "2.5,2.5", false},
	{"D10", "SELECT f FROM rv_mf UNION ALL SELECT f FROM rv_a WHERE id = 3", "2.5; 2.5", false},
	{"D12", "SELECT a.f, m.f FROM rv_a a JOIN rv_mf m ON a.id = m.id", "2.5,2.5", false},
	// A ROUND over the lost name: the coordinator-local pipeline.
	{"D01", "SELECT round(m.f), round(a.f) FROM rv_mf m JOIN rv_a a ON a.id = m.id", "3,2", true},
	{"D07", "SELECT round(f) FROM rv_mf UNION ALL SELECT round(f) FROM rv_a WHERE id = 3", "3; 2", true},
	{"D11", "SELECT round(f) FROM (SELECT f FROM rv_mf UNION ALL SELECT f FROM rv_a WHERE id = 3) u", "2; 2", true},
	{"J06", "SELECT round(f) FROM rv_mf WHERE id IN (SELECT id FROM rv_a)", "3", true},
	{"J06c", "SELECT CAST(f AS INTEGER) FROM rv_mf WHERE id IN (SELECT id FROM rv_a)", "3", true},
}

func TestArcRELostCategoryRoutesOnlyARoundingPlan(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	load := func(db *wadjet.DB) {
		for _, q := range rcrFixture {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	single, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { single.Close() })
	load(single)
	spilled, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 512 * 1024, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spilled.Close() })
	load(spilled)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		w, err := wadjet.Open(ctx, wadjet.Config{Store: infra.store, Bucket: "test", MetaKV: infra.kv})
		if err != nil {
			t.Fatal(err)
		}
		load(w)
		w.Close()
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	dags := []struct {
		name string
		c    *Coordinator
	}{
		{"dag", stand(nil)},
		{"dag-shuffled", stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })},
		{"dag-morsel4", stand(func(w *worker.Config) { w.MorselWorkers = 4 })},
	}
	for _, c := range rcrCells {
		t.Run(c.name, func(t *testing.T) {
			for _, db := range []struct {
				name string
				db   *wadjet.DB
			}{{"single", single}, {"spilled512k", spilled}} {
				if got := reRunSingle(ctx, db.db, c.sql); got != c.want {
					t.Errorf("%s: %s\n  got  %s\n  want %s", db.name, c.sql, got, c.want)
				}
			}
			for _, d := range dags {
				before := d.c.UnreachableOutputLocalRoutes()
				got := reRunDAG(ctx, d.c, c.sql)
				routed := d.c.UnreachableOutputLocalRoutes() != before
				if got != c.want {
					t.Errorf("%s: %s\n  got  %s\n  want %s", d.name, c.sql, got, c.want)
				}
				if routed != c.routes {
					t.Errorf("%s: %s\n  took the local route: %v, want %v", d.name, c.sql, routed, c.routes)
				}
			}
		})
	}
}
