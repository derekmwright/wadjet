// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A SEMI OR ANTI JOIN THE PLANNER BUILDS FROM ITS OUTER SIDE STILL EMITS ITS
// OUTER RELATION'S CATEGORIES (#381), on five arms.
//
// When the inner relation of IN / EXISTS / NOT EXISTS is estimated more than
// three times larger than the outer, buildJoin builds the outer side and
// probes with the inner (RightSemiJoin / RightAntiJoin). It swapped the
// LOGICAL node's children to do so, and the category walk a Project above
// the join runs after it read the inner relation as the side the join emits:
// `round(b)` over a numeric b (a derived `5 / 2.0 + id * 0`, a `sqrt(…)`, a
// stored column CREATE TABLE AS made from one) read the inner table's double
// precision column of that name, or none, and answered 2 where PostgreSQL
// 17.11 (and 89cea148) answer 3; the opposite direction (a float8 outer, a
// marked inner) answered 3 for PostgreSQL's 2. The swap now works on a copy
// of the node. The W cells are ka (8 rows) as the inner relation, the T
// cells the same statements over kt (1 row, no swap). Every want is
// PostgreSQL 17.11's answer to the same statements.

var rejFixture = []string{
	"CREATE TABLE rv_a (id BIGINT, f DOUBLE PRECISION, n NUMERIC(38,16), i INTEGER)",
	"INSERT INTO rv_a VALUES (1, 0.5, 0.5, 1), (2, 1.5, 1.5, 3), (3, 2.5, 2.5, 5), (4, 3.5, 3.5, 7), (5, -0.5, -0.5, -1), (6, -1.5, -1.5, -3), (7, -2.5, -2.5, -5), (8, NULL, NULL, NULL)",
	"CREATE TABLE rv_mf AS SELECT id, sqrt(6.25 + id * 0) AS f FROM rv_a WHERE id = 3",
	"CREATE TABLE rv_big AS SELECT id, sqrt(6.25 + id * 0) AS f FROM rv_a",
	"CREATE TABLE rv_one (id BIGINT, f DOUBLE PRECISION)",
	"INSERT INTO rv_one VALUES (3, 2.5)",
	"CREATE TABLE ks (id BIGINT, w DOUBLE PRECISION)",
	"INSERT INTO ks VALUES (3, 1.0)",
	"CREATE TABLE ka (id BIGINT, b DOUBLE PRECISION, f DOUBLE PRECISION)",
	"INSERT INTO ka VALUES (1, 0.5, 0.5), (2, 1.5, 1.5), (3, 2.5, 2.5), (4, 3.5, 3.5), (5, -0.5, -0.5), (6, -1.5, -1.5), (7, -2.5, -2.5), (8, NULL, NULL)",
	"CREATE TABLE kt (id BIGINT, b DOUBLE PRECISION, f DOUBLE PRECISION)",
	"INSERT INTO kt VALUES (3, 2.5, 2.5)",
	"CREATE TABLE ksm AS SELECT id, sqrt(6.25 + id * 0) AS b FROM ks",
}

var rejCells = []struct{ name, sql, want string }{
	{"J01", "SELECT round(f) FROM rv_mf m WHERE EXISTS (SELECT 1 FROM rv_a a WHERE a.id = m.id)", "3"},
	{"J02", "SELECT round(f) FROM rv_mf WHERE EXISTS (SELECT 1 FROM rv_a WHERE rv_a.id = rv_mf.id)", "3"},
	{"J03", "SELECT round(m.f), CAST(m.f AS INTEGER) FROM rv_mf m WHERE EXISTS (SELECT 1 FROM rv_a a WHERE a.id = m.id)", "3,3"},
	{"J04", "SELECT round(f) FROM rv_mf m WHERE NOT EXISTS (SELECT 1 FROM rv_a a WHERE a.id = m.id AND a.id > 5)", "3"},
	{"J05", "SELECT round(f) FROM rv_mf m WHERE m.id IN (SELECT a.id FROM rv_a a WHERE a.f > 0)", "3"},
	{"J06", "SELECT round(f) FROM rv_mf WHERE id IN (SELECT id FROM rv_a)", "3"},
	{"J07", "SELECT round(f) FROM rv_mf m WHERE EXISTS (SELECT 1 FROM rv_a a WHERE a.id = m.id AND a.f = m.f)", "3"},
	{"X01", "SELECT round(b) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE id IN (SELECT id FROM rv_a)", "3"},
	{"X02", "SELECT round(b) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE EXISTS (SELECT 1 FROM rv_a WHERE rv_a.id = s.id)", "3"},
	{"X03", "SELECT CAST(b AS INTEGER), b::bigint, CAST(ARRAY[b] AS BIGINT[]) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE id IN (SELECT id FROM rv_a)", "3,3,{3}"},
	{"X04", "SELECT round(f), CAST(f AS INTEGER) FROM rv_one WHERE id IN (SELECT id FROM rv_big)", "2,2"},
	{"X05", "SELECT round(f), CAST(f AS INTEGER) FROM rv_one o WHERE EXISTS (SELECT 1 FROM rv_big b WHERE b.id = o.id)", "2,2"},
	{"X18", "SELECT round(f) FROM rv_mf WHERE id IN (SELECT id FROM rv_a) AND f > 0", "3"},
	{"W01", "SELECT round(b), CAST(b AS INTEGER) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM ka)", "3,3"},
	{"W02", "SELECT round(b) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE EXISTS (SELECT 1 FROM ka WHERE ka.id = s.id)", "3"},
	{"W03", "SELECT round(b), b::bigint FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id NOT IN (SELECT ka.id FROM ka WHERE ka.id = s.id AND ka.id > 5)", "3,3"},
	{"W04", "SELECT round(b) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE NOT EXISTS (SELECT 1 FROM ka WHERE ka.id = s.id AND ka.id > 5)", "3"},
	{"W05", "SELECT round(b) FROM (SELECT b FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM ka)) d", "3"},
	{"W06", "SELECT round(max(b) OVER ()), row_number() OVER (ORDER BY id) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM ka)", "3,1"},
	{"W07", "SELECT round(b), CAST(b AS INTEGER), CAST(ARRAY[b] AS BIGINT[]) FROM ksm WHERE id IN (SELECT id FROM ka)", "3,3,{3}"},
	{"W08", "SELECT round(sum(b)), round(max(b)) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE EXISTS (SELECT 1 FROM ka WHERE ka.id = s.id)", "3,3"},
	{"W09", "SELECT round(d.b) FROM (SELECT id, b FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM ka)) d JOIN ks ON ks.id = d.id", "3"},
	{"W10", "SELECT round(b), count(*) FROM ksm WHERE EXISTS (SELECT 1 FROM ka WHERE ka.id = ksm.id) GROUP BY round(b)", "3,1"},
	{"W11", "SELECT round(f), CAST(f AS INTEGER) FROM rv_mf WHERE NOT EXISTS (SELECT 1 FROM ka WHERE ka.id = rv_mf.id AND ka.b < 0)", "3,3"},
	{"W12", "SELECT round(b) FROM ksm WHERE id IN (SELECT id FROM ka) UNION ALL SELECT round(b) FROM ka WHERE id = 3", "3; 2"},
	{"T01", "SELECT round(b), CAST(b AS INTEGER) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM kt)", "3,3"},
	{"T02", "SELECT round(b) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE EXISTS (SELECT 1 FROM kt WHERE kt.id = s.id)", "3"},
	{"T03", "SELECT round(b), b::bigint FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id NOT IN (SELECT kt.id FROM kt WHERE kt.id = s.id AND kt.id > 5)", "3,3"},
	{"T04", "SELECT round(b) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE NOT EXISTS (SELECT 1 FROM kt WHERE kt.id = s.id AND kt.id > 5)", "3"},
	{"T05", "SELECT round(b) FROM (SELECT b FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM kt)) d", "3"},
	{"T06", "SELECT round(max(b) OVER ()), row_number() OVER (ORDER BY id) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM kt)", "3,1"},
	{"T07", "SELECT round(b), CAST(b AS INTEGER), CAST(ARRAY[b] AS BIGINT[]) FROM ksm WHERE id IN (SELECT id FROM kt)", "3,3,{3}"},
	{"T08", "SELECT round(sum(b)), round(max(b)) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM ks) s WHERE EXISTS (SELECT 1 FROM kt WHERE kt.id = s.id)", "3,3"},
	{"T09", "SELECT round(d.b) FROM (SELECT id, b FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM ks) s WHERE id IN (SELECT id FROM kt)) d JOIN ks ON ks.id = d.id", "3"},
	{"T10", "SELECT round(b), count(*) FROM ksm WHERE EXISTS (SELECT 1 FROM kt WHERE kt.id = ksm.id) GROUP BY round(b)", "3,1"},
	{"T11", "SELECT round(f), CAST(f AS INTEGER) FROM rv_mf WHERE NOT EXISTS (SELECT 1 FROM kt WHERE kt.id = rv_mf.id AND kt.b < 0)", "3,3"},
	{"T12", "SELECT round(b) FROM ksm WHERE id IN (SELECT id FROM kt) UNION ALL SELECT round(b) FROM kt WHERE id = 3", "3; 2"},
}

func TestArcRESwappedSemiJoinKeepsItsOuterCategoryOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	load := func(db *wadjet.DB) {
		for _, q := range rejFixture {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	embedded := func(budget int64) *wadjet.DB {
		cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if budget > 0 {
			cfg.MemoryBudget = budget
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		load(db)
		return db
	}
	single := embedded(0)
	spilled := embedded(512 * 1024)
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
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	arms := []struct {
		name string
		run  func(string) string
	}{
		{"single", func(s string) string { return reRunSingle(ctx, single, s) }},
		{"spilled512k", func(s string) string { return reRunSingle(ctx, spilled, s) }},
		{"dag", func(s string) string { return reRunDAG(ctx, coord, s) }},
		{"dag-shuffled", func(s string) string { return reRunDAG(ctx, coordB, s) }},
		{"dag-morsel4", func(s string) string { return reRunDAG(ctx, coordM, s) }},
	}
	swapped, twins := 0, 0
	for _, c := range rejCells {
		switch {
		case strings.HasPrefix(c.name, "W"):
			swapped++
		case strings.HasPrefix(c.name, "T"):
			twins++
		}
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				if got := arm.run(c.sql); got != c.want {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", arm.name, c.sql, got, c.want)
				}
			}
		})
	}
	if swapped < 12 || twins != swapped {
		t.Fatalf("the table shrank: %d swapped shapes, %d unswapped twins", swapped, twins)
	}
}
