package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A NESTED PIPELINE DRAINS THE PARTITIONS ITS JOINS EVICTED — #1010.
//
// A grace hash join that evicts a build partition holds those rows on DISK,
// and the only thing that puts them back in the answer is its own flush.
// `exec.Pipeline.flushSpilledOps` runs that drain for the operators of the TOP
// pipeline; `physical.pipelineSource` is how a NESTED chain is driven — a
// join's build side, a set-operation arm, the inner side of a lateral — and it
// drove `Init`, `Next` and the bounded-output resumption but never the flush.
// The evicted rows were simply missing, silently.
//
// The whole result can be empty. Under a 512 KiB budget:
//
//	SELECT * FROM lat_ord o
//	JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i  WHERE i.order_id = o.id) s  ON true
//	JOIN LATERAL (SELECT COUNT(*)   AS c  FROM lat_item i2 WHERE i2.amount   = s.mx) s2 ON true
//	JOIN lat_item w ON w.order_id = o.id
//
// answered `cols=[] rows=0` — every probe row routed to a spilled partition,
// `HashJoinProbe.Execute` returned nil for each of them, and a star over more
// than one join declares nothing when no batch arrives — where PostgreSQL 17
// and the same query at a budget that does not spill answer four rows.
//
// THE ORDER MATTERS AND IS PART OF THE GATE. Whether the budget is crossed at
// all depends on what ran before in the same process (the tracker's forced
// bytes are outstanding, not per-query), which is why the filing reports the
// answer FLIPPING with query order. Both orders run here, twice each, against
// ONE engine.
//
// ENGAGEMENT IS ASSERTED, not assumed: `exec.JoinPartitionsEvicted` must move,
// or the cell compared two in-memory runs and would pass with the fix deleted
// (ADR-0027 decision 5). It is also FORCED, because at this budget it is a
// coin toss — the same order evicted a partition in one run of `go test` and
// not in the next — so `exec.ForceJoinPartitionEvictEvery(1)` makes the
// eviction the shape rather than the weather (ADR-0027 decision 6).
//
// `ForceSmallSpillRuns` and `ForceAggDrainEvery` are deliberately NOT armed:
// the first lowers the sort/window run floor and makes this shape answer
// CORRECTLY at base, so arming it cancels the defect the way #790's did. The
// eviction knob is orthogonal — it forces the condition the defect needs,
// never the path the defect is in.
func TestN1ANestedPipelineDrainsItsSpilledJoins(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate loads the shared type-matrix fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	const nested = "SELECT * FROM lat_ord o " +
		"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
		"JOIN LATERAL (SELECT COUNT(*) AS c FROM lat_item i2 WHERE i2.amount = s.mx) s2 ON true " +
		"JOIN lat_item w ON w.order_id = o.id " +
		"WHERE o.total > 0 ORDER BY o.id, w.id"
	const twoLaterals = "SELECT * FROM lat_ord o " +
		"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
		"JOIN LATERAL (SELECT MIN(amount) AS mn FROM lat_item i2 WHERE i2.order_id = o.id) s2 ON true " +
		"ORDER BY o.id"
	const plain = "SELECT o.id, o.customer FROM lat_ord o ORDER BY o.id"

	// PostgreSQL 17's answer. The column ORDER is wadjet's — an ordinary
	// inner join's sides are still ordered by the cost model, so `w` leads —
	// and it is identical on every arm and at every budget; what this gate is
	// about is that the ROWS are there at all.
	const wantNested = "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 " +
		"o.id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 c:INT64] rows=4 | " +
		"1,1,Widget,50,1,Alice,150,100,1 | 2,1,Gadget,100,1,Alice,150,100,1 | " +
		"3,2,Widget,75,2,Bob,200,125,1 | 4,2,Doohickey,125,2,Bob,200,125,1"
	const wantTwo = "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 mn:FLOAT64] rows=3 | " +
		"1,Alice,150,100,50 | 2,Bob,200,125,75 | 3,Carol,0,NULL,NULL"
	const wantPlain = "cols=[id:INT64 customer:STRING] rows=3 | 1,Alice | 2,Bob | 3,Carol"

	type step struct{ sql, want string }
	orders := []struct {
		name  string
		steps []step
	}{
		{"the shape twice, first in the process", []step{
			{nested, wantNested}, {nested, wantNested},
		}},
		{"two laterals first, then the shape twice", []step{
			{twoLaterals, wantTwo}, {nested, wantNested}, {nested, wantNested},
		}},
		{"a plain query, two laterals, then the shape twice", []step{
			{plain, wantPlain}, {twoLaterals, wantTwo},
			{nested, wantNested}, {nested, wantNested},
		}},
		{"the shape, then two laterals, then the shape", []step{
			{nested, wantNested}, {twoLaterals, wantTwo}, {nested, wantNested},
		}},
	}

	for _, ord := range orders {
		t.Run(ord.name, func(t *testing.T) {
			prev := exec.ForceJoinPartitionEvictEvery(1)
			defer exec.ForceJoinPartitionEvictEvery(prev)
			db := n1BudgetedLateralDB(t, ctx)
			evictedBefore := exec.JoinPartitionsEvicted.Load()
			forcedBefore := exec.ForcedJoinEvictions.Load()
			for i, st := range ord.steps {
				got, err := f1RenderSingle(ctx, db, st.sql)
				if err != nil {
					got = "ERR " + err.Error()
				}
				if got != st.want {
					t.Errorf("step %d/%d\n  %s\n  got  %s\n  want %s",
						i+1, len(ord.steps), st.sql, got, st.want)
				}
				if strings.HasPrefix(got, "cols=[] ") {
					t.Errorf("step %d/%d answered with NO COLUMNS, which is never an answer",
						i+1, len(ord.steps))
				}
			}
			if moved := exec.JoinPartitionsEvicted.Load() - evictedBefore; moved == 0 {
				t.Fatalf("no join partition was evicted in this order, so the drain "+
					"under test never ran and the cell proves nothing (%d bytes)", n1SpillBudget)
			}
			if moved := exec.ForcedJoinEvictions.Load() - forcedBefore; moved == 0 {
				t.Fatal("the eviction forcing knob never fired, so what this cell " +
					"measured was the machine rather than the shape")
			}
		})
	}
}

// n1SpillBudget is the budget these cells run under, and it is deliberately
// LARGE: the forcing knob decides whether a partition is evicted, and the
// budget's only job is to put the join on the spill-eligible dispatch at all
// (`Build` partitions on arrival when a MemTracker and a SpillManager are both
// set) without refusing anything.
//
// It was 512 KiB — "the smallest one at which this shape's joins evict a
// partition and the build is still admitted" — and that made the cells'
// disposition a property of the PROCESS: a full-package run had ~590 KiB of
// outstanding forced bytes ("spill tracking") by the third order, so the build
// refused `memory budget exceeded` and the gate went red in 1 of 2 full runs
// while passing alone every time. A gate whose trigger is a budget the load can
// push past is the coin toss ADR-0027 decision 6 exists to remove — and this
// arc added the knob that removes it, so the knob is what drives the eviction
// here.
const n1SpillBudget = 64 << 20

// n1BudgetedLateralDB is a budgeted engine over the LATERAL fixture alone.
//
// Not e3BudgetedStandalone: that one loads the whole shared type-matrix
// corpus, whose scan reservations alone stand at ~590 KiB of outstanding
// forced bytes, so every query under this budget refuses before it reaches a
// join. What this gate needs is a budget the build is admitted under and the
// partitions are evicted under, and that is a property of the fixture's SIZE.
func n1BudgetedLateralDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: n1SpillBudget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted engine: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{latOrdTable, latOrdSchema(), latOrdData()},
		{latItemTable, latItemSchema(), latItemData()},
	} {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}
