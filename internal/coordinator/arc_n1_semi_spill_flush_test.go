package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A SEMI / ANTI JOIN'S EVICTED PARTITIONS REACH THE ANSWER TOO — #1010 round 2.
//
// One rule, every join kind: the rows a grace hash join wrote to disk come back
// through its own flush, and whoever drives the chain FORWARDS what that flush
// produces. `rightSemiFlushSource` broke the second half. Its probe returns nil
// for every input batch — a RightSemi/RightAnti probe only marks matched build
// entries — so its drain loop pulled the nested `pipelineSource` to exhaustion
// and DISCARDED what came back, on the reasoning that nothing could.
//
// That reasoning stopped being true the moment `pipelineSource` learned to
// drain its operators' spilled partitions: the probe is in `innerOps`, it is a
// `FlushableOperator`, and its replayed partitions now arrive in that loop. The
// loop ate them and the `NextFlush` drain below it then found the probe already
// exhausted, so
//
//	SELECT k FROM n1semi_small s WHERE EXISTS (SELECT 1 FROM n1semi_big b WHERE b.k = s.k)
//
// answered 149 of PostgreSQL's 150 rows — silently, on the single-process path
// and on any worker fragment that runs this join. `joinFlushSource` (RIGHT and
// FULL) survived only because it RETURNS what its pipeline hands back; that
// difference between the two wrappers was the whole defect.
//
// THE BUDGET IS NOT THE TRIGGER — the knob is. At 4 MiB nothing here evicts on
// pressure (the build is 300 rows), so the DISARMED rows below are the
// reference and `ForceJoinPartitionEvictEvery(1)` is what makes the condition
// happen (ADR-0027 decision 6). A budget small enough to evict on its own would
// make this cell's disposition depend on what else is in the process, which is
// the hazard that decision exists to remove.
//
// THE BOUNDARY IS A CLAIM: RIGHT, FULL, LEFT and INNER over the same fixture
// run through `joinFlushSource` or through no wrapper at all, and their counts
// must not move either — they are the cells that say this is about the SEMI
// wrapper and not about the drain.
func TestN1ASemiJoinFlushReachesTheAnswer(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate ingests 3,300 rows")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	// PostgreSQL 17, measured live over the identical rows: 150 / 150 / 150 /
	// 150 for the four semi spellings, and their sums, which is what says
	// WHICH 150 rows. `SUM(bigint)` declares numeric on both engines
	// (ADR-0024), which is why the sum's type reads DECIMAL(38,0) here.
	cases := []struct {
		name, sql, want string
	}{
		{"EXISTS", "SELECT COUNT(*) AS n, SUM(k) AS s FROM n1semi_small x " +
			"WHERE EXISTS (SELECT 1 FROM n1semi_big b WHERE b.k = x.k)",
			"cols=[n:INT64 s:DECIMAL(38,0)] rows=1 | 150,11325"},
		{"NOT EXISTS", "SELECT COUNT(*) AS n, SUM(k) AS s FROM n1semi_small x " +
			"WHERE NOT EXISTS (SELECT 1 FROM n1semi_big b WHERE b.k = x.k)",
			"cols=[n:INT64 s:DECIMAL(38,0)] rows=1 | 150,33825"},
		{"IN", "SELECT COUNT(*) AS n, SUM(k) AS s FROM n1semi_small x " +
			"WHERE x.k IN (SELECT b.k FROM n1semi_big b)",
			"cols=[n:INT64 s:DECIMAL(38,0)] rows=1 | 150,11325"},
		{"NOT IN", "SELECT COUNT(*) AS n, SUM(k) AS s FROM n1semi_small x " +
			"WHERE x.k NOT IN (SELECT b.k FROM n1semi_big b)",
			"cols=[n:INT64 s:DECIMAL(38,0)] rows=1 | 150,33825"},
		// THE BOUNDARY. These four take a different wrapper (RIGHT and FULL
		// take `joinFlushSource`) or none, and none of them may move.
		{"ctl RIGHT", "SELECT COUNT(*) AS n FROM n1semi_small x " +
			"RIGHT JOIN n1semi_big b ON b.k = x.k", "cols=[n:INT64] rows=1 | 3000"},
		{"ctl FULL", "SELECT COUNT(*) AS n FROM n1semi_small x " +
			"FULL OUTER JOIN n1semi_big b ON b.k = x.k", "cols=[n:INT64] rows=1 | 3150"},
		{"ctl LEFT", "SELECT COUNT(*) AS n FROM n1semi_small x " +
			"LEFT JOIN n1semi_big b ON b.k = x.k", "cols=[n:INT64] rows=1 | 3150"},
		{"ctl INNER", "SELECT COUNT(*) AS n FROM n1semi_small x " +
			"JOIN n1semi_big b ON b.k = x.k", "cols=[n:INT64] rows=1 | 3000"},
	}

	for _, forced := range []bool{false, true} {
		t.Run(fmt.Sprintf("forced=%v", forced), func(t *testing.T) {
			if forced {
				prev := exec.ForceJoinPartitionEvictEvery(1)
				defer exec.ForceJoinPartitionEvictEvery(prev)
			}
			db := n1SemiDB(t, ctx)
			evictedBefore := exec.JoinPartitionsEvicted.Load()
			for _, tc := range cases {
				got, err := f1RenderSingle(ctx, db, tc.sql)
				if err != nil {
					got = "ERR " + err.Error()
				}
				if got != tc.want {
					t.Errorf("%s (forced=%v)\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
						tc.name, forced, got, tc.want, tc.sql)
				}
			}
			moved := exec.JoinPartitionsEvicted.Load() - evictedBefore
			if forced && moved == 0 {
				t.Fatal("no partition was evicted with the knob armed, so nothing here " +
					"reached the flush and the cells prove nothing")
			}
			if !forced && moved != 0 {
				t.Fatalf("%d partitions were evicted with the knob DISARMED: this "+
					"reference arm is meant to hold the un-spilled answer, and a "+
					"budget that evicts on its own makes the comparison cancel (#790)",
					moved)
			}
		})
	}
}

// n1SemiDB is the semi-join fixture: a 300-row probe side and a 3,000-row build
// side whose keys cover half of it.
//
// The SIZES are the shape: `assignJoinKeySides` swaps a semi join whose inner
// side is much larger into a RightSemi/RightAnti, which is the join kind whose
// wrapper this gate is about. The budget is deliberately LARGE — nothing here
// evicts on pressure, so the forcing knob is the only thing that can.
const n1SemiBudget = 4 << 20

func n1SemiDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: n1SemiBudget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}}
	small := make([]map[string]any, 0, 300)
	for i := 1; i <= 300; i++ {
		small = append(small, map[string]any{"k": int64(i)})
	}
	big := make([]map[string]any, 0, 3000)
	for i := 1; i <= 3000; i++ {
		big = append(big, map[string]any{"k": int64(1 + i%150)})
	}
	for _, tbl := range []struct {
		name string
		rows []map[string]any
	}{{"n1semi_small", small}, {"n1semi_big", big}} {
		if err := db.CreateTable(ctx, tbl.name, schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, schema, nil, ingest.Config{
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
