// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Cross-statement relief on one pool (1685e; kept by the 2026-11-24 ruling):
// statement A's reservation shortfall asks the pool's registry for relief,
// and the registry may pick an operator of statement B. It is safe only
// while B's state is evictable under B's own lifecycle lock (1706k's guards,
// 9704a10e). Each cell holds B at one lifecycle point, runs A against it,
// records which of B's operators A's relief reached, and checks B's answer
// (and A's) against PostgreSQL 17.11.

type reliefLog struct {
	mu    sync.Mutex
	calls []string
	ids   map[uint64]bool // instances a relief call was about to reach
}

func (l *reliefLog) hook(site memory.ReliefSite, op memory.AccountedOperator) {
	f := op.Inspect()
	l.mu.Lock()
	l.calls = append(l.calls, fmt.Sprintf("%s->%s#%d", site, f.Name, f.InstanceID))
	if l.ids == nil {
		l.ids = map[uint64]bool{}
	}
	l.ids[f.InstanceID] = true
	l.mu.Unlock()
}

func (l *reliefLog) reached(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ids[id]
}

func (l *reliefLog) names() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[string]bool{}
	for _, c := range l.calls {
		seen[c] = true
	}
	var out []string
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// reliefRequesterSQL is A: the same self-join over `bigrg`, the same rows in
// row groups of 2048, so A's first arrival (one row group) is larger than
// the room B leaves and A's reservation fails — the requester shape that
// asks the registry for relief (spillUntilCanReserve → RequestRelief).
// PostgreSQL 17.11: 4000 | 7998000.
const reliefRequesterSQL = "SELECT count(*) AS n, sum(pool_gate_1(a.k)) AS s, max(b.pad) AS m FROM bigrg a JOIN bigrg b ON a.k = b.k"

// reliefRequesterWant: PostgreSQL 17.11 answers 4000 | 7998000 | the pad of k = 3999.
var reliefRequesterWant = "[map[m:00003999" + strings.Repeat("x", 504) + " n:4000 s:7998000]]"

func addBigRowGroupCopy(t *testing.T, db *DB) {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "bigrg", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 4000)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(i), "pad": fmt.Sprintf("%08d", i) + strings.Repeat("x", 504)}
	}
	ing := db.NewIngester("bigrg", schema, nil, ingest.Config{MaxBufferRows: 4001, RowGroupSize: 2048})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
}

// countGate is a hold armed by armLiveGate: reached closes when B is held,
// release lets it go.
type countGate struct {
	reached chan struct{}
	release chan struct{}
}

// TestPoolReliefCrossesStatementsSafely. Cells (B's lifecycle point when A
// asks):
//   - join-mid-build: B's grace build is held at an arrival with partitions in
//     memory and still registered. A's build, short of room, may evict B's
//     partitions: legitimate relief, and B must still answer exactly.
//   - join-after-build: B sits in its probe; its relief is closed (1706k). A
//     must not reach B's join; A spills or is refused by name instead.
//   - aggregate-mid-consume: B's hash aggregate is registered and Active
//     with groups in memory. A's build may make it spill; B answers exactly.
func TestPoolReliefCrossesStatementsSafely(t *testing.T) {
	const budget = 614268
	ctx := context.Background()
	for _, cell := range []string{"join-mid-build", "join-after-build", "aggregate-mid-consume"} {
		t.Run(cell, func(t *testing.T) {
			db := wideJoinDB(t, 4000, 512, 512, budget)
			addBigRowGroupCopy(t, db)
			gates := armPoolGates(2) // gate 1 is A's, always open
			gates[1].open()
			log := &reliefLog{}
			defer memory.SetReliefCallHook(log.hook)()

			bSQL, bWant := poolJoinSQL(0), poolJoinWant
			bHeld := make(chan struct{})
			releaseB := make(chan struct{})
			switch cell {
			case "join-mid-build":
				gates[0].open()
				var arrivals atomic.Int64
				var holding atomic.Bool
				defer exec.SetArrivalReserveHook(func() {
					// B alone runs until its 6th arrival; it is held there
					// (the hook runs with B's join lock released). Every
					// later arrival is A's and passes.
					if arrivals.Add(1) == 6 && holding.CompareAndSwap(false, true) {
						close(bHeld)
						<-releaseB
					}
				})()
			case "join-after-build":
				go func() { <-gates[0].reached; close(bHeld); <-releaseB; gates[0].open() }()
			case "aggregate-mid-consume":
				// Held once B's aggregate is live with spillable bytes; the
				// gate is a filter below the aggregate (armLiveGate). One
				// scan worker: a parallel GROUP BY partitions its groups
				// into clones registered on tracking-only views (private
				// registries, pipeline.go), so its primary may hold nothing
				// another statement's relief can reach; the serial
				// aggregate is the one that does.
				t.Setenv("WADJET_SCAN_WORKERS", "1")
				g := armLiveGate(db, "HashAggregate")
				bSQL = "SELECT count(*) AS n, sum(c) AS s FROM (SELECT k, count(k) AS c FROM wide WHERE pool_count_gate(k) >= 0 GROUP BY k) q"
				bWant = "[map[n:4000 s:4000]]"
				go func() { <-g.reached; close(bHeld); <-releaseB; close(g.release) }()
			}

			bDone := make(chan struct{})
			var bAns string
			var bErr error
			go func() {
				defer close(bDone)
				res, err := db.Query(ctx, bSQL)
				if err != nil {
					bErr = err
					return
				}
				bAns = fmt.Sprint(res.Rows)
			}()
			select {
			case <-bHeld:
			case <-bDone:
				t.Fatalf("B ended before its %s point: %v", cell, bErr)
			case <-time.After(poolGateDeadline):
				t.Fatalf("B never reached its %s point", cell)
			}
			// B's live registrants, read before A starts: the operators of B
			// that A's relief could reach.
			bLive := map[uint64]string{}
			for _, f := range db.pool.SpillManager().Inspect() {
				if f.State == memory.OpActive {
					bLive[f.InstanceID] = f.Name
				}
			}
			bUsed := db.pool.Tracker().Used()
			evictedBefore := exec.JoinPartitionsEvicted.Load()
			aDone := make(chan struct{})
			var aAns string
			var aErr error
			go func() {
				defer close(aDone)
				res, err := db.Query(ctx, reliefRequesterSQL)
				if err != nil {
					aErr = err
					return
				}
				aAns = fmt.Sprint(res.Rows)
			}()
			select {
			case <-aDone:
			case <-time.After(poolGateDeadline):
				t.Fatalf("A hung beside B held at %s", cell)
			}
			evictedDuringA := exec.JoinPartitionsEvicted.Load() - evictedBefore
			close(releaseB)
			select {
			case <-bDone:
			case <-time.After(poolGateDeadline):
				t.Fatalf("B hung after release at %s", cell)
			}
			reachedB := []string{}
			for id, name := range bLive {
				if log.reached(id) {
					reachedB = append(reachedB, fmt.Sprintf("%s#%d", name, id))
				}
			}
			sort.Strings(reachedB)
			t.Logf("%s: B held with %d bytes on the pool, live registrants %v; A's relief reached B's %v; all relief calls %v; partitions evicted while A ran %d",
				cell, bUsed, bLive, reachedB, log.names(), evictedDuringA)
			if aErr != nil {
				t.Logf("%s: A refused: %v", cell, aErr)
			}
			switch cell {
			case "join-mid-build", "aggregate-mid-consume":
				// The point of the cell: A's shortfall reached a LIVE operator
				// of another statement (cross-statement relief, kept).
				if len(reachedB) == 0 {
					t.Errorf("A's relief never reached B's live operators %v: the cell did not exercise cross-statement relief", bLive)
				}
			case "join-after-build":
				if len(bLive) != 0 {
					t.Errorf("B in its probe still has live registrants %v: its relief should be closed (1706k)", bLive)
				}
			}

			if bErr != nil || bAns != bWant {
				t.Errorf("B at %s: %s %v, PostgreSQL 17.11 %s", cell, bAns, bErr, bWant)
			}
			if aErr != nil && !isBudgetRefusal(aErr) {
				t.Errorf("A failed with an unnamed error: %v", aErr)
			}
			if aErr == nil && aAns != reliefRequesterWant {
				t.Errorf("A: %s, PostgreSQL 17.11 %s", aAns, reliefRequesterWant)
			}
			assertDBPoolQuiescent(t, db)
		})
	}
}
