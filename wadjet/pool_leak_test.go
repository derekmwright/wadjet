// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// TestPoolManyStatementsLeaveNoState: an embedded DB lives for days and runs
// millions of statements on ONE pool now (1685e). 10,000 small statements of
// mixed fate — answered, refused while planned, failed in planning,
// cancelled before they start — plus a spilling join every 50th: after
// warm-up the heap plateaus and every per-instance structure of the pool's
// tracker and registry stays bounded; the transient ones are zero whenever
// no statement runs. Departed summaries persist by operator NAME by design
// (spill.go RegisterAccounted), so they are bounded, not zero; the tracker
// peak is lifetime-wide and not asserted.
func TestPoolManyStatementsLeaveNoState(t *testing.T) {
	if testing.Short() {
		t.Skip("10,000 statements")
	}
	const total, warm = 10000, 1000
	db := wideJoinDB(t, 4000, 512, 512, 256<<10) // the join spills at this budget
	gates := armPoolGates(1)
	gates[0].open()
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	kinds := map[string]int{}
	run := func(i int) {
		var err error
		switch {
		case i%50 == 0:
			_, err = db.Query(ctx, poolJoinSQL(0))
			kinds["join"]++
		case i%50 == 25:
			// A cross join refused while it is planned (closeAbandonedJoins).
			_, err = db.Query(ctx, "SELECT count(*) FROM wide a CROSS JOIN wide b WHERE a.k + b.k < 10")
			kinds["refused-cross"]++
			err = nil
		case i%50 == 10:
			// A window that spills: Finalize unregisters it and then flushes
			// its remainder, which used to re-publish the instance on the
			// pool's tracker forever (window.go flushSpillLocked).
			_, err = db.Query(ctx, "SELECT count(*) AS n, max(r) AS m FROM (SELECT row_number() OVER (ORDER BY pad DESC) AS r FROM wide) q")
			kinds["window-spill"]++
		case i%7 == 0:
			_, err = db.Query(ctx, "SELECT nosuch FROM wide")
			kinds["planning-error"]++
			err = nil
		case i%11 == 0:
			_, err = db.Query(cancelled, "SELECT count(*) FROM wide WHERE k < 100")
			kinds["cancelled"]++
			err = nil
		case i%13 == 0:
			_, err = db.Query(ctx, "SELECT k % 10 AS g, count(*) AS n FROM wide GROUP BY 1 ORDER BY 1")
			kinds["aggregate-sort"]++
		default:
			_, err = db.Query(ctx, fmt.Sprintf("SELECT count(*) FROM wide WHERE k < %d", i%4000))
			kinds["scan"]++
		}
		if err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}
	}
	type sample struct {
		heap   uint64
		census string
		owners int
		used   int64
	}
	take := func() sample {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		c := db.pool.SpillManager().Census()
		return sample{heap: ms.HeapAlloc, census: fmt.Sprintf("%+v", c),
			owners: len(db.pool.Tracker().OwnedSnapshot()), used: db.pool.Tracker().Used()}
	}
	for i := 0; i < warm; i++ {
		run(i)
	}
	first := take()
	var samples []sample
	for i := warm; i < total; i++ {
		run(i)
		if (i+1)%3000 == 0 || i == total-1 {
			s := take()
			samples = append(samples, s)
			assertDBPoolQuiescent(t, db)
		}
	}
	last := samples[len(samples)-1]
	t.Logf("statement kinds %v", kinds)
	t.Logf("after warm-up (%d): heap %d, %s", warm, first.heap, first.census)
	for i, s := range samples {
		t.Logf("sample %d: heap %d (%+d since warm-up), %s, owners %d, used %d",
			i, s.heap, int64(s.heap)-int64(first.heap), s.census, s.owners, s.used)
	}
	c := db.pool.SpillManager().Census()
	if c.Slots > 64 || c.Departed > 32 {
		t.Errorf("registry structures grew with the statement count: %+v", c)
	}
	// A plateau, not a ramp: 9,000 statements after warm-up may move the
	// heap by noise, not by a per-statement retention (a 1 KiB leak per
	// statement alone would be 9 MiB).
	if grow := int64(last.heap) - int64(first.heap); grow > 4<<20 {
		t.Errorf("heap grew %d bytes over %d statements after warm-up", grow, total-warm)
	}
}
