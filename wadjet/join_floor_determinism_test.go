package wadjet

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// #789's query, its budget, and the instant it is a question about.
const (
	floorSQL    = `SELECT z.c_i64 AS k, COUNT(*) AS n FROM typemx x JOIN typemx z ON x.id = z.id GROUP BY z.c_i64`
	floorBudget = 512 * 1024
	floorRuns   = 10
)

// The floor at the join build's first reservation is ONE NUMBER for one query
// on one fixture — when the query is the only thing deciding it.
//
// #789 reported six different values for `used` at that instant (465738,
// 447345, 449423, 482816, 492138, 660967) and asked what the residual was. Two
// things produced it and only one was a defect.
//
// The defect was the LEDGER: a grace build released either more or less than it
// reserved on every arrival batch, so `used` drifted by an amount that followed
// how many partitions had spilled — pressure and timing, not the query. That is
// fixed (`exec.TestAGraceBuildsLedgerIsConserved`).
//
// What remains is the scan's LIVE READ-AHEAD, and this gate measures it rather
// than asserting it away. A scan holds the row groups it has in flight and
// releases each when it is decoded, so how much it holds at any instant follows
// how far its decode workers ran ahead of the join. Serialize the scan and the
// number is the same on every run — which is what this asserts, and it asserts
// the SPLIT too: the snapshot carries the forced census, so "the floor is the
// scan's in-flight bytes" is a measurement here, not a story. Measured
// serially: 35,776 on all ten runs, ALL of it `scan decoded batch` (producer
// 3), nothing from the file load and nothing from the join's own index — which
// has not indexed a row at its first reserve.
//
// WHAT MAKES THIS FAIL. It is a property gate, not the revert proof for one
// hunk: at the FIRST reserve the join has charged nothing, so a ledger that
// drifts per arrival batch has not drifted yet, and reverting
// `reconcileArrivalCharge` leaves this green (that revert proof is
// `exec.TestAGraceBuildsLedgerIsConserved`). What this catches is a residual
// that OUTLIVES A QUERY — each run opens a fresh database, so anything a
// previous query left on a shared ledger shows up as a second value — and any
// producer whose charge at this instant becomes timing-dependent even with one
// scheduler thread and one scan worker. Those are the two shapes #789 asked
// about that the verdict gate below cannot see.
func TestTheJoinsFloorIsOneNumberWhenTheScanCannotRunAhead(t *testing.T) {
	// Serialize the whole engine for this test: one scheduler thread and one
	// scan worker. No test in this package runs in parallel, so this is
	// contained, and it is restored on the way out.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	t.Setenv("WADJET_SCAN_WORKERS", "1")

	seen := map[int64]int{}
	var first exec.JoinFloorSnapshot
	for i := 0; i < floorRuns; i++ {
		snap, rows, err := floorRun(t)
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if !snap.Seen {
			t.Fatalf("run %d: no grace build reserved anything — the shape stopped "+
				"reaching the partition-on-arrival path and this gate measured nothing", i)
		}
		if i == 0 {
			first = snap
		}
		seen[snap.Used]++
		t.Logf("run %2d: used=%d (scan file load %d, decoded batches %d, pooled bufs %d, "+
			"join index %d) rows=%d", i, snap.Used, snap.ScanFileLoad, snap.ScanDecodedBatch,
			snap.ScanPooledBuffer, snap.JoinIndex, rows)
	}

	if len(seen) != 1 {
		t.Errorf("the floor took %d different values across %d runs of one query on one "+
			"fixture with the scan serialized: %v. Serialized, nothing but the query "+
			"decides it, so a second value is a residual that outlives a query — the "+
			"thing #789 asked about", len(seen), floorRuns, seen)
	}

	// The split, so the number above is explained and not merely repeated. The
	// join has not indexed anything at its FIRST reserve, so its own forced
	// charge is zero and everything on the ledger belongs to the scan.
	if first.JoinIndex != 0 {
		t.Errorf("the join had already forced %d bytes of index at its first reserve; it "+
			"has not indexed a row yet", first.JoinIndex)
	}
	scan := first.ScanFileLoad + first.ScanDecodedBatch + first.ScanPooledBuffer
	if scan > first.Used {
		t.Errorf("the scan's forced charges are %d of a %d floor, which is more than the "+
			"whole of it", scan, first.Used)
	}
}

// The VERDICT is the property that must not vary, and it must not vary with the
// scheduler FREE either — that is the half of #789 the serialized gate above
// cannot speak to. The floor legitimately moves here (the scan's decode workers
// run ahead by different amounts); what may not move is whether the query
// answers, and with what.
func TestTheJoinsVerdictIsOneVerdictWithTheSchedulerFree(t *testing.T) {
	answers := map[string]int{}
	floors := map[int64]int{}
	for i := 0; i < floorRuns; i++ {
		snap, rows, err := floorRun(t)
		verdict := fmt.Sprintf("%d rows", rows)
		if err != nil {
			verdict = "REFUSED"
		}
		answers[verdict]++
		if snap.Seen {
			floors[snap.Used]++
		}
	}
	t.Logf("floors seen across %d runs: %d distinct values", floorRuns, len(floors))
	if len(answers) != 1 {
		t.Errorf("one query at one budget on one fixture reached %d different verdicts "+
			"across %d runs: %v", len(answers), floorRuns, answers)
	}
}

// floorRun opens a fresh database, runs #789's query once with the probe armed,
// and returns what the probe saw and how many rows came back.
func floorRun(t *testing.T) (exec.JoinFloorSnapshot, int, error) {
	t.Helper()
	db := floorOpen(t)
	read := exec.ArmJoinFloorProbe()
	res, err := db.Query(context.Background(), floorSQL)
	snap := read()
	if err != nil {
		return snap, 0, err
	}
	return snap, len(res.Rows), nil
}

func floorOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{
		Store:        objstore.NewMemStore(),
		Bucket:       "test",
		MemoryBudget: floorBudget,
		SpillDir:     t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := typematrix.Schema()
	if err := db.CreateTable(ctx, typematrix.Table, schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester(typematrix.Table, schema, nil, ingest.Config{
		MaxBufferRows: typematrix.Rows + 1, RowGroupSize: typematrix.RowGroup,
	})
	if err := ing.Ingest(ctx, typematrix.Data(typematrix.Rows)); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
