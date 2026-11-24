// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
// how far its decode workers ran ahead of the join.
//
// What decides the floor, then, is the ORDER of the scan worker's decode
// against the build's first reservation, and one scheduler thread with one
// scan worker does NOT fix that order: the worker can be descheduled mid-decode
// (a garbage collection is enough), and the build then reserves while the
// worker holds a row-group slab instead of a second decoded batch. At the base
// (8486b7ae) the floor took a second value, 106,083 = 17,888 (one decoded
// batch) + 88,195 (one row-group slab), in 3 of 25 processes at GOGC=75
// (1706i_r1.md §6). Until #1706 r2 this gate required one value with the
// scheduler free; the engine does not guarantee that, so the gate asserted the
// garbage collector's timing.
//
// So the order is FIXED where the gate asserts one number, with the hooks that
// order it (physical.ForceScanReadAheadOrder), and the free run is held to a
// bound derived from the fixture:
//
//   - DecodeFirst (every live scan's worker has run ahead as far as its
//     channel lets it before each build reservation): one floor on every run,
//     and all of it `scan decoded batch` — no file load, no slab, no join
//     index, nothing ordinary. Measured: 35,776, two decoded row groups.
//   - ConsumerFirst (a worker decodes only what its consumer asks for): the
//     floor is ZERO — nothing has run ahead, and the batch the build holds has
//     left the scan's charge.
//   - scheduler free: each term within its own bound — decoded read-ahead no
//     more than the DecodeFirst floor (the most the channel lets the scan
//     hold), forced row-group loads no more than one slab per scan (the
//     largest row group's byte range, read from the fixture's footer), no
//     pooled buffer, no join index, no other forced purpose — and the total no
//     more than the DecodeFirst floor plus one slab per scan. (Used and the
//     census are separate reads, so with the worker running their difference,
//     the ordinary reservations, is not exact and is not asserted.)
//
// And on every run, ordered or free, the query leaves NOTHING on its ledger:
// used and forced are both zero once its rows are back.
//
// WHAT MAKES THIS FAIL. It is a property gate, not the revert proof for one
// hunk: at the FIRST reserve the join has charged nothing, so a ledger that
// drifts per arrival batch has not drifted yet, and reverting
// `reconcileArrivalCharge` leaves this green (that revert proof is
// `exec.TestAGraceBuildsLedgerIsConserved`). What this catches is a residual
// that OUTLIVES A QUERY (the ledger after it; a second ordered value), a
// producer whose charge at this instant follows timing under a fixed order,
// and a scan holding more than its in-flight row groups at that instant — the
// whole file (#789's first producer) breaks both the ordered split and the
// free bound. Whether the query answers, and with what — the property that
// must not vary, whatever the floor — is the verdict gate below, unchanged.
func TestTheJoinsFloorIsOneNumberWhenTheScanCannotRunAhead(t *testing.T) {
	// One scheduler thread and one scan worker, as before: it keeps the free
	// run's bound tight. No test in this package runs in parallel, so this is
	// contained, and it is restored on the way out.
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	t.Setenv("WADJET_SCAN_WORKERS", "1")
	const scans = 2 // the self-join scans the table twice
	slab := floorLargestRowGroupBytes(t)

	run := func(arm string, i int) exec.JoinFloorSnapshot {
		t.Helper()
		snap, rows, err := floorRun(t)
		if err != nil {
			t.Fatalf("%s, run %d: %v", arm, i, err)
		}
		if !snap.Seen {
			t.Fatalf("%s, run %d: no grace build reserved anything — the shape stopped "+
				"reaching the partition-on-arrival path and this gate measured nothing", arm, i)
		}
		t.Logf("%s, run %2d: used=%d (scan file load %d, decoded batches %d, pooled bufs %d, "+
			"join index %d, forced %d) rows=%d", arm, i, snap.Used, snap.ScanFileLoad, snap.ScanDecodedBatch,
			snap.ScanPooledBuffer, snap.JoinIndex, snap.Forced, rows)
		if used, forced := snap.Ledger(); used != 0 || forced != 0 {
			t.Errorf("%s, run %d: the query left used=%d forced=%d on its ledger", arm, i, used, forced)
		}
		if snap.JoinIndex != 0 {
			t.Errorf("%s, run %d: the join had already forced %d bytes of index at its first reserve; it "+
				"has not indexed a row yet", arm, i, snap.JoinIndex)
		}
		if named := snap.ScanFileLoad + snap.ScanDecodedBatch + snap.ScanPooledBuffer; snap.Forced != named {
			t.Errorf("%s, run %d: %d of the %d forced bytes at the first reserve are not the scan's",
				arm, i, snap.Forced-named, snap.Forced)
		}
		return snap
	}
	ordered := func(name string, order int32) map[int64]int {
		defer physical.ForceScanReadAheadOrder(order)()
		seen := map[int64]int{}
		for i := 0; i < floorRuns; i++ {
			snap := run(name, i)
			seen[snap.Used]++
			// Ordered, nothing charges while the probe reads, so the split is
			// exact: everything on the ledger is decoded read-ahead.
			if snap.Used != snap.ScanDecodedBatch {
				t.Errorf("%s, run %d: floor %d is not all decoded read-ahead (decoded %d, file load %d, "+
					"pooled %d, ordinary %d)", name, i, snap.Used, snap.ScanDecodedBatch, snap.ScanFileLoad,
					snap.ScanPooledBuffer, snap.Used-snap.Forced)
			}
		}
		return seen
	}

	decodeFirst := ordered("decode first", physical.ReadAheadOrderDecodeFirst)
	if len(decodeFirst) != 1 {
		t.Fatalf("with every read-ahead landed before the build reserves, the floor took %d values across "+
			"%d runs of one query on one fixture: %v. The order is fixed, so a second value is a residual "+
			"that outlives a query or a producer whose charge here follows timing",
			len(decodeFirst), floorRuns, decodeFirst)
	}
	var full int64
	for v := range decodeFirst {
		full = v
	}
	if full <= 0 {
		t.Fatalf("with every read-ahead landed first the floor is %d: the hook no longer orders the "+
			"read-ahead before the build, so the bound below would prove nothing", full)
	}
	if consumerFirst := ordered("consumer first", physical.ReadAheadOrderConsumerFirst); consumerFirst[0] != floorRuns {
		t.Errorf("with the worker decoding only what the build asks for, the floor took values %v; nothing "+
			"has run ahead, so it is zero on every run", consumerFirst)
	}

	for i := 0; i < floorRuns; i++ {
		snap := run("scheduler free", i)
		if snap.ScanDecodedBatch > full {
			t.Errorf("scheduler free, run %d: %d bytes of decoded read-ahead, more than the %d the scan "+
				"holds when it has run as far ahead as it can", i, snap.ScanDecodedBatch, full)
		}
		if snap.ScanFileLoad > scans*slab {
			t.Errorf("scheduler free, run %d: %d bytes of forced file load, more than one row group "+
				"(%d bytes) in flight per scan", i, snap.ScanFileLoad, slab)
		}
		if snap.ScanPooledBuffer != 0 {
			t.Errorf("scheduler free, run %d: %d bytes of pooled whole-file buffers on the row-group path",
				i, snap.ScanPooledBuffer)
		}
		if snap.Used > full+scans*slab {
			t.Errorf("scheduler free, run %d: floor %d exceeds the scan's full read-ahead (%d) plus one row "+
				"group in flight per scan (%d x %d bytes)", i, snap.Used, full, scans, slab)
		}
	}
}

// floorLargestRowGroupBytes is the largest row group's byte range in the floor
// fixture's file: what one row-group load holds and charges.
func floorLargestRowGroupBytes(t *testing.T) int64 {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	floorOpenOn(t, store)
	objs, err := store.List(ctx, "test", objstore.ListOptions{Prefix: "tables/" + typematrix.Table + "/"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var largest int64
	files := 0
	for _, o := range objs {
		if !strings.HasSuffix(o.Key, ".parquet") {
			continue
		}
		rc, _, err := store.Get(ctx, "test", o.Key)
		if err != nil {
			t.Fatalf("get %s: %v", o.Key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", o.Key, err)
		}
		fr, err := parquet.OpenFileReaderFromBytes(data)
		if err != nil {
			t.Fatalf("open %s: %v", o.Key, err)
		}
		for rg := 0; rg < fr.NumRowGroups(); rg++ {
			start, end, ok, err := fr.RowGroupByteRange(rg)
			if err != nil || !ok {
				t.Fatalf("%s row group %d: no byte range (%v)", o.Key, rg, err)
			}
			largest = max(largest, end-start)
		}
		files++
	}
	if files == 0 || largest <= 0 {
		t.Fatalf("the floor fixture has %d files and a largest row group of %d bytes", files, largest)
	}
	t.Logf("largest row group in the floor fixture: %d bytes over %d file(s)", largest, files)
	return largest
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
	return floorOpenOn(t, objstore.NewMemStore())
}

func floorOpenOn(t *testing.T, store objstore.Store) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{
		Store:        store,
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
