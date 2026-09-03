package wadjet

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// #789's headline is CLOSED; this file pins what is left of it.
//
// The defect was: at a fixed budget, on fixed data, the same join reached
// different outcomes on different runs. Two charges made the floor move. The
// first — the scan's WHOLE FILE, held from the first row group's decode to the
// last — is gone: a scan reads one row group at a time now, charged and
// released per row group, and the eighteen type-matrix join cells that needed
// a doubled budget answer 20 of 20 runs at 512 KiB on both the free and the
// GOMAXPROCS=1 arms.
//
// The second is still here, and it is what this pins: `trackScanBatch`
// ForceReserves a decoded batch AFTER the decode, releasing it when the
// consumer takes it (ADR-0006 producer 3). A charge taken after the allocation
// bounds nothing, so a shape whose row groups are big enough carries itself
// past its budget on decoded read-ahead alone. This is that shape: 4,000 rows
// of 512-byte strings in row groups of 512, joined to itself at 1 MiB. It
// refuses on EVERY run, holding 1.04x-1.73x its budget when it does — not the
// join's state (the build has evicted every partition and indexed a few
// hundred rows), but read-ahead nobody granted room for.
//
// The fix for it is admission BEFORE the decode, against the projected
// columns' uncompressed bytes from the footer — the class ADR-0015 records for
// the worker's decode window. It was implemented in this arc and MEASURED, and
// it is refused on the measurement:
//
//   - with a bounded wait then a force (memory.ReserveOrForce's shape), this
//     shape answered 1 of 20 runs and the 20 runs took 156 s against 0.25 s;
//   - with the wait ended by a condition instead of a clock — wait while this
//     source holds decoded bytes its consumer has not taken, force when it
//     holds nothing — it answered 17 to 19 of 20 and took ~90 s.
//
// Both turn a deterministic refusal into a nondeterministic answer, which is
// the defect class, at 350x the wall. The reason is structural and is not the
// admission's: under a budget this tight the scan must serialize, and what it
// then waits on includes the join's index, which a grace eviction cannot free
// (#823's deferred half). Admission cannot bound what it cannot cause to be
// released. So the decode-ahead half is DEFERRED with its mechanism rather
// than bounded by something that measures worse.
//
// The DISPOSITION is what is asserted, and the byte magnitude is only logged.
// How far past the budget the tracker is when the build asks varies run to run
// (measured 915,984 to 1,812,204 against a 1 MiB budget) — that variance IS
// the defect, so a threshold on it would be pinning a coin toss, which is
// exactly what ADR-0027 decision 6 forbids.
//
// The ratchet runs in the direction an open defect needs it: a run that
// ANSWERS fails this test. It means the decoded read-ahead is bounded — delete
// the pin, that is the fix's proof.

const readAheadPinRuns = 20

// usedInRefusal reads the `used=` the tracker reports in a budget refusal.
var usedInRefusal = regexp.MustCompile(`used=(\d+)`)

func TestAScansDecodedReadAheadStillOverdrawsTheBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("20 runs")
	}
	const rows, padLen, rowGroup = 4000, 512, 512
	const budget int64 = 1 << 20
	ctx := context.Background()
	db := wideJoinDB(t, rows, padLen, rowGroup, budget)

	answered := 0
	var useds []int64
	for run := 0; run < readAheadPinRuns; run++ {
		_, err := tmRun(ctx, db, wideJoinSQL)
		if err == nil {
			answered++
			continue
		}
		if !strings.Contains(err.Error(), "memory budget exceeded") {
			t.Fatalf("run %d: %v", run, err)
		}
		m := usedInRefusal.FindStringSubmatch(err.Error())
		if m == nil {
			t.Fatalf("run %d: refusal carries no used=: %v", run, err)
		}
		v, cerr := strconv.ParseInt(m[1], 10, 64)
		if cerr != nil {
			t.Fatal(cerr)
		}
		useds = append(useds, v)
	}

	if answered > 0 {
		t.Fatalf("this shape ANSWERED on %d of %d runs. It is pinned as #789's remaining half — a "+
			"decoded batch charged AFTER its decode, so the scan's read-ahead carries the query "+
			"past its budget — so an answer means the read-ahead is bounded and that half is "+
			"closed. Delete this pin; that is the fix's proof. An answer on SOME runs is not: "+
			"read the measurements above before replacing a deterministic refusal with a "+
			"nondeterministic answer",
			answered, readAheadPinRuns)
	}
	lo, hi, over := useds[0], useds[0], 0
	for _, v := range useds {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
		if v > budget {
			over++
		}
	}
	t.Logf("#789's decode-ahead half reproduces on all %d runs: refused holding %d..%d bytes against a %d "+
		"budget (%.2fx..%.2fx), %d of them from OVER it", readAheadPinRuns, lo, hi, budget,
		float64(lo)/float64(budget), float64(hi)/float64(budget), over)
}
