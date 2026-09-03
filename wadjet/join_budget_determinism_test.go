package wadjet

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// #789 IS OPEN. This file pins its residual so a fix cannot land quietly.
//
// The defect: at a fixed budget, on fixed data, the same join reaches
// different outcomes on different runs, because the tracker's floor when the
// build's first Reserve lands carries however many row groups the scan has
// decoded ahead of its consumer. Two bounds were implemented and both are
// refused on measurement (ADR-0006's open-residual section carries the
// numbers): bounding read-ahead by the budget's free HEADROOM self-throttles,
// because `used` contains the scan's own whole-file buffer and only the LAST
// row group's decode releases it; bounding it by a fixed SHARE of the budget
// is a knob — the value decides which shapes are deterministic and it trades
// determinism against decode parallelism, which is not an invariant to encode.
//
// What is pinned here is the residual's loud, reproducible face: a shape whose
// row groups are big enough that the scan's decoded read-ahead alone carries
// the query past its budget, so the join refuses on EVERY run. That is not the
// join's state — the build has evicted every partition and indexed a few
// hundred rows — it is read-ahead nobody granted room for.
//
// The DISPOSITION is what is asserted, and the byte magnitude is only logged.
// How far past the budget the tracker is when the build asks varies run to run
// (measured 915,984 to 1,812,204 against a 1 MiB budget) — that variance IS
// the defect, so a threshold on it would be pinning a coin toss, which is
// exactly what ADR-0027 decision 6 forbids. The condition that reproduces is
// the refusal, and that is what is bounded.
//
// The ratchet runs in the direction an open defect needs it: a run that
// ANSWERS fails this test. It means the read-ahead is bounded and #789 is
// fixed — delete the pin, that is the fix's proof. (The withdrawn read-ahead
// share made this shape answer 5 of 5, so the pin discriminates a fix.)

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
		t.Fatalf("this shape ANSWERED on %d of %d runs. It is pinned as #789's residual — the "+
			"scan's decoded read-ahead carrying the query past its budget — so an answer means "+
			"the read-ahead is bounded and the defect is closed. Delete this pin and drop "+
			"joinBudget from the join_group_by_* cells in spill_type_matrix_test.go, whose "+
			"family ratchet will be saying the same thing; that is the fix's proof",
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
	t.Logf("#789 residual reproduces on all %d runs: refused holding %d..%d bytes against a %d "+
		"budget (%.2fx..%.2fx), %d of them from OVER it", readAheadPinRuns, lo, hi, budget,
		float64(lo)/float64(budget), float64(hi)/float64(budget), over)
}
