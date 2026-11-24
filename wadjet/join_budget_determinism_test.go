// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// #789's last half is narrowed, not closed (#1706, 2026-11-21; ADR-0006 records
// the window left); this file holds the proof for the shape it pins.
//
// The defect was: at a fixed budget, on fixed data, the same join reached
// different outcomes on different runs. Two charges made the floor move. The
// first — the scan's WHOLE FILE, held from the first row group's decode to the
// last — went in 2026-09-03: a scan reads one row group at a time, charged and
// released per row group.
//
// The second was `trackScanBatch` ForceReserving a decoded batch AFTER the
// decode (ADR-0006 producer 3). A charge taken after the allocation bounds
// nothing, so a shape whose row groups were big enough carried itself past its
// budget on decoded read-ahead alone. This is that shape: 4,000 rows of
// 512-byte strings in row groups of 512, joined to itself at 1 MiB. Until
// #1706 it refused on EVERY run, holding 1.04x-1.73x its budget.
//
// Two admissions BEFORE the decode were measured and refused in 2026-09
// (ADR-0006): a bounded wait then a force answered 1 of 20 runs in 156 s
// against 0.25 s; a wait ended by a condition — wait while this source holds
// decoded bytes its consumer has not taken — answered 17 to 19 of 20 in ~90 s.
// Both waited for the budget.
//
// What ships waits for nothing: a worker may decode a row group ahead of its
// consumer only when the room for the decode's own working set — read from the
// footer — exists on the ledger at that moment; when it does
// not, the row group is handed to the consumer undecoded and decoded on the
// consumer's goroutine when asked for, and the scan stays on demand from then
// on (planner/physical/scan_decode_admission.go). The read-ahead nobody granted
// room for no longer exists, so this shape answers — on every run, in both
// forced orders, with the unbudgeted engine's answer.
//
// The contrast arm keeps the old measurement live: with the rule's kill switch
// off, the same shape still refuses when the read-ahead's charges land before
// the build's reservations (DecodeFirst); with the scheduler free it may answer.

const readAheadPinRuns = 20

func TestAScansDecodedReadAheadNoLongerOverdrawsTheBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("20 runs")
	}
	const rows, padLen, rowGroup = 4000, 512, 512
	const budget int64 = 1 << 20
	ctx := context.Background()
	db := wideJoinDB(t, rows, padLen, rowGroup, budget)
	ref := wideJoinDB(t, rows, padLen, rowGroup, 0)
	withReadAheadAdmission(t, true)

	want, err := rowsText(t, ref, wideJoinSQL)
	if err != nil {
		t.Fatalf("unbudgeted: %v", err)
	}
	check := func(arm string, runs int) {
		for run := 0; run < runs; run++ {
			got, err := rowsText(t, db, wideJoinSQL)
			if err != nil {
				t.Errorf("%s, run %d: refused at %d bytes: %v", arm, run, budget, err)
				continue
			}
			if got != want {
				t.Errorf("%s, run %d: answered %s, the unbudgeted engine answers %s", arm, run, got, want)
			}
		}
	}
	check("scheduler free", readAheadPinRuns)
	for _, arm := range []struct {
		name  string
		order int32
	}{
		{"decode first", physical.ReadAheadOrderDecodeFirst},
		{"consumer first", physical.ReadAheadOrderConsumerFirst},
	} {
		restore := physical.ForceScanReadAheadOrder(arm.order)
		check(arm.name, 3)
		restore()
	}

	// Contrast: the rule off is the base, and the base's read-ahead overdraws
	// this shape's budget. It is asserted in the ORDER that makes the overdraw
	// certain — DecodeFirst lands every read-ahead's decoded charge before each
	// build reservation — so the refusal is the rule's absence and not the
	// scheduler's luck. (Until #1706 r2 this arm ran with the scheduler free and
	// required a refusal; that is not deterministic — at GOMAXPROCS=1 the base
	// itself answered in 2 of 25 processes at GOGC=75, 1706i_r1.md §6 — so it
	// asserted something the base does not guarantee.)
	withReadAheadAdmission(t, false)
	refusal := func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "memory budget exceeded") &&
			strings.Contains(err.Error(), "scan decoded batch")
	}
	//
	// How far the read-ahead CAN run is the scan's workers plus its queue, and
	// with one worker (WADJET_SCAN_WORKERS=1, or a host whose CPU count is 1 —
	// `taskset -c 0`) it fits this budget and the base answers even in this
	// order; from two workers up it does not (measured at 1, 2, 3, 4, 8). The
	// worker count is the precondition of the overdraw, not the scheduler's
	// call, so it is set here rather than inherited from the host.
	t.Setenv("WADJET_SCAN_WORKERS", "4")
	restore := physical.ForceScanReadAheadOrder(physical.ReadAheadOrderDecodeFirst)
	for run := 0; run < 3; run++ {
		_, err = tmRun(ctx, db, wideJoinSQL)
		if err == nil {
			t.Fatalf("rule off, decode first, run %d: this shape answered. With every read-ahead charge landed "+
				"before each build reservation the base refuses it, so either something else now bounds the "+
				"read-ahead or the switch no longer reaches it", run)
		}
		if !refusal(err) {
			t.Fatalf("rule off, decode first, run %d: refused for another reason than the read-ahead's "+
				"decoded charge: %v", run, err)
		}
	}
	restore()
	// With the scheduler free and the rule off, whether the read-ahead gets
	// ahead of the build is the scheduler's call, so this run cannot prove a
	// refusal. What it can prove: it gives the unbudgeted engine's answer or
	// that one named refusal, never a third outcome.
	got, err := rowsText(t, db, wideJoinSQL)
	switch {
	case err == nil && got != want:
		t.Fatalf("rule off, scheduler free: answered %s, the unbudgeted engine answers %s", got, want)
	case err != nil && !refusal(err):
		t.Fatalf("rule off, scheduler free: refused for another reason than the read-ahead's decoded charge: %v", err)
	}
}
