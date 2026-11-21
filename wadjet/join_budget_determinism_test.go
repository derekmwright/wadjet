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
// off, the same shape still refuses.

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

	// Contrast: the rule off is the base, and the base refuses this shape.
	withReadAheadAdmission(t, false)
	_, err = tmRun(ctx, db, wideJoinSQL)
	if err == nil {
		t.Fatal("with the read-ahead admission switched off this shape answered: the base refused it on " +
			"every run, so either something else now bounds the read-ahead or the switch no longer reaches it")
	}
	if !strings.Contains(err.Error(), "memory budget exceeded") {
		t.Fatalf("rule off: refused for another reason: %v", err)
	}
}
