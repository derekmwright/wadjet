// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The consumer's admission at the load gate (loadGate.acquireDemand) waits
// only while the room it needs comes back without any consumer: a holder
// counts as one the workers will release when every row group it still owes
// is an admitted read-ahead a worker is decoding (fileSlot.releasesWithoutConsumer).
// That verdict is read from per-slot counters which change outside the gate's
// lock. If, at any instant, a holder whose remaining row groups include one
// only the consumer decodes reads as worker-released, a consumer can start
// waiting for its bytes — and nothing wakes it, because the release needs that
// consumer (#1706 r2: the external challenger's interleaving, 1706i_codex_challenge.md §1).
//
// These gates pause a worker at the edge of such an instant with a test-only
// hook (readAheadReturnHook), drive the consumer into the gate inside it, and
// require the scan to finish with every row exactly once.

// gateFilesFixture is one table of the named files, each two row groups of 400
// rows of two INT64 columns; file f holds ids f*800 .. f*800+799. Footers are
// primed so each file loads row group at a time.
func gateFilesFixture(t *testing.T, names ...string) (*catalog.Catalog, []parquet.Column, map[string]catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}}
	rows := func(base int) []map[string]any {
		out := make([]map[string]any, 400)
		for i := range out {
			out[i] = map[string]any{"id": int64(base + i), "v": int64(7 * (base + i))}
		}
		return out
	}
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	files := map[string]catalog.FileEntry{}
	var entries []catalog.FileEntry
	for f, name := range names {
		data := writeTestParquetMultiRG(t, schema, rows(f*800), rows(f*800+400))
		path := "tables/items/" + name + ".parquet"
		if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		e := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: 800}
		entries = append(entries, e)
		files[name] = e
		primeFooterCache(t, cat, e)
	}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
	return cat, schema.Columns, files
}

// pauseReadAheadAt arms readAheadReturnHook for one unit: when that unit's
// read-ahead returns, the worker signals paused and waits for resume. The
// returned restore disarms it.
func pauseReadAheadAt(unit int) (paused <-chan struct{}, resume chan<- struct{}, restore func()) {
	p, r := make(chan struct{}), make(chan struct{})
	h := func(idx int) {
		if idx != unit {
			return
		}
		close(p)
		<-r
	}
	readAheadReturnHook.Store(&h)
	return p, r, func() { readAheadReturnHook.Store(nil) }
}

// slotOf is the slot of the scan's unit idx.
func slotOf(inner *scanSourceInner, idx int) *fileSlot { return inner.rgUnits[idx].slot }

// awaitConsumerAtGate waits until the consumer has either parked in the load
// gate's demand admission or been admitted for slot s (s holds the gate). It
// tells which, so the gate can name the interleaving it reached.
func awaitConsumerAtGate(t *testing.T, inner *scanSourceInner, s *fileSlot) (parked bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		inner.loadGate.mu.Lock()
		_, admitted := inner.loadGate.held[s]
		inner.loadGate.mu.Unlock()
		if admitted {
			return false
		}
		for _, g := range strings.Split(allStacks(), "\n\n") {
			if strings.Contains(g, "(*loadGate).acquireDemand") && strings.Contains(g, "(*scanSourceInner).next") &&
				!strings.Contains(g, "[runnable]") {
				return true
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the consumer neither waited in the gate nor was admitted")
	return false
}

func wantIDsUpTo(t *testing.T, ids []int64, n int) {
	t.Helper()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != n {
		t.Fatalf("got %d rows, want %d", len(ids), n)
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("row %d has id %d — a row group was lost or read twice", i, id)
		}
	}
}

// forEachLoadPathN is forEachLoadPath for a gate that loads `files` files,
// each exactly once.
func forEachLoadPathN(t *testing.T, files int64, gate func(t *testing.T)) {
	for _, rowGroup := range []bool{true, false} {
		name := "row-group"
		if !rowGroup {
			name = "whole-file"
		}
		t.Run(name, func(t *testing.T) {
			prev := scanRowGroupBuffers.Set(rowGroup)
			defer scanRowGroupBuffers.Set(prev)
			rg0, wf0 := RowGroupLoadStats()
			gate(t)
			rg1, wf1 := RowGroupLoadStats()
			want := [2]int64{files, 0}
			if !rowGroup {
				want = [2]int64{0, files}
			}
			if got := [2]int64{rg1 - rg0, wf1 - wf0}; got != want {
				t.Fatalf("loads (row-group, whole-file) = %v, want %v", got, want)
			}
		})
	}
}

// TestALoadGateHolderReadsWorkerReleasedOnlyWhileItIs is the window between a
// read-ahead's release of its row group and the end of its step. Units X0 X1
// Y0 Y1. X0's read-ahead is admitted, loads X (X holds the gate) and decodes;
// the worker is paused right after its row group's release, when X still owes
// X1 — a placeholder only the consumer decodes. Then the budget fills, X1, Y0
// and Y1 are handed over, and the consumer meets Y0 first: Y's load needs the
// room X holds. X must not read as a holder the workers will release, or the
// consumer waits for X1, which only it decodes, and nothing wakes it.
func TestALoadGateHolderReadsWorkerReleasedOnlyWhileItIs(t *testing.T) {
	forEachLoadPathN(t, 2, holderAfterItsReadAheadReleased)
}

func holderAfterItsReadAheadReleased(t *testing.T) {
	cat, cols, files := gateFilesFixture(t, "x", "y")
	inner, tracker := gateDemandScan(t, cat, cols, files, []string{"x0", "x1", "y0", "y1"}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	x, y := slotOf(inner, 0), slotOf(inner, 2)

	paused, resume, restore := pauseReadAheadAt(0)
	defer restore()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		workerStep(ctx, inner, 0) // X0: admitted, decoded, then paused
	}()
	<-paused
	if got := x.rgRemaining.Load(); got != 1 {
		t.Fatalf("X owes %d row groups at the pause, want 1 (X1)", got)
	}
	room := fillBudget(t, tracker)
	var markers [4]*batch.RecordBatch
	for idx := 1; idx < 4; idx++ {
		if _, deferred := inner.decodeOrDefer(ctx, idx); !deferred {
			t.Fatalf("unit %d was decoded ahead with the budget full", idx)
		}
		markers[idx] = &batch.RecordBatch{}
		inner.deferred.Store(markers[idx], idx)
	}
	got := consumeAll(ctx, inner)
	inner.send(ctx, markers[2]) // Y0: needs the room X holds
	parked := awaitConsumerAtGate(t, inner, y)
	close(resume)
	inner.send(ctx, markers[1]) // X1
	inner.send(ctx, markers[3]) // Y1
	go func() {
		<-workerDone
		close(inner.batchCh)
	}()

	ids := awaitResult(t, cancel, got, 10*time.Second)
	tracker.Release(room)
	wantIDsUpTo(t, ids, 1600)
	assertScanHoldsNothing(t, inner, tracker)
	if parked {
		// It finished only because something else woke it: the holder owed
		// it a row group, so it should have been admitted, not parked.
		t.Fatal("the consumer waited in the gate for a holder that owed it a row group")
	}
}

// TestALoadGateHolderReadsWorkerReleasedOnlyWhileItIsAfterAYield is the same
// window opened by a read-ahead that YIELDED at the gate. Units Y0 Y1 X0 X1 Z0
// Z1. Y0 is admitted and loads Y (the gate is Y's). X0's read-ahead is
// admitted and parks in the gate holding X's mutex. The budget fills; Y1, X1,
// Z0, Z1 are handed over. The consumer comes for X (X1): the read-ahead
// yields X0 to it (Y's room is owed to the consumer) and is paused right
// after, before its step ends. The consumer loads X past the gate and decodes
// X1, so X owes only X0 — the yielded row group, which only the consumer
// decodes. Y1 releases Y. Then Z0: Z's load needs the room X holds, and X must
// not read as a holder the workers will release.
func TestALoadGateHolderReadsWorkerReleasedOnlyWhileItIsAfterAYield(t *testing.T) {
	forEachLoadPathN(t, 3, holderAfterItsReadAheadYielded)
}

func holderAfterItsReadAheadYielded(t *testing.T) {
	cat, cols, files := gateFilesFixture(t, "x", "y", "z")
	// File order in the fixture fixes the ids: x 0..799, y 800..1599, z 1600..2399.
	inner, tracker := gateDemandScan(t, cat, cols, files, []string{"y0", "y1", "x0", "x1", "z0", "z1"}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	x, z := slotOf(inner, 2), slotOf(inner, 4)

	b0, deferred := inner.decodeOrDefer(ctx, 0)
	if deferred {
		t.Fatal("Y0's read-ahead was not admitted")
	}
	paused, resume, restore := pauseReadAheadAt(2)
	defer restore()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		workerStep(ctx, inner, 2) // X0: admitted, parks in the gate behind Y
	}()
	awaitStack(t, time.Now().Add(10*time.Second), "(*loadGate).acquireFor", "(*fileSlot).ensureLoaded")

	room := fillBudget(t, tracker)
	var markers [6]*batch.RecordBatch
	for _, idx := range []int{1, 3, 4, 5} {
		if _, deferred := inner.decodeOrDefer(ctx, idx); !deferred {
			t.Fatalf("unit %d was decoded ahead with the budget full", idx)
		}
		markers[idx] = &batch.RecordBatch{}
		inner.deferred.Store(markers[idx], idx)
	}
	got := consumeAll(ctx, inner)
	inner.send(ctx, inner.finishRG(0, b0))
	inner.send(ctx, markers[3]) // X1: the consumer comes for X; X0's read-ahead yields
	<-paused
	inner.send(ctx, markers[1]) // Y1: releases Y
	inner.send(ctx, markers[4]) // Z0: needs the room X holds
	parked := awaitConsumerAtGate(t, inner, z)
	if x.rgRemaining.Load() != 1 {
		t.Fatalf("X owes %d row groups, want 1 (the yielded X0)", x.rgRemaining.Load())
	}
	close(resume)
	inner.send(ctx, markers[5]) // Z1
	go func() {
		<-workerDone // sends X0's placeholder
		close(inner.batchCh)
	}()

	ids := awaitResult(t, cancel, got, 10*time.Second)
	tracker.Release(room)
	wantIDsUpTo(t, ids, 2400)
	assertScanHoldsNothing(t, inner, tracker)
	if parked {
		// It finished only because something else woke it: the holder owed
		// it a row group, so it should have been admitted, not parked.
		t.Fatal("the consumer waited in the gate for a holder that owed it a row group")
	}
}
