// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"context"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The load gate bounds a scan's concurrent file loads, and a slot's admitted
// bytes come back only when that file's LAST row group has been decoded
// (releaseRG). Once a budgeted scan is on demand (scan_decode_admission.go)
// its remaining row groups are decoded by the CONSUMER, so the bytes of a file
// whose last row group is still a deferred placeholder can only be released by
// the consumer. A consumer that waits in the gate for them waits for itself:
// TPC-H Q09, Q16 and Q18 at SF1 under a 16 MiB budget never returned in 5 of
// 10 runs (#1706, 1706h_r1.md §5). These gates build that cycle from its
// ingredients, deterministically and without a sleep, on the real decode path:
// the read-ahead admission, the hand-over placeholders, next(), readRG,
// ensureLoaded, the gate and releaseRG.
//
// The invariant they hold: no goroutine waits for an admission whose release
// requires that same goroutine to make progress.

// gateDemandFixture is one table of two files, X and Y, each holding two row
// groups of two INT64 columns (fixed width, so the footer bounds the decode and
// the read-ahead admission can admit one). The footers are primed the way
// buildRGUnits primes them, so both files load row group at a time.
func gateDemandFixture(t *testing.T) (*catalog.Catalog, []parquet.Column, catalog.FileEntry, catalog.FileEntry) {
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
	var entries []catalog.FileEntry
	for f, name := range []string{"x", "y"} {
		data := writeTestParquetMultiRG(t, schema, rows(f*800), rows(f*800+400))
		path := "tables/items/" + name + ".parquet"
		if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put %s: %v", path, err)
		}
		e := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: 800}
		entries = append(entries, e)
		primeFooterCache(t, cat, e)
	}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", entries); err != nil {
		t.Fatalf("add files: %v", err)
	}
	return cat, schema.Columns, entries[0], entries[1]
}

// peakOf is what the gate admits for a slot loading row group at a time.
func peakOf(t *testing.T, cat *catalog.Catalog, e catalog.FileEntry) int64 {
	t.Helper()
	rdr := parquet.LookupFooter(footerCacheIdentity(cat, e, e.SizeBytes))
	if rdr == nil {
		t.Fatal("footer not cached")
	}
	rdr.SetRowGroupBytes(nil, e.SizeBytes)
	starts, ends, ok := rowGroupRanges(rdr, []int{0, 1})
	if !ok {
		t.Fatal("no row-group ranges")
	}
	return peakRowGroupBytes(starts, ends)
}

// gateDemandScan builds a scan source over the fixture with its units in the
// order given (each a file and a row group), a budgeted tracker, and a load
// gate that holds either file's row group alone but not both — the situation
// at SF1, where the gate is a quarter of the budget.
func gateDemandScan(t *testing.T, cat *catalog.Catalog, cols []parquet.Column, files map[string]catalog.FileEntry,
	order []string, chCap int) (*scanSourceInner, *memory.Tracker) {
	t.Helper()
	tracker := memory.NewTracker("gate-demand", 1<<20)
	inner := &scanSourceInner{
		cat:        cat,
		tableName:  "items",
		schema:     cols,
		memTracker: tracker,
		batchCh:    make(chan *batch.RecordBatch, chCap),
		errCh:      make(chan error, 1),
	}
	slots := map[string]*fileSlot{}
	for name, e := range files {
		s := &fileSlot{entry: e, wantRG: []int{0, 1}}
		s.rgRemaining.Store(2)
		slots[name] = s
	}
	for _, u := range order {
		inner.rgUnits = append(inner.rgUnits, rgUnit{slot: slots[u[:1]], rgIndex: int(u[1] - '0'), numRows: 400})
	}
	// What each file's load admits: its largest row group, or the file on the
	// whole-file read.
	gate := int64(0)
	for _, e := range files {
		n := e.SizeBytes
		if scanRowGroupBuffers.On() {
			n = peakOf(t, cat, e)
		}
		gate = max(gate, n)
	}
	inner.loadGate = newLoadGate(gate, loadGateMaxLanes)
	return inner, tracker
}

// workerStep is one row group of rgWorker's loop: decode it when its
// read-ahead is admitted, or hand it over undecoded.
func workerStep(ctx context.Context, inner *scanSourceInner, idx int) {
	b, deferred := inner.decodeOrDefer(ctx, idx)
	if deferred {
		marker := &batch.RecordBatch{}
		inner.deferred.Store(marker, idx)
		inner.send(ctx, marker)
		return
	}
	if b = inner.finishRG(idx, b); b != nil {
		inner.send(ctx, b)
	}
}

// consumeAll drains the scan on its own goroutine and reports the ids it saw.
func consumeAll(ctx context.Context, inner *scanSourceInner) <-chan []int64 {
	out := make(chan []int64, 1)
	go func() {
		var ids []int64
		for {
			b, err := inner.next(ctx)
			if err != nil || b == nil {
				out <- ids
				return
			}
			for i := 0; i < b.ActiveLen(); i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				ids = append(ids, b.Columns[0].Int64Data[row])
			}
		}
	}()
	return out
}

// allStacks is every goroutine's stack.
func allStacks() string {
	buf := make([]byte, 1<<22)
	return string(buf[:runtime.Stack(buf, true)])
}

// awaitStack polls until some goroutine's stack contains every fragment.
func awaitStack(t *testing.T, deadline time.Time, fragments ...string) {
	t.Helper()
	for time.Now().Before(deadline) {
		for _, g := range strings.Split(allStacks(), "\n\n") {
			all := true
			for _, f := range fragments {
				all = all && strings.Contains(g, f)
			}
			if all {
				return
			}
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine reached %v", fragments)
}

// awaitResult waits for the consumer; past the deadline it reads the goroutine
// dump, names the waits it finds, cancels the scan and fails.
func awaitResult(t *testing.T, cancel context.CancelFunc, got <-chan []int64, d time.Duration) []int64 {
	t.Helper()
	select {
	case ids := <-got:
		return ids
	case <-time.After(d):
	}
	dump := allStacks()
	var waits []string
	for _, g := range strings.Split(dump, "\n\n") {
		switch {
		case strings.Contains(g, "(*loadGate).acquire") && strings.Contains(g, "(*scanSourceInner).next"):
			waits = append(waits, "the CONSUMER is parked in loadGate.acquire, decoding a row group handed over undecoded")
		case strings.Contains(g, "(*loadGate).acquire"):
			waits = append(waits, "a WORKER's admitted read-ahead is parked in loadGate.acquire")
		case strings.Contains(g, "sync.(*Mutex).Lock") && strings.Contains(g, "(*fileSlot).ensureLoaded") &&
			strings.Contains(g, "(*scanSourceInner).next"):
			waits = append(waits, "the CONSUMER is parked on a slot's mutex held by a worker waiting in the gate")
		case strings.Contains(g, "(*scanSourceInner).send"):
			waits = append(waits, "a WORKER is parked in scanSourceInner.send on the full batch channel")
		}
	}
	sort.Strings(waits)
	cancel()
	<-got
	t.Fatalf("the scan never finished within %v — a goroutine waits for an admission only it can release. "+
		"Waits in the goroutine dump:\n  %s", d, strings.Join(waits, "\n  "))
	return nil
}

func wantIDs(t *testing.T, ids []int64) {
	t.Helper()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 1600 {
		t.Fatalf("got %d rows, want 1600", len(ids))
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("row %d has id %d — a row group was lost or read twice", i, id)
		}
	}
}

// fillBudget takes the tracker's remaining room the way a consumer's
// reservation does, so the next read-ahead admission finds no room for its
// decode and the scan goes on demand.
func fillBudget(t *testing.T, tracker *memory.Tracker) int64 {
	t.Helper()
	room := tracker.Budget() - tracker.Used()
	if err := tracker.Reserve(room); err != nil {
		t.Fatalf("filling the budget: %v", err)
	}
	return room
}

// TestAConsumerNeverWaitsInTheLoadGateForBytesOnlyItCanRelease is the hang's
// cycle. Units in claim order X0 X1 Y0 Y1. X0's read-ahead is admitted: it
// loads X, which takes the gate, and decodes. Then the budget fills (the
// consumer's reservation) and X1, Y0 and Y1 are handed over undecoded; the
// worker that took Y0 sends before the one that took X1, so the consumer meets
// Y0 first. Loading Y needs the gate, which X holds until X1 — behind Y0 in the
// channel — has been decoded by that same consumer. The placeholders are sent
// by a goroutine that parks on the one-slot channel once the consumer stops:
// the second wait of every 1706h dump.
func TestAConsumerNeverWaitsInTheLoadGateForBytesOnlyItCanRelease(t *testing.T) {
	forEachLoadPath(t, consumerBehindItsOwnPlaceholder)
}

// forEachLoadPath runs a gate on the row-group load and on the whole-file read
// (WADJET_SCAN_RG_BUFFERS=0, or a file whose footer is not cached), which
// admit different byte counts on the same gate.
func forEachLoadPath(t *testing.T, gate func(t *testing.T)) {
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
			// Each file is loaded exactly once, on the path named: a read-ahead
			// that yielded at the gate loaded nothing and is not counted.
			want := [2]int64{2, 0}
			if !rowGroup {
				want = [2]int64{0, 2}
			}
			if got := [2]int64{rg1 - rg0, wf1 - wf0}; got != want {
				t.Fatalf("loads (row-group, whole-file) = %v, want %v", got, want)
			}
		})
	}
}

func consumerBehindItsOwnPlaceholder(t *testing.T) {
	cat, cols, x, y := gateDemandFixture(t)
	inner, tracker := gateDemandScan(t, cat, cols, map[string]catalog.FileEntry{"x": x, "y": y},
		[]string{"x0", "x1", "y0", "y1"}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// X0: admitted ahead of the consumer.
	b0, deferred := inner.decodeOrDefer(ctx, 0)
	if deferred {
		t.Fatal("X0's read-ahead was not admitted — the fixture does not reach a loaded slot")
	}
	if inner.onDemand.Load() {
		t.Fatal("on demand before the budget filled")
	}
	room := fillBudget(t, tracker)
	// X1, Y0, Y1: no room, handed over.
	var markers [4]*batch.RecordBatch
	for idx := 1; idx < 4; idx++ {
		if _, deferred := inner.decodeOrDefer(ctx, idx); !deferred {
			t.Fatalf("unit %d was decoded ahead with the budget full", idx)
		}
		markers[idx] = &batch.RecordBatch{}
		inner.deferred.Store(markers[idx], idx)
	}
	if !inner.onDemand.Load() {
		t.Fatal("the scan is not on demand")
	}
	got := consumeAll(ctx, inner)
	inner.send(ctx, inner.finishRG(0, b0))
	go func() {
		inner.send(ctx, markers[2]) // Y0, before X1
		inner.send(ctx, markers[1]) // X1
		inner.send(ctx, markers[3]) // Y1
		close(inner.batchCh)
	}()

	ids := awaitResult(t, cancel, got, 10*time.Second)
	tracker.Release(room)
	wantIDs(t, ids)
	assertScanHoldsNothing(t, inner, tracker)
}

// TestAConsumerNeverWaitsOnASlotWhoseLoaderWaitsInTheLoadGate is the variant
// two of the 1706h dumps add: a worker's read-ahead, admitted before the scan
// went on demand, is itself parked in the gate — holding its slot's mutex while
// it waits. Units Y0 Y1 X0 X1. Y0 is admitted and loads Y (the gate is Y's).
// X0 is admitted too and parks in the gate, holding X. Then the budget fills,
// Y1 and X1 are handed over, X1 ahead of Y1. The consumer decodes X1, which
// needs X's mutex; X's loader waits for Y's bytes; Y's bytes wait for Y1,
// behind X1, for the consumer.
func TestAConsumerNeverWaitsOnASlotWhoseLoaderWaitsInTheLoadGate(t *testing.T) {
	forEachLoadPath(t, consumerBehindAParkedLoader)
}

func consumerBehindAParkedLoader(t *testing.T) {
	cat, cols, x, y := gateDemandFixture(t)
	inner, tracker := gateDemandScan(t, cat, cols, map[string]catalog.FileEntry{"x": x, "y": y},
		[]string{"y0", "y1", "x0", "x1"}, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b0, deferred := inner.decodeOrDefer(ctx, 0)
	if deferred {
		t.Fatal("Y0's read-ahead was not admitted")
	}
	// X0's worker: admitted, then parked in the gate behind Y.
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		workerStep(ctx, inner, 2)
	}()
	awaitStack(t, time.Now().Add(10*time.Second), "(*loadGate).acquire", "(*fileSlot).ensureLoaded")

	room := fillBudget(t, tracker)
	var markers [4]*batch.RecordBatch
	// Y1 first: its admission finds no room and turns the scan on demand, so
	// X1's is refused without reading X's slot, whose mutex X0's loader holds
	// (at the base, an admission that reads it parks behind that loader too).
	for _, idx := range []int{1, 3} {
		if _, deferred := inner.decodeOrDefer(ctx, idx); !deferred {
			t.Fatalf("unit %d was decoded ahead with the budget full", idx)
		}
		markers[idx] = &batch.RecordBatch{}
		inner.deferred.Store(markers[idx], idx)
	}
	got := consumeAll(ctx, inner)
	inner.send(ctx, inner.finishRG(0, b0))
	inner.send(ctx, markers[3]) // X1
	inner.send(ctx, markers[1]) // Y1
	go func() {
		<-workerDone
		close(inner.batchCh)
	}()

	ids := awaitResult(t, cancel, got, 10*time.Second)
	tracker.Release(room)
	wantIDs(t, ids)
	assertScanHoldsNothing(t, inner, tracker)
}

// assertScanHoldsNothing: every gate byte and every tracker charge the scan
// took came back.
func assertScanHoldsNothing(t *testing.T, inner *scanSourceInner, tracker *memory.Tracker) {
	t.Helper()
	inner.loadGate.mu.Lock()
	inflight, lanes := inner.loadGate.inflight, inner.loadGate.lanes
	inner.loadGate.mu.Unlock()
	if inflight != 0 || lanes != 0 {
		t.Fatalf("the gate still holds %d bytes in %d lanes after the scan", inflight, lanes)
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("the tracker still holds %d bytes after the scan", used)
	}
	if f, _, _ := tracker.ForcedBytes(); f != 0 {
		t.Fatalf("the forced census still holds %d bytes after the scan", f)
	}
}
