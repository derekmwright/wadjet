// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// resetReadBufPoolsForTest empties every whole-file read buffer bucket, so a
// test starts from a pool it seeded itself rather than whatever the test before
// it left there.
func resetReadBufPoolsForTest() {
	readBufPoolMu.Lock()
	defer readBufPoolMu.Unlock()
	readBufPools = map[int]*sync.Pool{}
}

// seedReadBufPool puts n buffers of the given capacity into the pool. More
// than one, because sync.Pool keeps a single private slot per P that another P
// cannot take from; the rest go to the shared list every P can steal from.
func seedReadBufPool(capacity, n int) {
	for range n {
		putReadBuf(make([]byte, capacity))
	}
}

// holdGC turns the collector off for the test: sync.Pool empties itself over
// two GC cycles, which is what made #1706's flake depend on timing. With it
// off, a buffer left in the pool stays there, so these tests see the pool
// they seeded every time.
func holdGC(t *testing.T) {
	prev := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(prev) })
}

// TestReadBufPoolBoundsWhatItHandsOut: a request for n bytes gets a buffer of
// capacity in [n, readBufPoolBound*n], whatever the pool holds, and a buffer
// inside that bound is still reused (a bounded pool that never hits would be a
// regression of its own).
func TestReadBufPoolBoundsWhatItHandsOut(t *testing.T) {
	holdGC(t)
	resetReadBufPoolsForTest()
	t.Cleanup(resetReadBufPoolsForTest)

	const foreign = 1 << 20 // a megabyte left by another statement
	seedReadBufPool(foreign, 8)
	for _, n := range []int{297, 40 << 10, 300 << 10, 500 << 10} {
		buf := getReadBuf(n)
		if len(buf) != n {
			t.Fatalf("getReadBuf(%d): len %d", n, len(buf))
		}
		if cap(buf) > readBufPoolBound*n {
			t.Errorf("getReadBuf(%d) handed out a buffer of capacity %d, over the %dx bound (%d)",
				n, cap(buf), readBufPoolBound, readBufPoolBound*n)
		}
		putReadBuf(buf)
	}

	// Inside the bound, the pool still serves: from the request's own class
	// (cap above n, same power of two) and from the class above.
	for _, c := range []struct{ capacity, n int }{
		{700 << 10, 600 << 10}, // class 1 MiB, request's class
		{700 << 10, 400 << 10}, // class 1 MiB, the class above a 512 KiB request
		{64 << 10, 33 << 10},   // the smallest pooled capacity, just inside the bound
	} {
		resetReadBufPoolsForTest()
		seedReadBufPool(c.capacity, 4)
		h0, _ := ReadBufPoolStats()
		buf := getReadBuf(c.n)
		h1, _ := ReadBufPoolStats()
		if h1-h0 != 1 || cap(buf) != c.capacity {
			t.Errorf("getReadBuf(%d) with %d-byte buffers pooled: hits +%d, capacity %d; want a hit on the pooled buffer",
				c.n, c.capacity, h1-h0, cap(buf))
		}
	}
}

// noReaderAtStore hides GetReaderAt: embedding the Store interface exposes
// only its methods, so the scan takes the whole-object fallback for stores
// without ranged reads — the footer pass (buildRGUnits) reads the object into
// a pooled buffer and charges its capacity (trackPooledBuf), and the slot's
// load reconciles its reservation up to the buffer's capacity. It is a real
// MemStore, not a mock. It has no StoreID, so no footer is cached by identity
// and nothing this test reads is shared with another test.
type noReaderAtStore struct{ objstore.Store }

// forcedWarnings counts memory.ReserveOrForce's give-up warning for ONE
// tracker name, passing everything else to the handler it replaced.
type forcedWarnings struct {
	next    slog.Handler
	tracker string
	n       atomic.Int64
}

func (h *forcedWarnings) Enabled(ctx context.Context, l slog.Level) bool { return true }
func (h *forcedWarnings) WithAttrs(a []slog.Attr) slog.Handler           { return h }
func (h *forcedWarnings) WithGroup(string) slog.Handler                  { return h }
func (h *forcedWarnings) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "memory reservation forced past budget" {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "tracker" && a.Value.String() == h.tracker {
				h.n.Add(1)
				return false
			}
			return true
		})
	}
	if h.next.Enabled(ctx, r.Level) {
		return h.next.Handle(ctx, r)
	}
	return nil
}

// TestASmallFileIsNotChargedForAForeignPooledBuffer is #1706's mechanism,
// without the GC timing it depended on: the process-wide pool holds megabyte
// buffers another statement left, and a scan of one small file on a store
// without GetReaderAt, under a 512 KiB budget, must be charged within the
// pool's bound of its own file — not for a foreign buffer — and must not force
// a reservation. Before the bound, getReadBuf handed the 297-byte-class file a
// megabyte buffer, trackPooledBuf charged its capacity, the statement stood
// over budget before reading anything and its file load was forced.
func TestASmallFileIsNotChargedForAForeignPooledBuffer(t *testing.T) {
	holdGC(t)
	defer ForceFileLoadReserveWait(20 * time.Millisecond)()

	for _, c := range []struct {
		name string
		rows int // 2 rows: a few hundred bytes, under any pooled class; 20000: ~160 KiB, probes the pool
	}{{"tiny", 2}, {"mid", 20000}} {
		t.Run(c.name, func(t *testing.T) {
			resetReadBufPoolsForTest()
			t.Cleanup(resetReadBufPoolsForTest)

			ctx := context.Background()
			store := noReaderAtStore{objstore.NewMemStore()}
			schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
			cat := catalog.NewWithStore(store, "test")
			if err := cat.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := cat.CreateTable(ctx, "t", schema, nil); err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewSource(1706))
			rows := make([]map[string]any, c.rows)
			var wantSum int64
			for i := range rows {
				v := rng.Int63n(1 << 40) // incompressible enough to keep the file near 8 bytes a row
				rows[i] = map[string]any{"id": v}
				wantSum += v
			}
			data := writeTestParquetMultiRG(t, schema, rows)
			const path = "tables/t/chunk_0000.parquet"
			if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}
			if err := cat.AddFiles(ctx, "t", map[string]string{}, "tables/t/", []catalog.FileEntry{{
				Path: path, SizeBytes: int64(len(data)), NumRows: int64(c.rows), CreatedAt: time.Now(),
			}}); err != nil {
				t.Fatal(err)
			}
			size := int64(len(data))

			// The foreign buffers: megabytes, as part 1 of the rerun-cost test
			// leaves them, and well over the bound for this file.
			seedReadBufPool(1<<20, 8)

			const budget = 512 << 10
			name := "read-buf-pool-" + c.name
			tracker := memory.NewTracker(name, budget)
			// Not slog.Default().Handler(): with the standard default installed, that
			// handler writes through package log, which SetDefault points back at
			// the new default — a handler wrapping it deadlocks on its first record.
			counter := &forcedWarnings{next: slog.NewTextHandler(os.Stderr, nil), tracker: name}
			prevLogger := slog.Default()
			slog.SetDefault(slog.New(counter))
			t.Cleanup(func() { slog.SetDefault(prevLogger) })

			_, wf0 := RowGroupLoadStats()
			h0, m0 := ReadBufPoolStats()
			src := &catalogScanSource{catalog: cat, tableName: "t", requiredCols: []string{"id"}, memTracker: tracker}
			n, sum, err := scanCountSum(cat, src, nil)
			if err != nil || n != int64(c.rows) || sum != wantSum {
				t.Fatalf("scan: n=%d sum=%d err=%v, want n=%d sum=%d", n, sum, err, c.rows, wantSum)
			}
			// The path engaged: the slot's whole-file load (the reconcile) and
			// at least one pooled read happened. A scan that took another path
			// would prove nothing about the pool.
			_, wf1 := RowGroupLoadStats()
			h1, m1 := ReadBufPoolStats()
			if wf1-wf0 < 1 || (h1-h0)+(m1-m0) < 2 {
				t.Fatalf("the whole-file pooled path did not engage: whole-file loads +%d, pooled reads +%d", wf1-wf0, (h1-h0)+(m1-m0))
			}

			// Two buffers of this file can be charged at once — the footer
			// pass's (held to scan close) and the slot's load — each at most
			// readBufPoolBound times the file.
			bound := 2 * readBufPoolBound * size
			t.Logf("file %d bytes, peak charged %d, bound %d, budget %d, forced %d", size, tracker.Peak(), bound, budget, counter.n.Load())
			if peak := tracker.Peak(); peak > bound {
				t.Errorf("the scan of one %d-byte file peaked at %d charged bytes, over %d (two buffers at %dx the file): a foreign buffer was charged",
					size, peak, bound, readBufPoolBound)
			}
			if peak := tracker.Peak(); peak > budget {
				t.Errorf("the scan of one %d-byte file peaked at %d bytes, over its %d-byte budget", size, peak, budget)
			}
			if f := counter.n.Load(); f != 0 {
				t.Errorf("%d reservations were forced past the budget for a %d-byte file", f, size)
			}
			if used := tracker.Used(); used != 0 {
				t.Errorf("the tracker still holds %d bytes after the scan", used)
			}
		})
	}
}
