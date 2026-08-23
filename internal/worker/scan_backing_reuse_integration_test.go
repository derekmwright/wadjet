package worker

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// backingReuseToggle finds the scan backing-reuse kill switch by name. The
// toggle itself is unexported in package scan; the registry is how the
// invariance oracle reaches it too.
func backingReuseToggle(tb testing.TB) *optswitch.Toggle {
	tb.Helper()
	for _, t := range optswitch.All() {
		if t.Name == "scan-backing-reuse" {
			return t
		}
	}
	tb.Fatal("scan-backing-reuse toggle is not registered")
	return nil
}

// writeBackingReuseFixture writes one parquet file of groups × rowsPerGroup
// rows with a variable-width string column, so the reuse under test covers the
// BYTES arena and not only the fixed-width slots.
func writeBackingReuseFixture(tb testing.TB, store objstore.Store, bucket string, groups, rowsPerGroup int) string {
	tb.Helper()
	ctx := context.Background()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		tb.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}}
	cfg := parquet.DefaultWriterConfig()
	cfg.RowGroupSize = rowsPerGroup

	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	id := 0
	for g := 0; g < groups; g++ {
		rows := make([]map[string]any, rowsPerGroup)
		for r := range rows {
			rows[r] = map[string]any{
				"id":   int64(id),
				"val":  int64(id * 7),
				"name": fmt.Sprintf("row_%07d_%s", id, string(bytes.Repeat([]byte("y"), id%23))),
			}
			id++
		}
		if err := w.WriteRows(rows); err != nil {
			tb.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	key := "tables/t/part-0000.parquet"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
		tb.Fatal(err)
	}
	return key
}

type backingRunRow struct {
	id, val int64
	name    string
}

type backingRunResult struct {
	rows                  []backingRunRow
	hits, misses, claimed int64
	armed                 bool
}

// TestScanBackingReuseEndToEnd wires the three halves that were only ever
// tested apart: the real cachedFileStreamSource over a real multi-row-group
// parquet file, the morsel dispenser that fans each row group out as views,
// and the retire edge that hands the backing back. It asserts the reuse
// actually ENGAGES on this path (backing_hits > 0 — the pool is armed by the
// dispenser resolving the release hook, and the decode ring is running under a
// window small enough to make it recycle) and that every delivered value is
// identical to the same scan with the kill switch off.
func TestScanBackingReuseEndToEnd(t *testing.T) {
	const (
		groups       = 40
		rowsPerGroup = 4096
	)
	store := objstore.NewMemStore()
	key := writeBackingReuseFixture(t, store, "b", groups, rowsPerGroup)

	// A budget small enough that a 4096-row parent splits into views (the
	// SF100 shape: k consumers reading one parent concurrently), which is also
	// the retire path whose refcount gates the release.
	prevBudget := morselDispenserBudgetBytes
	morselDispenserBudgetBytes = 256 << 10
	t.Cleanup(func() { morselDispenserBudgetBytes = prevBudget })

	run := func(t *testing.T, reuse bool) backingRunResult {
		t.Helper()
		tg := backingReuseToggle(t)
		prev := tg.Set(reuse)
		t.Cleanup(func() { tg.Set(prev) })

		ctx := context.Background()
		e := &Executor{store: store, spillDir: t.TempDir()}
		// 4 MiB window: several row groups in flight, so the ring runs ahead
		// of the consumer and the free list is genuinely contended.
		e.SetScanDecodeAhead(true, 4<<20)
		src := newCachedFileStreamSource(e, "", "b", []string{key})
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}

		d := newMorselDispenser(4, true)
		prodErr := make(chan error, 1)
		go func() { prodErr <- d.run(ctx, src) }()

		var out []backingRunRow
		for m := range d.ch {
			b := m.b
			idCol := b.ColumnByName("id")
			valCol := b.ColumnByName("val")
			nameCol := b.ColumnByName("name")
			for i := 0; i < b.ActiveLen(); i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				out = append(out, backingRunRow{
					id:   idCol.Int64Data[row],
					val:  valCol.Int64Data[row],
					name: string(nameCol.BytesData.Value(row)),
				})
			}
			// Release edge: this consumer is done with the morsel.
			m.retire()
		}
		if err := <-prodErr; err != nil {
			t.Fatal(err)
		}
		armed := src.backing.Load() != nil
		if err := src.Close(); err != nil {
			t.Fatal(err)
		}
		hits, misses, claimed := e.ScanBackingStats()
		sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
		return backingRunResult{rows: out, hits: hits, misses: misses, claimed: claimed, armed: armed}
	}

	off := run(t, false)
	on := run(t, true)

	if len(off.rows) != groups*rowsPerGroup {
		t.Fatalf("switch-off run delivered %d rows, want %d", len(off.rows), groups*rowsPerGroup)
	}
	if len(on.rows) != len(off.rows) {
		t.Fatalf("row count differs: reuse=%d, no-reuse=%d", len(on.rows), len(off.rows))
	}
	for i := range on.rows {
		if on.rows[i] != off.rows[i] {
			t.Fatalf("row %d differs: reuse=%+v, no-reuse=%+v", i, on.rows[i], off.rows[i])
		}
	}
	if !on.armed || !off.armed {
		t.Fatalf("the dispenser's release hook must arm the pool on both arms (on=%v off=%v)", on.armed, off.armed)
	}
	if on.hits == 0 {
		t.Fatalf("reuse never engaged through the dispenser: hits=%d misses=%d claimed=%d", on.hits, on.misses, on.claimed)
	}
	if on.claimed != 0 {
		t.Fatalf("nothing on this path claims the batch, got claimed=%d", on.claimed)
	}
	if off.hits != 0 {
		t.Fatalf("the kill switch did not disable reuse: hits=%d", off.hits)
	}
}

// TestScanSourceWithoutReleaseEdgeHasNoPool: a consumer that never resolves
// the source's recycle hook — the shuffle task's plain Next loop,
// planner.StreamingSources, the hash-join build source, the post-breaker
// phases — must not get a backing pool at all. A pool it can never release
// into can never hand a backing to a later decode, and the old always-on
// construction is what let a registry of handed-out batches accumulate on
// exactly those sources.
func TestScanSourceWithoutReleaseEdgeHasNoPool(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	key := writeBackingReuseFixture(t, store, "b", 6, 512)

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", "b", []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	rows := 0
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		rows += b.ActiveLen()
	}
	if rows != 6*512 {
		t.Fatalf("plain Next loop read %d rows, want %d", rows, 6*512)
	}
	if p := src.backing.Load(); p != nil {
		t.Fatalf("a source with no release edge built a backing pool: %+v", p.Stats())
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if hits, misses, claimed := e.ScanBackingStats(); hits != 0 || misses != 0 || claimed != 0 {
		t.Fatalf("unarmed source reported pool activity: hits=%d misses=%d claimed=%d", hits, misses, claimed)
	}
}

// TestScanSourceArmingIsIdempotent: several fragment paths resolve the hook on
// the same source (the parallel runner does it before the warmup and again
// inside the dispenser's run). Arming twice must not replace a pool that is
// already handing out backings.
func TestScanSourceArmingIsIdempotent(t *testing.T) {
	store := objstore.NewMemStore()
	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", "b", nil)

	if got := batchRecyclerOf(src); got == nil {
		t.Fatal("the scan source must offer a release hook")
	}
	first := src.backing.Load()
	if first == nil {
		t.Fatal("resolving the hook did not arm the pool")
	}
	if got := batchRecyclerOf(src); got == nil {
		t.Fatal("second resolve returned no hook")
	}
	if src.backing.Load() != first {
		t.Fatal("arming twice replaced a live pool")
	}
	// A release naming a foreign batch is still a no-op on the armed pool.
	src.RecycleBatch(batch.NewRecordBatch(nil, 0), batch.MintStamp{})
}
