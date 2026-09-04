package physical

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The lifetime contract, at the granularity the row-group read introduced.
//
// A row group's buffer goes back to the pool the moment THAT row group has
// been decoded — not, as before, when the file's LAST row group has. So a
// buffer is recycled while OTHER row groups of the same file are still being
// decoded and while batches decoded from it are still travelling downstream.
// If any decoded value aliased that buffer, the next row group to take it out
// of the pool would rewrite somebody's rows in place, and the query would
// return wrong values with no error anywhere.
//
// It does not alias — every fixed-width and bytes payload is copied into the
// vector's own arena — and `TestReadRowGroupNative_StagedNoAliasing` asserts
// that for the staged path. This asserts it for THIS path, at the granularity
// where the recycle happens MID-FILE.
//
// Two things make the assertion able to fail, and an earlier version of this
// test had neither:
//
//   - The snapshot is taken AS EACH BATCH ARRIVES, not after the scan. A
//     snapshot taken at the end records whatever a mid-scan recycle already
//     did, and then compares it with itself.
//   - The recycle happens inside ONE scan source. Buffers are pooled per
//     source and per file shape, so a second source's scan can never be handed
//     the first's buffers; an arm built that way is vacuous by type.
//
// And PoisonReleasedSlabs makes the recycle visible: every buffer handed back
// from the pool is overwritten first, so an aliased value becomes 0xEE rather
// than "whatever the next row group happened to put there".

// fatRows makes row groups big enough to be worth pooling and gives every row
// a distinct value, so one rewritten row is visible.
//
// The columns are chosen for how their decoders get at the page bytes, not for
// variety. STRING goes through DecodePlainByteArray, which COPIES into a
// packed buffer before anything else sees it — two copies away from the
// hazard. UUID and IPv6 are FIXED_LEN_BYTE_ARRAY: they come back from
// `plainBody`/`RawValues` as a DIRECT SLICE of the page, which is a direct
// slice of the row group's buffer under CodecNone. If a recycled buffer can
// reach a live batch at all, it reaches it through these.
func fatRows(n, base int, fill byte) []map[string]any {
	const pad = 512
	rows := make([]map[string]any, n)
	for i := range rows {
		v := make([]byte, pad)
		for j := range v {
			v[j] = fill
		}
		copy(v, fmt.Sprintf("%c%08d", fill, base+i))
		rows[i] = map[string]any{
			"id":    int64(base + i),
			"value": string(v),
			"uu":    fmt.Sprintf("%08x-0000-4000-8000-%012x", base+i, base+i),
			"v6":    fmt.Sprintf("2001:db8:%x::%x", (base+i)&0xffff, base+i),
		}
	}
	return rows
}

// aliasFixture is scanAccountingFixture over the wider schema — the same one
// file, one table shape, with the direct-slice columns added.
func aliasFixture(t *testing.T, rowSets ...[]map[string]any) (*catalog.Catalog, catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: aliasSchemaCols()}
	data := writeTestParquetMultiRG(t, schema, rowSets...)
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	path := "tables/items/chunk_alias.parquet"
	if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("put file: %v", err)
	}
	var rows int64
	for _, rs := range rowSets {
		rows += int64(len(rs))
	}
	entry := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: rows}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{entry}); err != nil {
		t.Fatalf("add files: %v", err)
	}
	return cat, entry
}

func TestABatchOutlivesItsRowGroupsBuffer(t *testing.T) {
	const rgs, rowsPerRG = 8, 150
	prevPoison := PoisonReleasedSlabs(true)
	defer PoisonReleasedSlabs(prevPoison)

	sets := make([][]map[string]any, rgs)
	for i := range sets {
		sets[i] = fatRows(rowsPerRG, i*rowsPerRG, 'a')
	}
	cat, entry := aliasFixture(t, sets...)
	tracker := memory.NewTracker("scan-alias", 1<<30)
	inner := &scanSourceInner{
		cat: cat, memTracker: tracker,
		schema:  aliasSchemaCols(),
		batchCh: make(chan *batch.RecordBatch, 2), // small: batches queue behind the consumer
		errCh:   make(chan error, 1),
	}
	slot := rowGroupSlot(t, inner, cat, entry, rgs)
	releasesBefore := RowGroupSlabReleases()

	var rowOff int64
	for i := 0; i < rgs; i++ {
		inner.rgUnits = append(inner.rgUnits, rgUnit{
			slot: slot, rgIndex: i, rgRowOffset: rowOff, numRows: rowsPerRG})
		rowOff += rowsPerRG
	}
	inner.wg.Add(2)
	go inner.rgWorker(context.Background())
	go inner.rgWorker(context.Background())
	go func() {
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	// Snapshot AS EACH BATCH ARRIVES, and hold the batch. Row groups later in
	// the file are still loading — into buffers this batch's row group just
	// returned to the pool, overwritten with 0xEE on the way out.
	var held []*batch.RecordBatch
	var want [][]string
	ctx := context.Background()
	for {
		b, err := inner.next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		// Every column whose values could alias the buffer: the STRING
		// (copied by its decoder) and the two FIXED_LEN_BYTE_ARRAYs (sliced
		// straight out of the page).
		snap := make([]string, 0, b.Len*3)
		for _, col := range []int{1, 2, 3} {
			for r := 0; r < b.Len; r++ {
				snap = append(snap, b.Columns[col].BytesData.StringValue(r))
			}
		}
		held = append(held, b)
		want = append(want, snap)
	}
	if len(held) < rgs/2 {
		t.Fatalf("%d batches for %d row groups — too few to have overlapped a recycle", len(held), rgs)
	}
	// Vacuity: the direct-slice columns must actually carry values. A column
	// that decoded as all-NULL compares "" with "" forever.
	for _, col := range []int{2, 3} {
		b := held[0]
		for r := 0; r < b.Len && r < 4; r++ {
			if v := b.Columns[col].BytesData.StringValue(r); v == "" {
				t.Fatalf("column %d row %d is empty — the fixture's fixed-width column decoded to "+
					"nothing, so this gate is comparing blanks", col, r)
			}
		}
	}
	if rel := RowGroupSlabReleases() - releasesBefore; rel < int64(rgs) {
		t.Fatalf("%d of %d row-group buffers were released during this scan — every batch below "+
			"has to have been live while its own buffer went back to the pool, or nothing is "+
			"being tested", rel, rgs)
	}

	for i, b := range held {
		k := 0
		for _, col := range []int{1, 2, 3} {
			for r := 0; r < b.Len; r++ {
				got := b.Columns[col].BytesData.StringValue(r)
				if got != want[i][k] {
					t.Fatalf("batch %d column %d row %d read %q, but decoded as %q: a row group's "+
						"buffer was recycled — and overwritten — while a batch decoded from it was "+
						"still live", i, col, r, got, want[i][k])
				}
				if len(got) > 0 && got[0] == 0xEE {
					t.Fatalf("batch %d column %d row %d is poison: it aliases a recycled row-group "+
						"buffer", i, col, r)
				}
				k++
			}
		}
	}
}

// TestAReleasedSlabIsActuallyPoisoned: the gate above proves nothing unless
// the poison really lands. This is its precondition, asserted rather than
// assumed — and it depends on nothing but putSlab, so it cannot flake on
// sync.Pool's willingness to hand a buffer back.
func TestAReleasedSlabIsActuallyPoisoned(t *testing.T) {
	prevPoison := PoisonReleasedSlabs(true)
	defer PoisonReleasedSlabs(prevPoison)
	inner := &scanSourceInner{}
	buf := inner.getSlab(4096)
	for i := range buf {
		buf[i] = 0x01
	}
	inner.putSlab(buf)
	for i, c := range buf[:cap(buf)] {
		if c != 0xEE {
			t.Fatalf("byte %d of a released buffer is %#x, want the poison — PoisonReleasedSlabs "+
				"is not reaching the release path, so the aliasing gate cannot fail", i, c)
		}
	}
}

// TestARowGroupErrorMidDecodeLeavesNothingCharged: a row group demanded after
// its release — the shape a decode error takes at this seam — must be an error,
// and the slot must still give back every buffer and every charge. A leaking
// error path is a phantom reservation on the worker-lifetime tracker.
func TestARowGroupErrorMidDecodeLeavesNothingCharged(t *testing.T) {
	cat, entry := scanAccountingFixture(t, testRows(200, 0), testRows(200, 200), testRows(200, 400))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}
	slot := rowGroupSlot(t, inner, cat, entry, 3)
	inner.rgUnits = []rgUnit{{slot: slot, rgIndex: 0}, {slot: slot, rgIndex: 1}, {slot: slot, rgIndex: 2}}

	if _, _, err := slot.rgs.RowGroupBytes(0); err != nil {
		t.Fatalf("row group 0: %v", err)
	}
	slot.releaseRG(inner, 0)
	if _, _, err := slot.rgs.RowGroupBytes(0); err == nil {
		t.Fatal("a released row group was served again")
	}
	inner.drainSlotCharges()
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after an error and a drain, want 0", used)
	}
}

// TestACancelledScanMidFileLeavesNothingCharged: the context dies with the
// stream open and row groups resident. Every buffer and charge must come back.
func TestACancelledScanMidFileLeavesNothingCharged(t *testing.T) {
	cat, entry := scanAccountingFixture(t,
		testRows(200, 0), testRows(200, 200), testRows(200, 400), testRows(200, 600))
	tracker := memory.NewTracker("scan-test", 1<<30)
	inner := &scanSourceInner{cat: cat, memTracker: tracker}

	ctx, cancel := context.WithCancel(context.Background())
	primeFooterCache(t, cat, entry)
	slot := &fileSlot{entry: entry, wantRG: []int{0, 1, 2, 3}}
	slot.rgRemaining.Store(4)
	if _, err := slot.ensureLoaded(inner, ctx); err != nil {
		t.Fatalf("ensureLoaded: %v", err)
	}
	if slot.rgs == nil {
		t.Fatal("not in row-group mode")
	}
	inner.rgUnits = []rgUnit{
		{slot: slot, rgIndex: 0}, {slot: slot, rgIndex: 1},
		{slot: slot, rgIndex: 2}, {slot: slot, rgIndex: 3},
	}
	for rg := 0; rg < 2; rg++ {
		if _, _, err := slot.rgs.RowGroupBytes(rg); err != nil {
			t.Fatalf("row group %d: %v", rg, err)
		}
	}
	if used := tracker.Used(); used <= 0 {
		t.Fatal("two row groups resident and nothing charged")
	}
	cancel() // the scan dies mid-file, two row groups resident, stream open

	inner.drainSlotCharges()
	if used := tracker.Used(); used != 0 {
		t.Fatalf("used = %d after a cancelled scan was drained, want 0", used)
	}
	if n := inner.residentSlabs.Load(); n != 0 {
		t.Fatalf("%d row-group buffers still counted resident after the drain", n)
	}
}

// scanAccountingSchemaCols is the fixture's schema, which the scan source
// needs to decode with.
func scanAccountingSchemaCols() []parquet.Column {
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
	}
}

// aliasSchemaCols adds the FIXED_LEN_BYTE_ARRAY types whose decoders hand back
// a direct slice of the page — see fatRows.
func aliasSchemaCols() []parquet.Column {
	return []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "value", Type: parquet.TypeString},
		{Name: "uu", Type: parquet.TypeUUID},
		{Name: "v6", Type: parquet.TypeIPv6},
	}
}
