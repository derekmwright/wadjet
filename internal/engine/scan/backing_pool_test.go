package scan

import (
	"bytes"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// backingTestSchema is the read schema used by the backing-pool tests: one of
// every flat storage class the pool covers, including the variable-width
// BytesColumn whose arena is the biggest single allocation this lever removes.
func backingTestSchema() []pqt.Column {
	return []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "name", Type: pqt.TypeString},
		{Name: "amount", Type: pqt.TypeFloat64, Nullable: true},
		{Name: "flag", Type: pqt.TypeBool},
		{Name: "small", Type: pqt.TypeInt32},
	}
}

// writeBackingTestFile writes rows across several row groups so a decode loop
// exercises reuse more than once, and returns a reader over the bytes.
func writeBackingTestFile(tb testing.TB, rows, rowsPerGroup int) *pqt.Reader {
	tb.Helper()
	schema := pqt.Schema{Columns: backingTestSchema()}
	recs := make([]map[string]any, rows)
	for i := range recs {
		r := map[string]any{
			"id":    int64(i),
			"name":  fmt.Sprintf("value_%08d_%s", i, string(bytes.Repeat([]byte("x"), i%17))),
			"flag":  i%3 == 0,
			"small": int32(i % 1000),
		}
		if i%7 == 0 {
			r["amount"] = nil
		} else {
			r["amount"] = float64(i) * 0.25
		}
		recs[i] = r
	}
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = rowsPerGroup
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteRows(recs); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	rd, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		tb.Fatal(err)
	}
	return rd
}

// snapshotBatch copies every value a reader could observe out of b, so a later
// decode into the same backing cannot change what the snapshot reports.
type batchSnapshot struct {
	rows int
	vals []string
}

func snapshotBatch(b *batch.RecordBatch) batchSnapshot {
	s := batchSnapshot{rows: b.Len}
	for i := 0; i < b.Len; i++ {
		for _, c := range b.Columns {
			if c.Nulls.IsNull(i) {
				s.vals = append(s.vals, "<null>")
				continue
			}
			s.vals = append(s.vals, fmt.Sprint(c.GetValue(i)))
		}
	}
	return s
}

// release is the test-side release edge: it names the stamp the batch was
// handed under, which is what a real consumer captures at delivery (the
// dispenser at dispatch, the linear fragment path before push).
func release(p *BackingPool, b *batch.RecordBatch) {
	p.Recycle(b, b.Mint())
}

func (s batchSnapshot) equal(o batchSnapshot) bool {
	if s.rows != o.rows || len(s.vals) != len(o.vals) {
		return false
	}
	for i := range s.vals {
		if s.vals[i] != o.vals[i] {
			return false
		}
	}
	return true
}

// TestBackingReuseMatchesFreshAllocation is the bit-identity gate: decoding
// every row group of a file into pooled backings must produce exactly what
// decoding into fresh batches produces. ResetForWrite clears what it resizes,
// so a reused vector is indistinguishable from a fresh one — this asserts it
// against a real parquet file with nulls, variable-width strings and a
// non-multiple-of-64 row count.
func TestBackingReuseMatchesFreshAllocation(t *testing.T) {
	rd := writeBackingTestFile(t, 4501, 500)
	fr := rd.FileReader()
	schema := backingTestSchema()

	var want []batchSnapshot
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		b, err := ReadRowGroupNative(fr, rg, schema, nil)
		if err != nil {
			t.Fatalf("fresh decode rg=%d: %v", rg, err)
		}
		want = append(want, snapshotBatch(b))
	}
	if len(want) < 2 {
		t.Fatalf("need a multi-row-group file, got %d groups", len(want))
	}

	pool := NewBackingPool(BackingPoolOpts{})
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		b, err := ReadRowGroupNativeBacked(fr, rg, schema, nil, pool)
		if err != nil {
			t.Fatalf("pooled decode rg=%d: %v", rg, err)
		}
		if got := snapshotBatch(b); !got.equal(want[rg]) {
			t.Fatalf("rg=%d: pooled decode differs from fresh decode", rg)
		}
		// Release edge: nobody claimed it, so the next decode may reuse it.
		release(pool, b)
	}
	st := pool.Stats()
	if st.Hits == 0 {
		t.Fatalf("expected reuse after the first group, got %+v", st)
	}
	if st.Claimed != 0 {
		t.Fatalf("nothing was claimed, got %+v", st)
	}
}

// TestBackingReuseDoesNotAliasLiveBatch is the adversarial case: a consumer
// that has NOT released its batch must never see it overwritten by a later
// decode. Held-but-unreleased is the state every in-flight morsel is in while
// the decode-ahead ring decodes the next group.
func TestBackingReuseDoesNotAliasLiveBatch(t *testing.T) {
	rd := writeBackingTestFile(t, 3000, 500)
	fr := rd.FileReader()
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	live, err := ReadRowGroupNativeBacked(fr, 0, schema, nil, pool)
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotBatch(live)

	// Never released. Decode every remaining group; none of them may touch
	// the storage the caller is still holding.
	for rg := 1; rg < fr.NumRowGroups(); rg++ {
		other, err := ReadRowGroupNativeBacked(fr, rg, schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		if other == live {
			t.Fatalf("rg=%d reused a backing that was never released", rg)
		}
		if got := snapshotBatch(live); !got.equal(before) {
			t.Fatalf("rg=%d decode overwrote a live, unreleased batch", rg)
		}
	}
	if st := pool.Stats(); st.Hits != 0 {
		t.Fatalf("no backing was ever released; expected zero hits, got %+v", st)
	}
}

// TestBackingReuseVetoedByClaim covers the retention veto: a consumer that
// keeps the batch past the call claims it with Detach (Sort, Window, the join
// build, the collect sinks), and a claimed backing is surrendered permanently.
func TestBackingReuseVetoedByClaim(t *testing.T) {
	rd := writeBackingTestFile(t, 2000, 500)
	fr := rd.FileReader()
	schema := backingTestSchema()

	t.Run("batch Detach", func(t *testing.T) {
		pool := NewBackingPool(BackingPoolOpts{})
		b, err := ReadRowGroupNativeBacked(fr, 0, schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		kept := snapshotBatch(b)
		b.Detach() // a retaining consumer
		release(pool, b)

		next, err := ReadRowGroupNativeBacked(fr, 1, schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		if next == b {
			t.Fatal("a claimed backing was handed to a later decode")
		}
		if got := snapshotBatch(b); !got.equal(kept) {
			t.Fatal("the claimed batch was overwritten")
		}
		if st := pool.Stats(); st.Claimed != 1 {
			t.Fatalf("expected one claim-refused release, got %+v", st)
		}
	})

	t.Run("derived batch Detach claims the shared vectors", func(t *testing.T) {
		// ColumnPrune, the set-op emitter and partitioned aggregation mint a
		// NEW RecordBatch over the SAME *Vector pointers. The claim must come
		// back to us through the per-vector flag, not the shell.
		pool := NewBackingPool(BackingPoolOpts{})
		b, err := ReadRowGroupNativeBacked(fr, 0, schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		derived := &batch.RecordBatch{
			Columns: []*batch.Vector{b.Columns[1]},
			Schema:  []pqt.Column{schema[1]},
			Len:     b.Len,
		}
		derived.Detach()
		if b.Retained() {
			t.Fatal("precondition: the scan batch shell itself is not retained")
		}
		release(pool, b)
		if st := pool.Stats(); st.Claimed != 1 {
			t.Fatalf("a derived batch's claim did not veto the release: %+v", st)
		}
	})

	t.Run("downstream view Base claims the source column", func(t *testing.T) {
		// A late-materialization view minted downstream over one of our
		// columns propagates the claim back through Vector.Base.
		pool := NewBackingPool(BackingPoolOpts{})
		b, err := ReadRowGroupNativeBacked(fr, 0, schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		idx := make([]uint32, b.Len)
		for i := range idx {
			idx[i] = uint32(i)
		}
		view := batch.NewViewVector(b.Columns[0], idx)
		out := &batch.RecordBatch{
			Columns: []*batch.Vector{view},
			Schema:  []pqt.Column{schema[0]},
			Len:     b.Len,
		}
		out.Detach()
		release(pool, b)
		if st := pool.Stats(); st.Claimed != 1 {
			t.Fatalf("a downstream view's claim did not veto the release: %+v", st)
		}
	})
}

// TestBackingPoolIgnoresForeignBatch: the pool only ever takes back a batch it
// minted, so a WSHF shuffle chunk or a row-based fallback batch handed to the
// release edge can never gain a second owner.
func TestBackingPoolIgnoresForeignBatch(t *testing.T) {
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})
	foreign := batch.NewRecordBatch(schema, 128)
	release(pool, foreign)
	release(pool, foreign)
	if got := pool.get(schema, 128); got != nil {
		t.Fatal("the pool adopted a batch it never minted")
	}
}

// TestBackingPoolRecycleIsIdempotent: a second release of the same batch must
// not put it in the idle list twice — two decodes writing one backing is the
// worst failure this pool could produce.
func TestBackingPoolRecycleIsIdempotent(t *testing.T) {
	rd := writeBackingTestFile(t, 1000, 500)
	fr := rd.FileReader()
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	b, err := ReadRowGroupNativeBacked(fr, 0, schema, nil, pool)
	if err != nil {
		t.Fatal(err)
	}
	release(pool, b)
	release(pool, b)

	first := pool.get(schema, 100)
	second := pool.get(schema, 100)
	if first == nil {
		t.Fatal("expected the released backing back")
	}
	if second != nil {
		t.Fatal("the same backing was handed out twice")
	}
}

// TestBackingPoolCaps covers the idle-set bounds and the single-backing escape
// that mirrors the decode ring's always-admit-the-cursor rule.
func TestBackingPoolCaps(t *testing.T) {
	schema := backingTestSchema()

	t.Run("count cap", func(t *testing.T) {
		pool := NewBackingPool(BackingPoolOpts{MaxIdle: 2, MaxIdleBytes: 1 << 30})
		var minted []*batch.RecordBatch
		for i := 0; i < 4; i++ {
			b := batch.NewRecordBatch(schema, 64)
			pool.track(b, schema)
			minted = append(minted, b)
		}
		for _, b := range minted {
			release(pool, b)
		}
		got := 0
		for pool.get(schema, 64) != nil {
			got++
		}
		if got != 2 {
			t.Fatalf("MaxIdle=2 kept %d backings", got)
		}
	})

	t.Run("byte cap keeps at least one", func(t *testing.T) {
		pool := NewBackingPool(BackingPoolOpts{MaxIdle: 4, MaxIdleBytes: 1})
		a := batch.NewRecordBatch(schema, 4096)
		bb := batch.NewRecordBatch(schema, 4096)
		pool.track(a, schema)
		pool.track(bb, schema)
		release(pool, a)
		release(pool, bb)
		if pool.get(schema, 4096) == nil {
			t.Fatal("the always-keep-one escape did not fire")
		}
		if pool.get(schema, 4096) != nil {
			t.Fatal("the byte cap admitted a second oversized backing")
		}
	})
}

// TestBackingPoolShapeGuards: a schema the reset cannot serve is never pooled,
// and a shape mismatch costs a missed reuse rather than a mis-typed read.
func TestBackingPoolShapeGuards(t *testing.T) {
	pool := NewBackingPool(BackingPoolOpts{})

	rowSchema := []pqt.Column{{Name: "r", Type: pqt.TypeRow, Fields: []pqt.Column{
		{Name: "a", Type: pqt.TypeInt64},
	}}}
	rb := batch.NewRecordBatch(rowSchema, 16)
	pool.track(rb, rowSchema)
	release(pool, rb)
	if pool.get(rowSchema, 16) != nil {
		t.Fatal("a ROW schema was admitted to the pool")
	}

	dec4 := []pqt.Column{{Name: "d", Type: pqt.TypeDecimal, Scale: 4}}
	dec2 := []pqt.Column{{Name: "d", Type: pqt.TypeDecimal, Scale: 2}}
	db := batch.NewRecordBatch(dec4, 16)
	pool.track(db, dec4)
	release(pool, db)
	if pool.get(dec2, 16) != nil {
		t.Fatal("a scale-4 backing was handed to a scale-2 read")
	}
	if pool.get(dec4, 16) == nil {
		t.Fatal("the matching scale should have reused it")
	}
}

// TestBackingPoolKillSwitch: WADJET_SCAN_BACKING_REUSE=0 restores a fresh
// allocation per row group, byte-for-byte.
func TestBackingPoolKillSwitch(t *testing.T) {
	prev := scanBackingReuse.Set(false)
	t.Cleanup(func() { scanBackingReuse.Set(prev) })

	rd := writeBackingTestFile(t, 2000, 500)
	fr := rd.FileReader()
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	var seen []*batch.RecordBatch
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		b, err := ReadRowGroupNativeBacked(fr, rg, schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range seen {
			if p == b {
				t.Fatal("reuse happened with the kill switch off")
			}
		}
		seen = append(seen, b)
		release(pool, b)
	}
	if st := pool.Stats(); st.Hits != 0 {
		t.Fatalf("expected no hits with the switch off, got %+v", st)
	}
}

// TestBackingReuseThroughDecodeAheadIter drives the reuse through the decode
// ring itself — concurrent decode workers taking from and returning to one
// pool — and asserts the delivered batches are identical to the serial,
// unpooled decode.
func TestBackingReuseThroughDecodeAheadIter(t *testing.T) {
	rd := writeBackingTestFile(t, 6000, 300)
	fr := rd.FileReader()
	schema := backingTestSchema()

	var want []batchSnapshot
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		b, err := ReadRowGroupNative(fr, rg, schema, nil)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, snapshotBatch(b))
	}

	pool := NewBackingPool(BackingPoolOpts{})
	// Two passes over the same pool. Within one pass the hit RATE is
	// timing-dependent by design — the ring is allowed to run ahead of the
	// consumer, and only its byte window bounds how far — so the deterministic
	// reuse assertion is that pass 2 starts from pass 1's released backings.
	for pass := 0; pass < 2; pass++ {
		it, err := OpenDecodeAheadIter(rd, schema, nil, 0, 1, DecodeAheadOpts{Workers: 4, Backing: pool})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; ; i++ {
			b, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				if i != len(want) {
					t.Fatalf("pass %d delivered %d groups, want %d", pass, i, len(want))
				}
				break
			}
			if got := snapshotBatch(b); !got.equal(want[i]) {
				t.Fatalf("pass %d group %d differs from the unpooled decode", pass, i)
			}
			// The consumer is done: release edge.
			release(pool, b)
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		if pass == 0 && pool.Stats().IdleBytes == 0 {
			t.Fatal("pass 1 released nothing to the pool")
		}
	}
	if st := pool.Stats(); st.Hits == 0 {
		t.Fatalf("the ring never reused a backing across two passes: %+v", st)
	}
}

// TestBackingPoolHoldsNothingWithoutReleaseEdge is the resource-pin
// regression. The pool's identity used to be a map[*batch.RecordBatch] of
// everything it had handed out, so a consumer with NO release edge — the
// shuffle task's plain Next loop, the hash-join build source,
// planner.StreamingSources, a bloom-filtered-to-nothing row group the fragment
// simply drops — pinned whole decoded row groups (~280 MB each at SF100) for
// the source's lifetime, invisibly to the memory ledger. With the mint stamp
// the pool references nothing it has handed out, so the batches are collectable
// the moment the consumer lets go.
func TestBackingPoolHoldsNothingWithoutReleaseEdge(t *testing.T) {
	rd := writeBackingTestFile(t, 4000, 200)
	fr := rd.FileReader()
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	const decodes = 20
	var freed atomic.Int64
	for rg := 0; rg < decodes; rg++ {
		b, err := ReadRowGroupNativeBacked(fr, rg%fr.NumRowGroups(), schema, nil, pool)
		if err != nil {
			t.Fatal(err)
		}
		// Never released: this is the no-release-edge consumer.
		runtime.AddCleanup(b, func(int) { freed.Add(1) }, 0)
		b = nil
	}

	if st := pool.Stats(); st.Held != 0 || st.IdleBytes != 0 {
		t.Fatalf("the pool kept %d backings (%d bytes) it had handed out", st.Held, st.IdleBytes)
	}
	// And nothing else in the pool reaches them either.
	deadline := time.Now().Add(10 * time.Second)
	for freed.Load() < decodes && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(5 * time.Millisecond)
	}
	got := freed.Load()
	// The pool must be alive for the whole check — a live source holds it for
	// its lifetime, and without this the GC would collect the pool itself and
	// the registry with it, which would make this test pass for the wrong
	// reason.
	runtime.KeepAlive(pool)
	if got != decodes {
		t.Fatalf("%d/%d unreleased backings became collectable; the pool still references the rest", got, decodes)
	}
}

// TestBackingPoolMintingIsUncapped: the mint registry used to stop recording
// past 4*maxIdle+8 entries, so on any source that accumulated that many
// unreleased backings every later batch became "foreign" and reuse switched
// itself off silently — precisely on the sources that decode the most. The
// stamp has no such bound.
func TestBackingPoolMintingIsUncapped(t *testing.T) {
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	const minted = 4*4 + 8 + 100 // comfortably past the old registry cap
	var last *batch.RecordBatch
	for i := 0; i < minted; i++ {
		b := batch.NewRecordBatch(schema, 64)
		pool.track(b, schema)
		last = b
	}
	// Release only the LAST one: under the old cap it was never recorded, so
	// this release was ignored and the pool stayed empty forever.
	release(pool, last)
	if got := pool.get(schema, 64); got != last {
		t.Fatalf("a backing minted past the old registry cap was not recognized on release (got %v)", got)
	}
}

// TestBackingPoolRefusesStaleGenerationRelease: a release names the generation
// it was handed. A second release that arrives after the storage has been
// re-minted must be refused — re-admitting a LIVE backing would hand one
// buffer to two decoders, the one failure this pool must not have.
func TestBackingPoolRefusesStaleGenerationRelease(t *testing.T) {
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	b := batch.NewRecordBatch(schema, 64)
	pool.track(b, schema)
	stale := b.Mint()

	pool.Recycle(b, stale) // generation 1's release
	got := pool.get(schema, 64)
	if got != b {
		t.Fatal("the released backing was not handed back")
	}
	if got.Mint() == stale {
		t.Fatal("the re-mint did not advance the generation")
	}

	// The stale retire fires again while generation 2 is LIVE.
	pool.Recycle(b, stale)
	if again := pool.get(schema, 64); again != nil {
		t.Fatal("a stale release re-admitted a live backing to the free list")
	}
}

// TestBackingPoolDropRefusesLateRelease: once the owning source closes, a
// release still in flight must not re-grow the free list behind it — retained
// row-group storage must not outlive the source.
func TestBackingPoolDropRefusesLateRelease(t *testing.T) {
	schema := backingTestSchema()
	pool := NewBackingPool(BackingPoolOpts{})

	b := batch.NewRecordBatch(schema, 64)
	pool.track(b, schema)
	mint := b.Mint()

	pool.Drop()
	pool.Recycle(b, mint)
	if pool.get(schema, 64) != nil {
		t.Fatal("a release that landed after Drop re-grew the idle set")
	}
	if st := pool.Stats(); st.Held != 0 {
		t.Fatalf("Drop left %d backings held", st.Held)
	}
}
