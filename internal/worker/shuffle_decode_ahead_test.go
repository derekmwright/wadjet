package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// openDecodeAhead builds a streaming reader over wire and engages
// chunk-parallel decode with the given knobs (nil tokens/pressure = the
// production defaults minus budgeting).
func openDecodeAhead(tb testing.TB, wire []byte, workers int, tokens *cpuTokens, pressure func() bool, strict bool, stats *shuffleDecodeAheadStats) *streamingShuffleReader {
	tb.Helper()
	r := openStreaming(tb, wire)
	r.startDecodeAhead(workers, tokens, pressure, strict, stats)
	if r.da == nil {
		tb.Fatalf("decode-ahead did not engage (numChunks=%d)", r.numChunks)
	}
	return r
}

// insertEmptyChunk splices a bare zero row-count word between chunk
// payloads and patches the header count — the wire shape the serial
// reader's numRows==0 skip tolerates. Returns the modified wire.
func insertEmptyChunk(tb testing.TB, wire []byte) []byte {
	tb.Helper()
	// Walk to the end of the first chunk using the serial reader, which
	// tracks its position via the buffered stream: simplest is to re-read
	// the header + first chunk through a fresh reader over a counting
	// stream. Instead, splice at the very end — an empty trailing chunk
	// exercises the same skip path in both readers.
	out := append([]byte(nil), wire...)
	out = append(out, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(out[4:], binary.LittleEndian.Uint32(out[4:])+1)
	return out
}

// TestShuffleDecodeAhead_MatchesSerial is the core parity gate: same
// batches, same order, same values as the serial streaming reader, across
// every WSHF-supported column class, with an empty chunk in the wire.
func TestShuffleDecodeAhead_MatchesSerial(t *testing.T) {
	wire := insertEmptyChunk(t, buildMultiTypeWSHF(t, 42, 16, 257))
	want := drain(t, openStreaming(t, wire).Next)

	for _, workers := range []int{1, 3, 8} {
		r := openDecodeAhead(t, wire, workers, nil, nil, false, nil)
		got := drain(t, r.Next)
		requireBatchesEqual(t, want, got)
		if r.Delivered() != len(want) {
			t.Fatalf("workers=%d: Delivered()=%d, want %d", workers, r.Delivered(), len(want))
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestShuffleDecodeAhead_TruncationErrorPosition verifies a truncated
// stream delivers exactly the batches the serial reader would deliver and
// then surfaces an error at the same position (transport-fallback
// contract: Delivered() is the skip count).
func TestShuffleDecodeAhead_TruncationErrorPosition(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 7, 12, 128)
	cut := len(wire) - len(wire)/3 // mid-chunk somewhere past the header

	drainToErr := func(r *streamingShuffleReader) (int, error) {
		n := 0
		for {
			b, err := r.Next()
			if err != nil {
				return n, err
			}
			if b == nil {
				t.Fatalf("truncated stream ended with clean EOF after %d batches", n)
			}
			n++
		}
	}

	sN, sErr := drainToErr(openStreaming(t, wire[:cut]))
	r := openDecodeAhead(t, wire[:cut], 4, nil, nil, false, nil)
	pN, pErr := drainToErr(r)
	if pN != sN {
		t.Fatalf("batches before error: parallel %d, serial %d", pN, sN)
	}
	if pErr == nil || !strings.Contains(pErr.Error(), "truncated") {
		t.Fatalf("parallel error %v, want truncation (serial: %v)", pErr, sErr)
	}
	if r.Delivered() != sN {
		t.Fatalf("Delivered()=%d, want %d", r.Delivered(), sN)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestShuffleDecodeAhead_ZeroTokens verifies token exhaustion degrades to
// the cursor-exempt serial cadence and still completes with exact parity.
func TestShuffleDecodeAhead_ZeroTokens(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 3, 10, 200)
	want := drain(t, openStreaming(t, wire).Next)

	var stats shuffleDecodeAheadStats
	r := openDecodeAhead(t, wire, 4, newCPUTokens(0), nil, false, &stats)
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.tokenStallNs.Load() == 0 {
		t.Fatalf("expected token stalls with a zero-capacity pool")
	}
}

// TestShuffleDecodeAhead_TokensBalance verifies every token acquired for a
// non-cursor chunk is returned by the time the reader closes.
func TestShuffleDecodeAhead_TokensBalance(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 9, 20, 100)
	tokens := newCPUTokens(3)
	r := openDecodeAhead(t, wire, 4, tokens, nil, false, nil)
	_ = drain(t, r.Next)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("tokens leaked: InUse=%d after Close", got)
	}
}

// TestShuffleDecodeAhead_TokensBalanceOnEarlyClose closes the reader after
// one batch — tokens attached to staged-but-undecoded chunks must drain
// back to the pool and Close must not hang or leak goroutines.
func TestShuffleDecodeAhead_TokensBalanceOnEarlyClose(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 11, 32, 200)
	tokens := newCPUTokens(3)
	r := openDecodeAhead(t, wire, 4, tokens, nil, false, nil)
	if b, err := r.Next(); err != nil || b == nil {
		t.Fatalf("first Next: %v %v", b, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("tokens leaked: InUse=%d after early Close", got)
	}
}

// TestShuffleDecodeAhead_UnderPressure forces the pressure hook active for
// the whole run: admission collapses to the occupancy floor but the stream
// must still complete with exact parity (cursor exemption = liveness).
func TestShuffleDecodeAhead_UnderPressure(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 5, 10, 150)
	want := drain(t, openStreaming(t, wire).Next)

	for _, strict := range []bool{false, true} {
		var stats shuffleDecodeAheadStats
		r := openDecodeAhead(t, wire, 4, nil, func() bool { return true }, strict, &stats)
		got := drain(t, r.Next)
		requireBatchesEqual(t, want, got)
		if err := r.Close(); err != nil {
			t.Fatalf("strict=%v Close: %v", strict, err)
		}
		if stats.pressureNs.Load() == 0 {
			t.Fatalf("strict=%v: expected pressure stalls with hook forced on", strict)
		}
	}
}

// TestShuffleDecodeAhead_WindowBound shrinks the byte window below one
// chunk: every chunk is oversized, admission must alternate via the cursor
// exemption rather than deadlock, and parity must hold.
func TestShuffleDecodeAhead_WindowBound(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 13, 8, 300)
	want := drain(t, openStreaming(t, wire).Next)

	old := shuffleDecodeAheadWindowBytes
	shuffleDecodeAheadWindowBytes = 1 // below any real chunk
	defer func() { shuffleDecodeAheadWindowBytes = old }()

	var stats shuffleDecodeAheadStats
	r := openDecodeAhead(t, wire, 4, nil, nil, false, &stats)
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.windowFullNs.Load() == 0 {
		t.Fatalf("expected window-full stalls with a 1-byte window")
	}
}

// TestShuffleDecodeAhead_MinChunksGate verifies small files stay serial.
func TestShuffleDecodeAhead_MinChunksGate(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 1, shuffleDecodeAheadMinChunks-1, 8)
	r := openStreaming(t, wire)
	r.startDecodeAhead(4, nil, nil, false, nil)
	if r.da != nil {
		t.Fatalf("decode-ahead engaged below the min-chunks gate")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestShuffleDecodeAhead_WSHC runs parity through the s2-compressed
// envelope — the codec stream stays on the scanner.
func TestShuffleDecodeAhead_WSHC(t *testing.T) {
	plain := buildMultiTypeWSHF(t, 21, 10, 120)
	want := drain(t, openStreaming(t, plain).Next)

	var comp bytes.Buffer
	comp.Write(compressedMagic[:])
	w := acquireS2Writer(&comp)
	if _, err := w.Write(plain); err != nil {
		t.Fatalf("s2 write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("s2 close: %v", err)
	}
	releaseS2Writer(w)

	r := openDecodeAhead(t, comp.Bytes(), 4, nil, nil, false, nil)
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestShuffleDecodeAhead_StatsProgress sanity-checks the throughput
// markers move.
func TestShuffleDecodeAhead_StatsProgress(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 17, 10, 100)
	var stats shuffleDecodeAheadStats
	r := openDecodeAhead(t, wire, 4, nil, nil, false, &stats)
	_ = drain(t, r.Next)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.chunks.Load() != 10 {
		t.Fatalf("chunks=%d, want 10", stats.chunks.Load())
	}
	if stats.stageNs.Load() == 0 || stats.decodeNs.Load() == 0 {
		t.Fatalf("stage/decode spans did not move: stage=%d decode=%d",
			stats.stageNs.Load(), stats.decodeNs.Load())
	}
}

// TestShuffleDecodeAhead_CloseIdempotent mirrors the serial reader's
// idempotent-Close contract, mid-stream and at EOF.
func TestShuffleDecodeAhead_CloseIdempotent(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 23, 8, 50)
	r := openDecodeAhead(t, wire, 4, nil, nil, false, nil)
	if b, err := r.Next(); err != nil || b == nil {
		t.Fatalf("first Next: %v %v", b, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestShuffleDecodeAhead_SourceIntegration runs the production fragment
// input path (MemStore → cachedFileStreamSource tiered open → streaming
// pread reader) with the executor flag on and off and requires identical
// row/sum totals across multi-chunk, multi-file WSHF input — the
// engagement-site parity gate.
func TestShuffleDecodeAhead_SourceIntegration(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	var keys []string
	var wantSum int64
	var wantRows int
	for f := 0; f < 3; f++ {
		wire := buildMultiTypeWSHF(t, int64(100+f), 12, 300)
		for _, b := range drain(t, openStreaming(t, wire).Next) {
			for i := 0; i < b.Len; i++ {
				if b.Columns[0].Nulls.IsNull(i) {
					continue
				}
				wantSum += b.Columns[0].Int64Data[i]
				wantRows++
			}
		}
		key := fmt.Sprintf("queries/test/partition=%04d/task.wshf", f)
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(wire), int64(len(wire)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}

	for _, ahead := range []bool{false, true} {
		e := &Executor{store: store, spillDir: t.TempDir(), shuffleDecodeAhead: ahead, cpuTokens: newCPUTokens(4)}
		src := newCachedFileStreamSource(e, "", bucket, keys)
		if err := src.Init(ctx); err != nil {
			t.Fatal(err)
		}
		var sum int64
		var rows int
		for {
			b, err := src.Next(ctx)
			if err != nil {
				t.Fatalf("ahead=%v: %v", ahead, err)
			}
			if b == nil {
				break
			}
			for i := 0; i < b.Len; i++ {
				if b.Columns[0].Nulls.IsNull(i) {
					continue
				}
				sum += b.Columns[0].Int64Data[i]
				rows++
			}
		}
		if err := src.Close(); err != nil {
			t.Fatalf("ahead=%v Close: %v", ahead, err)
		}
		if sum != wantSum || rows != wantRows {
			t.Fatalf("ahead=%v: rows=%d sum=%d, want rows=%d sum=%d", ahead, rows, sum, wantRows, wantSum)
		}
		if got := e.cpuTokens.InUse(); got != 0 {
			t.Fatalf("ahead=%v: tokens leaked: %d", ahead, got)
		}
	}
}

// TestShuffleDecodeAheadPressure_RefaultExemption pins the pressure
// routing: ambient page-cache refaults must NOT throttle WSHF
// decode-ahead on non-edge envelopes (the 154200 window's 92s/worker
// pressure-stall pathology), while the heap gauge always does and
// strict/edge envelopes keep the full refault coupling.
func TestShuffleDecodeAheadPressure_RefaultExemption(t *testing.T) {
	origHeap, origCache, origBounded, origStrict, origCoupled :=
		heapPressureActive, pageCachePressureActive, pageCachePressureActiveBounded,
		scanDecodeAheadStrictPressure, shuffleDARefaultCoupled
	defer func() {
		heapPressureActive, pageCachePressureActive, pageCachePressureActiveBounded =
			origHeap, origCache, origBounded
		scanDecodeAheadStrictPressure = origStrict
		shuffleDARefaultCoupled = origCoupled
	}()

	var heap, cache bool
	heapPressureActive = func() bool { return heap }
	pageCachePressureActive = func() bool { return cache }
	pageCachePressureActiveBounded = func(time.Duration) bool { return cache }

	cases := []struct {
		name                        string
		heap, cache, strict, couple bool
		want                        bool
	}{
		{"quiet", false, false, false, false, false},
		{"ambient refaults, non-edge: EXEMPT", false, true, false, false, false},
		{"heap tide always binds", true, false, false, false, true},
		{"edge keeps refault coupling", false, true, true, false, true},
		{"edge quiet", false, false, true, false, false},
		{"kill switch restores coupling", false, true, false, true, true},
	}
	for _, tc := range cases {
		heap, cache = tc.heap, tc.cache
		strict := tc.strict
		scanDecodeAheadStrictPressure = func() bool { return strict }
		shuffleDARefaultCoupled = tc.couple
		if got := shuffleDecodeAheadPressure(); got != tc.want {
			t.Errorf("%s: pressure=%v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestShuffleDecodeAhead_PressureFlaps alternates the pressure hook per
// call — admission decisions flip mid-stream; parity and completion must
// survive the churn.
func TestShuffleDecodeAhead_PressureFlaps(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 29, 24, 64)
	want := drain(t, openStreaming(t, wire).Next)

	var n atomic.Int64
	r := openDecodeAhead(t, wire, 4, newCPUTokens(2), func() bool {
		return n.Add(1)%3 == 0
	}, false, nil)
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// waitTokenStalled polls until the reader's scanner parks in admit's token
// wait — the only state in which §2.2 donations are accepted.
func waitTokenStalled(tb testing.TB, d *shuffleDecodeAhead) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		stalled := d.tokenStalled
		d.mu.Unlock()
		if stalled {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatal("scanner never parked in a token stall")
}

// TestShuffleDecodeAhead_DonationAdmitsPastCursor pins §2.2 producer token
// donation: a token donated while the scanner is parked token-stalled is
// consumed for a non-cursor admission (the stall breaks without any pool
// release), parity holds, and the donated token drains back to the pool by
// Close.
func TestShuffleDecodeAhead_DonationAdmitsPastCursor(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 31, 12, 150)
	want := drain(t, openStreaming(t, wire).Next)

	tokens := newCPUTokens(1)
	if got := tokens.TryAcquire(1); got != 1 { // stand in for a consumer holding the pool
		t.Fatalf("TryAcquire = %d, want 1", got)
	}
	var stats shuffleDecodeAheadStats
	r := openDecodeAhead(t, wire, 4, tokens, nil, false, &stats)

	waitTokenStalled(t, r.da)
	if !r.da.tryDonate() {
		t.Fatal("tryDonate refused while the scanner is token-stalled")
	}
	got := drain(t, r.Next)
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if stats.donated.Load() != 1 {
		t.Fatalf("donated = %d, want 1", stats.donated.Load())
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 (donated token must drain back)", got)
	}
}

// TestShuffleDecodeAhead_DonationBalanceOnEarlyClose: tokens donated to a
// parked scanner must drain back to the pool even when the reader closes
// before consuming them (stopped-branch and scan-exit flushes).
func TestShuffleDecodeAhead_DonationBalanceOnEarlyClose(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 41, 16, 150)
	tokens := newCPUTokens(2)
	if got := tokens.TryAcquire(2); got != 2 {
		t.Fatalf("TryAcquire = %d, want 2", got)
	}
	r := openDecodeAhead(t, wire, 2, tokens, nil, false, nil)
	if b, err := r.Next(); err != nil || b == nil {
		t.Fatalf("first Next: %v %v", b, err)
	}
	waitTokenStalled(t, r.da)
	accepted := 0
	for i := 0; i < 2; i++ {
		if r.da.tryDonate() {
			accepted++
		}
	}
	if accepted == 0 {
		t.Fatal("no donation accepted while scanner token-stalled")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := int(tokens.InUse()); got != 2-accepted {
		t.Fatalf("InUse = %d, want %d (donations must flush on close)", got, 2-accepted)
	}
}

// TestShuffleDecodeAhead_DonationRefused: an unbudgeted reader (nil pool)
// never token-stalls so it must refuse, a closed reader must refuse, and a
// source with no active pipeline must refuse — the caller keeps its token
// in every case.
func TestShuffleDecodeAhead_DonationRefused(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 43, 8, 64)
	r := openDecodeAhead(t, wire, 2, nil, nil, false, nil)
	if r.da.tryDonate() {
		t.Fatal("donation accepted by an unbudgeted reader")
	}
	_ = drain(t, r.Next)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if r.da.tryDonate() {
		t.Fatal("donation accepted after Close")
	}
	var s cachedFileStreamSource
	if s.tryDonateToken() {
		t.Fatal("source with no decode-ahead accepted a donation")
	}
}

// TestShuffleDecodeAhead_DonationRace hammers tryDonate from several
// goroutines against a draining reader — race-detector coverage for the
// donation paths; parity and exact pool balance must hold.
func TestShuffleDecodeAhead_DonationRace(t *testing.T) {
	wire := buildMultiTypeWSHF(t, 47, 24, 100)
	want := drain(t, openStreaming(t, wire).Next)

	tokens := newCPUTokens(4)
	r := openDecodeAhead(t, wire, 4, tokens, nil, false, nil)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if tokens.TryAcquire(1) == 1 {
					if !r.da.tryDonate() {
						tokens.Release(1)
					}
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	got := drain(t, r.Next)
	close(stop)
	wg.Wait()
	requireBatchesEqual(t, want, got)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inUse := tokens.InUse(); inUse != 0 {
		t.Fatalf("tokens leaked: %d", inUse)
	}
}

// TestShuffleDecodeAhead_DonationEndToEnd runs the production seam: a real
// MemStore-backed cachedFileStreamSource with decode-ahead engaged and a
// starved token pool, a widthGate wired to it as donor (exactly as the
// fragment runner wires it), and a consumer-held token yielded while the
// scanner is token-stalled. The donation must transfer (no pool release),
// reach the reader (stats.donated), forward through manifestStreamSource,
// and drain back to the pool by Close.
func TestShuffleDecodeAhead_DonationEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	wire := buildMultiTypeWSHF(t, 53, 40, 200)
	key := "queries/test/partition=0000/task.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(wire), int64(len(wire)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	tokens := newCPUTokens(1)
	if got := tokens.TryAcquire(1); got != 1 { // the consumer's held width token
		t.Fatalf("TryAcquire = %d, want 1", got)
	}
	e := &Executor{store: store, spillDir: t.TempDir(), shuffleDecodeAhead: true, cpuTokens: tokens}
	src := newCachedFileStreamSource(e, "", bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := src.Next(ctx); err != nil || b == nil { // opens the file, engages decode-ahead
		t.Fatalf("first Next: %v %v", b, err)
	}
	da := src.shuffleDA.Load()
	if da == nil {
		t.Fatal("decode-ahead not engaged on the source")
	}
	waitTokenStalled(t, da)

	// Forwarding through the eager-consumer wrapper hits the same reader.
	ms := &manifestStreamSource{}
	ms.donorInner.Store(src)

	gate := newWidthGate(tokens)
	gate.donor = ms
	gate.yield(slotToken) // the §2.2 moment: dry consumer yields its token
	if got := gate.donations.Load(); got != 1 {
		t.Fatalf("gate donations = %d, want 1", got)
	}
	if got := e.shuffleDecodeAheadStats.donated.Load(); got != 1 {
		t.Fatalf("reader donated = %d, want 1", got)
	}

	for { // drain
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 (donated token must drain back)", got)
	}
	// With the pipeline gone, the wrapper refuses and the plain release
	// path is the fallback again.
	if ms.tryDonateToken() {
		t.Fatal("donation accepted after source Close")
	}
}

// TestShuffleDecodeAhead_ClaimDonationEndToEnd runs the §2.3 deep-starvation
// seam: the pool is exhausted by another holder, the scanner is
// token-stalled, and the fragment's only extra consumer is a slot-less
// waiter parked inside widthGate.claim — the §2.2 yield precondition (a
// HELD token going dry) can never occur. The grant that lands on the parked
// claim must cede to the scanner (reader stats.donated, gate
// width_claim_donations), the scanner must admit past the cursor and its
// decode worker's release must re-grant the consumer, whose second grant
// sticks; the pool balances by Close.
func TestShuffleDecodeAhead_ClaimDonationEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	const bucket = "test"
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	wire := buildMultiTypeWSHF(t, 59, 40, 200)
	key := "queries/test/partition=0000/task.wshf"
	if _, err := store.Put(ctx, bucket, key, bytes.NewReader(wire), int64(len(wire)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	tokens := newCPUTokens(1)
	if got := tokens.TryAcquire(1); got != 1 { // another fragment holds the pool
		t.Fatalf("TryAcquire = %d, want 1", got)
	}
	e := &Executor{store: store, spillDir: t.TempDir(), shuffleDecodeAhead: true, cpuTokens: tokens}
	src := newCachedFileStreamSource(e, "", bucket, []string{key})
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := src.Next(ctx); err != nil || b == nil { // opens the file, engages decode-ahead
		t.Fatalf("first Next: %v %v", b, err)
	}
	da := src.shuffleDA.Load()
	if da == nil {
		t.Fatal("decode-ahead not engaged on the source")
	}
	waitTokenStalled(t, da)

	ms := &manifestStreamSource{}
	ms.donorInner.Store(src)
	gate := newWidthGate(tokens)
	gate.donor = ms

	base, _ := gate.claim(ctx) // the baseline consumer
	if base != slotBaseline {
		t.Fatalf("claim = %v, want baseline", base)
	}
	done := make(chan widthSlot, 1)
	go func() { // the slot-less deep-starvation waiter
		s, err := gate.claim(ctx)
		if err != nil {
			t.Errorf("claim: %v", err)
		}
		done <- s
	}()
	time.Sleep(20 * time.Millisecond) // let the claim park FIFO

	tokens.Release(1) // the other fragment finishes: grant lands on the waiter
	deadline := time.Now().Add(5 * time.Second)
	for e.shuffleDecodeAheadStats.donated.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("reader donated = %d, want 1 (claim grant not redirected)",
				e.shuffleDecodeAheadStats.donated.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if got := gate.claimDonations.Load(); got != 1 {
		t.Fatalf("gate claimDonations = %d, want 1", got)
	}
	// The donated token admits past the cursor; its decode worker's release
	// is what re-grants the parked consumer.
	select {
	case s := <-done:
		if s != slotToken {
			t.Fatalf("claim = %v, want token on the second grant", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer not re-granted after the donated token's decode release")
	}

	for { // drain; scanner exits, so the yield below is refused → pool release
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
	}
	gate.yield(slotToken)
	gate.yield(base)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if got := tokens.InUse(); got != 0 {
		t.Fatalf("InUse = %d, want 0 (all tokens must drain back)", got)
	}
}
