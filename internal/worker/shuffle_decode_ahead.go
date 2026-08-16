package worker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/diskio"
)

// Shuffle decode-ahead (docs/design/shuffle-decode-ahead.md): the WSHF
// streaming reader's per-chunk work is two phases — STAGE (walk the chunk's
// length-prefixed column segments off the stream; sequential I/O, also the
// only way chunk boundaries are discovered) and DECODE (RecordBatch alloc +
// readColumnData column loop; the dominant, allocation-heavy cost). Serially
// both run on the one morsel-producer goroutine, which the q08 width-plateau
// attribution measured as the fragment pacer (~40 parents/s starving 15
// consumers, consumer dry-wait 5–10.6 widths, producer_wait=0).
//
// Here one scanner goroutine keeps the stage phase (the stream stays
// single-reader; every file byte still arrives via read syscalls — the
// frozen-spin GC-safety property of the pread arc is preserved), and k
// workers run the decode phase on staged heap buffers. Delivery is strictly
// in chunk order, so batches, errors (position-exact), and the transport
// fallback's Delivered() count are byte-identical to the serial reader.
const (
	// shuffleDecodeAheadMinChunks gates engagement: below this there is
	// nothing to overlap (keeps the low-volume gather/reply class serial).
	shuffleDecodeAheadMinChunks = 4
)

// shuffleDecodeAheadWorkers is the decode worker ceiling per reader;
// actual width is governed per chunk by the CPU-token pool, so this is a
// cap, not a commitment. The extent-index window re-attributed the warm
// join-6 trio's eff ~6.5 plateau to this cap (4 decode widths feeding an
// op-chain ~1.6× heavier per row — memo
// shuffle-extent-index-sf100-2026-08-15.md finding 1); WADJET_SHUFFLE_
// DA_WORKERS overrides it for the ceiling-lift A/B arm. Package var so
// tests can pin it.
var shuffleDecodeAheadWorkers = func() int {
	if v, err := strconv.Atoi(os.Getenv("WADJET_SHUFFLE_DA_WORKERS")); err == nil && v > 0 {
		return v
	}
	return 4
}()

// shuffleIndexReadaheadBytes is how far ahead of the scan cursor the
// index scanner advises FADV_WILLNEED over upcoming extents (the 3-arm
// window's finding 1: k interleaved worker preads defeat kernel
// readahead on not-yet-resident files, costing the early-warm runs what
// the walk path's sequential read got for free — memo
// shuffle-index-3arm-2026-08-15.md). The scanner is idle by construction
// in index mode and holds every extent, so the advice costs one syscall
// per window step. Package var so tests can shrink it; 0 disables.
var shuffleIndexReadaheadBytes int64 = 32 << 20

// shuffleDecodeAheadWindowBytes bounds staged+decoded-but-undelivered
// bytes per reader (charged at exact staged size — WSHF is uncompressed,
// decoded ≈ wire bytes). The morsel dispenser's 512 MiB budget bounds
// the next stage downstream, exactly as before. Package-level var so
// tests can shrink it.
var shuffleDecodeAheadWindowBytes int64 = 128 << 20

// shuffleDARefaultCoupled restores the refault channel on non-edge
// envelopes (WADJET_SHUFFLE_DA_REFAULT=1) — the same-binary A/B arm for
// the exemption below. Default off.
var shuffleDARefaultCoupled = os.Getenv("WADJET_SHUFFLE_DA_REFAULT") == "1"

// shuffleDecodeAheadPressure gates WSHF decode-ahead admission beyond
// the delivery cursor: the Go-heap tide gauge ALWAYS (staged+decoded
// chunks are our own heap, and the window is what must yield first),
// but the page-cache refault channel only on strict/edge envelopes.
//
// The SF100 validation window (results/20260815-154200, memo
// shuffle-decode-ahead-sf100-2026-08-15.md) measured pressure_stall_ms
// ≈ 92s/worker against window_full_ms 5.7s — the sensor was denying
// admission to a near-empty window, the scan-decode-ahead memo §9.4
// pathology. At SF100 partial residency the refault rate is dominated
// by ambient dataset-vs-cache thrash that WSHF admission cannot
// relieve: this window holds exact-charged bytes bounded at 128 MiB
// per reader that retire within one probe pass — orders below the
// scan windows whose displacement the sensor was built to shed.
// Edge-class envelopes keep the full coupling: the capped repro
// measured even one extra in-flight unit as harmful there, and its
// thrash is window-caused by construction.
func shuffleDecodeAheadPressure() bool {
	if heapPressureActive() {
		return true
	}
	if scanDecodeAheadStrictPressure() {
		return pageCachePressureActive()
	}
	if shuffleDARefaultCoupled {
		return pageCachePressureActiveBounded(refaultEpisodeCap)
	}
	return false
}

// shuffleDecodeAheadStats is the executor-owned counter sink shared by every
// reader (readers add directly; no per-reader fold step).
type shuffleDecodeAheadStats struct {
	chunks       atomic.Int64
	windowFullNs atomic.Int64
	tokenStallNs atomic.Int64
	pressureNs   atomic.Int64
	stageNs      atomic.Int64
	decodeNs     atomic.Int64
	donated      atomic.Int64
	preadNs      atomic.Int64 // index mode: worker-side extent read time
	indexedFiles atomic.Int64 // readers that engaged WIDX index staging
}

// shuffleDecodeSlot is one staged chunk moving through the pipeline: the
// scanner fills numRows/buf/est and sends it to the ordered delivery queue
// and the work queue; a worker fills b/err and closes done; next() waits on
// done in delivery order.
type shuffleDecodeSlot struct {
	chunkIdx uint32
	numRows  int
	buf      []byte
	off, end int64 // index mode: the chunk's file extent (incl. row-count word)
	est      int64
	token    bool // holds one cpu token, released by the decoding worker
	counted  bool // admitted against the window/occupancy (false for error slots)
	done     chan struct{}
	b        *batch.RecordBatch
	err      error
}

type shuffleDecodeAhead struct {
	r        *streamingShuffleReader
	tokens   *cpuTokens
	pressure func() bool
	strict   bool
	stats    *shuffleDecodeAheadStats

	delivery chan *shuffleDecodeSlot // ordered; consumed by next()
	jobs     chan *shuffleDecodeSlot // unordered; consumed by workers
	quit     chan struct{}
	wg       sync.WaitGroup
	bufPool  sync.Pool

	// mu guards the byte window and occupancy; cond is broadcast on every
	// delivery, on every donation, and on stop, waking a scanner parked in
	// admit.
	mu          sync.Mutex
	cond        *sync.Cond
	inflight    int64
	undelivered int
	limit       int64
	stopped     bool

	// Producer token donation (§2.2). tokenStalled is set only while the
	// scanner is parked in admit's token wait; tryDonate accepts a
	// consumer's yielded pool token only in that state. donated holds
	// accepted tokens until the scanner's next admission consumes them or
	// a non-token path flushes them back to the pool. Both under mu.
	tokenStalled bool
	donated      int

	// WIDX index staging (docs/design/shuffle-extent-index.md). Non-nil
	// idx switches the scanner to scanIndexed (extent slots, no staging;
	// workers pread from r.file). dropCursor, set only for owned
	// single-pass temps, drops page cache behind the in-order delivery
	// cursor — the walk path's DropBehindReader equivalent. Advanced only
	// from next()'s credit path (single goroutine).
	idx        []int64
	dropCursor *diskio.DropBehindCursor
}

// startDecodeAhead switches r from serial Next to chunk-parallel decode.
// Must be called before the first Next; no-ops (leaving r serial) when the
// file is too small to benefit. tokens/pressure may be nil (unbudgeted).
func (r *streamingShuffleReader) startDecodeAhead(workers int, tokens *cpuTokens, pressure func() bool, strict bool, stats *shuffleDecodeAheadStats) {
	if r.numChunks < shuffleDecodeAheadMinChunks {
		return
	}
	if workers <= 0 {
		workers = shuffleDecodeAheadWorkers
	}
	if mp := runtime.GOMAXPROCS(0); workers > mp {
		workers = mp
	}
	if stats == nil {
		stats = &shuffleDecodeAheadStats{}
	}
	d := &shuffleDecodeAhead{
		r:        r,
		tokens:   tokens,
		pressure: pressure,
		strict:   strict,
		stats:    stats,
		delivery: make(chan *shuffleDecodeSlot, 4*workers),
		jobs:     make(chan *shuffleDecodeSlot, workers),
		quit:     make(chan struct{}),
		limit:    shuffleDecodeAheadWindowBytes,
	}
	d.cond = sync.NewCond(&d.mu)
	if r.idx != nil {
		d.idx = r.idx
		if r.idxOwned {
			d.dropCursor = diskio.NewDropBehindCursor(r.file)
		}
	}
	d.wg.Add(1 + workers)
	go func() {
		defer d.wg.Done()
		if d.idx != nil {
			d.scanIndexed()
		} else {
			d.scan()
		}
	}()
	for i := 0; i < workers; i++ {
		go func() {
			defer d.wg.Done()
			d.worker()
		}()
	}
	r.da = d
}

// scan is the stage phase: the serial reader's stream walk, staging each
// non-empty chunk into a pooled buffer and emitting it — delivery first
// (order), then jobs. Errors surface as a terminal error slot AFTER every
// staged chunk, which is exactly where the serial reader would surface
// them (chunks 0..N-1 delivered, error at N).
func (d *shuffleDecodeAhead) scan() {
	defer close(d.jobs)
	defer close(d.delivery)
	// Donations deposited during the scanner's final token wait but never
	// consumed (file end, error, quit) go back to the pool. Deposits are
	// impossible after this runs: tokenStalled is false once admit returns.
	defer d.flushDonated()
	r := d.r
	for r.chunk < r.numChunks {
		if _, err := io.ReadFull(r.br, r.hdr[:4]); err != nil {
			d.fail(fmt.Errorf("shuffle stream truncated: chunk %d/%d row-count: %w", r.chunk, r.numChunks, err))
			return
		}
		numRows := int(binary.LittleEndian.Uint32(r.hdr[:4]))
		r.chunk++
		if numRows == 0 {
			continue
		}
		if numRows > streamMaxRows {
			d.fail(fmt.Errorf("shuffle stream chunk %d: implausible row count %d", r.chunk-1, numRows))
			return
		}
		t0 := time.Now()
		var base []byte
		if p, ok := d.bufPool.Get().([]byte); ok {
			base = p
		}
		buf, err := r.readChunkBytesInto(base[:0], numRows)
		d.stats.stageNs.Add(time.Since(t0).Nanoseconds())
		if err != nil {
			d.fail(fmt.Errorf("shuffle stream truncated: chunk %d/%d: %w", r.chunk-1, r.numChunks, err))
			return
		}
		est := int64(len(buf))
		token, ok := d.admit(est)
		if !ok {
			return // stopped
		}
		slot := &shuffleDecodeSlot{
			chunkIdx: r.chunk - 1,
			numRows:  numRows,
			buf:      buf,
			est:      est,
			token:    token,
			counted:  true,
			done:     make(chan struct{}),
		}
		select {
		case d.delivery <- slot:
		case <-d.quit:
			d.retireUndispatched(slot)
			return
		}
		select {
		case d.jobs <- slot:
		case <-d.quit:
			// Already in delivery; a worker will never see it. Resolve it
			// here so a concurrent next() cannot wait forever on done.
			if slot.token {
				d.tokens.Release(1)
				slot.token = false
			}
			slot.err = fmt.Errorf("shuffle decode-ahead: reader closed")
			close(slot.done)
			return
		}
		d.stats.chunks.Add(1)
	}
}

// scanIndexed is the stage phase over a validated WIDX extent index: no
// chunk bytes are read here — the scanner walks the offset table, admits
// each non-empty extent under exactly the same window/pressure/token/
// donation rules (est = extent payload size, cursor exemption intact), and
// emits extent slots for workers to pread, validate, and decode. The
// fragment's serial floor collapses from full-file staging to this loop.
// Quit/stop/flush semantics mirror scan().
func (d *shuffleDecodeAhead) scanIndexed() {
	defer close(d.jobs)
	defer close(d.delivery)
	defer d.flushDonated()
	r := d.r
	d.stats.indexedFiles.Add(1)
	// Extent readahead: keep FADV_WILLNEED issued a bounded distance ahead
	// of the scan cursor so worker preads copy from page cache instead of
	// blocking on NVMe under a held CPU token. Advisory; nil on non-Linux.
	advise := fdWillNeedAdviser(r.file)
	fileEnd := d.idx[len(d.idx)-1]
	advisedUpTo := d.idx[r.chunk]
	for r.chunk < r.numChunks {
		i := r.chunk
		start, end := d.idx[i], d.idx[i+1]
		r.chunk++
		if end-start == 4 {
			continue // bare row-count word: the empty-chunk encoding
		}
		if advise != nil && shuffleIndexReadaheadBytes > 0 {
			target := start + shuffleIndexReadaheadBytes
			if target < end {
				target = end
			}
			if target > fileEnd {
				target = fileEnd
			}
			if target > advisedUpTo {
				advise(advisedUpTo, target-advisedUpTo)
				advisedUpTo = target
			}
		}
		est := end - start - 4
		token, ok := d.admit(est)
		if !ok {
			return // stopped
		}
		slot := &shuffleDecodeSlot{
			chunkIdx: i,
			off:      start,
			end:      end,
			est:      est,
			token:    token,
			counted:  true,
			done:     make(chan struct{}),
		}
		select {
		case d.delivery <- slot:
		case <-d.quit:
			d.retireUndispatched(slot)
			return
		}
		select {
		case d.jobs <- slot:
		case <-d.quit:
			// Already in delivery; a worker will never see it. Resolve it
			// here so a concurrent next() cannot wait forever on done.
			if slot.token {
				d.tokens.Release(1)
				slot.token = false
			}
			slot.err = fmt.Errorf("shuffle decode-ahead: reader closed")
			close(slot.done)
			return
		}
		d.stats.chunks.Add(1)
	}
}

// admit blocks until est fits: the chunk at the delivery cursor
// (undelivered == 0) is always admitted token-exempt — serial progress is
// never gated, so an oversized chunk alternates instead of deadlocking and
// token/pressure exhaustion degrades to exactly the serial cadence. Beyond
// the cursor, admission needs window room, pressure clearance (occupancy-
// floored: one chunk ahead, cursor-only in strict/edge mode), and one CPU
// token per chunk (released by the worker when the decode parks — no
// goroutine ever holds a token while stalled).
func (d *shuffleDecodeAhead) admit(est int64) (token, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		if d.stopped {
			d.flushDonatedLocked()
			return false, false
		}
		if d.undelivered == 0 {
			// A donated token found at the cursor is attached to the chunk
			// (its decode worker releases it) rather than left parked —
			// cursor progress itself stays token-free either way.
			if d.donated > 0 {
				d.donated--
				token = true
			}
			d.inflight += est
			d.undelivered++
			return token, true
		}
		if d.inflight+est > d.limit {
			d.timedWait(&d.stats.windowFullNs)
			continue
		}
		if d.pressure != nil && d.pressure() {
			floor := 1
			if d.strict {
				floor = 0
			}
			if d.undelivered > floor {
				// Never park holding donated capacity: pressure waits are
				// unbounded from the pool's point of view.
				d.flushDonatedLocked()
				d.timedWait(&d.stats.pressureNs)
				continue
			}
		}
		if d.tokens != nil {
			if d.donated > 0 {
				d.donated--
				token = true
			} else if d.tokens.TryAcquire(1) == 1 {
				token = true
			} else {
				d.tokenStalled = true
				d.timedWait(&d.stats.tokenStallNs)
				d.tokenStalled = false
				continue
			}
		}
		d.inflight += est
		d.undelivered++
		return token, true
	}
}

// tryDonate transfers one cpu token from a yielding morsel consumer to this
// reader's scanner, accepted only while the scanner is parked in a token
// stall (§2.2 producer token donation). Ownership passes without touching
// the pool; the decode worker releases the token exactly like a
// TryAcquire'd one. Returns false when the caller should Release to the
// pool instead.
func (d *shuffleDecodeAhead) tryDonate() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped || !d.tokenStalled {
		return false
	}
	d.donated++
	d.stats.donated.Add(1)
	d.cond.Broadcast()
	return true
}

// flushDonatedLocked returns unconsumed donated tokens to the pool. Caller
// holds d.mu; the reader lock → pool lock order is safe because cpuTokens
// never calls back into the reader.
func (d *shuffleDecodeAhead) flushDonatedLocked() {
	if d.donated > 0 {
		d.tokens.Release(d.donated)
		d.donated = 0
	}
}

// flushDonated is flushDonatedLocked for callers not holding d.mu.
func (d *shuffleDecodeAhead) flushDonated() {
	d.mu.Lock()
	d.flushDonatedLocked()
	d.mu.Unlock()
}

// timedWait waits on the delivery condvar, adding the parked span to ns.
// Caller holds d.mu. Progress is guaranteed: undelivered > 0 on every wait
// path, so a delivery (or stop) broadcast always arrives.
func (d *shuffleDecodeAhead) timedWait(ns *atomic.Int64) {
	t0 := time.Now()
	d.cond.Wait()
	ns.Add(time.Since(t0).Nanoseconds())
}

func (d *shuffleDecodeAhead) worker() {
	for {
		select {
		case slot, ok := <-d.jobs:
			if !ok {
				return
			}
			var b *batch.RecordBatch
			var err error
			if d.idx != nil {
				b, err = d.decodeIndexed(slot)
			} else {
				t0 := time.Now()
				b, err = decodeShuffleChunk(d.r.schema, slot.numRows, slot.buf, slot.chunkIdx)
				d.stats.decodeNs.Add(time.Since(t0).Nanoseconds())
				d.putBuf(slot.buf)
				slot.buf = nil
			}
			if slot.token {
				d.tokens.Release(1)
				slot.token = false
			}
			slot.b, slot.err = b, err
			close(slot.done)
		case <-d.quit:
			return
		}
	}
}

// decodeIndexed stages and decodes one extent slot on the worker: a single
// pread of the chunk's extent (read syscall — GC-safe, ReadAt is
// goroutine-safe), the stage walk's bounds validation over the in-memory
// bytes (readColumnData slices unchecked, and pread bytes deserve exactly
// the trust stream bytes get: none), then the shared decode. Errors carry
// the chunk position and surface at the serial reader's exact error
// position via ordered delivery.
func (d *shuffleDecodeAhead) decodeIndexed(slot *shuffleDecodeSlot) (*batch.RecordBatch, error) {
	n := int(slot.end - slot.off)
	var buf []byte
	if p, ok := d.bufPool.Get().([]byte); ok && cap(p) >= n {
		buf = p[:n]
	} else {
		buf = make([]byte, n)
	}
	t0 := time.Now()
	if _, err := d.r.file.ReadAt(buf, slot.off); err != nil {
		d.putBuf(buf)
		return nil, fmt.Errorf("shuffle file truncated: chunk %d/%d extent [%d,%d): %w",
			slot.chunkIdx, d.r.numChunks, slot.off, slot.end, err)
	}
	d.stats.preadNs.Add(time.Since(t0).Nanoseconds())
	numRows := int(binary.LittleEndian.Uint32(buf[:4]))
	if numRows <= 0 || numRows > streamMaxRows {
		d.putBuf(buf)
		return nil, fmt.Errorf("shuffle chunk %d: implausible row count %d", slot.chunkIdx, numRows)
	}
	if err := validateShuffleChunkBytes(d.r.schema, numRows, buf[4:]); err != nil {
		d.putBuf(buf)
		return nil, fmt.Errorf("shuffle chunk %d: %w", slot.chunkIdx, err)
	}
	t1 := time.Now()
	b, err := decodeShuffleChunk(d.r.schema, numRows, buf[4:], slot.chunkIdx)
	d.stats.decodeNs.Add(time.Since(t1).Nanoseconds())
	d.putBuf(buf)
	return b, err
}

// next is the consumer face: strictly ordered delivery, one window credit
// per delivered slot. Called only from the source's producer goroutine.
func (d *shuffleDecodeAhead) next() (*batch.RecordBatch, error) {
	for {
		slot, ok := <-d.delivery
		if !ok {
			return nil, nil // scanner drained every promised chunk
		}
		<-slot.done
		d.credit(slot)
		if slot.err != nil {
			return nil, slot.err
		}
		d.r.delivered++
		return slot.b, nil
	}
}

// credit returns a delivered slot's window reservation and wakes the
// scanner. Error slots were never admitted (counted == false). Called only
// from next() (the single consumer goroutine), which is what makes the
// drop cursor's ascending-watermark contract hold.
func (d *shuffleDecodeAhead) credit(slot *shuffleDecodeSlot) {
	if !slot.counted {
		return
	}
	slot.counted = false
	d.mu.Lock()
	d.inflight -= slot.est
	d.undelivered--
	d.cond.Broadcast()
	d.mu.Unlock()
	if d.dropCursor != nil && slot.end > 0 {
		d.dropCursor.Advance(slot.end)
	}
}

// fail emits a terminal, pre-resolved error slot. The delivery queue is
// FIFO, so every previously staged chunk still delivers first — the error
// surfaces at the exact position the serial reader would have raised it.
func (d *shuffleDecodeAhead) fail(err error) {
	slot := &shuffleDecodeSlot{err: err, done: make(chan struct{})}
	close(slot.done)
	select {
	case d.delivery <- slot:
	case <-d.quit:
	}
}

// retireUndispatched releases a slot that reached neither queue fully.
func (d *shuffleDecodeAhead) retireUndispatched(slot *shuffleDecodeSlot) {
	if slot.token {
		d.tokens.Release(1)
		slot.token = false
	}
	slot.err = fmt.Errorf("shuffle decode-ahead: reader closed")
	close(slot.done)
}

// stop quiesces the pipeline: wakes a parked scanner, joins scanner and
// workers, then releases tokens still attached to undecoded queued slots.
// Called from Close AFTER the transport body is closed (which unblocks a
// scanner parked in a read syscall). Idempotent via r.closed.
func (d *shuffleDecodeAhead) stop() {
	close(d.quit)
	d.mu.Lock()
	d.stopped = true
	d.cond.Broadcast()
	d.mu.Unlock()
	d.wg.Wait()
	for {
		select {
		case slot, ok := <-d.jobs:
			if !ok {
				return
			}
			if slot.token {
				d.tokens.Release(1)
				slot.token = false
			}
		default:
			return
		}
	}
}

// putBuf returns a staging buffer to the pool. Oversized buffers are
// dropped so one pathological chunk cannot pin its high-water mark in the
// pool for the reader's lifetime.
func (d *shuffleDecodeAhead) putBuf(buf []byte) {
	const maxPooled = 64 << 20
	if buf == nil || cap(buf) > maxPooled {
		return
	}
	d.bufPool.Put(buf[:0])
}
