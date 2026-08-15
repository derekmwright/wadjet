package worker

import (
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
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
	// shuffleDecodeAheadWorkersDefault is the decode worker ceiling per
	// reader; actual width is governed per chunk by the CPU-token pool.
	shuffleDecodeAheadWorkersDefault = 4

	// shuffleDecodeAheadMinChunks gates engagement: below this there is
	// nothing to overlap (keeps the low-volume gather/reply class serial).
	shuffleDecodeAheadMinChunks = 4
)

// shuffleDecodeAheadWindowBytes bounds staged+decoded-but-undelivered
// bytes per reader (charged at exact staged size — WSHF is uncompressed,
// decoded ≈ wire bytes). The morsel dispenser's 512 MiB budget bounds
// the next stage downstream, exactly as before. Package-level var so
// tests can shrink it.
var shuffleDecodeAheadWindowBytes int64 = 128 << 20

// shuffleDecodeAheadStats is the executor-owned counter sink shared by every
// reader (readers add directly; no per-reader fold step).
type shuffleDecodeAheadStats struct {
	chunks       atomic.Int64
	windowFullNs atomic.Int64
	tokenStallNs atomic.Int64
	pressureNs   atomic.Int64
	stageNs      atomic.Int64
	decodeNs     atomic.Int64
}

// shuffleDecodeSlot is one staged chunk moving through the pipeline: the
// scanner fills numRows/buf/est and sends it to the ordered delivery queue
// and the work queue; a worker fills b/err and closes done; next() waits on
// done in delivery order.
type shuffleDecodeSlot struct {
	chunkIdx uint32
	numRows  int
	buf      []byte
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
	// delivery and on stop, waking a scanner parked in admit.
	mu          sync.Mutex
	cond        *sync.Cond
	inflight    int64
	undelivered int
	limit       int64
	stopped     bool
}

// startDecodeAhead switches r from serial Next to chunk-parallel decode.
// Must be called before the first Next; no-ops (leaving r serial) when the
// file is too small to benefit. tokens/pressure may be nil (unbudgeted).
func (r *streamingShuffleReader) startDecodeAhead(workers int, tokens *cpuTokens, pressure func() bool, strict bool, stats *shuffleDecodeAheadStats) {
	if r.numChunks < shuffleDecodeAheadMinChunks {
		return
	}
	if workers <= 0 {
		workers = shuffleDecodeAheadWorkersDefault
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
	d.wg.Add(1 + workers)
	go func() {
		defer d.wg.Done()
		d.scan()
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
			return false, false
		}
		if d.undelivered == 0 {
			d.inflight += est
			d.undelivered++
			return false, true
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
				d.timedWait(&d.stats.pressureNs)
				continue
			}
		}
		if d.tokens != nil {
			if d.tokens.TryAcquire(1) == 0 {
				d.timedWait(&d.stats.tokenStallNs)
				continue
			}
			token = true
		}
		d.inflight += est
		d.undelivered++
		return token, true
	}
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
			t0 := time.Now()
			b, err := decodeShuffleChunk(d.r.schema, slot.numRows, slot.buf, slot.chunkIdx)
			d.stats.decodeNs.Add(time.Since(t0).Nanoseconds())
			d.putBuf(slot.buf)
			slot.buf = nil
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
// scanner. Error slots were never admitted (counted == false).
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
