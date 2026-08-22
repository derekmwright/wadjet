package worker

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
)

// Producer-local row accumulation for unpartitionedStageSink.
//
// Before this, Consume held the sink mutex across appendBatchRowsBulk: the
// row COPY itself ran inside the critical section, so every producer
// goroutine feeding a stage output serialized on one lock for the duration
// of a full batch copy. The first SF100 block/mutex profiles (2026-08-21)
// measured unpartitionedStageSink.Consume at 32.6% of all worker mutex
// delay, 95.8% of it in the real sync.Mutex.Unlock handoff, 39-44s of mutex
// delay per worker per suite run. The floor on total serialized time is the
// total copy time, so amortizing lock ACQUISITIONS cannot fix it — the copy
// has to leave the critical section.
//
// partitionedShuffleSink had the identical shape (appendAndMaybeFlush was
// 64% of worker mutex block before e50fd1b, 0.8% after) and its fix is the
// template: a consumer appends into a checked-out local buffer with no
// shared lock and touches shared state only at a handoff boundary. The
// unpartitioned sink goes one step further than that template. There is
// exactly ONE output stream here (not numParts accumulators), so a filled
// local slab is written as its own chunk instead of being copied a second
// time into a shared accumulator: the sink lock is then only ever held to
// hand over stream ownership and bump counters, never to copy a row.
//
// Chunk sizing is unchanged. A slab flushes at the sink's own thresholds
// (flushRows / flushBytesT), so a serial producer produces byte-identical
// chunk boundaries to the pre-change accumulator — the coalescing decision
// stays the SINK's, exactly as docs/design/morsel-execution.md §4.1.1
// requires (chunk-per-consume fragmented stage output ~100x and cost 15% of
// the SF10 suite). The one new bound is stageSlabBudgetFactor below.
var stageSinkAccum = optswitch.Register("stage-sink-accum", "WADJET_STAGE_SINK_ACCUM",
	"producer-local accumulation in the unpartitioned stage sink (row copy outside the sink lock)")

// stageSlabBudgetFactor bounds the sink's total buffered rows as a multiple
// of one chunk's byte threshold. Pre-change the sink held at most two
// accumulators (the double-buffered flush), i.e. 2x flushBytesT ~ 32 MB;
// with one slab per concurrent consumer the natural bound is instead
// GOMAXPROCS x flushBytesT. The budget caps it: a slab whose append pushes
// the sink's buffered total past factor x flushBytesT flushes early, so
// worst-case residency is 4x flushBytesT (~64 MB per sink, 2x the old peak)
// and chunks shrink only when high consumer parallelism meets wide rows —
// precisely the case where the old bound would have been blown instead.
// Narrow schemas never reach it: they trip flushRows (64k rows) first.
const stageSlabBudgetFactor = 4

// stageSlab is one consumer's accumulation buffer. A Consume call checks a
// slab out for its duration (acquireSlab/releaseSlab); slabs carry buffered
// rows BETWEEN consumes, which is why the freelist is explicit and
// registered rather than a sync.Pool — an entry dropped by GC would
// silently lose rows. Finalize drains every registered slab.
type stageSlab struct {
	buf   *batch.RecordBatch
	rows  int
	bytes int
	idx   []uint32 // identity row-index scratch ([0,1,2,...])
}

// rowIdx returns the identity index slice [0..n) used to append a dense
// (Sel-free) batch and to re-read a full slab.
func (sl *stageSlab) rowIdx(n int) []uint32 {
	for len(sl.idx) < n {
		sl.idx = append(sl.idx, uint32(len(sl.idx)))
	}
	return sl.idx[:n]
}

// acquireSlab checks a slab out of the freelist, creating and registering a
// new one when none is free (so the slab count settles at the peak number
// of simultaneous Consume callers, bounded by GOMAXPROCS). LIFO reuse keeps
// a serial producer on one slab, which is what makes serial output
// byte-identical to the pre-change accumulator.
func (s *unpartitionedStageSink) acquireSlab() *stageSlab {
	s.slabsMu.Lock()
	defer s.slabsMu.Unlock()
	if n := len(s.slabFree); n > 0 {
		sl := s.slabFree[n-1]
		s.slabFree = s.slabFree[:n-1]
		return sl
	}
	sl := &stageSlab{}
	s.slabAll = append(s.slabAll, sl)
	return sl
}

func (s *unpartitionedStageSink) releaseSlab(sl *stageSlab) {
	s.slabsMu.Lock()
	s.slabFree = append(s.slabFree, sl)
	s.slabsMu.Unlock()
}

// consumeSlab is the producer-local path: copy the active rows into a
// checked-out slab with no sink lock held, and write the slab as a chunk
// when it reaches a flush threshold. Flat schemas only (appendBatchRowsBulk
// has no nested arms) — the caller gates on batchSchemaIsFlat.
func (s *unpartitionedStageSink) consumeSlab(b *batch.RecordBatch, n int) error {
	if s.closed.Load() {
		return fmt.Errorf("unpartitionedStageSink: Consume after Close")
	}
	if b.HasViews() {
		// Own-null view columns (outer-join fill) can't ride
		// appendBatchRowsBulk's row translation — flatten them. b is this
		// call's private batch (caller contract, the same one the copy
		// itself relies on), so the in-place mutation is single-threaded,
		// as in consumeDirectChunk.
		for _, col := range b.Columns {
			if col.IsView() && col.Nulls.HasNulls() {
				col.Flatten()
				exec.LateMatFlattens.Add(1)
			}
		}
		// Warm the lazily-memoized HasNulls of every surviving view's BASE
		// under the sink lock. A post-join view's base is the join's build
		// batch, which IS shared across concurrent consumers, and
		// Bitmap.HasNulls memoizes into a plain field (bitmap.go) — it is
		// the one piece of state the out-of-lock append reads that is not
		// private to this call, and it stayed race-free before only because
		// the whole append was serialized. The hold is a handful of cached
		// loads, not a copy.
		s.mu.Lock()
		for _, col := range b.Columns {
			if col.Base != nil {
				_ = col.Base.Nulls.HasNulls()
			}
		}
		s.mu.Unlock()
	}

	sl := s.acquireSlab()
	if sl.buf == nil {
		sl.buf = newShuffleAccumBatch(b.Schema, n)
	}
	rows := b.Sel
	if rows == nil {
		rows = sl.rowIdx(n)
	}
	added := appendBatchRowsBulk(sl.buf, b, rows)
	sl.rows += n
	sl.bytes += added
	// Charge the sink's buffered-byte accounting at accumulate time, not at
	// handoff: the budget below is what keeps per-sink residency bounded
	// now that buffers are per consumer.
	s.totalRows.Add(int64(n))
	buffered := s.bufferedBytes.Add(int64(added))

	if sl.rows < s.flushRows && sl.bytes < s.flushBytesT &&
		buffered < int64(s.flushBytesT)*stageSlabBudgetFactor {
		s.releaseSlab(sl)
		return nil
	}
	err := s.writeSlabChunk(sl)
	s.releaseSlab(sl)
	return err
}

// writeSlabChunk writes a full slab as one chunk and resets it. The lock is
// taken only to acquire stream ownership (the flushing flag admits exactly
// one writer, since the shuffleWriter and its bufio are single-threaded)
// and to bump counters; the encode + write runs outside it, so other
// consumers keep appending into their own slabs meanwhile.
func (s *unpartitionedStageSink) writeSlabChunk(sl *stageSlab) error {
	if sl.buf == nil || sl.rows == 0 {
		return nil
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return fmt.Errorf("unpartitionedStageSink: Consume after Close")
	}
	// Wait for stream ownership BEFORE the lazy writer init: writeHeader
	// touches bufFile, which an in-flight writer owns outside the lock.
	for s.flushing {
		s.flushCond.Wait()
	}
	if s.writer == nil {
		s.writer = newShuffleWriter(s.bufFile, sl.buf.Schema)
		if err := s.writer.writeHeader(); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("writing wshf header: %w", err)
		}
	}
	if s.coalesce == 0 {
		s.coalesce = 1 // flat schema, per the caller's gate
	}
	s.flushing = true
	w := s.writer
	s.mu.Unlock()

	err := w.writeChunk(sl.buf.Columns, nil, sl.rows)

	s.mu.Lock()
	s.flushing = false
	s.flushCond.Broadcast()
	if err == nil {
		s.numChunks++
	}
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("writing wshf chunk: %w", err)
	}
	s.bufferedBytes.Add(-int64(sl.bytes))
	resetShuffleAccumBatch(sl.buf)
	sl.rows, sl.bytes = 0, 0
	return nil
}

// drainAllSlabs writes every registered slab's residual rows as chunks.
// Called from Finalize under the no-Consume-after-Finalize pipeline
// contract, so no slab is concurrently appended to. This is the drain that
// makes row loss on finalize impossible; TestUnpartitionedStageSink_SlabFinalizeDrain
// is its regression gate.
func (s *unpartitionedStageSink) drainAllSlabs() error {
	s.slabsMu.Lock()
	slabs := append([]*stageSlab(nil), s.slabAll...)
	s.slabsMu.Unlock()
	for _, sl := range slabs {
		if err := s.writeSlabChunk(sl); err != nil {
			return err
		}
	}
	return nil
}
