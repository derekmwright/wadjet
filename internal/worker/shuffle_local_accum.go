package worker

import (
	"os"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// sinkLocalAccumEnabled gates consumer-local partition pre-accumulation in
// partitionedShuffleSink. Post-join shuffle sinks see a stream of tiny
// consumes — the q08 join-6 profile (width-growcap memo §2) measured ~13
// surviving rows per consume across ~100K consumes, paying the partition
// scatter reset plus a per-partition lock acquire and bulk-append setup for
// ~1-row slices, 24–28s cumulative sink_ms per task. Small consumes now
// append lock-free into a consumer-local per-partition slab and only touch
// the shared partition writers when a slab fills (localAccumFlushBytes), so
// lock traffic and append setup amortize over hundreds of rows.
// WADJET_SINK_LOCAL_ACCUM=0 is the kill switch.
var sinkLocalAccumEnabled = os.Getenv("WADJET_SINK_LOCAL_ACCUM") != "0"

// localAccumFlushBytes is the per-partition slab size at which a local slab
// drains into the shared partition accumulator. A quarter of the shared
// flush threshold: big enough to amortize the lock (hundreds of rows per
// drain vs ~1 before), small enough that resident slab memory stays modest —
// numParts × 16 KB × concurrent consumers (≤ GOMAXPROCS), ~6 MB per shuffle
// task at 24 partitions on a 16-core worker.
const localAccumFlushBytes = flushPartitionBytes / 4

// localAccum is one consumer's set of per-partition pre-accumulation slabs.
// A Consume call checks one out for its duration (acquireLocal/releaseLocal);
// slabs carry buffered rows BETWEEN consumes, which is why the freelist is
// explicit and registered rather than a sync.Pool — a pool entry dropped by
// GC would silently lose rows. Finalize drains every registered set.
type localAccum struct {
	bufs     []*batch.RecordBatch // per partition, lazily allocated
	bufRows  []int
	bufBytes []int
	idx      []uint32 // identity row-index scratch for draining ([0,1,2,...])
}

// rowIdx returns the identity index slice [0..n) used to bulk-append a
// slab's full contents.
func (la *localAccum) rowIdx(n int) []uint32 {
	for len(la.idx) < n {
		la.idx = append(la.idx, uint32(len(la.idx)))
	}
	return la.idx[:n]
}

// acquireLocal checks a localAccum out of the freelist, creating and
// registering a new one when none is free (bounded by the number of
// concurrent Consume callers).
func (s *partitionedShuffleSink) acquireLocal() *localAccum {
	s.localsMu.Lock()
	defer s.localsMu.Unlock()
	if n := len(s.localFree); n > 0 {
		la := s.localFree[n-1]
		s.localFree = s.localFree[:n-1]
		return la
	}
	la := &localAccum{
		bufs:     make([]*batch.RecordBatch, s.numParts),
		bufRows:  make([]int, s.numParts),
		bufBytes: make([]int, s.numParts),
	}
	s.localAll = append(s.localAll, la)
	return la
}

func (s *partitionedShuffleSink) releaseLocal(la *localAccum) {
	s.localsMu.Lock()
	s.localFree = append(s.localFree, la)
	s.localsMu.Unlock()
}

// consumeLocalAccum is the small-consume path: scatter rows into the checked-
// out localAccum's slabs (no locks), draining any slab that crossed
// localAccumFlushBytes into its shared partition writer in one bulk append.
func (s *partitionedShuffleSink) consumeLocalAccum(b *batch.RecordBatch, sc *consumeScratch) error {
	la := s.acquireLocal()
	defer s.releaseLocal(la)
	for p := 0; p < s.numParts; p++ {
		rows := sc.perPartRows[p]
		if len(rows) == 0 {
			continue
		}
		if la.bufs[p] == nil {
			la.bufs[p] = newShuffleAccumBatch(b.Schema, len(rows))
		}
		la.bufBytes[p] += appendBatchRowsBulk(la.bufs[p], b, rows)
		la.bufRows[p] += len(rows)
		if la.bufBytes[p] >= localAccumFlushBytes {
			if err := s.drainLocalPartition(la, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// drainLocalPartition bulk-appends slab p's rows into the shared partition
// accumulator (one lock acquire for the whole slab) and resets the slab in
// place. No-op on an empty slab.
func (s *partitionedShuffleSink) drainLocalPartition(la *localAccum, p int) error {
	lb := la.bufs[p]
	nr := la.bufRows[p]
	if lb == nil || nr == 0 {
		return nil
	}
	if err := s.appendAndMaybeFlush(p, lb, la.rowIdx(nr)); err != nil {
		return err
	}
	resetShuffleAccumBatch(lb)
	la.bufRows[p] = 0
	la.bufBytes[p] = 0
	return nil
}

// drainAllLocals flushes every registered localAccum's residual rows into
// the shared partition accumulators. Called from Finalize under the
// no-Consume-after-Finalize pipeline contract, so no slab is concurrently
// appended to.
func (s *partitionedShuffleSink) drainAllLocals() error {
	s.localsMu.Lock()
	locals := append([]*localAccum(nil), s.localAll...)
	s.localsMu.Unlock()
	for _, la := range locals {
		for p := 0; p < s.numParts; p++ {
			if err := s.drainLocalPartition(la, p); err != nil {
				return err
			}
		}
	}
	return nil
}
