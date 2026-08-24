package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/diskio"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// partitionedShuffleSink is an exec.Sink that hash-partitions incoming batches
// into N output .wshf files, one per partition. Each partition's writer flushes
// its accumulated rows once a per-partition buffer threshold is reached, so
// peak memory is bounded by (N partitions × 2 × flush threshold — accumulator
// plus its ping-pong spare), independent of the total input size.
//
// This is the build-side and probe-side output sink for the shuffle execution
// path. The N output files are uploaded by the executor to S3 under a stable
// per-partition prefix, and downstream join tasks read all files at their
// assigned partition prefix via partitionShardSource.
//
// Concurrency model: the lock is PER PARTITION, not sink-wide. Hashing and
// row scatter are per-call private state (pooled scratch), so concurrent
// Consume calls — morsel-parallel fragment consumers, or exec.Pipeline's
// parallel workers — contend only when appending to the SAME partition. The
// previous sink-wide mutex serialized the entire consume (hash + scatter +
// append + flush) across k consumers, which is what kept join/probe
// fragments +12-27% slower under morsel-auto at SF100 (2026-07-07
// default-flip gate) — the sink was the fragment's dominant cost and only
// one consumer could be inside it.
//
// Large consume slices additionally take the DIRECT-CHUNK path (see
// writeDirectChunk and docs/design/sink-direct-chunk.md): a per-partition
// slice whose estimated bytes already exceed the flush threshold skips the
// accumulator and encodes straight from the source batch into the partition
// stream OUTSIDE pw.mu, guarded by pw.flushing. The 2026-08-12 SF100 block
// profile showed the per-partition locks themselves as 64.3% of all worker
// mutex block time — nearly all of it concurrent 64K-row morsel consumes
// holding a lock for the accumulator copy plus encode. With the direct path
// the lock covers only counter updates for those slices, and the row data is
// copied once (source→wire) instead of twice (source→accumulator→wire).
// WADJET_SINK_DIRECT_CHUNK=0 is the kill switch.
type partitionedShuffleSink struct {
	spillDir   string
	keys       []string // partition key column names
	numParts   int
	schema     []parquet.Column
	flushBytes int // per-partition row buffer flush threshold

	parts     []*partitionWriter // immutable after Init; per-partition state guarded by partitionWriter.mu
	closed    atomic.Bool
	finalized atomic.Bool

	// Partition-key column indices, resolved once against the first batch.
	keyOnce sync.Once
	keyIdxs []int
	keyErr  error

	// Per-consume scratch (partition assignment, hash accumulator, scatter
	// lists). Pooled because concurrent Consume calls each need private
	// scratch; a sink-level buffer would reintroduce the global lock.
	scratchPool sync.Pool

	// Consumer-local pre-accumulation slabs (shuffle_local_accum.go).
	// Explicit freelist + registry, NOT a sync.Pool: slabs hold row data
	// between consumes, and a GC-dropped pool entry would lose rows.
	// localAll is drained by Finalize.
	localsMu  sync.Mutex
	localFree []*localAccum
	localAll  []*localAccum

	// Per-phase attribution counters (atomic ns), logged once per task by
	// fragmentExchangeSink.finalize as "shuffle sink phases". Added
	// 2026-08-15 after two sink-lock theories (acquire count e50fd1b,
	// encode-under-lock f47a6e8) each failed to move q08 join-6's ~26s/task
	// sink_ms — the counters say where a consume's ~260µs mean actually
	// goes instead of a third inference.
	phaseFlattenNs atomic.Int64 // view normalization at Consume top
	phaseHashNs    atomic.Int64 // partition hashing + row scatter
	phaseAppendNs  atomic.Int64 // slab/accumulator appends incl. lock waits
	phaseEncodeNs  atomic.Int64 // chunk encode+write (outside mu since f47a6e8)
	consumeCalls   atomic.Int64
	consumeRows    atomic.Int64
}

// consumeScratch is the per-Consume private working set.
type consumeScratch struct {
	partScratch []int      // per-row partition assignment
	hashScratch []uint64   // per-row uint64 hash accumulator
	perPartRows [][]uint32 // per-partition source-row indices
}

// partitionWriter holds the open file handle and incremental state for one
// output partition. mu guards ALL mutable fields; the file handle itself is
// immutable after Init. Chunks written by different Consume calls may
// interleave within a partition file — a .wshf file is a self-contained
// chunk sequence, so interleaving is valid as long as individual chunk
// writes are serialized. Two exclusion domains provide that: mu guards the
// accumulator (rowBuf and counters) and any writer access made while
// holding it; flushing marks a direct-chunk writer streaming into
// writer/bufFile OUTSIDE mu (set and cleared under mu, waiters park on
// flushCond). Accumulator flushes and writer lazy-init wait for !flushing
// before touching the stream.
type partitionWriter struct {
	mu        sync.Mutex
	flushing  bool       // a direct-chunk writer owns writer/bufFile outside mu
	flushCond *sync.Cond // signaled when a direct-chunk write completes

	file    *os.File
	bufFile *bufio.Writer // wraps file so the many small Writes from
	// shuffleWriter coalesce into syscall-sized
	// chunks. Pre-2026-04-30 the shuffleWriter
	// emitted ~1 syscall per non-null bytes-row,
	// and that was 90%+ of shuffle CPU.
	writer   *shuffleWriter
	rowBuf   *batch.RecordBatch // accumulator: rows destined for this partition
	bufRows  int                // rows currently in rowBuf
	bufBytes int                // approximate byte count of buffered rows
	numRows  int64              // total rows written to disk
	// spareBuf is the ping-pong partner of rowBuf: a threshold flush
	// detaches the full accumulator under mu, installs spareBuf (or nil —
	// appendRowsBulkLocked lazily allocates), and encodes the detached
	// batch OUTSIDE mu under the flushing handoff. The encoded batch is
	// recycled back here. Without the swap, the 64KB chunk encode ran
	// inside mu and every consumer targeting the partition blocked behind
	// it — measured at ~6.5ms mean Consume latency on q08's join-6
	// (2026-08-15 sink_ms counter, unmoved by lock-count reduction alone).
	spareBuf *batch.RecordBatch
}

// flushPartitionBytes is the per-partition accumulator size (in approx bytes
// of row data) at which we flush a chunk to disk. 64 KB keeps memory bounded:
// 4N partitions × 64 KB = ~768 KB per shuffle task at N=3.
const flushPartitionBytes = 64 * 1024

// shuffleBurstGateRows is the consume size below which per-partition appends
// run inline instead of fanning out an errgroup burst (see Consume).
const shuffleBurstGateRows = 4096

func newPartitionedShuffleSink(spillDir string, keys []string, numParts int, schema []parquet.Column) *partitionedShuffleSink {
	return &partitionedShuffleSink{
		spillDir:   spillDir,
		keys:       keys,
		numParts:   numParts,
		schema:     schema,
		flushBytes: flushPartitionBytes,
		parts:      make([]*partitionWriter, numParts),
	}
}

// AcceptsViews: Consume normalizes view columns itself (key and own-null
// columns flatten single-threaded before the per-partition fan-out; the
// rest serialize through appendBatchRowsBulk's row translation, copy-free),
// so the pipeline's defensive flatten is unnecessary.
func (s *partitionedShuffleSink) AcceptsViews() bool { return true }

// Init creates the per-partition spill files.
func (s *partitionedShuffleSink) Init(_ context.Context) error {
	if s.spillDir == "" {
		return fmt.Errorf("partitionedShuffleSink: spillDir empty")
	}
	if err := os.MkdirAll(s.spillDir, 0o755); err != nil {
		return fmt.Errorf("creating spill dir: %w", err)
	}
	for p := 0; p < s.numParts; p++ {
		path := filepath.Join(s.spillDir, fmt.Sprintf("part-%04d.wshf", p))
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("creating partition %d: %w", p, err)
		}
		pw := &partitionWriter{file: f}
		pw.flushCond = sync.NewCond(&pw.mu)
		s.parts[p] = pw
	}
	return nil
}

// Consume hash-partitions each row in b into its target partition, appending
// to that partition's row buffer. Buffers are flushed when they exceed
// flushBytes worth of accumulated rows. Safe for concurrent calls: hashing
// and scatter use per-call pooled scratch, and partition state is guarded by
// per-partition locks (see the type comment for the concurrency model). Each
// call operates on its own batch b — callers never share a batch across
// concurrent Consume calls (morsel views and pipeline batches are
// single-owner by contract).
func (s *partitionedShuffleSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	if s.closed.Load() {
		return fmt.Errorf("partitionedShuffleSink: Consume after Close")
	}

	// Resolve key column indices from schema on the first batch. Use the
	// bidirectional fallback so qualified planner-emitted keys ("n1.n_name")
	// still resolve against unqualified scan output ("n_name") and
	// vice-versa for self-join chain output. All batches share one schema
	// (pipeline guarantee), so first-batch resolution binds for all.
	s.keyOnce.Do(func() {
		idxs := make([]int, len(s.keys))
		for i, k := range s.keys {
			idx := exec.ColumnIndexFallback(b, k)
			if idx < 0 {
				s.keyErr = fmt.Errorf("partitioned shuffle: key %q not in schema", k)
				return
			}
			idxs[i] = idx
		}
		s.keyIdxs = idxs
	})
	if s.keyErr != nil {
		return s.keyErr
	}

	if b.HasViews() {
		// Normalize views single-threaded, before the per-partition goroutine
		// fan-out: the hash pass below reads key columns positionally, and
		// own-null views (outer-join fill) can't ride appendBatchRowsBulk's
		// per-column row translation — flattening either inside the burst
		// goroutines would race on the shared vectors. Remaining views defer
		// nulls to their base and serialize through the translation, copy-free.
		tFlat := time.Now()
		for _, ki := range s.keyIdxs {
			if b.Columns[ki].IsView() {
				b.FlattenColumn(ki)
				exec.LateMatFlattens.Add(1)
			}
		}
		for _, col := range b.Columns {
			if col.IsView() && col.Nulls.HasNulls() {
				col.Flatten()
				exec.LateMatFlattens.Add(1)
			}
		}
		s.phaseFlattenNs.Add(time.Since(tFlat).Nanoseconds())
	}

	// Vectorized partition assignment: one pass per key column over the
	// active rows, mixing into a per-row hash accumulator. Pre-2026-04-30
	// this code did `fnv.New64a()` per row → 1M heap allocations of the
	// hasher struct on a SF1 lineitem shuffle. Inlined FNV-1a here is
	// allocation-free. Scratch is pooled per call so concurrent consumers
	// never share it.
	n := b.ActiveLen()
	s.consumeCalls.Add(1)
	s.consumeRows.Add(int64(n))
	tHash := time.Now()
	sc, _ := s.scratchPool.Get().(*consumeScratch)
	if sc == nil {
		sc = &consumeScratch{perPartRows: make([][]uint32, s.numParts)}
	}
	defer s.scratchPool.Put(sc)
	if cap(sc.partScratch) < n {
		sc.partScratch = make([]int, n)
	}
	if cap(sc.hashScratch) < n {
		sc.hashScratch = make([]uint64, n)
	}
	parts := sc.partScratch[:n]
	hashRowsIntoPartitions(b, s.keyIdxs, s.numParts, sc.hashScratch[:n], parts)

	// Group rows by partition so each partition's columns are appended in
	// one bulk pass with the type switch hoisted outside the row loop. The
	// alternative (per-row appendRow) does a 13-arm type switch per column
	// per row, which on Q03 SF1 was ~64M switch dispatches for the lineitem
	// shuffle and was the dominant wall-time cost.
	for p := range sc.perPartRows {
		sc.perPartRows[p] = sc.perPartRows[p][:0]
	}
	if b.Sel != nil {
		for i := 0; i < n; i++ {
			sc.perPartRows[parts[i]] = append(sc.perPartRows[parts[i]], b.Sel[i])
		}
	} else {
		for i := 0; i < n; i++ {
			sc.perPartRows[parts[i]] = append(sc.perPartRows[parts[i]], uint32(i))
		}
	}

	s.phaseHashNs.Add(time.Since(tHash).Nanoseconds())

	// Direct-chunk gate input: estimated bytes per row, so each partition
	// slice can decide accumulator vs direct encode. 0 = direct path off.
	rowBytes := 0
	if sinkDirectChunkEnabled {
		rowBytes = approxRowBytes(b)
	}

	// Pre-warm the source batch's lazy Bitmap.HasNulls() cache on every
	// column before per-partition appends can read it from multiple
	// goroutines: HasNulls memoizes its result on first call
	// (bitmap.go:127-158), and parallel readers calling HasNulls on the same
	// column race on that write. This call's batch is private to this call,
	// so the single-threaded touch here covers both the burst goroutines
	// below and nothing else — concurrent Consume calls carry their own
	// batches.
	for _, col := range b.Columns {
		_ = col.Nulls.HasNulls()
		if col.Base != nil {
			// View columns read their base's bitmap in the per-partition
			// appends — warm that memoization too.
			_ = col.Base.Nulls.HasNulls()
		}
	}

	// Small consumes skip the goroutine fan-out below: with ~24 partitions a
	// 2048-row batch leaves ~85 rows per partition, and spawning+joining up
	// to GOMAXPROCS goroutines per Consume call costs more than the appends
	// themselves. Morsel-parallel fragments (docs/design/morsel-execution.md
	// §4.1.1) consume at morsel granularity, so this path is hot there; the
	// SF10 v1.5 A/B (2026-07-03) measured the per-consume burst as a
	// wall-time serializer.
	if n < shuffleBurstGateRows {
		// Consumer-local pre-accumulation (shuffle_local_accum.go): tiny
		// post-join consumes append lock-free into a checked-out slab set
		// and touch the shared partition writers only when a slab fills.
		tApp := time.Now()
		var err error
		if sinkLocalAccumEnabled {
			err = s.consumeLocalAccum(b, sc)
		} else {
			for p := 0; p < s.numParts; p++ {
				if len(sc.perPartRows[p]) == 0 {
					continue
				}
				if err = s.appendPartition(p, b, sc.perPartRows[p], rowBytes); err != nil {
					break
				}
			}
		}
		s.phaseAppendNs.Add(time.Since(tApp).Nanoseconds())
		return err
	}

	// Each partitionWriter is fully independent (own file handle, bufio.Writer,
	// shuffleWriter, rowBuf). Run per-partition append + threshold-flush in
	// parallel: 2026-05-22 SF100 Q18 profile showed appendRowsBulk 84.5s cum +
	// flushIfNeeded 54.8s cum, all serial in this loop, while workers used only
	// ~9% of 16 available cores. Bounded at min(numParts, GOMAXPROCS) to avoid
	// goroutine explosion on hot shuffles.
	limit := s.numParts
	if gp := runtime.GOMAXPROCS(0); limit > gp {
		limit = gp
	}
	tApp := time.Now()
	g := new(errgroup.Group)
	g.SetLimit(limit)
	for p := 0; p < s.numParts; p++ {
		if len(sc.perPartRows[p]) == 0 {
			continue
		}
		p := p
		g.Go(func() error {
			return s.appendPartition(p, b, sc.perPartRows[p], rowBytes)
		})
	}
	err := g.Wait()
	s.phaseAppendNs.Add(time.Since(tApp).Nanoseconds())
	return err
}

// appendPartition routes one partition's row slice: a slice whose estimated
// bytes already exceed the flush threshold would only pass through the
// accumulator to be flushed immediately, so it encodes directly from b into
// the partition stream instead (writeDirectChunk); everything else takes the
// locked accumulator path. rowBytes == 0 means the direct path is disabled
// (WADJET_SINK_DIRECT_CHUNK=0).
func (s *partitionedShuffleSink) appendPartition(p int, b *batch.RecordBatch, rows []uint32, rowBytes int) error {
	if rowBytes > 0 && len(rows)*rowBytes >= s.flushBytes {
		return s.writeDirectChunk(p, b, rows)
	}
	return s.appendAndMaybeFlush(p, b, rows)
}

// writeDirectChunk encodes rows of b as one complete chunk straight into
// partition p's stream, bypassing the accumulator. The expensive work — the
// row gather and chunk encode in writeChunk — runs OUTSIDE pw.mu; exclusive
// ownership of writer/bufFile during that window is marked by pw.flushing
// (accumulator flushes and other direct writers wait on pw.flushCond). The
// mutex is held only to acquire/release that ownership and bump counters.
//
// Safe against the source batch: b is this Consume call's private batch
// (caller contract), Consume pre-warmed every column's HasNulls memoization
// and flattened own-null views before the fan-out, and writeChunk reads
// remaining view columns through composed selections without mutation.
func (s *partitionedShuffleSink) writeDirectChunk(p int, b *batch.RecordBatch, rows []uint32) error {
	pw := s.parts[p]
	pw.mu.Lock()
	for pw.flushing {
		pw.flushCond.Wait()
	}
	if err := s.ensureWriterLocked(p); err != nil {
		pw.mu.Unlock()
		return err
	}
	w := pw.writer
	pw.flushing = true
	pw.mu.Unlock()

	tEnc := time.Now()
	err := w.writeChunk(b.Columns, rows, len(rows))
	s.phaseEncodeNs.Add(time.Since(tEnc).Nanoseconds())

	pw.mu.Lock()
	pw.flushing = false
	pw.flushCond.Broadcast()
	if err == nil {
		pw.numRows += int64(len(rows))
	}
	pw.mu.Unlock()
	if err != nil {
		return fmt.Errorf("partition %d direct chunk: %w", p, err)
	}
	return nil
}

// appendAndMaybeFlush appends rows to partition p's accumulator under that
// partition's lock. When the byte threshold trips, the full accumulator is
// DETACHED (spareBuf swapped in) and encoded outside mu under the flushing
// stream handoff, so concurrent appends to the same partition proceed during
// the chunk encode+write. Pre-swap, the encode ran inside mu and every
// consumer targeting a flushing partition blocked for the encode's duration
// — the dominant q08 join-6 sink cost. (Slices big enough to flush on their
// own bypass this via writeDirectChunk — see appendPartition.)
func (s *partitionedShuffleSink) appendAndMaybeFlush(p int, b *batch.RecordBatch, rows []uint32) error {
	pw := s.parts[p]
	pw.mu.Lock()
	if err := s.appendRowsBulkLocked(p, b, rows); err != nil {
		pw.mu.Unlock()
		return err
	}
	if pw.rowBuf == nil || pw.bufRows == 0 || pw.bufBytes < s.flushBytes {
		pw.mu.Unlock()
		return nil
	}
	// The stream may be owned by an in-flight direct-chunk writer or
	// another accumulator flusher; wait it out (Wait releases mu, so
	// appends by OTHER consumers proceed). The buffer may have been
	// swapped away while we waited — re-check the threshold after.
	for pw.flushing {
		pw.flushCond.Wait()
	}
	if pw.rowBuf == nil || pw.bufRows == 0 || pw.bufBytes < s.flushBytes {
		pw.mu.Unlock()
		return nil
	}
	if err := s.ensureWriterLocked(p); err != nil {
		pw.mu.Unlock()
		return err
	}
	full, fullRows := pw.rowBuf, pw.bufRows
	pw.rowBuf = pw.spareBuf // may be nil — appendRowsBulkLocked lazily allocates
	pw.spareBuf = nil
	pw.bufRows, pw.bufBytes = 0, 0
	w := pw.writer
	pw.flushing = true
	pw.mu.Unlock()

	tEnc := time.Now()
	err := w.writeChunk(full.Columns, nil, fullRows)
	s.phaseEncodeNs.Add(time.Since(tEnc).Nanoseconds())

	pw.mu.Lock()
	pw.flushing = false
	pw.flushCond.Broadcast()
	if err == nil {
		pw.numRows += int64(fullRows)
		resetShuffleAccumBatch(full)
		if pw.spareBuf == nil {
			pw.spareBuf = full // recycle for the next swap
		}
	}
	pw.mu.Unlock()
	if err != nil {
		return fmt.Errorf("partition %d chunk: %w", p, err)
	}
	return nil
}

// appendRowsBulkLocked appends `len(rows)` rows from b into the accumulator
// buffer for partition p, where rows[i] is the source-row index in b for the
// i-th destination row. Caller must hold s.parts[p].mu. The type switch is
// hoisted OUTSIDE the row loop so that each column is processed in a tight
// typed loop — on Q03 SF1 this converts ~64M switch dispatches into ~16 (one
// per column).
//
// The destination column slices are pre-grown once to fit all rows; null
// state defaults to non-null (Bitmap.Grow leaves new bits set), so we only
// emit SetNull on actual source nulls. Offsets for bytes/string columns are
// pre-grown to end+1 so BytesData.Set can write Offsets[di+1] in place
// without per-row growth.
func (s *partitionedShuffleSink) appendRowsBulkLocked(p int, b *batch.RecordBatch, rows []uint32) error {
	if len(rows) == 0 {
		return nil
	}
	pw := s.parts[p]
	if pw.rowBuf == nil {
		pw.rowBuf = newShuffleAccumBatch(b.Schema, len(rows))
	}

	bytesAdded := appendBatchRowsBulk(pw.rowBuf, b, rows)
	pw.bufRows += len(rows)
	pw.bufBytes += bytesAdded
	return nil
}

// newShuffleAccumBatch allocates an append-target accumulator batch: Len 0,
// BytesData offsets reset for incremental append. Sized by the first append's
// row count (floor 256 for tiny incoming batches) so the typical case avoids
// any growth. Shared by the partition writers' accumulators and the
// consumer-local pre-accumulators (shuffle_local_accum.go).
func newShuffleAccumBatch(schema []parquet.Column, initCap int) *batch.RecordBatch {
	if initCap < 256 {
		initCap = 256
	}
	rb := batch.NewRecordBatch(schema, initCap)
	rb.Len = 0
	for i, col := range rb.Columns {
		if batch.IsContainerType(col.Type) {
			// Container columns start EMPTY, not pre-sized: appendBatchRowsBulk
			// grows them through CopyValueFrom, whose ARRAY/ROW children are
			// append-built. NewRecordBatch pre-sizes ROW children to initCap,
			// which would put those children on the indexed-write branch at
			// rows the parent has not reached. NewVectorLike keeps the shape
			// (child element types, field names, VECTOR dim) and drops the
			// storage.
			rb.Columns[i] = batch.NewVectorLike(col)
			continue
		}
		if col.BytesData.Offsets != nil {
			col.BytesData.Offsets = col.BytesData.Offsets[:1]
			col.BytesData.Offsets[0] = 0
			col.BytesData.Data = col.BytesData.Data[:0]
		}
	}
	return rb
}

// resetShuffleAccumBatch resets an accumulator batch in place, reusing its
// memory. Mirror of the reset flushPartitionLocked performs on pw.rowBuf.
func resetShuffleAccumBatch(rb *batch.RecordBatch) {
	rb.Len = 0
	for i, col := range rb.Columns {
		col.Len = 0
		col.Nulls.ResetNonNull(0)
		switch col.Type {
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			col.BytesData.Data = col.BytesData.Data[:0]
			col.BytesData.Offsets = col.BytesData.Offsets[:1]
			col.BytesData.Offsets[0] = 0
		case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
			// Element storage is append-built at every level, so an
			// in-place reset would have to walk the whole tree; mint the
			// same shape empty instead. Container columns are off the
			// measured paths (no TPC-H or ClickBench query has one), so
			// the allocation buys correctness at no benchmark cost.
			rb.Columns[i] = batch.NewVectorLike(col)
		}
	}
}

// appendBatchRowsBulk copies the given source rows of b into dst (appending
// at dst.Len) and returns the approximate byte count added. Flat columns
// take a bulk typed arm; ARRAY/MAP/ROW/VECTOR take a row-at-a-time
// CopyValueFrom arm; anything else PANICS (see the default). Shared by the
// partition writers and the unpartitioned stage sink's chunk-coalescing
// accumulator.
func appendBatchRowsBulk(dst *batch.RecordBatch, b *batch.RecordBatch, srcRows []uint32) int {
	start := dst.Len
	end := start + len(srcRows)

	// Pre-grow all column storage to fit `end` rows in one allocation per
	// column (versus one append-of-one per row in the old code).
	growBatchTo(dst, end)

	bytesAdded := 0
	nRows := len(srcRows)

	var viewRows []uint32 // lazy scratch for view-column row translation
	for ci, srcCol := range b.Columns {
		dstCol := dst.Columns[ci]
		rows := srcRows
		if srcCol.Base != nil {
			// View column (own-null views were normalized by the caller's
			// single-threaded Consume): translate the row list through the
			// view's indices so every typed arm below reads the base
			// directly — one copy, straight into the accumulator, instead
			// of a flatten copy followed by this copy.
			if viewRows == nil {
				viewRows = make([]uint32, nRows)
			}
			for i, r := range srcRows {
				viewRows[i] = srcCol.Indices[r]
			}
			srcCol = srcCol.Base
			rows = viewRows
			exec.LateMatViewColumnsSerialized.Add(1)
		}
		hasNulls := srcCol.Nulls.HasNulls()
		switch dstCol.Type {
		case parquet.TypeBool:
			dstSlice := dstCol.BoolData[start : start+nRows]
			srcData := srcCol.BoolData
			if !hasNulls {
				for i, row := range rows {
					dstSlice[i] = srcData[row]
				}
			} else {
				for i, row := range rows {
					if srcCol.Nulls.IsNullFast(int(row)) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows

		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			dstSlice := dstCol.Int32Data[start : start+nRows]
			srcData := srcCol.Int32Data
			if !hasNulls {
				for i, row := range rows {
					dstSlice[i] = srcData[row]
				}
			} else {
				for i, row := range rows {
					if srcCol.Nulls.IsNullFast(int(row)) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 4

		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			dstSlice := dstCol.Int64Data[start : start+nRows]
			srcData := srcCol.Int64Data
			if !hasNulls {
				for i, row := range rows {
					dstSlice[i] = srcData[row]
				}
			} else {
				for i, row := range rows {
					if srcCol.Nulls.IsNullFast(int(row)) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 8

		case parquet.TypeFloat32:
			dstSlice := dstCol.Float32Data[start : start+nRows]
			srcData := srcCol.Float32Data
			if !hasNulls {
				for i, row := range rows {
					dstSlice[i] = srcData[row]
				}
			} else {
				for i, row := range rows {
					if srcCol.Nulls.IsNullFast(int(row)) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 4

		case parquet.TypeFloat64:
			dstSlice := dstCol.Float64Data[start : start+nRows]
			srcData := srcCol.Float64Data
			if !hasNulls {
				for i, row := range rows {
					dstSlice[i] = srcData[row]
				}
			} else {
				for i, row := range rows {
					if srcCol.Nulls.IsNullFast(int(row)) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 8

		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			// Bytes path: copy CONTIGUOUS RUNS of source rows with one
			// append each instead of per-row SetFrom (the 2026-07-17 SF100
			// treatment profile ranked this copy class the top addressable
			// engine work). The dense no-Sel path (unpartitionedStageSink
			// rows[i]=i) collapses to a single run per column; clustered
			// keys (lineitem l_orderkey: ~4 consecutive rows per order land
			// in one partition) coalesce ~4×; true singletons pay only one
			// extra compare per row — measured at parity. Runs break at
			// source nulls: a null position's Offsets[i+1] may be a
			// malformed descending pair (see BytesColumn.Value), so null
			// rows advance the offset individually, exactly as before.
			srcOffsets := srcCol.BytesData.Offsets
			srcData := srcCol.BytesData.Data
			dstB := &dstCol.BytesData
			for i := 0; i < nRows; {
				row := int(rows[i])
				if hasNulls && srcCol.Nulls.IsNullFast(row) {
					di := start + i
					dstCol.Nulls.SetNull(di)
					dstB.Offsets[di+1] = uint32(len(dstB.Data))
					i++
					continue
				}
				runLen := 1
				for i+runLen < nRows && int(rows[i+runLen]) == row+runLen &&
					!(hasNulls && srcCol.Nulls.IsNullFast(row+runLen)) {
					runLen++
				}
				if runLen == 1 {
					// Singleton fast path: hash-scattered layouts (unique
					// keys, post-shuffle stages) hit this every row; the
					// run machinery's fixed cost measured +33% there.
					s, e := srcOffsets[row], srcOffsets[row+1]
					dstB.Data = append(dstB.Data, srcData[s:e]...)
					dstB.Offsets[start+i+1] = uint32(len(dstB.Data))
					bytesAdded += int(e-s) + 4
					i++
					continue
				}
				runStart, runEnd := srcOffsets[row], srcOffsets[row+runLen]
				dstBase := uint32(len(dstB.Data))
				if runEnd > runStart {
					dstB.Data = append(dstB.Data, srcData[runStart:runEnd]...)
				}
				for k := 0; k < runLen; k++ {
					dstB.Offsets[start+i+k+1] = dstBase + (srcOffsets[row+k+1] - runStart)
				}
				bytesAdded += int(runEnd-runStart) + 4*runLen
				i += runLen
			}

		case parquet.TypeDecimal:
			dstSlice := dstCol.DecimalData.Data[start : start+nRows]
			srcData := srcCol.DecimalData.Data
			if !hasNulls {
				for i, row := range rows {
					dstSlice[i] = srcData[row]
				}
			} else {
				for i, row := range rows {
					if srcCol.Nulls.IsNullFast(int(row)) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 16

		case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
			// Container columns copy row-at-a-time through the engine's
			// nested-aware typed primitive: offsets, child elements, ROW
			// children and the VECTOR stride all have to advance together,
			// and CopyValueFrom is the one place that knows how (it also
			// advances a NULL row's bookkeeping, which is what keeps every
			// later row aligned). Sequential di, which the start..end walk
			// guarantees.
			//
			// Until #397 there was no arm here AND no default, so these
			// columns silently advanced dst.Len without appending anything
			// and the failure surfaced later, at the writer.
			for i, row := range rows {
				dstCol.CopyValueFrom(start+i, srcCol, int(row))
			}
			bytesAdded += nRows * 16 // rough: a gate figure, not accounting

		default:
			// A 23rd column type must not reach the silent path the four
			// container types spent #397 in. Panics rather than returns
			// because every caller is a sink Consume with no per-column
			// error seam; the worker's task-level recover turns it into a
			// query error.
			name := ""
			if ci < len(dst.Schema) {
				name = dst.Schema[ci].Name
			}
			panic(fmt.Sprintf("appendBatchRowsBulk: no arm for column %d (%s) of type %v",
				ci, name, dstCol.Type))
		}
	}

	dst.Len = end
	return bytesAdded
}

// growBatchTo ensures the destination batch has storage for n rows in each
// column. Grows in one allocation per column when capacity is exceeded
// (vs. the per-row appends in the legacy growBatch). After this call, the
// null bitmaps default to non-null in the new range — callers must explicitly
// SetNull for source rows that are null.
//
// Capacity grows GEOMETRICALLY (at least 2×): callers append a few rows at a
// time — a low-selectivity morsel-parallel probe hands the sink ~2 surviving
// rows per consume — and exact-size growth made every such append reallocate
// and copy the whole accumulator, O(n²) (q17 join-5's 55s-per-task sink
// convoy, SF100 2026-08-14).
func growBatchTo(dst *batch.RecordBatch, n int) {
	for _, col := range dst.Columns {
		col.Len = n
		switch col.Type {
		case parquet.TypeBool:
			if cap(col.BoolData) < n {
				grew := make([]bool, n, growCap(cap(col.BoolData), n))
				copy(grew, col.BoolData)
				col.BoolData = grew
			} else if len(col.BoolData) < n {
				col.BoolData = col.BoolData[:n]
			}
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			if cap(col.Int32Data) < n {
				grew := make([]int32, n, growCap(cap(col.Int32Data), n))
				copy(grew, col.Int32Data)
				col.Int32Data = grew
			} else if len(col.Int32Data) < n {
				col.Int32Data = col.Int32Data[:n]
			}
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			if cap(col.Int64Data) < n {
				grew := make([]int64, n, growCap(cap(col.Int64Data), n))
				copy(grew, col.Int64Data)
				col.Int64Data = grew
			} else if len(col.Int64Data) < n {
				col.Int64Data = col.Int64Data[:n]
			}
		case parquet.TypeFloat32:
			if cap(col.Float32Data) < n {
				grew := make([]float32, n, growCap(cap(col.Float32Data), n))
				copy(grew, col.Float32Data)
				col.Float32Data = grew
			} else if len(col.Float32Data) < n {
				col.Float32Data = col.Float32Data[:n]
			}
		case parquet.TypeFloat64:
			if cap(col.Float64Data) < n {
				grew := make([]float64, n, growCap(cap(col.Float64Data), n))
				copy(grew, col.Float64Data)
				col.Float64Data = grew
			} else if len(col.Float64Data) < n {
				col.Float64Data = col.Float64Data[:n]
			}
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			// Offsets must have len ≥ n+1 so BytesData.Set/SetFrom can write
			// Offsets[di+1] in place. The values in the grown range are
			// uninitialized — but bulk callers always write rows in
			// contiguous order from start..n-1, so each entry will be
			// overwritten by the next Set/SetFrom.
			needed := n + 1
			if cap(col.BytesData.Offsets) < needed {
				grew := make([]uint32, needed, growCap(cap(col.BytesData.Offsets), needed))
				copy(grew, col.BytesData.Offsets)
				col.BytesData.Offsets = grew
			} else if len(col.BytesData.Offsets) < needed {
				col.BytesData.Offsets = col.BytesData.Offsets[:needed]
			}
		case parquet.TypeDecimal:
			if cap(col.DecimalData.Data) < n {
				grew := make([]batch.Int128, n, growCap(cap(col.DecimalData.Data), n))
				copy(grew, col.DecimalData.Data)
				col.DecimalData.Data = grew
			} else if len(col.DecimalData.Data) < n {
				col.DecimalData.Data = col.DecimalData.Data[:n]
			}
		case parquet.TypeArray, parquet.TypeMap:
			// Only the offsets array is indexed (CopyValueFrom writes
			// Offsets[di] and Offsets[di+1]); child elements are appended.
			needed := n + 1
			if cap(col.Offsets) < needed {
				grew := make([]int32, needed, growCap(cap(col.Offsets), needed))
				copy(grew, col.Offsets)
				col.Offsets = grew
			} else if len(col.Offsets) < needed {
				col.Offsets = col.Offsets[:needed]
			}
		case parquet.TypeRow, parquet.TypeVector:
			// Nothing indexed at this level: ROW children are append-built
			// and CopyValueFrom grows a VECTOR's stride itself.
		default:
			// Same contract as appendBatchRowsBulk's default — a type with
			// no arm here would advance Len over storage that was never
			// grown.
			panic(fmt.Sprintf("growBatchTo: no arm for column type %v", col.Type))
		}
		col.Nulls = col.Nulls.Grow(n)
	}
}

// growCap is the allocation capacity for growing a slice of capacity c to
// hold n elements: at least double the old capacity, so per-consume growth
// amortizes to O(1) per element.
func growCap(c, n int) int {
	if g := 2 * c; g > n {
		return g
	}
	return n
}

// ensureWriterLocked lazily opens partition p's stream (buffered writer +
// header). Caller must hold s.parts[p].mu with pw.flushing false — the init
// writes header bytes into bufFile, which a direct-chunk writer would
// otherwise be streaming into outside the lock.
func (s *partitionedShuffleSink) ensureWriterLocked(p int) error {
	pw := s.parts[p]
	if pw.writer != nil {
		return nil
	}
	// 256 KB stream buffer keeps the syscall count down to ~1 per
	// 256 KB of column data. The previous unbuffered path issued a
	// syscall per row of bytes-typed columns (writeBytesData), which
	// made that the dominant shuffle cost on Q03 SF1 (~95% of CPU).
	wf, _ := diskio.NewWriter(pw.file, diskio.KeepResident)
	pw.bufFile = bufio.NewWriterSize(wf, 256*1024)
	pw.writer = newShuffleWriter(pw.bufFile, s.schema)
	if err := pw.writer.writeHeader(); err != nil {
		return fmt.Errorf("partition %d header: %w", p, err)
	}
	return nil
}

// flushPartitionLocked writes partition p's accumulator as one chunk and
// resets it. Caller must hold s.parts[p].mu with pw.flushing false.
func (s *partitionedShuffleSink) flushPartitionLocked(p int) error {
	pw := s.parts[p]
	if pw.rowBuf == nil || pw.bufRows == 0 {
		return nil
	}
	if err := s.ensureWriterLocked(p); err != nil {
		return err
	}
	if err := pw.writer.writeChunk(pw.rowBuf.Columns, nil, pw.bufRows); err != nil {
		return fmt.Errorf("partition %d chunk: %w", p, err)
	}
	pw.numRows += int64(pw.bufRows)

	// Reset the accumulator in-place. Re-use the same RecordBatch memory.
	resetShuffleAccumBatch(pw.rowBuf)
	pw.bufRows = 0
	pw.bufBytes = 0
	return nil
}

// Finalize flushes all partition buffers, then patches the chunk count in each
// file header (mirrors shuffleStreamSink.Finalize exactly for the patch logic).
//
// On error, partitions after the failing one may be left with numChunks=0
// in their headers (an inconsistent on-disk state). Callers MUST NOT upload
// any partition file if Finalize returns a non-nil error.
func (s *partitionedShuffleSink) Finalize(_ context.Context) error {
	if !s.finalized.CompareAndSwap(false, true) {
		return nil
	}
	// Drain consumer-local pre-accumulation slabs first: their residual rows
	// land in the shared partition accumulators, which the loop below
	// flushes to disk. No Consume is in flight (pipeline contract).
	if err := s.drainAllLocals(); err != nil {
		return err
	}
	// Per-partition flush + header-patch (+ fsync only when
	// WADJET_SHUFFLE_FSYNC=1 — see stage_fsync.go). All steps touch only the
	// target partition's state; each goroutine takes its partition's lock,
	// which also quiesces any straggling Consume appends (callers guarantee
	// no NEW Consume starts after Finalize — the pipeline contract).
	limit := s.numParts
	if gp := runtime.GOMAXPROCS(0); limit > gp {
		limit = gp
	}
	g := new(errgroup.Group)
	g.SetLimit(limit)
	for p := range s.parts {
		p := p
		g.Go(func() error {
			pw := s.parts[p]
			pw.mu.Lock()
			defer pw.mu.Unlock()
			// Callers guarantee no NEW Consume after Finalize, but wait out
			// any straggling direct-chunk writer that still owns the stream.
			for pw.flushing {
				pw.flushCond.Wait()
			}
			if err := s.flushPartitionLocked(p); err != nil {
				return err
			}
			if pw.writer == nil {
				// Empty partition — leave file at zero bytes; downstream treats as no rows.
				return syncStageFile(pw.file)
			}
			// Extent-index footer appends through the buffered stream, so it
			// precedes the flush; the patch below only overwrites in place.
			if err := pw.writer.writeFooter(); err != nil {
				return fmt.Errorf("partition %d extent footer: %w", p, err)
			}
			// The bufio.Writer must be flushed before we Seek the underlying
			// file: a Seek bypasses the buffer, so any unflushed bytes would
			// land at the wrong offset.
			if pw.bufFile != nil {
				if err := pw.bufFile.Flush(); err != nil {
					return fmt.Errorf("partition %d flush: %w", p, err)
				}
			}
			// Patch chunk count in header. Layout (see shuffle_format.go):
			//   offset 0..4 = magic "WSHF"
			//   offset 4..8 = numChunks (uint32 LE) — placeholder written by writeHeader
			if _, err := pw.file.Seek(4, 0); err != nil {
				return fmt.Errorf("partition %d seek: %w", p, err)
			}
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], pw.writer.numChunks)
			if _, err := pw.file.Write(buf[:]); err != nil {
				return fmt.Errorf("partition %d patch: %w", p, err)
			}
			if _, err := pw.file.Seek(0, 2); err != nil {
				return fmt.Errorf("partition %d seek end: %w", p, err)
			}
			return syncStageFile(pw.file)
		})
	}
	return g.Wait()
}

// Close releases all file handles. Idempotent.
func (s *partitionedShuffleSink) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	for _, pw := range s.parts {
		if pw == nil {
			continue
		}
		pw.mu.Lock()
		if pw.file != nil {
			_ = pw.file.Close()
		}
		pw.mu.Unlock()
	}
	return nil
}

// PartitionFiles returns the local file paths produced for each partition,
// indexed by partition id. An empty string indicates the partition received
// zero rows.
//
// Must be called AFTER Finalize. Calling it earlier will return empty strings
// for partitions whose buffered rows had not yet flushed.
func (s *partitionedShuffleSink) PartitionFiles() []string {
	out := make([]string, s.numParts)
	for p, pw := range s.parts {
		if pw == nil || pw.numRows == 0 {
			continue
		}
		out[p] = pw.file.Name()
	}
	return out
}

// PartitionRowCounts returns the number of rows written to each partition
// file, indexed by partition id. Like PartitionFiles, must be called AFTER
// Finalize — earlier calls miss rows still sitting in the accumulators.
func (s *partitionedShuffleSink) PartitionRowCounts() []int64 {
	out := make([]int64, s.numParts)
	for p, pw := range s.parts {
		if pw == nil {
			continue
		}
		out[p] = pw.numRows
	}
	return out
}

// FNV-1a 64-bit constants.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// hashRowsIntoPartitions computes hash(b.Columns[keyIdxs][row]) % numParts
// for every active row of b, writing the result into out (which must have
// len ≥ b.ActiveLen()). Inlined FNV-1a — zero allocations, no interface
// calls. Pre-2026-04-30 the per-row variant called fnv.New64a() per row,
// allocating a new hasher struct ~1M times for an SF1 lineitem shuffle.
//
// Multi-column keys: hash byte-stream is column1 || column2 || …, mixed
// into the per-row accumulator in the same order.
//
// Caller-supplied scratch slice for the per-row uint64 accumulator. If the
// scratch is too small the function returns false; caller should grow it
// and retry. This avoids re-allocating a uint64 buffer per Consume.
func hashRowsIntoPartitions(b *batch.RecordBatch, keyIdxs []int, numParts int, hashScratch []uint64, out []int) bool {
	n := b.ActiveLen()
	if cap(hashScratch) < n || len(out) < n {
		return false
	}
	hashes := hashScratch[:n]
	if len(keyIdxs) == 0 {
		for i := 0; i < n; i++ {
			out[i] = 0
		}
		return true
	}
	for i := range hashes {
		hashes[i] = fnvOffset64
	}
	useSel := b.Sel != nil
	for _, idx := range keyIdxs {
		col := b.Columns[idx]
		switch col.Type {
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			data := col.Int32Data
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					v := uint32(data[row])
					h = (h ^ uint64(byte(v))) * fnvPrime64
					h = (h ^ uint64(byte(v>>8))) * fnvPrime64
					h = (h ^ uint64(byte(v>>16))) * fnvPrime64
					h = (h ^ uint64(byte(v>>24))) * fnvPrime64
				}
				hashes[i] = h
			}
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			data := col.Int64Data
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					v := uint64(data[row])
					h = (h ^ uint64(byte(v))) * fnvPrime64
					h = (h ^ uint64(byte(v>>8))) * fnvPrime64
					h = (h ^ uint64(byte(v>>16))) * fnvPrime64
					h = (h ^ uint64(byte(v>>24))) * fnvPrime64
					h = (h ^ uint64(byte(v>>32))) * fnvPrime64
					h = (h ^ uint64(byte(v>>40))) * fnvPrime64
					h = (h ^ uint64(byte(v>>48))) * fnvPrime64
					h = (h ^ uint64(byte(v>>56))) * fnvPrime64
				}
				hashes[i] = h
			}
		case parquet.TypeFloat32:
			data := col.Float32Data
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					// Canonical bits, not raw: -0.0 and +0.0 are one value
					// and every NaN payload is one value (kernel/
					// float_order.go), so they must route to ONE partition
					// or the shuffle join and the shuffle aggregate would
					// answer differently from the single-process ones that
					// key them alike (#459).
					v := kernel.KeyFloat32Bits(data[row])
					h = (h ^ uint64(byte(v))) * fnvPrime64
					h = (h ^ uint64(byte(v>>8))) * fnvPrime64
					h = (h ^ uint64(byte(v>>16))) * fnvPrime64
					h = (h ^ uint64(byte(v>>24))) * fnvPrime64
				}
				hashes[i] = h
			}
		case parquet.TypeFloat64:
			data := col.Float64Data
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					v := kernel.KeyFloat64Bits(data[row])
					h = (h ^ uint64(byte(v))) * fnvPrime64
					h = (h ^ uint64(byte(v>>8))) * fnvPrime64
					h = (h ^ uint64(byte(v>>16))) * fnvPrime64
					h = (h ^ uint64(byte(v>>24))) * fnvPrime64
					h = (h ^ uint64(byte(v>>32))) * fnvPrime64
					h = (h ^ uint64(byte(v>>40))) * fnvPrime64
					h = (h ^ uint64(byte(v>>48))) * fnvPrime64
					h = (h ^ uint64(byte(v>>56))) * fnvPrime64
				}
				hashes[i] = h
			}
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					for _, c := range col.BytesData.Value(row) {
						h = (h ^ uint64(c)) * fnvPrime64
					}
				}
				hashes[i] = h
			}
		case parquet.TypeBool:
			data := col.BoolData
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					var v byte
					if data[row] {
						v = 1
					}
					h = (h ^ uint64(v)) * fnvPrime64
				}
				hashes[i] = h
			}
		case parquet.TypeDecimal:
			data := col.DecimalData.Data
			decScale := col.DecimalData.Scale
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) || data == nil {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					// The CANONICAL key, not the raw Int128: the unscaled
					// integer alone makes the partition depend on the
					// column's declared SCALE, so a shuffle join between a
					// DECIMAL(9,2) and a DECIMAL(18,4) holding the same
					// quantity would send equal values to different
					// partitions and match none of them - the cross-scale
					// case #474's key encoding exists to serve.
					var kb [batch.MaxDecimalKeyLen]byte
					for _, c := range batch.AppendDecimalKey(kb[:0], data[row], decScale) {
						h = (h ^ uint64(c)) * fnvPrime64
					}
				}
				hashes[i] = h
			}
		case parquet.TypeVector:
			dim := col.VectorDim
			for i := 0; i < n; i++ {
				row := i
				if useSel {
					row = int(b.Sel[i])
				}
				h := hashes[i]
				if col.Nulls.IsNullFast(row) || dim <= 0 || (row+1)*dim > len(col.Float32Data) {
					h = (h ^ 0xff) * fnvPrime64
				} else {
					// Canonical bits, not raw: -0.0 and +0.0 are one value
					// and every NaN payload is one value (kernel/
					// float_order.go), matching the TypeFloat32 arm above
					// and appendVectorKey (engine/exec/aggregate.go) — a
					// VECTOR element keyed by raw bits here would split a
					// group that the router and the single-process merge
					// both call one group (#459).
					for _, f := range col.Float32Data[row*dim : (row+1)*dim] {
						v := kernel.KeyFloat32Bits(f)
						h = (h ^ uint64(byte(v))) * fnvPrime64
						h = (h ^ uint64(byte(v>>8))) * fnvPrime64
						h = (h ^ uint64(byte(v>>16))) * fnvPrime64
						h = (h ^ uint64(byte(v>>24))) * fnvPrime64
					}
				}
				hashes[i] = h
			}
		default:
			// ARRAY, MAP and ROW keys still mix a constant: hashing them
			// means walking offsets and child vectors recursively, which is
			// a bigger change than #397's encoder and has no caller today
			// (the planner does not choose a container column as a
			// partition key). Deterministic, so never a wrong answer — but
			// every row of such a key routes to ONE partition.
			for i := 0; i < n; i++ {
				hashes[i] = (hashes[i] ^ 0x00) * fnvPrime64
			}
		}
	}
	mod := uint64(numParts)
	if mod == 0 {
		mod = 1
	}
	for i := 0; i < n; i++ {
		out[i] = int(hashes[i] % mod)
	}
	return true
}

// hashVectorValue mixes a single column value at the given row into h using
// FNV-1a. It must mix the SAME byte stream as hashRowsIntoPartitions above,
// arm for arm — the sink routes with that one and the tests verify the
// routing with this one, so a divergence would read as a routing bug.
// ARRAY/MAP/ROW still hash a zero byte (deterministic but skewed — every
// row of such a key lands in one partition; see the default there).
func hashVectorValue(h interface{ Write([]byte) (int, error) }, col *batch.Vector, row int, scratch []byte) {
	if col.Nulls.IsNullFast(row) {
		// Null contributes a distinct marker so that null rows are consistently
		// routed (all nulls land in the same partition).
		_, _ = h.Write([]byte{0xff})
		return
	}
	switch col.Type {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
		binary.LittleEndian.PutUint32(scratch[:4], uint32(col.Int32Data[row]))
		_, _ = h.Write(scratch[:4])
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
		binary.LittleEndian.PutUint64(scratch[:8], uint64(col.Int64Data[row]))
		_, _ = h.Write(scratch[:8])
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		_, _ = h.Write(col.BytesData.Value(row))
	case parquet.TypeBool:
		var v byte
		if col.BoolData[row] {
			v = 1
		}
		_, _ = h.Write([]byte{v})
	case parquet.TypeDecimal:
		if col.DecimalData.Data == nil {
			_, _ = h.Write([]byte{0xff})
			return
		}
		// Canonical, scale-normalized - see the DECIMAL arm of
		// hashRowsIntoPartitions.
		var kb [batch.MaxDecimalKeyLen]byte
		_, _ = h.Write(batch.AppendDecimalKey(kb[:0], col.DecimalData.Data[row], col.DecimalData.Scale))
	case parquet.TypeVector:
		dim := col.VectorDim
		if dim <= 0 || (row+1)*dim > len(col.Float32Data) {
			_, _ = h.Write([]byte{0xff})
			return
		}
		// Must mix the SAME byte stream as the TypeVector arm of
		// hashRowsIntoPartitions above: canonical bits, not raw (#459).
		for _, f := range col.Float32Data[row*dim : (row+1)*dim] {
			binary.LittleEndian.PutUint32(scratch[:4], kernel.KeyFloat32Bits(f))
			_, _ = h.Write(scratch[:4])
		}
	default:
		_, _ = h.Write([]byte{0x00})
	}
}
