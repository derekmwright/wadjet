package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/diskio"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// unpartitionedStageSink streams batches into a single .wshf file via a
// bufio.Writer-wrapped temp file. Replaces the old "collect every batch into
// CollectSink, then encode the whole thing into bytes.Buffer in
// writeUnpartitionedWSHF" path that materialised the entire stage output in
// heap before upload — at SF10 a single lineitem scan task produced ~5 GB of
// filtered batches, so two concurrent scan tasks pushed the worker process
// past 10 GB live heap and OOM'd before the upload phase even started
// (project_q04_sf10_followup.md, 2026-04-30).
//
// Memory bound is one batch in flight + the 256 KB bufio buffer, regardless
// of stage output size.
//
// Concurrency: exec.Pipeline parallelises Consume across goroutines. The
// shuffleWriter and bufio.Writer are not goroutine-safe, so stream ownership
// is handed over under mu (the `flushing` flag). The ROW COPY does not run
// under mu — each consumer appends into a checked-out slab of its own
// (stage_sink_accum.go); mu is taken only at chunk boundaries.
type unpartitionedStageSink struct {
	spillPath string

	mu        sync.Mutex
	file      *os.File
	bufFile   *bufio.Writer
	writer    *shuffleWriter
	numChunks uint32
	finalized bool

	// totalRows and closed are read outside mu by the producer-local path
	// (stage_sink_accum.go), which counts rows at accumulate time and
	// checks for Consume-after-Close without touching the sink lock.
	totalRows atomic.Int64
	closed    atomic.Bool

	// Producer-local accumulation (stage_sink_accum.go). slabAll is the
	// registry Finalize drains; slabFree is the LIFO checkout freelist.
	// bufferedBytes is the sum of rows resident in slabs, charged at
	// accumulate time and released at chunk write, which bounds per-sink
	// residency now that buffers are per consumer.
	slabsMu       sync.Mutex
	slabFree      []*stageSlab
	slabAll       []*stageSlab
	bufferedBytes atomic.Int64

	// Chunk coalescing. A .wshf chunk is the downstream stage's batch AND
	// its decode unit (one s2 block + crc per chunk), so chunk size must be
	// the SINK's decision, not an echo of the caller's consume granularity —
	// morsel-parallel fragments consume at ~2048-row morsels, and writing
	// chunk-per-consume fragmented stage outputs ~100× (SF10 v1.5 A/B
	// 2026-07-03: s2Decode 26.6s→35.6s, suite +15%). Consumed rows accumulate
	// in rowBuf and flush at unpartitionedFlushRows/-Bytes. Flat schemas only
	// (appendBatchRowsBulk/growBatchTo have no nested arms); nested schemas
	// keep the legacy chunk-per-consume path. coalesce: 0=undecided,
	// 1=coalescing, -1=legacy.
	//
	// The flush is DOUBLE-BUFFERED (docs/design/morsel-execution.md §4.1.1
	// v1.7 follow-up): when the accumulator trips a threshold, it is swapped
	// out under mu and the s2 encode + write of up to flushBytes happens
	// OUTSIDE the lock, so concurrent consumers keep appending into the
	// spare buffer instead of stalling behind a 16 MB encode. `flushing`
	// admits exactly one flusher (the writer/bufFile are single-threaded);
	// flushCond backpressures a consumer whose freshly-refilled buffer trips
	// the threshold while a flush is still in flight — memory stays bounded
	// at two accumulators. The legacy (nested-schema) path never sets
	// flushing and keeps writer access under mu, which is exclusive with the
	// coalescing path by the coalesce flag.
	coalesce   int8
	rowBuf     *batch.RecordBatch // active accumulator (guarded by mu)
	spareBuf   *batch.RecordBatch // recycled accumulator awaiting reuse (guarded by mu)
	bufRows    int
	bufBytes   int
	flushing   bool
	flushCond  *sync.Cond // signaled when an out-of-lock flush completes
	rowScratch []uint32
	// Flush thresholds; fields (not consts) so tests can force frequent
	// flushes. Default to unpartitionedFlushRows/-Bytes.
	flushRows   int
	flushBytesT int
}

// Coalesced-chunk flush thresholds: whichever trips first. 64k rows keeps
// downstream batches big enough to amortize per-chunk codec/framing costs;
// 16 MB bounds the single accumulator (one per fragment sink — at
// max_concurrent=4 that is ≤64 MB per worker, tracked nowhere but bounded).
const (
	unpartitionedFlushRows  = 64 << 10
	unpartitionedFlushBytes = 16 << 20
)

// batchSchemaIsFlat reports whether every column type has an arm in
// appendBatchRowsBulk/growBatchTo.
func batchSchemaIsFlat(schema []parquet.Column) bool {
	for _, c := range schema {
		switch c.Type {
		case parquet.TypeBool,
			parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate,
			parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration,
			parquet.TypeFloat32, parquet.TypeFloat64,
			parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID,
			parquet.TypeDecimal:
		default:
			return false
		}
	}
	return true
}

// newUnpartitionedStageSink constructs the sink; spillDir must already exist
// (or be created by Init), and taskID is used to name the spill file.
func newUnpartitionedStageSink(spillDir, taskID string) *unpartitionedStageSink {
	s := &unpartitionedStageSink{
		spillPath:   filepath.Join(spillDir, "stage-"+taskID+".wshf"),
		flushRows:   unpartitionedFlushRows,
		flushBytesT: unpartitionedFlushBytes,
	}
	s.flushCond = sync.NewCond(&s.mu)
	return s
}

// AcceptsViews: Consume flattens own-null view columns (in the consuming
// goroutine — every batch is private to its Consume call) and the rest
// serialize through their indirection (writeChunk sel composition /
// appendBatchRowsBulk row translation) — the pipeline's defensive flatten
// would just add the copy this sink exists to skip.
func (s *unpartitionedStageSink) AcceptsViews() bool { return true }

// Init creates the spill file. Schema is discovered lazily on first Consume,
// so empty stages don't pay for a file open.
func (s *unpartitionedStageSink) Init(_ context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.spillPath), 0o755); err != nil {
		return fmt.Errorf("creating spill dir: %w", err)
	}
	f, err := os.Create(s.spillPath)
	if err != nil {
		return fmt.Errorf("creating spill file %s: %w", s.spillPath, err)
	}
	s.file = f
	// KeepResident: the file is uploaded (and possibly adopted into the
	// LocalStageCache and mmap'd) right after Finalize, so only the dirty
	// bound applies — pages stay resident for the imminent reader.
	wf, _ := diskio.NewWriter(f, diskio.KeepResident)
	s.bufFile = bufio.NewWriterSize(wf, 256*1024)
	return nil
}

// Consume appends one batch as a chunk in the .wshf stream. The schema is
// captured from the first non-empty batch; subsequent batches are assumed to
// share that schema (which the pipeline guarantees). bufio coalesces the many
// small writes from shuffleWriter into syscall-sized chunks — without it,
// ~95% of stage-output CPU was unbuffered file Writes (the same fix from the
// 2026-04-30 shuffle-sink bufio refactor).
func (s *unpartitionedStageSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	if b == nil {
		return nil
	}
	n := b.ActiveLen()
	if n == 0 {
		return nil
	}
	// Direct-chunk bypass: a batch that alone meets a flush threshold would
	// only pass through the accumulator to be flushed immediately — encode
	// it straight from b instead, outside the lock (same flushing-flag
	// exclusion as the double-buffered flush; docs/design/sink-direct-chunk.md).
	// Flat schemas only, mirroring the coalesce gate: for flat schemas the
	// sink always coalesces, so the two paths never mix with the legacy one.
	if sinkDirectChunkEnabled &&
		(n >= s.flushRows || n*approxRowBytes(b) >= s.flushBytesT) &&
		batchSchemaIsFlat(b.Schema) {
		return s.consumeDirectChunk(b, n)
	}
	// Producer-local accumulation: the row copy runs with no sink lock held
	// and the lock is taken only to hand over stream ownership at a chunk
	// boundary (stage_sink_accum.go). Flat schemas only, mirroring the
	// coalesce gate below — nested schemas keep the legacy locked path, and
	// the two never mix because the schema is constant for the sink.
	if stageSinkAccum.On() && batchSchemaIsFlat(b.Schema) {
		return s.consumeSlab(b, n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return fmt.Errorf("unpartitionedStageSink: Consume after Close")
	}
	// Own-null view columns (outer-join fill) can't ride writeChunk's sel
	// composition or appendBatchRowsBulk's row translation — flatten them
	// here under the lock. Other views serialize through their indirection.
	if b.HasViews() {
		for _, col := range b.Columns {
			if col.IsView() && col.Nulls.HasNulls() {
				col.Flatten()
				exec.LateMatFlattens.Add(1)
			}
		}
	}
	if s.writer == nil {
		s.writer = newShuffleWriter(s.bufFile, b.Schema)
		if err := s.writer.writeHeader(); err != nil {
			return fmt.Errorf("writing wshf header: %w", err)
		}
	}
	if s.coalesce == 0 {
		if batchSchemaIsFlat(b.Schema) {
			s.coalesce = 1
		} else {
			s.coalesce = -1
		}
	}
	if s.coalesce < 0 {
		if err := s.writer.writeChunk(b.Columns, b.Sel, n); err != nil {
			return fmt.Errorf("writing wshf chunk: %w", err)
		}
		s.numChunks++
		s.totalRows.Add(int64(n))
		return nil
	}

	// Coalescing path: copy active rows into the accumulator, flush at the
	// thresholds. Copying (not retaining) matters — morsel-parallel callers
	// hand in zero-copy views whose parent bytes retire after Consume
	// returns.
	if s.rowBuf == nil {
		s.rowBuf = s.takeAccumulator(b, n)
	}
	rows := b.Sel
	if rows == nil {
		// Identity row list, extended lazily (values are position-stable, so
		// a previously filled prefix stays valid for any smaller n).
		if cap(s.rowScratch) < n {
			s.rowScratch = make([]uint32, 0, n)
		}
		for i := len(s.rowScratch); i < n; i++ {
			s.rowScratch = append(s.rowScratch, uint32(i))
		}
		rows = s.rowScratch[:n]
	}
	s.bufBytes += appendBatchRowsBulk(s.rowBuf, b, rows)
	s.bufRows += n
	s.totalRows.Add(int64(n))
	if s.bufRows < s.flushRows && s.bufBytes < s.flushBytesT {
		return nil
	}

	// Threshold tripped. If a flush is already in flight, wait for it — the
	// active accumulator is full and the spare is out with the flusher, so
	// bounded memory means this consumer backpressures until the spare
	// returns. After the wait the accumulator may have been flushed by our
	// own re-check below on another consumer; re-test the threshold.
	for s.flushing {
		s.flushCond.Wait()
		if s.bufRows < s.flushRows && s.bufBytes < s.flushBytesT {
			return nil
		}
	}
	// Become the flusher: swap the full accumulator out, release the lock,
	// encode+write outside it. Only one flusher exists (flushing flag), so
	// the writer/bufFile are single-threaded past writeHeader.
	s.flushing = true
	full, fullRows := s.rowBuf, s.bufRows
	s.rowBuf = s.spareBuf // may be nil; next Consume allocates lazily
	s.spareBuf = nil
	s.bufRows = 0
	s.bufBytes = 0
	writer := s.writer
	s.mu.Unlock()

	err := writer.writeChunk(full.Columns, nil, fullRows)
	resetAccumulator(full)

	s.mu.Lock() // re-acquire for the deferred Unlock
	s.spareBuf = full
	s.flushing = false
	s.flushCond.Broadcast()
	if err != nil {
		return fmt.Errorf("writing wshf chunk: %w", err)
	}
	s.numChunks++
	return nil
}

// consumeDirectChunk writes b as one chunk straight into the stream,
// bypassing the accumulator — the caller established that b alone meets a
// flush threshold, so accumulating it would only add a copy. The encode runs
// OUTSIDE mu under the same flushing-flag exclusion the double-buffered
// flusher uses; the lock covers only writer init and counters.
func (s *unpartitionedStageSink) consumeDirectChunk(b *batch.RecordBatch, n int) error {
	// Own-null view columns (outer-join fill) can't ride writeChunk's sel
	// composition. b is this call's private batch (caller contract, same as
	// the coalescing path relies on for its copy), so flattening outside the
	// lock is single-threaded.
	if b.HasViews() {
		for _, col := range b.Columns {
			if col.IsView() && col.Nulls.HasNulls() {
				col.Flatten()
				exec.LateMatFlattens.Add(1)
			}
		}
	}
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return fmt.Errorf("unpartitionedStageSink: Consume after Close")
	}
	// Wait for stream ownership BEFORE the lazy writer init — writeHeader
	// touches bufFile, which an in-flight flusher owns outside the lock.
	// (flushing can only be set once the writer exists, but keep the order
	// robust rather than rely on that.)
	for s.flushing {
		s.flushCond.Wait()
	}
	if s.writer == nil {
		s.writer = newShuffleWriter(s.bufFile, b.Schema)
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

	err := w.writeChunk(b.Columns, b.Sel, n)

	s.mu.Lock()
	s.flushing = false
	s.flushCond.Broadcast()
	if err == nil {
		s.numChunks++
		s.totalRows.Add(int64(n))
	}
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("writing wshf chunk: %w", err)
	}
	return nil
}

// takeAccumulator returns a fresh or recycled accumulator batch for the
// coalescing path. Caller holds s.mu.
func (s *unpartitionedStageSink) takeAccumulator(b *batch.RecordBatch, n int) *batch.RecordBatch {
	if s.spareBuf != nil {
		buf := s.spareBuf
		s.spareBuf = nil
		return buf
	}
	initCap := n
	if initCap < 256 {
		initCap = 256
	}
	buf := batch.NewRecordBatch(b.Schema, initCap)
	buf.Len = 0
	for _, col := range buf.Columns {
		if col.BytesData.Offsets != nil {
			col.BytesData.Offsets = col.BytesData.Offsets[:1]
			col.BytesData.Offsets[0] = 0
			col.BytesData.Data = col.BytesData.Data[:0]
		}
	}
	return buf
}

// resetAccumulator empties an accumulator batch in place for reuse.
func resetAccumulator(buf *batch.RecordBatch) {
	buf.Len = 0
	for _, col := range buf.Columns {
		col.Len = 0
		col.Nulls.ResetNonNull(0)
		switch col.Type {
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			col.BytesData.Data = col.BytesData.Data[:0]
			col.BytesData.Offsets = col.BytesData.Offsets[:1]
			col.BytesData.Offsets[0] = 0
		}
	}
}

// flushCoalescedLocked writes the accumulator remainder as one chunk. Callers
// hold s.mu with NO flush in flight (Finalize waits first).
func (s *unpartitionedStageSink) flushCoalescedLocked() error {
	if s.rowBuf == nil || s.bufRows == 0 {
		return nil
	}
	if err := s.writer.writeChunk(s.rowBuf.Columns, nil, s.bufRows); err != nil {
		return fmt.Errorf("writing wshf chunk: %w", err)
	}
	s.numChunks++
	resetAccumulator(s.rowBuf)
	s.bufRows = 0
	s.bufBytes = 0
	return nil
}

// Finalize flushes the bufio writer and patches the chunk-count placeholder
// at offset 4 of the file (writeHeader stamped a zero placeholder there). No
// further Consume calls are valid after Finalize.
func (s *unpartitionedStageSink) Finalize(_ context.Context) error {
	s.mu.Lock()
	if s.finalized {
		s.mu.Unlock()
		return nil
	}
	s.finalized = true
	s.mu.Unlock()
	// Drain every producer-local slab first: their residual rows become
	// chunks in the stream. No Consume is in flight (pipeline contract), so
	// no slab is concurrently appended to. Draining before the lock below
	// matters — writeSlabChunk takes mu itself.
	if err := s.drainAllSlabs(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Quiesce any in-flight double-buffered flush before touching the
	// writer — the flusher owns it outside the lock until flushing clears.
	for s.flushing {
		s.flushCond.Wait()
	}
	if s.bufFile == nil {
		return nil
	}
	// Write any coalesced remainder before flushing the stream.
	if err := s.flushCoalescedLocked(); err != nil {
		return err
	}
	s.rowBuf = nil
	s.spareBuf = nil
	// Extent-index footer appends through the buffered stream before the
	// flush; the WriteAt patch below only overwrites in place.
	if s.writer != nil {
		if err := s.writer.writeFooter(); err != nil {
			return fmt.Errorf("writing wshf extent footer: %w", err)
		}
	}
	if err := s.bufFile.Flush(); err != nil {
		return fmt.Errorf("flushing wshf bufio: %w", err)
	}
	if s.numChunks > 0 {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], s.numChunks)
		if _, err := s.file.WriteAt(hdr[:], 4); err != nil {
			return fmt.Errorf("patching chunk count: %w", err)
		}
		if err := syncStageFile(s.file); err != nil {
			return fmt.Errorf("syncing wshf file: %w", err)
		}
	}
	return nil
}

// Close releases the file descriptor. The spill file itself remains on disk
// for the executor to upload; callers are responsible for unlinking it (e.g.
// via os.Remove on the spill dir, like the other stage paths).
func (s *unpartitionedStageSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.file == nil {
		return nil
	}
	f := s.file
	s.file = nil
	return f.Close()
}

// Path returns the spill file path. Valid after Finalize.
func (s *unpartitionedStageSink) Path() string { return s.spillPath }

// NumChunks returns the count of chunks written.
func (s *unpartitionedStageSink) NumChunks() uint32 { return s.numChunks }

// TotalRows returns the cumulative active-length of all consumed batches.
func (s *unpartitionedStageSink) TotalRows() int64 { return s.totalRows.Load() }

// Schema returns the schema captured from the first non-empty batch, or nil
// if no batches were consumed.
func (s *unpartitionedStageSink) Schema() []parquet.Column {
	if s.writer == nil {
		return nil
	}
	return s.writer.schema
}
