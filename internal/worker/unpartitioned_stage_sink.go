package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/diskio"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
// Concurrency: exec.Pipeline parallelises Consume across goroutines, so the
// sink serialises writes through mu. The shuffleWriter and bufio.Writer are
// not goroutine-safe.
type unpartitionedStageSink struct {
	spillPath string

	mu        sync.Mutex
	file      *os.File
	bufFile   *bufio.Writer
	writer    *shuffleWriter
	numChunks uint32
	totalRows int64
	finalized bool
	closed    bool

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
	coalesce   int8
	rowBuf     *batch.RecordBatch
	bufRows    int
	bufBytes   int
	rowScratch []int
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
	return &unpartitionedStageSink{
		spillPath: filepath.Join(spillDir, "stage-"+taskID+".wshf"),
	}
}

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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("unpartitionedStageSink: Consume after Close")
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
		s.totalRows += int64(n)
		return nil
	}

	// Coalescing path: copy active rows into the accumulator, flush at the
	// thresholds. Copying (not retaining) matters — morsel-parallel callers
	// hand in zero-copy views whose parent bytes retire after Consume
	// returns.
	if s.rowBuf == nil {
		initCap := n
		if initCap < 256 {
			initCap = 256
		}
		s.rowBuf = batch.NewRecordBatch(b.Schema, initCap)
		s.rowBuf.Len = 0
		for _, col := range s.rowBuf.Columns {
			if col.BytesData.Offsets != nil {
				col.BytesData.Offsets = col.BytesData.Offsets[:1]
				col.BytesData.Offsets[0] = 0
				col.BytesData.Data = col.BytesData.Data[:0]
			}
		}
	}
	if cap(s.rowScratch) < n {
		s.rowScratch = make([]int, n)
	}
	rows := s.rowScratch[:n]
	if b.Sel != nil {
		for i := 0; i < n; i++ {
			rows[i] = int(b.Sel[i])
		}
	} else {
		for i := 0; i < n; i++ {
			rows[i] = i
		}
	}
	s.bufBytes += appendBatchRowsBulk(s.rowBuf, b, rows)
	s.bufRows += n
	s.totalRows += int64(n)
	if s.bufRows >= unpartitionedFlushRows || s.bufBytes >= unpartitionedFlushBytes {
		return s.flushCoalesced()
	}
	return nil
}

// flushCoalesced writes the accumulator as one chunk and resets it in place.
// Callers hold s.mu.
func (s *unpartitionedStageSink) flushCoalesced() error {
	if s.rowBuf == nil || s.bufRows == 0 {
		return nil
	}
	if err := s.writer.writeChunk(s.rowBuf.Columns, nil, s.bufRows); err != nil {
		return fmt.Errorf("writing wshf chunk: %w", err)
	}
	s.numChunks++
	s.rowBuf.Len = 0
	s.bufRows = 0
	s.bufBytes = 0
	for _, col := range s.rowBuf.Columns {
		col.Len = 0
		col.Nulls.ResetNonNull(0)
		switch col.Type {
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			col.BytesData.Data = col.BytesData.Data[:0]
			col.BytesData.Offsets = col.BytesData.Offsets[:1]
			col.BytesData.Offsets[0] = 0
		}
	}
	return nil
}

// Finalize flushes the bufio writer and patches the chunk-count placeholder
// at offset 4 of the file (writeHeader stamped a zero placeholder there). No
// further Consume calls are valid after Finalize.
func (s *unpartitionedStageSink) Finalize(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalized {
		return nil
	}
	s.finalized = true
	if s.bufFile == nil {
		return nil
	}
	// Write any coalesced remainder before flushing the stream.
	if err := s.flushCoalesced(); err != nil {
		return err
	}
	s.rowBuf = nil
	if err := s.bufFile.Flush(); err != nil {
		return fmt.Errorf("flushing wshf bufio: %w", err)
	}
	if s.numChunks > 0 {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], s.numChunks)
		if _, err := s.file.WriteAt(hdr[:], 4); err != nil {
			return fmt.Errorf("patching chunk count: %w", err)
		}
		if err := s.file.Sync(); err != nil {
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
	if s.closed {
		return nil
	}
	s.closed = true
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
func (s *unpartitionedStageSink) TotalRows() int64 { return s.totalRows }

// Schema returns the schema captured from the first non-empty batch, or nil
// if no batches were consumed.
func (s *unpartitionedStageSink) Schema() []parquet.Column {
	if s.writer == nil {
		return nil
	}
	return s.writer.schema
}
