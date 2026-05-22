package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// partitionedShuffleSink is an exec.Sink that hash-partitions incoming batches
// into N output .wshf files, one per partition. Each partition's writer flushes
// its accumulated rows once a per-partition buffer threshold is reached, so
// peak memory is bounded by (N partitions × flush threshold), independent of
// the total input size.
//
// This is the build-side and probe-side output sink for the shuffle execution
// path. The N output files are uploaded by the executor to S3 under a stable
// per-partition prefix, and downstream join tasks read all files at their
// assigned partition prefix via partitionShardSource.
type partitionedShuffleSink struct {
	spillDir   string
	keys       []string // partition key column names
	numParts   int
	schema     []parquet.Column
	flushBytes int // per-partition row buffer flush threshold

	mu             sync.Mutex
	parts          []*partitionWriter
	closed         bool
	keyIdxs        []int    // resolved column indices for keys (set on first Consume)
	partScratch    []int    // per-row partition assignment, reused across batches
	hashScratch    []uint64 // per-row uint64 hash accumulator, reused across batches
	perPartRows    [][]int  // per-partition source-row indices, reused across batches
}

// partitionWriter holds the open file handle and incremental state for one
// output partition.
type partitionWriter struct {
	file     *os.File
	bufFile  *bufio.Writer     // wraps file so the many small Writes from
	                           // shuffleWriter coalesce into syscall-sized
	                           // chunks. Pre-2026-04-30 the shuffleWriter
	                           // emitted ~1 syscall per non-null bytes-row,
	                           // and that was 90%+ of shuffle CPU.
	writer   *shuffleWriter
	rowBuf   *batch.RecordBatch // accumulator: rows destined for this partition
	bufRows  int                // rows currently in rowBuf
	bufBytes int                // approximate byte count of buffered rows
	numRows  int64              // total rows written to disk
}

// flushPartitionBytes is the per-partition accumulator size (in approx bytes
// of row data) at which we flush a chunk to disk. 64 KB keeps memory bounded:
// 4N partitions × 64 KB = ~768 KB per shuffle task at N=3.
const flushPartitionBytes = 64 * 1024

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
		s.parts[p] = &partitionWriter{file: f}
	}
	return nil
}

// Consume hash-partitions each row in b into its target partition, appending
// to that partition's row buffer. Buffers are flushed when they exceed
// flushBytes worth of accumulated rows. Safe for concurrent calls from
// parallel pipeline workers — serialized via mu.
func (s *partitionedShuffleSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.keyIdxs) == 0 {
		// Resolve key column indices from schema on first batch. Use
		// the bidirectional fallback so qualified planner-emitted keys
		// ("n1.n_name") still resolve against unqualified scan output
		// ("n_name") and vice-versa for self-join chain output.
		s.keyIdxs = make([]int, len(s.keys))
		for i, k := range s.keys {
			idx := exec.ColumnIndexFallback(b, k)
			if idx < 0 {
				return fmt.Errorf("partitioned shuffle: key %q not in schema", k)
			}
			s.keyIdxs[i] = idx
		}
	}

	// Vectorized partition assignment: one pass per key column over the
	// active rows, mixing into a per-row hash accumulator. Pre-2026-04-30
	// this code did `fnv.New64a()` per row → 1M heap allocations of the
	// hasher struct on a SF1 lineitem shuffle. Inlined FNV-1a here is
	// allocation-free.
	n := b.ActiveLen()
	if cap(s.partScratch) < n {
		s.partScratch = make([]int, n)
	}
	if cap(s.hashScratch) < n {
		s.hashScratch = make([]uint64, n)
	}
	parts := s.partScratch[:n]
	hashRowsIntoPartitions(b, s.keyIdxs, s.numParts, s.hashScratch[:n], parts)

	// Group rows by partition so each partition's columns are appended in
	// one bulk pass with the type switch hoisted outside the row loop. The
	// alternative (per-row appendRow) does a 13-arm type switch per column
	// per row, which on Q03 SF1 was ~64M switch dispatches for the lineitem
	// shuffle and was the dominant wall-time cost.
	if s.perPartRows == nil {
		s.perPartRows = make([][]int, s.numParts)
	}
	for p := range s.perPartRows {
		s.perPartRows[p] = s.perPartRows[p][:0]
	}
	if b.Sel != nil {
		for i := 0; i < n; i++ {
			s.perPartRows[parts[i]] = append(s.perPartRows[parts[i]], int(b.Sel[i]))
		}
	} else {
		for i := 0; i < n; i++ {
			s.perPartRows[parts[i]] = append(s.perPartRows[parts[i]], i)
		}
	}

	// Pre-warm the source batch's lazy Bitmap.HasNulls() cache on every
	// column before launching parallel goroutines: HasNulls memoizes its
	// result on first call (bitmap.go:127-158), and parallel readers calling
	// HasNulls on the same column race on that write. Touching each column's
	// Nulls.HasNulls() once here, single-threaded, populates the cache so all
	// subsequent reads in appendRowsBulk hit the fast path (b.hasNulls >= 0).
	for _, col := range b.Columns {
		_ = col.Nulls.HasNulls()
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
	g := new(errgroup.Group)
	g.SetLimit(limit)
	for p := 0; p < s.numParts; p++ {
		if len(s.perPartRows[p]) == 0 {
			continue
		}
		p := p
		g.Go(func() error {
			if err := s.appendRowsBulk(p, b, s.perPartRows[p]); err != nil {
				return err
			}
			pw := s.parts[p]
			if pw.rowBuf == nil || pw.bufRows == 0 || pw.bufBytes < s.flushBytes {
				return nil
			}
			return s.flushPartition(p)
		})
	}
	return g.Wait()
}

// appendRowsBulk appends `len(rows)` rows from b into the accumulator buffer
// for partition p, where rows[i] is the source-row index in b for the i-th
// destination row. The type switch is hoisted OUTSIDE the row loop so that
// each column is processed in a tight typed loop — on Q03 SF1 this converts
// ~64M switch dispatches into ~16 (one per column).
//
// The destination column slices are pre-grown once to fit all rows; null
// state defaults to non-null (Bitmap.Grow leaves new bits set), so we only
// emit SetNull on actual source nulls. Offsets for bytes/string columns are
// pre-grown to end+1 so BytesData.Set can write Offsets[di+1] in place
// without per-row growth.
func (s *partitionedShuffleSink) appendRowsBulk(p int, b *batch.RecordBatch, rows []int) error {
	if len(rows) == 0 {
		return nil
	}
	pw := s.parts[p]
	if pw.rowBuf == nil {
		// Allocate buffer with a reasonable initial capacity to reduce resizes.
		// Size by the first batch's per-partition row count (fall back to 256
		// for tiny incoming batches) so the typical case avoids any growth.
		initCap := len(rows)
		if initCap < 256 {
			initCap = 256
		}
		pw.rowBuf = batch.NewRecordBatch(b.Schema, initCap)
		pw.rowBuf.Len = 0
		// Reset BytesData offsets so we can append incrementally.
		for _, col := range pw.rowBuf.Columns {
			if col.BytesData.Offsets != nil {
				col.BytesData.Offsets = col.BytesData.Offsets[:1]
				col.BytesData.Offsets[0] = 0
				col.BytesData.Data = col.BytesData.Data[:0]
			}
		}
	}

	dst := pw.rowBuf
	start := dst.Len
	end := start + len(rows)

	// Pre-grow all column storage to fit `end` rows in one allocation per
	// column (versus one append-of-one per row in the old code).
	growBatchTo(dst, end)

	bytesAdded := 0
	nRows := len(rows)

	for ci, srcCol := range b.Columns {
		dstCol := dst.Columns[ci]
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
					if srcCol.Nulls.IsNullFast(row) {
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
					if srcCol.Nulls.IsNullFast(row) {
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
					if srcCol.Nulls.IsNullFast(row) {
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
					if srcCol.Nulls.IsNullFast(row) {
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
					if srcCol.Nulls.IsNullFast(row) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 8

		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			// Bytes path uses SetFrom for non-null (avoids the intermediate
			// slice header) and Set(di, nil) for null (advances the offset
			// without writing data). Offsets pre-grown by growBatchTo.
			srcOffsets := srcCol.BytesData.Offsets
			if !hasNulls {
				for i, row := range rows {
					dstCol.BytesData.SetFrom(start+i, &srcCol.BytesData, row)
					bytesAdded += int(srcOffsets[row+1]-srcOffsets[row]) + 4
				}
			} else {
				for i, row := range rows {
					di := start + i
					if srcCol.Nulls.IsNullFast(row) {
						dstCol.Nulls.SetNull(di)
						dstCol.BytesData.Set(di, nil)
					} else {
						dstCol.BytesData.SetFrom(di, &srcCol.BytesData, row)
						bytesAdded += int(srcOffsets[row+1]-srcOffsets[row]) + 4
					}
				}
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
					if srcCol.Nulls.IsNullFast(row) {
						dstCol.Nulls.SetNull(start + i)
					} else {
						dstSlice[i] = srcData[row]
					}
				}
			}
			bytesAdded += nRows * 16
		}
	}

	dst.Len = end
	pw.bufRows += len(rows)
	pw.bufBytes += bytesAdded
	return nil
}

// growBatchTo ensures the destination batch has storage for n rows in each
// column. Grows in one allocation per column when capacity is exceeded
// (vs. the per-row appends in the legacy growBatch). After this call, the
// null bitmaps default to non-null in the new range — callers must explicitly
// SetNull for source rows that are null.
func growBatchTo(dst *batch.RecordBatch, n int) {
	for _, col := range dst.Columns {
		col.Len = n
		switch col.Type {
		case parquet.TypeBool:
			if cap(col.BoolData) < n {
				grew := make([]bool, n)
				copy(grew, col.BoolData)
				col.BoolData = grew
			} else if len(col.BoolData) < n {
				col.BoolData = col.BoolData[:n]
			}
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			if cap(col.Int32Data) < n {
				grew := make([]int32, n)
				copy(grew, col.Int32Data)
				col.Int32Data = grew
			} else if len(col.Int32Data) < n {
				col.Int32Data = col.Int32Data[:n]
			}
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			if cap(col.Int64Data) < n {
				grew := make([]int64, n)
				copy(grew, col.Int64Data)
				col.Int64Data = grew
			} else if len(col.Int64Data) < n {
				col.Int64Data = col.Int64Data[:n]
			}
		case parquet.TypeFloat32:
			if cap(col.Float32Data) < n {
				grew := make([]float32, n)
				copy(grew, col.Float32Data)
				col.Float32Data = grew
			} else if len(col.Float32Data) < n {
				col.Float32Data = col.Float32Data[:n]
			}
		case parquet.TypeFloat64:
			if cap(col.Float64Data) < n {
				grew := make([]float64, n)
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
				grew := make([]uint32, needed)
				copy(grew, col.BytesData.Offsets)
				col.BytesData.Offsets = grew
			} else if len(col.BytesData.Offsets) < needed {
				col.BytesData.Offsets = col.BytesData.Offsets[:needed]
			}
		case parquet.TypeDecimal:
			if cap(col.DecimalData.Data) < n {
				grew := make([]batch.Int128, n)
				copy(grew, col.DecimalData.Data)
				col.DecimalData.Data = grew
			} else if len(col.DecimalData.Data) < n {
				col.DecimalData.Data = col.DecimalData.Data[:n]
			}
		}
		col.Nulls = col.Nulls.Grow(n)
	}
}

func (s *partitionedShuffleSink) flushPartition(p int) error {
	pw := s.parts[p]
	if pw.rowBuf == nil || pw.bufRows == 0 {
		return nil
	}
	if pw.writer == nil {
		// 256 KB stream buffer keeps the syscall count down to ~1 per
		// 256 KB of column data. The previous unbuffered path issued a
		// syscall per row of bytes-typed columns (writeBytesData), which
		// made that the dominant shuffle cost on Q03 SF1 (~95% of CPU).
		pw.bufFile = bufio.NewWriterSize(pw.file, 256*1024)
		pw.writer = newShuffleWriter(pw.bufFile, s.schema)
		if err := pw.writer.writeHeader(); err != nil {
			return fmt.Errorf("partition %d header: %w", p, err)
		}
	}
	if err := pw.writer.writeChunk(pw.rowBuf.Columns, nil, pw.bufRows); err != nil {
		return fmt.Errorf("partition %d chunk: %w", p, err)
	}
	pw.numRows += int64(pw.bufRows)

	// Reset the accumulator in-place. Re-use the same RecordBatch memory.
	pw.rowBuf.Len = 0
	pw.bufRows = 0
	pw.bufBytes = 0
	for _, col := range pw.rowBuf.Columns {
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

// Finalize flushes all partition buffers, then patches the chunk count in each
// file header (mirrors shuffleStreamSink.Finalize exactly for the patch logic).
//
// On error, partitions after the failing one may be left with numChunks=0
// in their headers (an inconsistent on-disk state). Callers MUST NOT upload
// any partition file if Finalize returns a non-nil error.
func (s *partitionedShuffleSink) Finalize(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Per-partition flush + header-patch + fsync. All steps touch only the
	// target partition's writer; safe to run in parallel across partitions.
	limit := s.numParts
	if gp := runtime.GOMAXPROCS(0); limit > gp {
		limit = gp
	}
	g := new(errgroup.Group)
	g.SetLimit(limit)
	for p := range s.parts {
		p := p
		g.Go(func() error {
			if err := s.flushPartition(p); err != nil {
				return err
			}
			pw := s.parts[p]
			if pw.writer == nil {
				// Empty partition — leave file at zero bytes; downstream treats as no rows.
				return pw.file.Sync()
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
			return pw.file.Sync()
		})
	}
	return g.Wait()
}

// Close releases all file handles. Idempotent.
func (s *partitionedShuffleSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, pw := range s.parts {
		if pw != nil && pw.file != nil {
			_ = pw.file.Close()
		}
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
					v := math.Float32bits(data[row])
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
					v := math.Float64bits(data[row])
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
		default:
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
// FNV-1a. Supports the join-key types we encounter in TPC-H/SIEM workloads.
// For unsupported types, hashes a zero byte (deterministic but skewed — the
// planner is responsible for not picking these as partition keys).
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
	default:
		_, _ = h.Write([]byte{0x00})
	}
}
