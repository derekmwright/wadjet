package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

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

	mu          sync.Mutex
	parts       []*partitionWriter
	closed      bool
	keyIdxs     []int    // resolved column indices for keys (set on first Consume)
	partScratch []int    // per-row partition assignment, reused across batches
	hashScratch []uint64 // per-row uint64 hash accumulator, reused across batches
}

// partitionWriter holds the open file handle and incremental state for one
// output partition.
type partitionWriter struct {
	file    *os.File
	writer  *shuffleWriter
	rowBuf  *batch.RecordBatch // accumulator: rows destined for this partition
	bufRows int                // rows currently in rowBuf
	bufBytes int               // approximate byte count of buffered rows
	numRows int64              // total rows written to disk
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

	for i := 0; i < n; i++ {
		row := i
		if b.Sel != nil {
			row = int(b.Sel[i])
		}
		if err := s.appendRow(parts[i], b, row); err != nil {
			return err
		}
	}
	return s.flushIfNeeded()
}

// appendRow copies a single row from b at physical row index `row` into the
// accumulator buffer for partition p. The buffer is a RecordBatch that grows
// one row at a time; we copy typed column data directly, mirroring the approach
// used by batch.Compact.
func (s *partitionedShuffleSink) appendRow(p int, b *batch.RecordBatch, row int) error {
	pw := s.parts[p]
	if pw.rowBuf == nil {
		// Allocate buffer with a reasonable initial capacity to reduce resizes.
		const initCap = 256
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
	di := dst.Len

	// Grow typed column storage if needed.
	s.growBatch(dst, di)

	// Copy each column value at physical row `row` from b into dst at index di.
	var rowBytes int
	for ci, srcCol := range b.Columns {
		dstCol := dst.Columns[ci]

		if srcCol.Nulls.IsNullFast(row) {
			dstCol.Nulls.SetNull(di)
			// For bytes/string columns keep offsets aligned.
			switch dstCol.Type {
			case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
				dstCol.BytesData.Set(di, nil)
			}
			continue
		}

		dstCol.Nulls.SetValid(di)

		switch dstCol.Type {
		case parquet.TypeBool:
			dstCol.BoolData[di] = srcCol.BoolData[row]
			rowBytes++

		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			dstCol.Int32Data[di] = srcCol.Int32Data[row]
			rowBytes += 4

		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			dstCol.Int64Data[di] = srcCol.Int64Data[row]
			rowBytes += 8

		case parquet.TypeFloat32:
			dstCol.Float32Data[di] = srcCol.Float32Data[row]
			rowBytes += 4

		case parquet.TypeFloat64:
			dstCol.Float64Data[di] = srcCol.Float64Data[row]
			rowBytes += 8

		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			val := srcCol.BytesData.Value(row)
			dstCol.BytesData.Set(di, val)
			rowBytes += len(val) + 4 // data + offset entry

		case parquet.TypeDecimal:
			dstCol.DecimalData.Data[di] = srcCol.DecimalData.Data[row]
			rowBytes += 16
		}
	}

	dst.Len = di + 1
	pw.bufRows++
	pw.bufBytes += rowBytes
	return nil
}

// growBatch ensures the destination batch has storage for at least di+1 rows.
// We grow by doubling when capacity is exhausted — same strategy as Go slices.
func (s *partitionedShuffleSink) growBatch(dst *batch.RecordBatch, di int) {
	for _, col := range dst.Columns {
		col.Len = di + 1 // keep Len in sync for null bitmap ops

		switch col.Type {
		case parquet.TypeBool:
			for len(col.BoolData) <= di {
				col.BoolData = append(col.BoolData, false)
			}
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDate:
			for len(col.Int32Data) <= di {
				col.Int32Data = append(col.Int32Data, 0)
			}
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeIPv4, parquet.TypeMAC, parquet.TypeDuration:
			for len(col.Int64Data) <= di {
				col.Int64Data = append(col.Int64Data, 0)
			}
		case parquet.TypeFloat32:
			for len(col.Float32Data) <= di {
				col.Float32Data = append(col.Float32Data, 0)
			}
		case parquet.TypeFloat64:
			for len(col.Float64Data) <= di {
				col.Float64Data = append(col.Float64Data, 0)
			}
		case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
			// BytesData.Offsets must have di+2 entries (offsets[0..di+1]).
			for len(col.BytesData.Offsets) <= di+1 {
				col.BytesData.Offsets = append(col.BytesData.Offsets, col.BytesData.Offsets[len(col.BytesData.Offsets)-1])
			}
		case parquet.TypeDecimal:
			for len(col.DecimalData.Data) <= di {
				col.DecimalData.Data = append(col.DecimalData.Data, batch.Int128{})
			}
		}

		// Grow the null bitmap if needed.
		col.Nulls = col.Nulls.Grow(di + 1)
	}
}

// flushIfNeeded writes any partition whose buffer exceeds the flush threshold.
func (s *partitionedShuffleSink) flushIfNeeded() error {
	for p, pw := range s.parts {
		if pw.rowBuf == nil || pw.bufRows == 0 {
			continue
		}
		if pw.bufBytes < s.flushBytes {
			continue
		}
		if err := s.flushPartition(p); err != nil {
			return err
		}
	}
	return nil
}

func (s *partitionedShuffleSink) flushPartition(p int) error {
	pw := s.parts[p]
	if pw.rowBuf == nil || pw.bufRows == 0 {
		return nil
	}
	if pw.writer == nil {
		pw.writer = newShuffleWriter(pw.file, s.schema)
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
	for p := range s.parts {
		if err := s.flushPartition(p); err != nil {
			return err
		}
	}
	for p, pw := range s.parts {
		if pw.writer == nil {
			// Empty partition — leave file at zero bytes; downstream treats as no rows.
			if err := pw.file.Sync(); err != nil {
				return err
			}
			continue
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
		if err := pw.file.Sync(); err != nil {
			return err
		}
	}
	return nil
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
