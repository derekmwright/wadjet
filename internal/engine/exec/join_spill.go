package exec

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/diskio"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// spillFileSeq is a global atomic counter for unique spill file names.
// Eliminates TOCTOU race in the previous os.Stat-based approach where
// concurrent hash joins could claim the same filename.
var spillFileSeq atomic.Int64

// Grace Hash Join spill-to-disk implementation.
//
// When build-side data exceeds the memory budget, rows are partitioned by a
// secondary hash into numSpillPartitions buckets. Partitions that don't fit
// in memory are spilled to disk. During probe, rows destined for spilled
// partitions are buffered to disk as well. After the main probe pipeline
// completes, spilled partitions are processed one at a time: load build data,
// build hash table, replay probe data, emit results.

const (
	numSpillPartitions = 64
	spillPartMask      = numSpillPartitions - 1
)

// spillPartition returns the partition ID for an int64 key.
// Uses a different multiplicative constant than the primary hash table
// to ensure independent distribution.
func spillPartition(key int64) int {
	h := uint64(key) * 0x517CC1B727220A95
	return int((h >> 58) & spillPartMask) // top 6 bits for 64 partitions
}

// spillPartitionBytes returns the partition ID for a byte-serialized key.
func spillPartitionBytes(key []byte) int {
	h := uint64(0xcbf29ce484222325)
	for _, b := range key {
		h ^= uint64(b)
		h *= 0x100000001b3
	}
	return int((h >> 58) & spillPartMask)
}

// spillState tracks Grace Hash Join partition state during build and probe.
type spillState struct {
	dir           string
	numPartitions int
	spilledParts  map[int]bool // which partitions have been spilled to disk

	// Build-side: per-partition batch lists (in-memory) and spill files (on disk).
	// partBuildWriters is a long-lived per-partition writer that appends
	// subsequent batches for a spilled partition to a single file instead of
	// producing one tiny file per batch (see writeBuildBatch for rationale).
	partBuildBatches map[int][]*batch.RecordBatch
	partBuildFiles   map[int][]string // spilled build file paths per partition
	partBuildWriters map[int]*spillBatchWriter

	// Probe-side: per-partition buffered batches (flushed to disk files)
	partProbeFiles   map[int][]string
	partProbeWriters map[int]*spillBatchWriter
	probeMu          sync.Mutex // protects partProbeWriters during parallel probe

	// Track memory per partition for choosing what to spill
	partMemory map[int]int64

	// batchPartID maps a global HashJoin.buildBatches index to its partition.
	// Populated only by the partition-on-arrival path so spillOneInMemoryPartition
	// can locate and free a partition's batches in O(len(buildBatches)). Stays
	// nil under the legacy reactive-spill path, which never needs this lookup.
	batchPartID []int

	// --- Partition-on-arrival reusable build scatter state ---
	// partRowScratch[p] holds the row indices of the CURRENT arrival batch that
	// hash to partition p. Reused across arrival batches: each is truncated to
	// [:0] (capacity retained) at the top of partitionAndIndexBatch. This
	// replaces the per-batch map[int][]int + 64 fresh []int allocations.
	partRowScratch [numSpillPartitions][]int

	// openAccum[p] is the in-progress per-partition accumulator batch. Rows are
	// appended into it across arrival batches; when it fills to accumFlushRows
	// (or at build end / before its partition spills) it is "frozen": indexed
	// into the hash table, appended to buildBatches, and openAccum[p] cleared so
	// the next arrival batch mints a fresh one. A frozen accumulator backs live
	// buildRefs and is NEVER mutated or Reset while reachable from buildBatches.
	// partMemory is charged tightly at append time, so an open accumulator's
	// rows are already counted for spill selection before the freeze.
	openAccum [numSpillPartitions]*batch.RecordBatch

	// openAccumData[p] accumulates the appendRows data-byte charges (the
	// MemBytes-comparable portion, excluding null-bitmap words) made into
	// partition p's open accumulator. At freeze it is reconciled against the
	// accumulator's true MemBytes() so the growable accumulator's residual
	// capacity slack (bitmap words + bytes-arena cap) is charged and later
	// released honestly rather than undercounted.
	openAccumData [numSpillPartitions]int64

	// accumEligible reports whether the build schema is composed only of column
	// types whose storage EnsureLen can grow in place (scalar/bytes/decimal).
	// Schemas with nested ARRAY/MAP/ROW/VECTOR build columns fall back to one
	// tightly-sized indexed batch per arrival, since append-grow cannot extend
	// nested element storage. Computed once from the first arrival batch.
	accumEligible bool
	accumChecked  bool

	schema []parquet.Column // build-side schema
}

func newSpillState(dir string, schema []parquet.Column) *spillState {
	return &spillState{
		dir:              dir,
		numPartitions:    numSpillPartitions,
		spilledParts:     make(map[int]bool),
		partBuildBatches: make(map[int][]*batch.RecordBatch),
		partBuildFiles:   make(map[int][]string),
		partBuildWriters: make(map[int]*spillBatchWriter),
		partProbeFiles:   make(map[int][]string),
		partProbeWriters: make(map[int]*spillBatchWriter),
		partMemory:       make(map[int]int64),
		schema:           schema,
	}
}

// writeBuildBatch appends a build-side batch for an already-spilled partition
// to that partition's long-lived spill writer. Creates the writer lazily on
// first write. This replaces a prior pattern that created one tiny file per
// batch, producing tens of thousands of ~100 KB files under SF100 workloads
// and dominating wall time with file-open/close syscalls during probe flush.
func (s *spillState) writeBuildBatch(partID int, b *batch.RecordBatch) error {
	w, ok := s.partBuildWriters[partID]
	if !ok {
		var err error
		w, err = newSpillBatchWriter(s.dir, "build-spill")
		if err != nil {
			return err
		}
		s.partBuildWriters[partID] = w
	}
	return w.writeBatch(b)
}

// closeBuildWriters flushes and closes all build-side spill writers, attaching
// their output files to partBuildFiles. Caller must invoke this before probe
// begins reading the spilled partitions.
func (s *spillState) closeBuildWriters() error {
	for partID, w := range s.partBuildWriters {
		path, err := w.close()
		if err != nil {
			return err
		}
		if path != "" {
			s.partBuildFiles[partID] = append(s.partBuildFiles[partID], path)
		}
	}
	s.partBuildWriters = nil
	return nil
}

// largestInMemoryPartition returns the partition ID with the most memory usage.
func (s *spillState) largestInMemoryPartition() int {
	bestPart := -1
	var bestMem int64
	for p, mem := range s.partMemory {
		if !s.spilledParts[p] && mem > bestMem {
			bestMem = mem
			bestPart = p
		}
	}
	return bestPart
}

// writeProbeRow buffers a probe batch for a spilled partition.
// Thread-safe: called concurrently from parallel pipeline workers.
func (s *spillState) writeProbeRow(partID int, b *batch.RecordBatch) error {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	w, ok := s.partProbeWriters[partID]
	if !ok {
		var err error
		w, err = newSpillBatchWriter(s.dir, fmt.Sprintf("probe-part-%d", partID))
		if err != nil {
			return err
		}
		s.partProbeWriters[partID] = w
	}
	return w.writeBatch(b)
}

// closeProbeWriters flushes and closes all probe spill writers.
func (s *spillState) closeProbeWriters() error {
	for partID, w := range s.partProbeWriters {
		path, err := w.close()
		if err != nil {
			return err
		}
		if path != "" {
			s.partProbeFiles[partID] = append(s.partProbeFiles[partID], path)
		}
	}
	s.partProbeWriters = nil
	return nil
}

// cleanup removes all spill files and clears the partition bookkeeping.
// Clearing matters under morsel-parallel fragments: cloned HashJoinProbes
// share this spillState with per-probe drain cursors, and the fragment
// runner drains every clone's chain. The first drain processes all spilled
// partitions and lands here; without the clears, the next clone's
// HasPendingFlush would see stale spilledParts entries and re-open files
// this call just deleted.
func (s *spillState) cleanup() {
	for _, files := range s.partBuildFiles {
		for _, f := range files {
			os.Remove(f)
		}
	}
	for _, files := range s.partProbeFiles {
		for _, f := range files {
			os.Remove(f)
		}
	}
	clear(s.spilledParts)
	clear(s.partBuildFiles)
	clear(s.partProbeFiles)
}

// ---- Columnar Batch Spill I/O ----

// spillBatchWriter accumulates batches and writes them to a single file.
type spillBatchWriter struct {
	dir    string
	prefix string
	f      *os.File
	fl     *diskio.Flusher
	w      *bufio.Writer
	count  int
	path   string
}

func newSpillBatchWriter(dir, prefix string) (*spillBatchWriter, error) {
	seq := spillFileSeq.Add(1)
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.%d.bin", prefix, os.Getpid(), seq))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating spill file: %w", err)
	}
	wf, fl := diskio.NewWriter(f, diskio.Spill)
	w := bufio.NewWriterSize(wf, 64*1024) // EXPERIMENT
	// Reserve space for batch count (will be written on close)
	var buf [4]byte
	w.Write(buf[:4])
	return &spillBatchWriter{dir: dir, prefix: prefix, f: f, fl: fl, w: w, path: path}, nil
}

func (sw *spillBatchWriter) writeBatch(b *batch.RecordBatch) error {
	if err := writeColumnarBatch(sw.w, b); err != nil {
		return err
	}
	sw.count++
	return nil
}

// abort closes the writer and unconditionally removes the partial file.
// Error paths must use this instead of close(): a half-written run is
// unusable, and in the worker-lifetime shared spill dir it would outlive
// the failed query — compounding exactly the ENOSPC condition that most
// often causes the failure in the first place. Skips the page-cache
// writeback (the data is about to be unlinked). Idempotent.
func (sw *spillBatchWriter) abort() {
	if sw.f == nil {
		return
	}
	sw.f.Close()
	sw.f = nil
	os.Remove(sw.path)
}

func (sw *spillBatchWriter) close() (string, error) {
	if sw.f == nil {
		return "", nil
	}
	// On any failure the file is unusable — remove it rather than strand
	// a partial run in the shared spill dir (see abort).
	if err := sw.w.Flush(); err != nil {
		sw.abort()
		return "", fmt.Errorf("flushing spill: %w", err)
	}
	// Seek back and write batch count
	if _, err := sw.f.Seek(0, 0); err != nil {
		sw.abort()
		return "", err
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(sw.count))
	if _, err := sw.f.Write(buf[:]); err != nil {
		sw.abort()
		return "", err
	}
	sw.fl.Finish()
	if err := sw.f.Close(); err != nil {
		sw.f = nil
		os.Remove(sw.path)
		return "", err
	}
	sw.f = nil
	if sw.count == 0 {
		os.Remove(sw.path)
		return "", nil
	}
	return sw.path, nil
}

// writeSpillBatches writes multiple batches to a new spill file.
func writeSpillBatches(dir string, batches []*batch.RecordBatch) (string, error) {
	seq := spillFileSeq.Add(1)
	path := filepath.Join(dir, fmt.Sprintf("build-spill-%d.%d.bin", os.Getpid(), seq))

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("creating spill file: %w", err)
	}
	defer f.Close()

	// 64 KB matches the convention in newSpillBatchWriter and
	// aggregate_partial_spill — local-disk spill writes coalesce fine at
	// this size, and a smaller buffer reduces transient heap during the
	// moment spill is firing.
	wf, fl := diskio.NewWriter(f, diskio.Spill)
	w := bufio.NewWriterSize(wf, 64*1024)

	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(batches)))
	w.Write(buf[:4])

	for _, b := range batches {
		if err := writeColumnarBatch(w, b); err != nil {
			return "", err
		}
	}

	if err := w.Flush(); err != nil {
		return "", fmt.Errorf("flushing spill: %w", err)
	}
	fl.Finish()
	return path, nil
}

// readSpillBatches reads all batches from a columnar spill file.
// Used by the build-side reload path which is bounded; the probe-side
// streaming path uses spillBatchReader.
func readSpillBatches(path string) ([]*batch.RecordBatch, error) {
	r, err := openSpillBatchReader(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	batches := make([]*batch.RecordBatch, 0, r.numBatches)
	for {
		b, err := r.Next()
		if err != nil {
			return nil, err
		}
		if b == nil {
			break
		}
		batches = append(batches, b)
	}
	return batches, nil
}

// spillBatchReader streams batches one at a time from a columnar spill
// file. Used by the spill-flush probe path so we don't pin every probe
// batch from a partition in memory at once.
type spillBatchReader struct {
	f          *os.File
	r          *bufio.Reader
	numBatches int
	idx        int
}

func openSpillBatchReader(path string) (*spillBatchReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening spill file: %w", err)
	}
	// Drop-behind: spill runs are read back once and deleted; dropping
	// consumed windows keeps a multi-GB merge from displacing reusable
	// page-cache pages. (The empty-PARTITION-BY window two-pass evaluator
	// re-reads its runs once more from NVMe — accepted, the runs would
	// rarely still be resident in the regimes where this matters.)
	r := bufio.NewReaderSize(diskio.NewDropBehindReader(f), 1024*1024)
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	return &spillBatchReader{
		f:          f,
		r:          r,
		numBatches: int(binary.LittleEndian.Uint32(buf[:])),
	}, nil
}

// Next reads the next batch from the file, or returns (nil, nil) at EOF.
func (sr *spillBatchReader) Next() (*batch.RecordBatch, error) {
	if sr.idx >= sr.numBatches {
		return nil, nil
	}
	b, err := readColumnarBatch(sr.r)
	if err != nil {
		return nil, fmt.Errorf("reading batch %d: %w", sr.idx, err)
	}
	sr.idx++
	return b, nil
}

func (sr *spillBatchReader) Close() error {
	if sr.f == nil {
		return nil
	}
	err := sr.f.Close()
	sr.f = nil
	return err
}

// writeColumnarBatch writes a single RecordBatch in columnar binary format.
// Format: [numRows:u32] [numCols:u32] per-column: [typeID:u8] [nameLen:u16] [name] [hasNulls:u8] [nullBitmap?] [data]
func writeColumnarBatch(w *bufio.Writer, b *batch.RecordBatch) error {
	var buf [8]byte
	numRows := b.Len
	numCols := len(b.Columns)

	binary.LittleEndian.PutUint32(buf[:4], uint32(numRows))
	w.Write(buf[:4])
	binary.LittleEndian.PutUint32(buf[:4], uint32(numCols))
	w.Write(buf[:4])

	for i, col := range b.Columns {
		// Type
		w.WriteByte(byte(col.Type))

		// Name
		name := b.Schema[i].Name
		binary.LittleEndian.PutUint16(buf[:2], uint16(len(name)))
		w.Write(buf[:2])
		w.WriteString(name)

		// DECIMAL scale+precision: the data section stores raw scaled
		// integers, so a reader without the scale rebuilds a scale-0
		// vector and every spilled decimal renders 10^scale too large
		// (same class as the WSHF header fix — issue #144).
		if col.Type == batch.TypeDecimal {
			w.WriteByte(byte(col.DecimalData.Scale))
			w.WriteByte(byte(b.Schema[i].Precision))
		}

		// Nullable flag from schema
		if b.Schema[i].Nullable {
			w.WriteByte(1)
		} else {
			w.WriteByte(0)
		}

		// Null bitmap
		hasNulls := col.Nulls.HasNulls()
		if hasNulls {
			w.WriteByte(1)
			bitmapLen := (numRows + 7) / 8
			bitmapData := make([]byte, bitmapLen)
			for j := 0; j < numRows; j++ {
				if col.Nulls.IsNullFast(j) {
					bitmapData[j/8] |= 1 << (uint(j) % 8)
				}
			}
			w.Write(bitmapData)
		} else {
			w.WriteByte(0)
		}

		// Typed data
		if err := writeColumnData(w, col, numRows, buf[:]); err != nil {
			return err
		}
	}
	return nil
}

func writeColumnData(w *bufio.Writer, v *batch.Vector, n int, buf []byte) error {
	switch v.Type {
	case batch.TypeBool:
		for i := 0; i < n; i++ {
			if v.BoolData[i] {
				w.WriteByte(1)
			} else {
				w.WriteByte(0)
			}
		}

	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint32(buf[:4], uint32(v.Int32Data[i]))
			w.Write(buf[:4])
		}

	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint64(buf[:8], uint64(v.Int64Data[i]))
			w.Write(buf[:8])
		}

	case batch.TypeFloat32:
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(v.Float32Data[i]))
			w.Write(buf[:4])
		}

	case batch.TypeFloat64:
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint64(buf[:8], math.Float64bits(v.Float64Data[i]))
			w.Write(buf[:8])
		}

	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		// Write data length + data + offsets
		dataLen := uint32(len(v.BytesData.Data))
		binary.LittleEndian.PutUint32(buf[:4], dataLen)
		w.Write(buf[:4])
		w.Write(v.BytesData.Data)
		// Offsets: n+1 uint32 values
		for i := 0; i <= n; i++ {
			binary.LittleEndian.PutUint32(buf[:4], v.BytesData.Offsets[i])
			w.Write(buf[:4])
		}

	case batch.TypeDecimal:
		for i := 0; i < n; i++ {
			d := v.DecimalData.Data[i]
			binary.LittleEndian.PutUint64(buf[:8], d.Lo)
			w.Write(buf[:8])
			binary.LittleEndian.PutUint64(buf[:8], uint64(d.Hi))
			w.Write(buf[:8])
		}

	case batch.TypeVector:
		// [dim:u32] then n*dim float32s. dim is per-column metadata that the
		// Vector carries outside the schema, so it must ride in the file.
		binary.LittleEndian.PutUint32(buf[:4], uint32(v.VectorDim))
		w.Write(buf[:4])
		total := n * v.VectorDim
		for i := 0; i < total; i++ {
			binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(v.Float32Data[i]))
			w.Write(buf[:4])
		}

	case batch.TypeArray, batch.TypeMap:
		// [offsets:(n+1)*u32] [child vector]. MAP shares ARRAY's layout (its
		// child is a ROW(key,value) vector), so the recursion covers it.
		if len(v.Offsets) < n+1 {
			return fmt.Errorf("columnar spill: %v column has %d offsets, need %d", v.Type, len(v.Offsets), n+1)
		}
		for i := 0; i <= n; i++ {
			binary.LittleEndian.PutUint32(buf[:4], uint32(v.Offsets[i]))
			w.Write(buf[:4])
		}
		childLen := int(v.Offsets[n])
		if v.Child == nil && childLen > 0 {
			return fmt.Errorf("columnar spill: %v column has %d child elements but nil child vector", v.Type, childLen)
		}
		if err := writeNestedVector(w, v.Child, childLen, buf); err != nil {
			return err
		}

	case batch.TypeRow:
		// [numChildren:u32] then per child: [fieldNameLen:u16][fieldName]
		// [child vector]. Children are parallel arrays of the parent's length.
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(v.Children)))
		w.Write(buf[:4])
		for j, ch := range v.Children {
			var name string
			if j < len(v.FieldNames) {
				name = v.FieldNames[j]
			}
			binary.LittleEndian.PutUint16(buf[:2], uint16(len(name)))
			w.Write(buf[:2])
			w.WriteString(name)
			if err := writeNestedVector(w, ch, n, buf); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("columnar spill: unsupported column type %v", v.Type)
	}
	return nil
}

// nestedNilChildMarker is the type byte written for a nil child vector (only
// legal when the child has zero elements). 0xFF is outside the TypeID space.
const nestedNilChildMarker = 0xFF

// writeNestedVector serializes a child vector of a nested column:
// [typeID:u8] [hasNulls:u8] [nullBitmap?] [typed data via writeColumnData].
// Mirrors the per-column framing of writeColumnarBatch minus name/nullable,
// which only exist at the schema level.
func writeNestedVector(w *bufio.Writer, v *batch.Vector, n int, buf []byte) error {
	if v == nil {
		if n > 0 {
			return fmt.Errorf("columnar spill: nil nested vector with %d elements", n)
		}
		w.WriteByte(nestedNilChildMarker)
		return nil
	}
	w.WriteByte(byte(v.Type))
	// DECIMAL children carry their scale (same rationale as the top-level
	// column header — the data section is raw scaled integers).
	if v.Type == batch.TypeDecimal {
		w.WriteByte(byte(v.DecimalData.Scale))
	}
	if v.Nulls.HasNulls() {
		w.WriteByte(1)
		bitmapLen := (n + 7) / 8
		bitmapData := make([]byte, bitmapLen)
		for j := 0; j < n; j++ {
			if v.Nulls.IsNullFast(j) {
				bitmapData[j/8] |= 1 << (uint(j) % 8)
			}
		}
		w.Write(bitmapData)
	} else {
		w.WriteByte(0)
	}
	return writeColumnData(w, v, n, buf)
}

// readNestedVector mirrors writeNestedVector.
func readNestedVector(r *bufio.Reader, n int, buf []byte) (*batch.Vector, error) {
	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("reading nested vector type: %w", err)
	}
	if typeByte == nestedNilChildMarker {
		if n > 0 {
			return nil, fmt.Errorf("columnar spill: nil nested vector marker with %d elements", n)
		}
		return nil, nil
	}
	scale := 0
	if parquet.TypeID(typeByte) == parquet.TypeDecimal {
		sb, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("reading nested decimal scale: %w", err)
		}
		scale = int(sb)
	}
	v := batch.NewVectorWithScale(parquet.TypeID(typeByte), n, scale)
	hasNulls, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("reading nested hasNulls: %w", err)
	}
	if hasNulls == 1 {
		bitmapLen := (n + 7) / 8
		bitmapData := make([]byte, bitmapLen)
		if _, err := io.ReadFull(r, bitmapData); err != nil {
			return nil, fmt.Errorf("reading nested null bitmap: %w", err)
		}
		for j := 0; j < n; j++ {
			if bitmapData[j/8]&(1<<(uint(j)%8)) != 0 {
				v.Nulls.SetNull(j)
			}
		}
	}
	if err := readColumnData(r, v, n, buf); err != nil {
		return nil, fmt.Errorf("reading nested vector data: %w", err)
	}
	return v, nil
}

// readColumnarBatch reads a single RecordBatch from the columnar binary format.
// Schema and data are interleaved per-column to match the write order.
func readColumnarBatch(r *bufio.Reader) (*batch.RecordBatch, error) {
	var buf [8]byte

	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return nil, fmt.Errorf("reading numRows: %w", err)
	}
	numRows := int(binary.LittleEndian.Uint32(buf[:4]))

	if _, err := io.ReadFull(r, buf[:4]); err != nil {
		return nil, fmt.Errorf("reading numCols: %w", err)
	}
	numCols := int(binary.LittleEndian.Uint32(buf[:4]))

	// Read each column's schema + data interleaved (matches write order)
	schema := make([]parquet.Column, numCols)
	cols := make([]*batch.Vector, numCols)

	for i := 0; i < numCols; i++ {
		// Type
		typeByte, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("reading column type: %w", err)
		}

		// Name
		if _, err := io.ReadFull(r, buf[:2]); err != nil {
			return nil, fmt.Errorf("reading name length: %w", err)
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[:2]))
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBuf); err != nil {
			return nil, fmt.Errorf("reading column name: %w", err)
		}

		typeID := parquet.TypeID(typeByte)

		// DECIMAL scale+precision (written after the name — see
		// writeColumnarBatch).
		scale, precision := 0, 0
		if typeID == parquet.TypeDecimal {
			sb, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("reading decimal scale: %w", err)
			}
			pb, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("reading decimal precision: %w", err)
			}
			scale, precision = int(sb), int(pb)
		}

		// Nullable
		nullable, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("reading nullable flag: %w", err)
		}

		schema[i] = parquet.Column{
			Name:      string(nameBuf),
			Type:      typeID,
			Nullable:  nullable == 1,
			Scale:     scale,
			Precision: precision,
		}

		// Create vector for this column
		col := batch.NewVectorWithScale(typeID, numRows, scale)
		cols[i] = col

		// Null bitmap
		hasNulls, err2 := r.ReadByte()
		if err2 != nil {
			return nil, fmt.Errorf("reading hasNulls: %w", err2)
		}
		if hasNulls == 1 {
			bitmapLen := (numRows + 7) / 8
			bitmapData := make([]byte, bitmapLen)
			if _, err := io.ReadFull(r, bitmapData); err != nil {
				return nil, fmt.Errorf("reading null bitmap: %w", err)
			}
			for j := 0; j < numRows; j++ {
				if bitmapData[j/8]&(1<<(uint(j)%8)) != 0 {
					col.Nulls.SetNull(j)
				}
			}
		}

		// Data
		if err := readColumnData(r, col, numRows, buf[:]); err != nil {
			return nil, fmt.Errorf("reading column %d data: %w", i, err)
		}
	}

	return &batch.RecordBatch{
		Columns: cols,
		Schema:  schema,
		Len:     numRows,
	}, nil
}

func readColumnData(r *bufio.Reader, v *batch.Vector, n int, buf []byte) error {
	switch v.Type {
	case batch.TypeBool:
		for i := 0; i < n; i++ {
			b, err := r.ReadByte()
			if err != nil {
				return err
			}
			v.BoolData[i] = b == 1
		}

	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i := 0; i < n; i++ {
			if _, err := io.ReadFull(r, buf[:4]); err != nil {
				return err
			}
			v.Int32Data[i] = int32(binary.LittleEndian.Uint32(buf[:4]))
		}

	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i := 0; i < n; i++ {
			if _, err := io.ReadFull(r, buf[:8]); err != nil {
				return err
			}
			v.Int64Data[i] = int64(binary.LittleEndian.Uint64(buf[:8]))
		}

	case batch.TypeFloat32:
		for i := 0; i < n; i++ {
			if _, err := io.ReadFull(r, buf[:4]); err != nil {
				return err
			}
			v.Float32Data[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[:4]))
		}

	case batch.TypeFloat64:
		for i := 0; i < n; i++ {
			if _, err := io.ReadFull(r, buf[:8]); err != nil {
				return err
			}
			v.Float64Data[i] = math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
		}

	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		// Read data
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return err
		}
		dataLen := int(binary.LittleEndian.Uint32(buf[:4]))
		data := make([]byte, dataLen)
		if dataLen > 0 {
			if _, err := io.ReadFull(r, data); err != nil {
				return err
			}
		}
		// Read offsets
		offsets := make([]uint32, n+1)
		for i := 0; i <= n; i++ {
			if _, err := io.ReadFull(r, buf[:4]); err != nil {
				return err
			}
			offsets[i] = binary.LittleEndian.Uint32(buf[:4])
		}
		v.BytesData.Data = data
		v.BytesData.Offsets = offsets

	case batch.TypeDecimal:
		for i := 0; i < n; i++ {
			if _, err := io.ReadFull(r, buf[:8]); err != nil {
				return err
			}
			lo := binary.LittleEndian.Uint64(buf[:8])
			if _, err := io.ReadFull(r, buf[:8]); err != nil {
				return err
			}
			hi := int64(binary.LittleEndian.Uint64(buf[:8]))
			v.DecimalData.Data[i] = batch.Int128{Lo: lo, Hi: hi}
		}

	case batch.TypeVector:
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return err
		}
		dim := int(binary.LittleEndian.Uint32(buf[:4]))
		v.VectorDim = dim
		v.Float32Data = make([]float32, n*dim)
		for i := range v.Float32Data {
			if _, err := io.ReadFull(r, buf[:4]); err != nil {
				return err
			}
			v.Float32Data[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[:4]))
		}

	case batch.TypeArray, batch.TypeMap:
		// NewVector pre-allocated Offsets to n+1.
		for i := 0; i <= n; i++ {
			if _, err := io.ReadFull(r, buf[:4]); err != nil {
				return err
			}
			v.Offsets[i] = int32(binary.LittleEndian.Uint32(buf[:4]))
		}
		childLen := int(v.Offsets[n])
		child, err := readNestedVector(r, childLen, buf)
		if err != nil {
			return err
		}
		v.Child = child

	case batch.TypeRow:
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return err
		}
		numChildren := int(binary.LittleEndian.Uint32(buf[:4]))
		v.Children = make([]*batch.Vector, numChildren)
		v.FieldNames = make([]string, numChildren)
		for j := 0; j < numChildren; j++ {
			if _, err := io.ReadFull(r, buf[:2]); err != nil {
				return err
			}
			nameLen := int(binary.LittleEndian.Uint16(buf[:2]))
			nameBuf := make([]byte, nameLen)
			if _, err := io.ReadFull(r, nameBuf); err != nil {
				return err
			}
			v.FieldNames[j] = string(nameBuf)
			ch, err := readNestedVector(r, n, buf)
			if err != nil {
				return err
			}
			v.Children[j] = ch
		}

	default:
		return fmt.Errorf("columnar spill: unsupported column type %v", v.Type)
	}
	return nil
}

// ---- Spilled Partition Processing ----

// preloadedBuild holds pre-fetched build batches for a spilled partition.
type preloadedBuild struct {
	batches []*batch.RecordBatch
	err     error
}

// loadBuildBatches reads all build spill files for a partition from disk.
func (h *HashJoin) loadBuildBatches(partID int) ([]*batch.RecordBatch, error) {
	ss := h.spillState
	var buildBatches []*batch.RecordBatch
	for _, path := range ss.partBuildFiles[partID] {
		batches, err := readSpillBatches(path)
		if err != nil {
			return nil, fmt.Errorf("reading build spill: %w", err)
		}
		buildBatches = append(buildBatches, batches...)
	}
	return buildBatches, nil
}

// NOTE: the previous processSpilledPartitions helper has been replaced by
// HashJoinProbe.NextFlush, which walks spilled partitions lazily one at a
// time and yields each output batch as it's produced. The old eager-collect
// helper accumulated every joined batch from every spilled partition into a
// single in-memory slice and was the dominant heap consumer for SF100 Q05.

// buildTempJoinFromBatches constructs a temporary HashJoin over a single
// spilled partition's pre-loaded build batches. Used by the streaming
// spill-flush path to assemble per-partition state without producing any
// joined output up-front.
func (h *HashJoin) buildTempJoinFromBatches(buildBatches []*batch.RecordBatch) (*HashJoin, error) {
	if len(buildBatches) == 0 {
		return nil, nil
	}
	tmpJoin := &HashJoin{
		JoinType:  h.JoinType,
		LeftKeys:  h.LeftKeys,
		RightKeys: h.RightKeys,
		// The pair's resolved COMMON type travels with the keys (#615).
		// Without it a spilled partition rebuilds its index at each
		// column's own encoding and the cross-width join stops matching
		// PAST THE SPILL BOUNDARY only — the hardest class of wrong answer
		// to see, and the one ADR-0023 exists to keep out of this path.
		KeyTypes:        h.KeyTypes,
		SemiAntiFilter:  h.SemiAntiFilter,
		Residual:        h.Residual,
		SemiAntiKeyOnly: h.SemiAntiKeyOnly,
		BuildTableAlias: h.BuildTableAlias,
		keyBuf:          make([]byte, 0, 128),
		// A NULL anywhere in the build poisons a null-aware anti join's whole
		// answer, and grace partitioning sends every NULL key to ONE partition
		// — so the per-partition join has to inherit what the whole build saw
		// rather than what its own partition holds (#507).
		NullAwareAnti:   h.NullAwareAnti,
		buildHasNullKey: h.buildHasNullKey,
	}

	totalRows := 0
	for _, b := range buildBatches {
		totalRows += b.Len
	}
	tmpJoin.BuildRowHint = int64(totalRows)

	tmpJoin.buildSchema = buildBatches[0].Schema
	tmpJoin.buildKeyIdx = make([]int, len(tmpJoin.RightKeys))
	for i, col := range tmpJoin.RightKeys {
		// columnIndexFallback, not ColumnIndex: the key is spelled the way the
		// PLAN wrote it (`ON b.bk = p.pk` keeps the qualifier) while the
		// spilled batch carries the build relation's own bare column names.
		// Every other build path resolves it with the fallback; this one did
		// not, so an alias-qualified key resolved to -1 here, the replay keyed
		// every spilled build row on a lone null flag byte, and NOTHING in a
		// spilled partition ever matched — silently, on every join type.
		tmpJoin.buildKeyIdx[i] = columnIndexFallback(buildBatches[0], col)
	}
	tmpJoin.tryEnableIntKey(buildBatches[0])
	if tmpJoin.buildKeyErr != nil {
		return nil, tmpJoin.buildKeyErr
	}

	if !tmpJoin.SemiAntiKeyOnly {
		tmpJoin.arena = make([]buildRef, 0, totalRows)
		tmpJoin.arenaNext = make([]int32, 0, totalRows)
	}

	// Pre-size the hash index to fit totalRows. tryEnableIntKey already sized
	// the int path via BuildRowHint above; the string path needs an explicit
	// pre-size here. Without it, indexBuildBatch's PutNoGrow on a 64-bucket
	// default table loops forever once load exceeds 100% — the spilled-
	// partition replay path used to silently bypass this for int keys but
	// would deadlock for string keys.
	if !tmpJoin.useIntKey && !tmpJoin.useDualIntKey {
		tmpJoin.strIndex = newStrHashTable(totalRows)
	}

	for _, b := range buildBatches {
		batchIdx := int32(len(tmpJoin.buildBatches))
		tmpJoin.buildBatches = append(tmpJoin.buildBatches, b)
		if tmpJoin.SemiAntiKeyOnly {
			tmpJoin.indexBuildBatchKeyOnly(b)
		} else {
			tmpJoin.indexBuildBatch(b, batchIdx)
		}
	}

	// Same rule as the two full-build paths (join.go, join_partition_arrival.go):
	// RightSemi/RightAnti mark build entries during the probe and read the
	// marks back out of the arena afterwards, so they need the bitmap too.
	// Without it FlushAntiMatched reads a nil arenaMatched as "nothing
	// matched" and emits the partition's whole build side.
	if (tmpJoin.JoinType == RightJoin || tmpJoin.JoinType == FullOuterJoin ||
		tmpJoin.JoinType == RightSemiJoin || tmpJoin.JoinType == RightAntiJoin) && len(tmpJoin.arena) > 0 {
		tmpJoin.arenaMatched = make([]bool, len(tmpJoin.arena))
	}
	tmpJoin.buildBloom()
	tmpJoin.warmBuildNullBitmaps()
	tmpJoin.buildDone = true
	return tmpJoin, nil
}

// indexBuildBatch indexes a build batch into the hash table (full row storage path).
func (h *HashJoin) indexBuildBatch(b *batch.RecordBatch, batchIdx int32) {
	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				key, ok := intKeyFromVector(col, int(si))
				if !ok {
					h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					continue
				}
				h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				h.buildRows++
			}
		} else if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.arenaAppendInt(int64(data[rowIdx]), buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				}
			default:
				data := col.Int64Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.arenaAppendInt(data[rowIdx], buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				}
			}
			h.buildRows += int64(b.Len)
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					continue
				}
				h.arenaAppendInt(key, buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		}
	} else if h.useDualIntKey {
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, int(si))
				if !ok {
					h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
					continue
				}
				h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
					h.nullBuildKey(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
					continue
				}
				h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		}
	} else {
		if h.strIndex == nil {
			h.strIndex = newStrHashTable(64)
		}
		if b.Sel != nil {
			for _, si := range b.Sel {
				if !h.buildKeyFromBatch(b, int(si)) {
					h.buildHasNullKey = true
				}
				h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				if !h.buildKeyFromBatch(b, rowIdx) {
					h.buildHasNullKey = true
				}
				h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(rowIdx)})
				h.buildRows++
			}
		}
	}
}

// indexBuildBatchKeyOnly indexes a build batch (key-only mode for semi/anti without filter).
func (h *HashJoin) indexBuildBatchKeyOnly(b *batch.RecordBatch) {
	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				key, ok := intKeyFromVector(col, int(si))
				if !ok {
					h.nullBuildKeyOnly()
					continue
				}
				h.intIndex.Put(key, 0)
			}
		} else if !col.Nulls.HasNulls() {
			switch col.Type {
			case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
				data := col.Int32Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.intIndex.Put(int64(data[rowIdx]), 0)
				}
			default:
				data := col.Int64Data
				for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
					h.intIndex.Put(data[rowIdx], 0)
				}
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				key, ok := intKeyFromVector(col, rowIdx)
				if !ok {
					h.nullBuildKeyOnly()
					continue
				}
				h.intIndex.Put(key, 0)
			}
		}
	} else if h.useDualIntKey {
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, int(si))
				if !ok {
					h.nullBuildKeyOnly()
					continue
				}
				h.intIndex.Put(dualIntHash(a, bb), 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
					h.nullBuildKeyOnly()
					continue
				}
				h.intIndex.Put(dualIntHash(a, bb), 0)
			}
		}
	} else {
		if h.strIndex == nil {
			h.strIndex = newStrHashTable(64)
		}
		if b.Sel != nil {
			for _, si := range b.Sel {
				if !h.buildKeyFromBatch(b, int(si)) {
					h.nullBuildKeyOnly()
				}
				h.strIndex.GetOrInsert(h.keyBuf, 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				if !h.buildKeyFromBatch(b, rowIdx) {
					h.nullBuildKeyOnly()
				}
				h.strIndex.GetOrInsert(h.keyBuf, 0)
			}
		}
	}
	h.buildRows += int64(b.ActiveLen())
}

// ---- FlushableOperator implementation for HashJoinProbe ----

// hasPendingUnmatched reports whether this join still owes a RIGHT/FULL
// join's unmatched build rows. It is true before the first flush attempt even
// when there turn out to be none — the attempt itself is what settles that,
// and it is idempotent.
func (p *HashJoinProbe) hasPendingUnmatched() bool {
	h := p.join
	if h.JoinType != RightJoin && h.JoinType != FullOuterJoin {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.unmatchedFlushed
}

// HasPendingFlush returns true if there are spilled partitions still left to
// process, or if a RIGHT/FULL join still owes its unmatched build rows.
func (p *HashJoinProbe) HasPendingFlush() bool {
	if p.hasPendingUnmatched() {
		return true
	}
	ss := p.join.spillState
	if ss == nil {
		return false
	}
	// Before first NextFlush call: any spilled partitions to begin with?
	if !p.spillFlushInit {
		return len(ss.spilledParts) > 0
	}
	// Mid-flush: more probe data in the current partition or more partitions?
	if p.spillFlushTmpJoin != nil && !p.spillFlushDone {
		return true
	}
	return p.spillFlushPartIdx < len(p.spillFlushPartIDs)
}

// NextFlush returns the next result batch from spilled-partition processing,
// fully streaming. One partition is held in memory at a time; within that
// partition probe batches are read from disk one at a time and the resulting
// joined batch is yielded immediately. The previous implementation
// accumulated every joined output from every spilled partition into a single
// in-memory slice before yielding any — for SF100 Q05 lineitem⋈orders that
// pinned tens of GB of joined output simultaneously.
func (p *HashJoinProbe) NextFlush(ctx context.Context) (*batch.RecordBatch, error) {
	ss := p.join.spillState
	if ss == nil {
		// No spill: the only thing a drained probe still owes is a RIGHT or
		// FULL join's unmatched build rows.
		return p.FlushUnmatchedRows(), nil
	}

	// One-time init: collect partition IDs and close probe + build writers so
	// spill files are flushed and readable before the probe reader opens them.
	if !p.spillFlushInit {
		if err := ss.closeProbeWriters(); err != nil {
			return nil, fmt.Errorf("closing probe writers: %w", err)
		}
		if err := ss.closeBuildWriters(); err != nil {
			return nil, fmt.Errorf("closing build writers: %w", err)
		}
		p.spillFlushPartIDs = make([]int, 0, len(ss.spilledParts))
		for id := range ss.spilledParts {
			p.spillFlushPartIDs = append(p.spillFlushPartIDs, id)
		}
		// Kick off the first partition's build prefetch.
		if len(p.spillFlushPartIDs) > 0 {
			p.spillFlushPrefetch = make(chan preloadedBuild, 1)
			firstID := p.spillFlushPartIDs[0]
			ch := p.spillFlushPrefetch
			go func() {
				// Decoding a spill run on a goroutine nobody joins: a panic
				// in the decoder ends the process unless it is delivered
				// down the same channel the error travels (#511).
				defer CatchQueryPanic(ctx, "join spill build prefetch", func(err error) {
					ch <- preloadedBuild{nil, err}
				})
				b, err := p.join.loadBuildBatches(firstID)
				ch <- preloadedBuild{b, err}
			}()
		}
		p.spillFlushInit = true
	}

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// If we don't have an open partition, advance to the next one.
		if p.spillFlushTmpJoin == nil {
			if p.spillFlushPartIdx >= len(p.spillFlushPartIDs) {
				// All partitions consumed. The in-memory partitions' own
				// unmatched build rows are still owed for RIGHT/FULL.
				ss.cleanup()
				return p.FlushUnmatchedRows(), nil
			}
			if err := p.openNextSpillPartition(ctx); err != nil {
				return nil, err
			}
			// openNextSpillPartition may have set spillFlushTmpJoin=nil if the
			// partition had no build rows; loop to advance.
			if p.spillFlushTmpJoin == nil {
				continue
			}
		}

		// Try to read the next probe batch from the current partition.
		out, err := p.nextSpilledProbeBatch(ctx)
		if err != nil {
			return nil, err
		}
		if out != nil {
			return out, nil
		}

		// Current partition exhausted: emit the build-side rows this join
		// type owes for the partition, then close it down. The main join's
		// own flush SKIPS every arena entry whose partition was evicted
		// (HashJoin.residentBuildBatch), so this is the only place those rows
		// are emitted — and the partition replay read them from disk, which
		// is what makes them readable at all (#550).
		if !p.spillFlushDone {
			p.spillFlushDone = true
			var pending *batch.RecordBatch
			switch p.spillFlushTmpJoin.JoinType {
			case RightJoin, FullOuterJoin:
				pending = p.spillFlushTmpProbe.FlushUnmatched(p.join.spillLeftSchema)
			case RightAntiJoin:
				pending = p.spillFlushTmpProbe.FlushAntiMatched()
			case RightSemiJoin:
				pending = p.spillFlushTmpProbe.FlushMatched()
			}
			if pending != nil {
				pending.Detach()
				// Close the partition AFTER returning the batch. Mark it done
				// so the next call advances.
				p.closeCurrentSpillPartition()
				return pending, nil
			}
		}
		p.closeCurrentSpillPartition()
		// Loop to advance to the next partition.
	}
}

// openNextSpillPartition advances to the next spilled partition: receives
// its pre-fetched build batches, builds a temp HashJoin, kicks off the next
// partition's prefetch, and opens its first probe spill file.
func (p *HashJoinProbe) openNextSpillPartition(ctx context.Context) error {
	partID := p.spillFlushPartIDs[p.spillFlushPartIdx]
	p.spillFlushPartIdx++

	pf := <-p.spillFlushPrefetch
	if pf.err != nil {
		return fmt.Errorf("processing spilled partition %d: %w", partID, pf.err)
	}

	// Kick off the next partition's prefetch (overlap I/O with build+probe).
	if p.spillFlushPartIdx < len(p.spillFlushPartIDs) {
		nextID := p.spillFlushPartIDs[p.spillFlushPartIdx]
		p.spillFlushPrefetch = make(chan preloadedBuild, 1)
		ch := p.spillFlushPrefetch
		go func() {
			// See the first prefetch above: the panic has to come back as
			// this partition's error, not as a dead process (#511).
			defer CatchQueryPanic(ctx, "join spill build prefetch", func(err error) {
				ch <- preloadedBuild{nil, err}
			})
			b, err := p.join.loadBuildBatches(nextID)
			ch <- preloadedBuild{b, err}
		}()
	} else {
		p.spillFlushPrefetch = nil
	}

	if len(pf.batches) == 0 {
		// No build rows for this partition: nothing to probe against.
		p.spillFlushDone = true
		return nil
	}

	tmpJoin, err := p.join.buildTempJoinFromBatches(pf.batches)
	if err != nil {
		return fmt.Errorf("building hash table for spilled partition %d: %w", partID, err)
	}
	probe := tmpJoin.Probe()
	probe.OutputFilter = p.join.spillOutputFilter
	probe.LateMaterialize = p.LateMaterialize
	// nextSpilledProbeBatch drains NextOutput before reading the next probe
	// batch, so the partition probe may bound its fan-out exactly like the
	// streaming one — spilled partitions are where the skewed keys land.
	if p.boundOutput {
		probe.EnableBoundedOutput()
	}
	if err := probe.Init(ctx); err != nil {
		return fmt.Errorf("initialising probe for spilled partition %d: %w", partID, err)
	}

	p.spillFlushTmpJoin = tmpJoin
	p.spillFlushTmpProbe = probe
	p.spillFlushProbeFiles = p.join.spillState.partProbeFiles[partID]
	p.spillFlushProbeFileIx = 0
	p.spillFlushDone = false
	return nil
}

// nextSpilledProbeBatch reads the next probe batch from disk for the current
// partition and runs it through the temp probe operator. Returns (nil, nil)
// when the current partition's probe data is exhausted.
func (p *HashJoinProbe) nextSpilledProbeBatch(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		// Drain any fan-out the partition probe suspended on the batch we
		// last handed it, before reading another one: the suspended state
		// still points into that batch, and the spill reader reuses its
		// buffers across Next() calls.
		if p.spillFlushTmpProbe.HasPendingOutput() {
			out, err := p.spillFlushTmpProbe.NextOutput(ctx)
			if err != nil {
				return nil, fmt.Errorf("probing spilled partition: %w", err)
			}
			if out != nil {
				out.Detach()
				return out, nil
			}
			continue
		}

		// Open a probe file if we don't have one.
		if p.spillFlushReader == nil {
			if p.spillFlushProbeFileIx >= len(p.spillFlushProbeFiles) {
				return nil, nil // partition exhausted
			}
			path := p.spillFlushProbeFiles[p.spillFlushProbeFileIx]
			p.spillFlushProbeFileIx++
			r, err := openSpillBatchReader(path)
			if err != nil {
				return nil, fmt.Errorf("opening probe spill: %w", err)
			}
			p.spillFlushReader = r
		}

		pb, err := p.spillFlushReader.Next()
		if err != nil {
			return nil, fmt.Errorf("reading probe spill: %w", err)
		}
		if pb == nil {
			// End of this file; close it and try the next.
			p.spillFlushReader.Close()
			p.spillFlushReader = nil
			continue
		}

		out, err := p.spillFlushTmpProbe.Execute(ctx, pb)
		if err != nil {
			return nil, fmt.Errorf("probing spilled partition: %w", err)
		}
		if out != nil {
			out.Detach()
			return out, nil
		}
		// Probe produced no output for this batch (e.g., no matches in
		// inner join); read the next one.
	}
}

// closeCurrentSpillPartition tears down all per-partition state to make the
// build batches and hash tables eligible for GC before the next partition is
// opened.
func (p *HashJoinProbe) closeCurrentSpillPartition() {
	if p.spillFlushReader != nil {
		p.spillFlushReader.Close()
		p.spillFlushReader = nil
	}
	if p.spillFlushTmpJoin != nil {
		p.spillFlushTmpJoin.buildBatches = nil
		p.spillFlushTmpJoin.arena = nil
		p.spillFlushTmpJoin.arenaNext = nil
		p.spillFlushTmpJoin.intIndex = nil
		p.spillFlushTmpJoin.strIndex = nil
		p.spillFlushTmpJoin = nil
	}
	p.spillFlushTmpProbe = nil
	p.spillFlushProbeFiles = nil
	p.spillFlushProbeFileIx = 0
	p.spillFlushDone = false
}

// partitionProbeBatch splits a probe batch by partition, writing spilled-partition
// rows to disk. Returns a selection vector of rows belonging to in-memory partitions.
func (p *HashJoinProbe) partitionProbeBatch(in *batch.RecordBatch) ([]uint32, error) {
	h := p.join
	ss := h.spillState

	// Compute partition for each row and separate in-memory vs spilled
	var inMemSel []uint32
	spillRows := make(map[int][]int) // partID → row indices

	iterateRows := func(row int) {
		partID := p.probePartition(in, row)
		if ss.spilledParts[partID] {
			spillRows[partID] = append(spillRows[partID], row)
		} else {
			inMemSel = append(inMemSel, uint32(row))
		}
	}

	if in.Sel != nil {
		for _, idx := range in.Sel {
			iterateRows(int(idx))
		}
	} else {
		for i := 0; i < in.Len; i++ {
			iterateRows(i)
		}
	}

	// Write spilled partition rows to disk
	for partID, rows := range spillRows {
		if len(rows) == 0 {
			continue
		}
		// Create a compacted batch with only the spilled rows
		spillBatch := compactBatchForRows(in, rows)
		if err := ss.writeProbeRow(partID, spillBatch); err != nil {
			return nil, fmt.Errorf("buffering probe for partition %d: %w", partID, err)
		}
	}

	return inMemSel, nil
}

// probePartition returns the partition ID for a probe row.
func (p *HashJoinProbe) probePartition(in *batch.RecordBatch, row int) int {
	h := p.join
	if h.useIntKey {
		key, ok := h.intProbeKey(in, row)
		if !ok {
			return 0
		}
		return spillPartition(key)
	}
	if h.useDualIntKey {
		h.resolveProbeKeyIdx(in)
		col0, col1 := in.Columns[h.probeKeyIdx[0]], in.Columns[h.probeKeyIdx[1]]
		a, b, ok := dualIntKeyFromVectors(col0, col1, row)
		if !ok {
			return 0
		}
		return spillPartition(dualIntHash(a, b))
	}
	// The key's matchability is irrelevant to ROUTING: a NULL-keyed probe row
	// still needs a deterministic partition, the same way the integer arms
	// above send one to partition 0.
	_ = p.buildProbeKey(in, row)
	return spillPartitionBytes(p.keyBuf)
}

// compactBatchForRows creates a new dense batch containing only the specified
// rows. It is a thin wrapper over appendRows that mints a tightly-sized
// destination; callers that reuse a destination across many source batches use
// appendRows directly (see the partition-on-arrival accumulator path).
func compactBatchForRows(in *batch.RecordBatch, rows []int) *batch.RecordBatch {
	out := batch.NewRecordBatch(in.Schema, len(rows))
	appendRows(out, in, rows, 0)
	return out
}

// appendRows copies src's rows[di] into dst at positions [dstStart, dstStart+len(rows)).
// It returns the data-byte footprint of the copied rows (matching Vector.MemBytes
// per-type sizing, summed over just these rows, excluding null-bitmap words and
// the per-row hash overhead) so callers can charge a TIGHT memory figure that is
// independent of dst's physical capacity — required when dst is a reused
// accumulator sized larger than its logical row count.
//
// Preconditions the caller MUST satisfy:
//   - dst.Sel == nil and dst's fixed-width column slices have length >=
//     dstStart+len(rows).
//   - dst.Nulls is pre-cleared to all-valid over [dstStart, dstStart+len(rows))
//     (true for a freshly NewRecordBatch'd or Reset() batch); appendRows only
//     ever sets nulls, never clears them.
//   - For BytesColumn columns, rows are appended in strictly increasing dst
//     position with dst.BytesData.Offsets[dstStart] == len(dst.BytesData.Data)
//     (true at dstStart==0 after Reset, and maintained as the tail grows).
//
// appendRows does NOT update dst.Len; the caller owns the logical row count.
func appendRows(dst, src *batch.RecordBatch, rows []int, dstStart int) int64 {
	n := int64(len(rows))
	var dataBytes int64
	for j, col := range src.Columns {
		d := dst.Columns[j]
		switch col.Type {
		case batch.TypeBool:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
				} else {
					d.BoolData[dstStart+di] = col.BoolData[si]
				}
			}
			dataBytes += n
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
				} else {
					d.Int32Data[dstStart+di] = col.Int32Data[si]
				}
			}
			dataBytes += n * 4
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
				} else {
					d.Int64Data[dstStart+di] = col.Int64Data[si]
				}
			}
			dataBytes += n * 8
		case batch.TypeFloat32:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
				} else {
					d.Float32Data[dstStart+di] = col.Float32Data[si]
				}
			}
			dataBytes += n * 4
		case batch.TypeFloat64:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
				} else {
					d.Float64Data[dstStart+di] = col.Float64Data[si]
				}
			}
			dataBytes += n * 8
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
					d.BytesData.Set(dstStart+di, nil) // maintain offset continuity
				} else {
					start := col.BytesData.Offsets[si]
					end := col.BytesData.Offsets[si+1]
					dataBytes += int64(end - start)
					d.BytesData.SetFrom(dstStart+di, &col.BytesData, si)
				}
			}
			dataBytes += n * 4 // offsets, one uint32 per row
		case batch.TypeDecimal:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					d.Nulls.SetNull(dstStart + di)
				} else {
					d.DecimalData.Data[dstStart+di] = col.DecimalData.Data[si]
				}
			}
			dataBytes += n * 16
		default:
			// Typed copy (nested ARRAY/ROW/MAP/VECTOR). The old boxed
			// GetValue/SetValue fallback passed nil through for null rows,
			// which (pre-WriteNullAt fix) skipped nested slots — corrupting
			// the frozen build batch and truncating ARRAY spills via a
			// stale Offsets[n]. CopyValueFrom handles nulls and both child
			// shapes directly.
			for di, si := range rows {
				d.CopyValueFrom(dstStart+di, col, si)
			}
			dataBytes += n * 8 // coarse estimate; nested types are off the join hot path
		}
	}
	return dataBytes
}
