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

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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

	// Build-side: per-partition batch lists (in-memory) and spill files (on disk)
	partBuildBatches map[int][]*batch.RecordBatch
	partBuildFiles   map[int][]string // spilled build file paths per partition

	// Probe-side: per-partition buffered batches (flushed to disk files)
	partProbeFiles   map[int][]string
	partProbeWriters map[int]*spillBatchWriter
	probeMu          sync.Mutex // protects partProbeWriters during parallel probe

	// Track memory per partition for choosing what to spill
	partMemory map[int]int64

	schema []parquet.Column // build-side schema
}

func newSpillState(dir string, schema []parquet.Column) *spillState {
	return &spillState{
		dir:              dir,
		numPartitions:    numSpillPartitions,
		spilledParts:     make(map[int]bool),
		partBuildBatches: make(map[int][]*batch.RecordBatch),
		partBuildFiles:   make(map[int][]string),
		partProbeFiles:   make(map[int][]string),
		partProbeWriters: make(map[int]*spillBatchWriter),
		partMemory:       make(map[int]int64),
		schema:           schema,
	}
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

// spillBuildPartition writes a partition's build batches to disk and frees memory.
// Returns the number of bytes freed.
func (s *spillState) spillBuildPartition(partID int) (int64, error) {
	batches := s.partBuildBatches[partID]
	if len(batches) == 0 {
		s.spilledParts[partID] = true
		return 0, nil
	}

	path, err := writeSpillBatches(s.dir, batches)
	if err != nil {
		return 0, fmt.Errorf("spilling partition %d: %w", partID, err)
	}

	s.partBuildFiles[partID] = append(s.partBuildFiles[partID], path)
	freed := s.partMemory[partID]
	delete(s.partBuildBatches, partID)
	delete(s.partMemory, partID)
	s.spilledParts[partID] = true
	return freed, nil
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

// cleanup removes all spill files.
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
}

// partitionBuildBatch assigns rows from a build batch to partitions.
// The batch is split by partition ID and stored in the partition's build list.
func (h *HashJoin) partitionBuildBatch(b *batch.RecordBatch) {
	ss := h.spillState

	// Compute partition for each row
	partRows := make(map[int][]int) // partID → row indices

	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		for i := 0; i < b.Len; i++ {
			key, ok := intKeyFromVector(col, i)
			if !ok {
				partRows[0] = append(partRows[0], i)
				continue
			}
			p := spillPartition(key)
			partRows[p] = append(partRows[p], i)
		}
	} else if h.useDualIntKey {
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		for i := 0; i < b.Len; i++ {
			a, bb, ok := dualIntKeyFromVectors(col0, col1, i)
			if !ok {
				partRows[0] = append(partRows[0], i)
				continue
			}
			p := spillPartition(dualIntHash(a, bb))
			partRows[p] = append(partRows[p], i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			h.buildKeyFromBatch(b, i)
			p := spillPartitionBytes(h.keyBuf)
			partRows[p] = append(partRows[p], i)
		}
	}

	// Create per-partition batches
	for partID, rows := range partRows {
		if ss.spilledParts[partID] {
			// This partition is already spilled — write directly to disk
			partBatch := compactBatchForRows(b, rows)
			// Best effort: write to partition's build files
			path, err := writeSpillBatches(ss.dir, []*batch.RecordBatch{partBatch})
			if err == nil {
				ss.partBuildFiles[partID] = append(ss.partBuildFiles[partID], path)
			}
			continue
		}
		partBatch := compactBatchForRows(b, rows)
		ss.partBuildBatches[partID] = append(ss.partBuildBatches[partID], partBatch)
		ss.partMemory[partID] += EstimateBatchBytes(partBatch)
	}
}

// ---- Columnar Batch Spill I/O ----

// spillBatchWriter accumulates batches and writes them to a single file.
type spillBatchWriter struct {
	dir    string
	prefix string
	f      *os.File
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
	w := bufio.NewWriterSize(f, 1024*1024)
	// Reserve space for batch count (will be written on close)
	var buf [4]byte
	w.Write(buf[:4])
	return &spillBatchWriter{dir: dir, prefix: prefix, f: f, w: w, path: path}, nil
}

func (sw *spillBatchWriter) writeBatch(b *batch.RecordBatch) error {
	if err := writeColumnarBatch(sw.w, b); err != nil {
		return err
	}
	sw.count++
	return nil
}

func (sw *spillBatchWriter) close() (string, error) {
	if sw.f == nil {
		return "", nil
	}
	if err := sw.w.Flush(); err != nil {
		sw.f.Close()
		return "", fmt.Errorf("flushing spill: %w", err)
	}
	// Seek back and write batch count
	if _, err := sw.f.Seek(0, 0); err != nil {
		sw.f.Close()
		return "", err
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(sw.count))
	if _, err := sw.f.Write(buf[:]); err != nil {
		sw.f.Close()
		return "", err
	}
	if err := sw.f.Close(); err != nil {
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

	w := bufio.NewWriterSize(f, 1024*1024)

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
	return path, nil
}

// readSpillBatches reads all batches from a columnar spill file.
func readSpillBatches(path string) ([]*batch.RecordBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening spill file: %w", err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1024*1024)

	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("reading batch count: %w", err)
	}
	numBatches := int(binary.LittleEndian.Uint32(buf[:]))

	batches := make([]*batch.RecordBatch, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		b, err := readColumnarBatch(r)
		if err != nil {
			return nil, fmt.Errorf("reading batch %d: %w", i, err)
		}
		batches = append(batches, b)
	}
	return batches, nil
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
	}
	return nil
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

		// Nullable
		nullable, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("reading nullable flag: %w", err)
		}

		typeID := parquet.TypeID(typeByte)
		schema[i] = parquet.Column{
			Name:     string(nameBuf),
			Type:     typeID,
			Nullable: nullable == 1,
		}

		// Create vector for this column
		col := batch.NewVector(typeID, numRows)
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

// processSpilledPartitions processes all spilled partitions one at a time.
// For each partition: loads build data, builds hash table, replays probe data,
// and returns result batches. Only one partition's data is in memory at a time.
// Uses pre-fetching: loads partition N+1's build data while processing partition N.
func (h *HashJoin) processSpilledPartitions(ctx context.Context) ([]*batch.RecordBatch, error) {
	ss := h.spillState
	if ss == nil {
		return nil, nil
	}

	// Close probe writers to flush buffered data
	if err := ss.closeProbeWriters(); err != nil {
		return nil, fmt.Errorf("closing probe writers: %w", err)
	}

	// Collect partition IDs for ordered iteration (needed for pre-fetch)
	partIDs := make([]int, 0, len(ss.spilledParts))
	for id := range ss.spilledParts {
		partIDs = append(partIDs, id)
	}

	var allResults []*batch.RecordBatch

	// Pre-fetch first partition's build data
	var prefetch *preloadedBuild
	if len(partIDs) > 0 {
		ch := make(chan preloadedBuild, 1)
		go func() {
			b, err := h.loadBuildBatches(partIDs[0])
			ch <- preloadedBuild{b, err}
		}()
		pf := <-ch
		prefetch = &pf
	}

	for i, partID := range partIDs {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Use pre-fetched build data for this partition
		buildBatches := prefetch.batches
		if prefetch.err != nil {
			return nil, fmt.Errorf("processing spilled partition %d: %w", partID, prefetch.err)
		}

		// Start pre-fetching next partition's build data while we process this one
		var nextCh chan preloadedBuild
		if i+1 < len(partIDs) {
			nextCh = make(chan preloadedBuild, 1)
			nextPartID := partIDs[i+1]
			go func() {
				b, err := h.loadBuildBatches(nextPartID)
				nextCh <- preloadedBuild{b, err}
			}()
		}

		results, err := h.processOnePartitionWithBuild(ctx, partID, buildBatches)
		if err != nil {
			// Drain prefetch goroutine before returning
			if nextCh != nil {
				<-nextCh
			}
			return nil, fmt.Errorf("processing spilled partition %d: %w", partID, err)
		}
		allResults = append(allResults, results...)

		// Collect pre-fetched data for next iteration
		if nextCh != nil {
			pf := <-nextCh
			prefetch = &pf
		}
	}

	return allResults, nil
}

// processOnePartition loads one spilled partition's build data, constructs a
// temporary hash table, then replays probe data against it.
func (h *HashJoin) processOnePartition(ctx context.Context, partID int) ([]*batch.RecordBatch, error) {
	buildBatches, err := h.loadBuildBatches(partID)
	if err != nil {
		return nil, err
	}
	return h.processOnePartitionWithBuild(ctx, partID, buildBatches)
}

// processOnePartitionWithBuild processes a partition using pre-loaded build batches.
func (h *HashJoin) processOnePartitionWithBuild(ctx context.Context, partID int, buildBatches []*batch.RecordBatch) ([]*batch.RecordBatch, error) {
	if len(buildBatches) == 0 {
		return nil, nil
	}

	ss := h.spillState

	// Build a temporary hash join for this partition
	tmpJoin := &HashJoin{
		JoinType:        h.JoinType,
		LeftKeys:        h.LeftKeys,
		RightKeys:       h.RightKeys,
		SemiAntiFilter:  h.SemiAntiFilter,
		SemiAntiKeyOnly: h.SemiAntiKeyOnly,
		BuildTableAlias: h.BuildTableAlias,
		keyBuf:          make([]byte, 0, 128),
	}

	// Count total rows for pre-sizing
	totalRows := 0
	for _, b := range buildBatches {
		totalRows += b.Len
	}
	tmpJoin.BuildRowHint = int64(totalRows)

	// Build hash table from loaded batches
	tmpJoin.buildSchema = buildBatches[0].Schema
	tmpJoin.buildKeyIdx = make([]int, len(tmpJoin.RightKeys))
	for i, col := range tmpJoin.RightKeys {
		tmpJoin.buildKeyIdx[i] = buildBatches[0].ColumnIndex(col)
	}
	tmpJoin.tryEnableIntKey(buildBatches[0])

	// Pre-allocate
	if !tmpJoin.SemiAntiKeyOnly {
		tmpJoin.arena = make([]buildRef, 0, totalRows)
		tmpJoin.arenaNext = make([]int32, 0, totalRows)
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

	if (tmpJoin.JoinType == RightJoin || tmpJoin.JoinType == FullOuterJoin) && len(tmpJoin.arena) > 0 {
		tmpJoin.arenaMatched = make([]bool, len(tmpJoin.arena))
	}
	tmpJoin.buildBloom()
	tmpJoin.buildDone = true

	// Create probe operator
	probe := tmpJoin.Probe()
	probe.OutputFilter = h.spillOutputFilter
	if err := probe.Init(ctx); err != nil {
		return nil, err
	}

	// Load and replay probe batches
	var results []*batch.RecordBatch
	for _, path := range ss.partProbeFiles[partID] {
		probeBatches, err := readSpillBatches(path)
		if err != nil {
			return nil, fmt.Errorf("reading probe spill: %w", err)
		}
		for _, pb := range probeBatches {
			out, err := probe.Execute(ctx, pb)
			if err != nil {
				return nil, err
			}
			if out != nil {
				out.Detach()
				results = append(results, out)
			}
		}
	}

	// For right/full outer: flush unmatched build rows
	if tmpJoin.JoinType == RightJoin || tmpJoin.JoinType == FullOuterJoin {
		if unmatched := probe.FlushUnmatched(h.spillLeftSchema); unmatched != nil {
			results = append(results, unmatched)
		}
	}

	// Free partition data
	tmpJoin.buildBatches = nil
	tmpJoin.arena = nil
	tmpJoin.arenaNext = nil
	tmpJoin.intIndex = nil
	tmpJoin.strIndex = nil

	return results, nil
}

// indexBuildBatch indexes a build batch into the hash table (full row storage path).
func (h *HashJoin) indexBuildBatch(b *batch.RecordBatch, batchIdx int32) {
	if h.useIntKey {
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				key, ok := intKeyFromVector(col, int(si))
				if !ok {
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
					continue
				}
				h.arenaAppendInt(dualIntHash(a, bb), buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
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
				h.buildKeyFromBatch(b, int(si))
				h.arenaAppendStr(buildRef{batchIdx: batchIdx, rowIdx: int32(si)})
				h.buildRows++
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				h.buildKeyFromBatch(b, rowIdx)
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
					continue
				}
				h.intIndex.Put(dualIntHash(a, bb), 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, rowIdx)
				if !ok {
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
				h.buildKeyFromBatch(b, int(si))
				h.strIndex.GetOrInsert(h.keyBuf, 0)
			}
		} else {
			for rowIdx := 0; rowIdx < b.Len; rowIdx++ {
				h.buildKeyFromBatch(b, rowIdx)
				h.strIndex.GetOrInsert(h.keyBuf, 0)
			}
		}
	}
	h.buildRows += int64(b.ActiveLen())
}

// ---- FlushableOperator implementation for HashJoinProbe ----

// HasPendingFlush returns true if there are spilled partitions to process.
func (p *HashJoinProbe) HasPendingFlush() bool {
	ss := p.join.spillState
	if ss == nil {
		return false
	}
	// Before first NextFlush call: check if there are spilled partitions
	if p.spillFlushResults == nil {
		return len(ss.spilledParts) > 0
	}
	// After processing: check if there are remaining results to emit
	return p.spillFlushIdx < len(p.spillFlushResults)
}

// NextFlush returns the next result batch from spilled partition processing.
// On first call, processes all spilled partitions and caches results.
func (p *HashJoinProbe) NextFlush(ctx context.Context) (*batch.RecordBatch, error) {
	if p.spillFlushResults == nil {
		// Process all spilled partitions
		results, err := p.join.processSpilledPartitions(ctx)
		if err != nil {
			return nil, err
		}
		p.spillFlushResults = results
		p.spillFlushIdx = 0
	}

	if p.spillFlushIdx >= len(p.spillFlushResults) {
		// Cleanup spill files
		if p.join.spillState != nil {
			p.join.spillState.cleanup()
		}
		return nil, nil
	}

	b := p.spillFlushResults[p.spillFlushIdx]
	p.spillFlushIdx++
	return b, nil
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
	p.buildProbeKey(in, row)
	return spillPartitionBytes(p.keyBuf)
}

// compactBatchForRows creates a new batch containing only the specified rows.
func compactBatchForRows(in *batch.RecordBatch, rows []int) *batch.RecordBatch {
	out := batch.NewRecordBatch(in.Schema, len(rows))
	for j, col := range in.Columns {
		dst := out.Columns[j]
		switch col.Type {
		case batch.TypeBool:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.BoolData[di] = col.BoolData[si]
				}
			}
		case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int32Data[di] = col.Int32Data[si]
				}
			}
		case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Int64Data[di] = col.Int64Data[si]
				}
			}
		case batch.TypeFloat32:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float32Data[di] = col.Float32Data[si]
				}
			}
		case batch.TypeFloat64:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.Float64Data[di] = col.Float64Data[si]
				}
			}
		case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
					dst.BytesData.Set(di, nil) // maintain offset continuity
				} else {
					dst.BytesData.SetFrom(di, &col.BytesData, si)
				}
			}
		case batch.TypeDecimal:
			for di, si := range rows {
				if col.Nulls.IsNullFast(si) {
					dst.Nulls.SetNull(di)
				} else {
					dst.DecimalData.Data[di] = col.DecimalData.Data[si]
				}
			}
		default:
			// Fallback via GetValue/SetValue
			for di, si := range rows {
				dst.SetValue(di, col.GetValue(si))
			}
		}
	}
	return out
}
