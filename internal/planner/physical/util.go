package physical

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func readAll(rc io.ReadCloser) ([]byte, error) {
	return io.ReadAll(rc)
}

// readBufPool reuses []byte buffers across queries to reduce allocation
// pressure on the GC. At SF1, file reads allocate ~7.6GB cumulatively
// across 22 TPC-H queries — pooling cuts this by 60-70% since the same
// Parquet files are re-read by multiple queries.
var readBufPool sync.Pool

// getReadBuf returns a []byte of at least size bytes from the pool,
// or allocates a new one.
func getReadBuf(size int) []byte {
	if v := readBufPool.Get(); v != nil {
		buf := v.([]byte)
		if cap(buf) >= size {
			return buf[:size]
		}
	}
	return make([]byte, size)
}

// putReadBuf returns a buffer to the pool for reuse. Only pool buffers
// above 64KB to avoid polluting the pool with tiny allocations.
func putReadBuf(buf []byte) {
	if cap(buf) >= 64*1024 {
		readBufPool.Put(buf[:cap(buf)])
	}
}

// readAllSized reads an io.ReadCloser into a pre-allocated buffer when the
// file size is known. Avoids io.ReadAll's grow-from-512-bytes pattern which
// causes O(log n) reallocations and copies for large files (e.g., 40MB Parquet
// file triggers ~17 doublings with ~80MB of intermediate copy overhead).
//
// When pooled is true, the returned buffer comes from readBufPool and must be
// returned via putReadBuf when no longer needed (after the parquet reader is
// closed). When false, the buffer is a fresh allocation.
func readAllSized(rc io.ReadCloser, sizeBytes int64, pooled bool) ([]byte, error) {
	if sizeBytes <= 0 {
		return io.ReadAll(rc)
	}
	var buf []byte
	if pooled {
		buf = getReadBuf(int(sizeBytes))
	} else {
		buf = make([]byte, sizeBytes)
	}
	n, err := io.ReadFull(rc, buf)
	if err == io.ErrUnexpectedEOF {
		// File was smaller than expected — return what we got
		return buf[:n], nil
	}
	if err != nil {
		if pooled {
			putReadBuf(buf)
		}
		return nil, err
	}
	// Read any remaining data beyond sizeBytes. This handles the case
	// where the catalog entry has an incorrect (stale) SizeBytes. Parquet
	// files have their footer at the end — truncating silently produces
	// unreadable files that are then silently skipped by buildRGUnits.
	extra, _ := io.ReadAll(rc)
	if len(extra) > 0 {
		// Extra data means size was wrong — can't reuse the pooled buffer
		// since append may reallocate.
		combined := make([]byte, n+len(extra))
		copy(combined, buf[:n])
		copy(combined[n:], extra)
		if pooled {
			putReadBuf(buf)
		}
		return combined, nil
	}
	return buf[:n], nil
}

func bytesReader(data []byte) *bytes.Reader {
	return bytes.NewReader(data)
}

func fromRows(schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(schema, rows)
}

// readBatchDirect reads a parquet file directly into a RecordBatch using
// column-at-a-time page reading via the native FileReader. This avoids
// parquet-go entirely. For schemas with nested types (ARRAY, MAP), falls
// back to row-level reading.
func readBatchDirect(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string, preds ...scanPredicate) *batch.RecordBatch {
	// Nested types: fall back to row reader
	pqSchema := parquet.Schema{Columns: schema}
	if pqSchema.HasNestedColumns() {
		return readBatchViaRows(pqReader, schema, requiredCols)
	}

	fr := pqReader.FileReader()
	numRGs := fr.NumRowGroups()
	if numRGs == 0 {
		return nil
	}

	readSchema := buildReadSchema(schema, requiredCols)

	// Collect active (non-pruned) row group indices.
	activeRGs := make([]int, 0, numRGs)
	for rgIdx := 0; rgIdx < numRGs; rgIdx++ {
		if len(preds) > 0 {
			stats := pqReader.RowGroupStats(rgIdx)
			pruned := false
			for _, pred := range preds {
				op := mapPredOp(pred.Op)
				if op >= 0 {
					sp := scan.StatsPredicate{Column: pred.Column, Op: op, Value: pred.Value}
					if scan.CanPruneRowGroup(sp, stats) {
						pruned = true
						break
					}
				}
			}
			if pruned {
				continue
			}
		}
		activeRGs = append(activeRGs, rgIdx)
	}

	if len(activeRGs) == 0 {
		return nil
	}

	// Count total rows across active row groups.
	var totalRows int64
	for _, rgIdx := range activeRGs {
		totalRows += fr.RowGroupNumRows(rgIdx)
	}
	if totalRows == 0 {
		return nil
	}

	// Read each active row group via the native reader and merge.
	var batches []*batch.RecordBatch
	for _, rgIdx := range activeRGs {
		b, err := scan.ReadRowGroupNative(fr, rgIdx, readSchema, nil)
		if err != nil {
			continue
		}
		if b != nil {
			batches = append(batches, b)
		}
	}

	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return batches[0]
	}

	result := batch.NewRecordBatch(readSchema, int(totalRows))
	offset := 0
	for _, b := range batches {
		for j := range readSchema {
			copyVecRange(result.Columns[j], offset, b.Columns[j], 0, b.Len)
		}
		offset += b.Len
	}
	result.Len = offset
	return result
}

// readBatchViaRows reads a parquet file using parquet-go's row-level reader,
// which handles nested types (LIST, MAP, STRUCT) natively. Used as fallback
// when the schema contains nested columns.
func readBatchViaRows(pqReader *parquet.Reader, schema []parquet.Column, requiredCols []string) *batch.RecordBatch {
	// Dotted ROW-field references require their parent column — see
	// buildReadSchema (issue #147). ReadRows matches file columns by
	// exact name, so "attrs.score" must become "attrs" here or the
	// parent is never read and every downstream field access is NULL.
	selectedCols := expandRowFieldRequirements(schema, requiredCols)
	if len(selectedCols) == 0 {
		selectedCols = make([]string, len(schema))
		for i, c := range schema {
			selectedCols[i] = c.Name
		}
	}
	rows, err := pqReader.ReadRows(selectedCols)
	if err != nil || len(rows) == 0 {
		return nil
	}

	// Build the read schema (only columns we're reading)
	readSchema := schema
	if len(selectedCols) > 0 {
		needed := make(map[string]bool, len(selectedCols))
		for _, c := range selectedCols {
			needed[c] = true
		}
		filtered := make([]parquet.Column, 0, len(requiredCols))
		for _, col := range schema {
			if needed[col.Name] {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) > 0 {
			readSchema = filtered
		}
	}

	return fromRows(readSchema, rows)
}

// mapPredOp converts a logical predicate operator string to an exec.CompareOp.
func mapPredOp(op string) exec.CompareOp {
	switch strings.ToLower(op) {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return -1
	}
}

// decimalFromBytes converts big-endian bytes to Int128.
func decimalFromBytes(b []byte) batch.Int128 {
	if len(b) == 0 {
		return batch.Int128{}
	}
	// Parquet stores decimals as big-endian two's complement
	var hi int64
	var lo uint64
	if len(b) <= 8 {
		// Fits in a single int64 — sign-extend
		if b[0]&0x80 != 0 {
			hi = -1
		}
		for _, c := range b {
			lo = (lo << 8) | uint64(c)
		}
		if hi < 0 {
			lo |= ^uint64(0) << (uint(len(b)) * 8)
		}
		return batch.Int128From(int64(lo))
	}
	// 9-16 bytes: split into hi/lo
	split := len(b) - 8
	hiBytes := b[:split]
	loBytes := b[split:]
	if hiBytes[0]&0x80 != 0 {
		hi = -1
	}
	for _, c := range hiBytes {
		hi = (hi << 8) | int64(c)
	}
	for _, c := range loBytes {
		lo = (lo << 8) | uint64(c)
	}
	return batch.Int128{Hi: hi, Lo: lo}
}

// --- Row-group-level parallel scan ---

// rgUnit is a work unit for parallel row-group scanning. The file's bytes
// are loaded LAZILY via slot.ensureLoaded() the first time any row group
// from that file is read, and released as soon as the slot's last row
// group has been processed. This caps peak heap usage from the scan source
// at (load-gate budget) instead of (total_files × file_size),
// which at SF100 is the difference between ~1 GB and ~6 GB per scan.
type rgUnit struct {
	slot        *fileSlot
	rgRowOffset int64 // cumulative row offset within the file (for delete markers)
	rgIndex     int   // row group index within the file
	numRows     int64
}

// fileSlot owns the lazy lifecycle of one parquet file's bytes during a
// row-group-parallel scan. Row-group metadata for pruning comes from the
// catalog blob or a footer read during buildRGUnits (held there, not on
// the slot); the row group bytes are loaded on first read and released
// after the last row group from the file has been consumed.
type fileSlot struct {
	entry        catalog.FileEntry
	mu           sync.Mutex
	loaded       bool
	loadErr      error
	reader       *parquet.Reader     // full reader (data + metadata) once loaded
	nativeReader *parquet.FileReader // alias of reader.FileReader()
	dataBuf      []byte              // pooled buffer holding the file bytes; returned to pool on releaseRG
	stagedFile   *os.File            // staged pread mode: local fd; column chunks read on demand (no dataBuf)
	chargedBytes int64               // bytes reserved on inner.memTracker for dataBuf; released with it
	gateBytes    int64               // bytes admitted on inner.loadGate; released with the buffer
	rgRemaining  atomic.Int64        // refcount of unprocessed row groups; release on 0
}

// newPreloadedFileSlot wraps an already-loaded *parquet.FileReader as a
// fileSlot for tests that build rgUnits directly without going through
// the buildRGUnits / lazy-loader path. The slot reports loaded=true so
// ensureLoaded becomes a no-op and never touches the loader semaphore.
func newPreloadedFileSlot(entry catalog.FileEntry, fr *parquet.FileReader) *fileSlot {
	s := &fileSlot{
		entry:        entry,
		loaded:       true,
		nativeReader: fr,
	}
	return s
}

// ensureLoaded loads the file's bytes on first call. Subsequent callers
// see the cached reader. Returns the native reader for page reads.
func (s *fileSlot) ensureLoaded(inner *scanSourceInner, ctx context.Context) (*parquet.FileReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded {
		return s.nativeReader, s.loadErr
	}
	s.loaded = true

	// Staged pread mode (docs/design/scan-pread-reads.md, extended to the
	// embedded/standalone scan 2026-08-16): when the store hands back a
	// real file descriptor, skip the whole-file read entirely — each
	// projected column chunk is read with one ranged pread on first use
	// by the decode path (FileReader.ColumnPages staged mode). On a
	// 105-column ClickBench hits part where a query touches a handful of
	// columns this is the difference between reading 137 MB and a few MB
	// per file; the c6a recon run's ~56s/query floor was exactly
	// full-dataset reads at the gp2 throughput cap. Wide-projection scans
	// still read most bytes but as per-chunk preads, so no whole-file
	// heap buffer, no load-gate admission, and no big tracker charge.
	// Non-local stores (S3) keep the whole-file path: one object GET beats
	// per-chunk ranged GETs for the narrow TPC-H tables it serves.
	if ras, ok := inner.cat.Store().(objstore.ReaderAtStore); ok {
		if ra, size, err := ras.GetReaderAt(ctx, inner.cat.Bucket(), s.entry.Path); err == nil {
			if f, isFile := ra.(*os.File); isFile {
				fr, ferr := parquet.OpenFileReaderAt(f, size)
				if ferr == nil {
					s.stagedFile = f
					s.nativeReader = fr
					return s.nativeReader, nil
				}
				f.Close()
			} else {
				ra.Close()
			}
		}
	}

	// Admit this load against the byte-budgeted gate so peak heap from
	// loaded files is bounded by the gate's budget regardless of how many
	// files are in the scan. The admitted bytes are released when the
	// slot's last row group is consumed (releaseRG) or the scan is torn
	// down (drainAbandoned).
	if inner.loadGate != nil {
		if err := inner.loadGate.acquire(ctx, s.entry.SizeBytes); err != nil {
			s.loadErr = err
			return nil, s.loadErr
		}
		s.gateBytes = s.entry.SizeBytes
		if s.gateBytes < 1 {
			s.gateBytes = 1
		}
	}

	// Reserve the file's bytes BEFORE the allocation so admission and spill
	// pressure see the load coming instead of discovering it as drift. On a
	// budget miss this asks operators to spill the shortfall and waits
	// briefly; it never fails the load — worst case the reservation is
	// forced and the ledger stays honest. Released by releaseCharge (error
	// paths) or releaseRG (with the buffer).
	if inner.memTracker != nil && s.entry.SizeBytes > 0 {
		memory.ReserveOrForce(ctx, inner.memTracker, inner.spillMgr, s.entry.SizeBytes, fileLoadReserveWait, "scan file load")
		s.chargedBytes = s.entry.SizeBytes
	}

	rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), s.entry.Path)
	if err != nil {
		s.releaseCharge(inner)
		s.releaseGate(inner)
		s.loadErr = fmt.Errorf("get %s: %w", s.entry.Path, err)
		return nil, s.loadErr
	}
	data, err := readAllSized(rc, s.entry.SizeBytes, true)
	rc.Close()
	if err != nil {
		s.releaseCharge(inner)
		s.releaseGate(inner)
		s.loadErr = fmt.Errorf("read %s: %w", s.entry.Path, err)
		return nil, s.loadErr
	}
	// Reconcile the up-front reservation to the pooled buffer's real
	// capacity — pool reuse can hand back a larger buffer than the file.
	if s.chargedBytes > 0 {
		if d := int64(cap(data)) - s.chargedBytes; d != 0 {
			if d > 0 {
				inner.memTracker.ForceReserve(d)
			} else {
				inner.memTracker.Release(-d)
			}
			s.chargedBytes = int64(cap(data))
		}
	}
	reader, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		s.releaseCharge(inner)
		s.releaseGate(inner)
		// Buffer wasn't installed in a slot — return it directly.
		putReadBuf(data)
		s.loadErr = fmt.Errorf("parquet %s (%d bytes): %w", s.entry.Path, len(data), err)
		return nil, s.loadErr
	}
	s.reader = reader
	s.nativeReader = reader.FileReader()
	// Hold the pooled buffer on the SLOT (not the global tracked list) so
	// releaseRG can return it to readBufPool as soon as the slot's last
	// row group is consumed. The previous design tracked all loaded
	// buffers in inner.pooledBufs and only returned them at scan close,
	// which defeated the lazy loader's bounded-in-flight design — buffers
	// stayed live for the entire scan even after their slot was released.
	s.dataBuf = data
	return s.nativeReader, nil
}

// fileLoadReserveWait bounds how long a file load waits for a clean
// reservation after asking operators for relief. Past it the reservation
// is forced — the load is never failed or deadlocked on the budget.
const fileLoadReserveWait = 2 * time.Second

// releaseCharge releases the memTracker reservation made for this slot's
// data buffer. Caller must hold s.mu.
func (s *fileSlot) releaseCharge(inner *scanSourceInner) {
	if s.chargedBytes > 0 && inner.memTracker != nil {
		inner.memTracker.Release(s.chargedBytes)
	}
	s.chargedBytes = 0
}

// releaseGate returns this slot's admitted bytes to the load gate.
// Idempotent. Caller must hold s.mu.
func (s *fileSlot) releaseGate(inner *scanSourceInner) {
	if s.gateBytes > 0 && inner.loadGate != nil {
		inner.loadGate.release(s.gateBytes)
	}
	s.gateBytes = 0
}

// releaseRG decrements the row-group refcount and frees the file bytes
// once the last row group has been processed. Safe to call from any worker.
func (s *fileSlot) releaseRG(inner *scanSourceInner) {
	if s.rgRemaining.Add(-1) > 0 {
		return
	}
	s.mu.Lock()
	s.reader = nil
	s.nativeReader = nil
	buf := s.dataBuf
	s.dataBuf = nil
	sf := s.stagedFile
	s.stagedFile = nil
	s.releaseCharge(inner)
	gateN := s.gateBytes
	s.gateBytes = 0
	s.mu.Unlock()
	// Return the file's pooled buffer to the read buf pool so it's
	// immediately available for the next file the loader brings in. This
	// is what makes the byte-budgeted load gate actually bound peak heap.
	if buf != nil {
		putReadBuf(buf)
	}
	if sf != nil {
		sf.Close()
	}
	if gateN > 0 && inner.loadGate != nil {
		inner.loadGate.release(gateN)
	}
}

// drainAbandoned releases everything a slot still holds when its scan is
// torn down before all row groups were consumed (ctx cancel, LIMIT
// satisfied, downstream error): the pooled buffer, the load-gate bytes, and —
// critically — the memTracker charge, which lives on the worker-lifetime
// SHARED tracker and would otherwise remain a permanent phantom reservation
// (up to the load-gate budget per cancelled scan), starving later
// tasks' admission and forcing chronic over-spilling. Callers must ensure
// the rg workers have exited (Close does wg.Wait first).
func (s *fileSlot) drainAbandoned(inner *scanSourceInner) {
	s.mu.Lock()
	s.reader = nil
	s.nativeReader = nil
	buf := s.dataBuf
	s.dataBuf = nil
	sf := s.stagedFile
	s.stagedFile = nil
	s.releaseCharge(inner)
	gateN := s.gateBytes
	s.gateBytes = 0
	s.mu.Unlock()
	if buf != nil {
		putReadBuf(buf)
	}
	if sf != nil {
		sf.Close()
	}
	if gateN > 0 && inner.loadGate != nil {
		inner.loadGate.release(gateN)
	}
}

// prefetchResult holds a speculatively read row group batch and its unit index.
type prefetchResult struct {
	idx   int
	batch *batch.RecordBatch
}

// buildRGUnits enumerates row groups from every file in the scan WITHOUT
// loading the file bytes upfront. Row-group metadata (row counts + column
// stats for pruning) comes from the catalog's persisted RG-metadata blob
// when the table has one (Catalog.TableRGMeta — one cached GET per table
// per manifest version); files not covered by the blob fall back to one
// footer-sized range read each. Before the blob existed the footer pass
// was a per-query barrier: 600 S3 round-trips per lineitem scan at SF10,
// ~2-5s of metadata latency before any data download, on every query.
// Predicate / bloom / dynamic-range pruning is applied across the row
// groups and an rgUnit is emitted per survivor, pointing at a lazy
// fileSlot.
//
// The slot's bytes are loaded on first read by the rg worker pool and
// released as soon as that file's last row group has been consumed (see
// fileSlot.ensureLoaded / releaseRG). The byte-budgeted inner.loadGate
// keeps peak heap from the scan source at the gate's budget regardless
// of how many files the scan covers — replaces the previous "load all
// files into heap upfront" behaviour that OOMed SF100 workers when
// lineitem partitions hit 21 × 283 MB ≈ 6 GB per scan per task.
func (inner *scanSourceInner) buildRGUnits(ctx context.Context) {
	// Check for nested types — fall back to file-level scan
	pqSchema := parquet.Schema{Columns: inner.schema}
	if pqSchema.HasNestedColumns() {
		inner.hasNestedTypes = true
		return
	}

	// Bound concurrent file LOADs (data bytes, not metadata) by BYTES.
	// The old 4-slot semaphore was a byte bound in file units ("4 × 300 MB
	// = 1.2 GB peak") and collapsed on small-file layouts: 600 × 6.5 MB
	// SF10 files at 4 lanes left a 16-core box ~98% idle on cold S3. A
	// byte budget holds the same heap ceiling for SF100-sized files
	// (~3 lanes at 300 MB) while giving small files real download
	// parallelism (lane-capped at loadGateMaxLanes). When the query has a
	// memory budget, the gate takes a quarter of it at most so scans
	// can't crowd out join/aggregate state.
	loadBudget := defaultLoadBudgetBytes
	if inner.memTracker != nil {
		if b := inner.memTracker.Budget(); b > 0 && b/4 < loadBudget {
			loadBudget = b / 4
		}
	}
	inner.loadGate = newLoadGate(loadBudget, loadGateMaxLanes)

	// Catalog-persisted row-group metadata. Best-effort: nil on any
	// error, and files missing from the map take the footer-read path.
	var rgMeta map[string][]parquet.RowGroupStats
	if inner.cat != nil {
		rgMeta, _ = inner.cat.TableRGMeta(ctx, inner.tableName)
	}

	slots := make([]*fileSlot, len(inner.files))
	fileRGStats := make([][]parquet.RowGroupStats, len(inner.files))
	var footerFiles []int // indices into inner.files needing a footer read
	for i, entry := range inner.files {
		if gs, ok := rgMeta[entry.Path]; ok {
			slots[i] = &fileSlot{entry: entry}
			fileRGStats[i] = gs
		} else {
			footerFiles = append(footerFiles, i)
		}
	}

	var failedFiles atomic.Int64
	var firstErr atomic.Value // stores first error for diagnostics

	if len(footerFiles) > 0 {
		var metaWg sync.WaitGroup
		var metaIdx int64

		// Read each uncovered file's footer concurrently. A footer is a
		// few KB so this stays cheap even at SF1000. We use GetReaderAt
		// when available so the store can issue a range request rather
		// than downloading the full file.
		type readerAtStore interface {
			GetReaderAt(ctx context.Context, bucket, key string) (objstore.ReaderAtCloser, int64, error)
		}
		metaWorkers := runtime.NumCPU()
		if metaWorkers > len(footerFiles) {
			metaWorkers = len(footerFiles)
		}
		if metaWorkers < 1 {
			metaWorkers = 1
		}

		metaWg.Add(metaWorkers)
		for i := 0; i < metaWorkers; i++ {
			go func() {
				defer metaWg.Done()
				for {
					n := int(atomic.AddInt64(&metaIdx, 1) - 1)
					if n >= len(footerFiles) {
						return
					}
					if ctx.Err() != nil {
						return
					}
					idx := footerFiles[n]
					entry := inner.files[idx]

					var meta *parquet.FileReader
					if ras, ok := inner.cat.Store().(readerAtStore); ok {
						ra, sz, err := ras.GetReaderAt(ctx, inner.cat.Bucket(), entry.Path)
						if err == nil {
							meta, err = parquet.OpenFileReaderMetadata(ra, sz)
							ra.Close()
							if err != nil {
								if failedFiles.Add(1) == 1 {
									firstErr.Store(fmt.Errorf("metadata %s: %w", entry.Path, err))
								}
								continue
							}
						} else {
							if failedFiles.Add(1) == 1 {
								firstErr.Store(fmt.Errorf("get metadata %s: %w", entry.Path, err))
							}
							continue
						}
					} else {
						// Fallback for stores without ReaderAt: full Get + bytes reader.
						rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), entry.Path)
						if err != nil {
							if failedFiles.Add(1) == 1 {
								firstErr.Store(fmt.Errorf("get %s: %w", entry.Path, err))
							}
							continue
						}
						data, err := readAllSized(rc, entry.SizeBytes, true)
						rc.Close()
						if err != nil {
							if failedFiles.Add(1) == 1 {
								firstErr.Store(fmt.Errorf("read %s: %w", entry.Path, err))
							}
							continue
						}
						reader, err := parquet.NewReaderFromBytes(data)
						if err != nil {
							if failedFiles.Add(1) == 1 {
								firstErr.Store(fmt.Errorf("parquet %s (%d bytes): %w", entry.Path, len(data), err))
							}
							continue
						}
						inner.trackPooledBuf(data)
						meta = reader.FileReader()
					}

					stats := make([]parquet.RowGroupStats, meta.NumRowGroups())
					for rg := range stats {
						stats[rg] = meta.RowGroupStats(rg)
					}
					slots[idx] = &fileSlot{entry: entry}
					fileRGStats[idx] = stats
				}
			}()
		}
		metaWg.Wait()
	}

	if failed := failedFiles.Load(); failed > 0 {
		inner.failedFiles = int(failed)
		if v := firstErr.Load(); v != nil {
			inner.firstFileErr = v.(error)
		}
	}

	// Enumerate row groups from each file's metadata, applying pruning.
	// Surviving rgUnits hold the slot pointer; the file bytes are loaded
	// lazily on first rg read.
	for si, slot := range slots {
		if slot == nil {
			continue
		}
		var rowOffset int64
		var keptInFile int64
		for rgIdx, stats := range fileRGStats[si] {
			rgNumRows := stats.NumRows
			pruned := false

			// Predicate-based row group pruning
			if len(inner.scanPreds) > 0 {
				for _, pred := range inner.scanPreds {
					op := mapPredOp(pred.Op)
					if op >= 0 {
						sp := scan.StatsPredicate{Column: pred.Column, Op: op, Value: pred.Value}
						if scan.CanPruneRowGroup(sp, stats) {
							pruned = true
							break
						}
					}
				}
			}

			// Bloom filter row group pruning
			if !pruned && inner.bloomFilter != nil {
				if canBloomPruneRowGroup(inner.bloomFilter, stats) {
					pruned = true
				}
			}

			// Dynamic min/max range row group pruning
			if !pruned && len(inner.dynamicFilter) > 0 {
				if canRangePruneRowGroup(inner.dynamicFilter, stats) {
					pruned = true
				}
			}

			if pruned {
				rowOffset += rgNumRows
				continue
			}

			inner.rgUnits = append(inner.rgUnits, rgUnit{
				slot:        slot,
				rgRowOffset: rowOffset,
				rgIndex:     rgIdx,
				numRows:     rgNumRows,
			})
			keptInFile++
			rowOffset += rgNumRows
		}

		// Initialise the slot's refcount with the number of row groups
		// that survived pruning. releaseRG decrements after each rg is
		// processed and frees the file bytes when this reaches zero.
		// If keptInFile is 0, we still leave the slot with refcount 0
		// (it never gets loaded, which is what we want).
		slot.rgRemaining.Store(keptInFile)
	}
}

// canBloomPruneRowGroup checks whether a row group can be skipped based on
// bloom filter analysis of the join key column's min/max statistics.
// For integer join keys with a small range (<=1024), every value is checked
// against the bloom filter. If ALL return "not present", the row group is skipped.
func canBloomPruneRowGroup(bf *exec.BloomScanFilter, stats parquet.RowGroupStats) bool {
	if !bf.UseIntKey {
		return false
	}
	colStats, ok := stats.Columns[bf.Column]
	if !ok || !colStats.HasStats {
		return false
	}
	if colStats.MinValue == nil || colStats.MaxValue == nil {
		return false
	}
	if colStats.NullCount == stats.NumRows {
		return false
	}

	minVal := toBloomInt64(colStats.MinValue)
	maxVal := toBloomInt64(colStats.MaxValue)
	if minVal == 0 && maxVal == 0 && !isIntType(colStats.MinValue) {
		return false
	}

	const maxRangeSize = 1024
	rangeSize := maxVal - minVal + 1
	if rangeSize <= 0 || rangeSize > maxRangeSize {
		return false
	}

	for v := minVal; v <= maxVal; v++ {
		if exec.BloomContains(bf.Bloom, bf.BloomMask, exec.BloomHashInt(v)) {
			return false
		}
	}
	return true
}

func toBloomInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int32:
		return int64(tv)
	case int:
		return int64(tv)
	case float64:
		return int64(tv)
	case float32:
		return int64(tv)
	default:
		return 0
	}
}

func isIntType(v any) bool {
	switch v.(type) {
	case int64, int32, int:
		return true
	default:
		return false
	}
}

// canRangePruneRowGroup checks whether a row group can be skipped based on
// dynamic min/max range filters from the hash join build side. A row group
// is pruned if its value range has no overlap with the build-side range for
// any filter column.
func canRangePruneRowGroup(ranges []exec.DynamicRange, stats parquet.RowGroupStats) bool {
	for _, r := range ranges {
		colStats, ok := stats.Columns[r.Column]
		if !ok || !colStats.HasStats {
			continue
		}
		if colStats.MinValue == nil || colStats.MaxValue == nil {
			continue
		}
		if colStats.NullCount == stats.NumRows {
			continue
		}
		// No overlap: row group max < build min, or row group min > build max
		if scan.CompareValues(colStats.MaxValue, r.MinValue) < 0 {
			return true
		}
		if scan.CompareValues(colStats.MinValue, r.MaxValue) > 0 {
			return true
		}
	}
	return false
}

// rgWorker processes row group work units in parallel. Each worker
// speculatively prefetches one row group ahead AFTER sending the current
// batch downstream. This overlaps I/O with consumer processing without
// delaying batch delivery.
func (inner *scanSourceInner) rgWorker(ctx context.Context) {
	defer inner.wg.Done()

	var prefetched *prefetchResult

	for {
		if ctx.Err() != nil {
			return
		}

		var idx int
		var b *batch.RecordBatch

		if prefetched != nil {
			idx = prefetched.idx
			b = prefetched.batch
			prefetched = nil
		} else {
			idx = int(atomic.AddInt64(&inner.rgIdx, 1) - 1)
			if idx >= len(inner.rgUnits) {
				return
			}
			b = inner.readRG(ctx, idx)
		}

		if b == nil || b.Len == 0 {
			inner.releaseScanBatch(b)
			continue
		}

		// Apply delete markers with row offset adjustment
		unit := inner.rgUnits[idx]
		if delSet := inner.deleteMarkers[unit.slot.entry.Path]; len(delSet) > 0 {
			sel := make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				absRow := unit.rgRowOffset + int64(i)
				if !delSet[absRow] {
					sel = append(sel, uint32(i))
				}
			}
			if len(sel) == 0 {
				inner.releaseScanBatch(b)
				continue
			}
			if len(sel) < b.Len {
				b.Sel = sel
			}
		}

		atomic.AddInt64(&inner.rowsScanned, int64(b.ActiveLen()))

		// Send batch downstream FIRST, then prefetch while consumer processes.
		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}

		// Prefetch next row group while consumer processes the current one.
		// Each worker holds at most 1 extra row group (~10MB at SF10).
		// 8 workers * 1 RG = ~80MB overhead, negligible on a 32GB machine.
		if prefetched == nil {
			nextIdx := int(atomic.AddInt64(&inner.rgIdx, 1) - 1)
			if nextIdx < len(inner.rgUnits) {
				prefetched = &prefetchResult{
					idx:   nextIdx,
					batch: inner.readRG(ctx, nextIdx),
				}
			}
		}
	}
}

// readRG reads a row group using the native page decoder. Loads the file's
// bytes lazily on first call (bounded by inner.loadGate) and releases them
// once this slot's last row group has been processed. The decoded batch is
// charged to the memory tracker until it leaves the scan source.
func (inner *scanSourceInner) readRG(ctx context.Context, unitIdx int) *batch.RecordBatch {
	unit := inner.rgUnits[unitIdx]
	if unit.slot == nil {
		return nil
	}
	rdr, err := unit.slot.ensureLoaded(inner, ctx)
	if err != nil || rdr == nil {
		unit.slot.releaseRG(inner)
		return nil
	}
	// Dictionary-probe pruning: an equality conjunct whose constant is
	// absent from a pure-dictionary chunk proves the row group empty for
	// this query — skip the decode entirely (see scan/dict_prune.go).
	if len(inner.eqProbes) > 0 && scan.CanDictPruneRowGroup(rdr, unit.rgIndex, inner.eqProbes) {
		unit.slot.releaseRG(inner)
		return nil
	}
	b, err := scan.ReadRowGroupNative(rdr, unit.rgIndex, inner.readSchema(), inner.pool)
	unit.slot.releaseRG(inner)
	if err != nil {
		// Surface the first decode error so the scan FAILS instead of
		// silently dropping this row group's rows — a swallowed error here
		// is indistinguishable from a correct partial result (the same
		// silent-0-rows class as the nested-branch bug, issue #144).
		select {
		case inner.errCh <- fmt.Errorf("decoding %s row group %d: %w", unit.slot.entry.Path, unit.rgIndex, err):
		default:
		}
		return nil
	}
	if inner.emitRowLoc && b != nil && b.Len > 0 {
		stampRowLoc(b, unitIdx)
	}
	inner.trackScanBatch(b)
	return b
}

// stampRowLoc appends the synthetic __row_loc column: rgUnit ordinal in
// the high 32 bits, row-within-group in the low 32. Positional (before any
// Sel is applied), so delete-marker selection composes naturally. Schema
// append copies the slice header capacity-clamped so a pooled/shared
// backing array is never mutated in place.
func stampRowLoc(b *batch.RecordBatch, unitIdx int) {
	loc := batch.NewVector(batch.TypeInt64, b.Len)
	base := int64(unitIdx) << 32
	for i := 0; i < b.Len; i++ {
		loc.Int64Data[i] = base | int64(i)
	}
	b.Columns = append(b.Columns[:len(b.Columns):len(b.Columns)], loc)
	b.Schema = append(b.Schema[:len(b.Schema):len(b.Schema)], parquet.Column{Name: RowLocColumn, Type: parquet.TypeInt64})
}

// RefetchRows reads back the full-width rows named by __row_loc values, in
// locs order (the caller's sort order). Files are reopened independently
// of the scan's slot lifecycle — by refetch time the narrow phase has
// already released every slot's bytes. At most one row-group decode per
// distinct (file, row group) among the winners, so the cost is bounded by
// len(locs) wide row groups.
func (inner *scanSourceInner) RefetchRows(ctx context.Context, locs []int64) (*batch.RecordBatch, error) {
	byOrd := make(map[int64][]int, 4)
	for i, loc := range locs {
		byOrd[loc>>32] = append(byOrd[loc>>32], i)
	}
	// Two passes because Bytes-class vectors are offset-append storage:
	// writes must be sequential. Pass 1 stages winners row-group by
	// row-group (sequential writes into stage, one wide decode live at a
	// time); pass 2 gathers stage rows into final sorted order (sequential
	// writes into out, random reads from stage).
	stage := batch.NewRecordBatch(inner.schema, len(locs))
	stageIdx := make([]int, len(locs))
	si := 0
	for ord, positions := range byOrd {
		if ord < 0 || ord >= int64(len(inner.rgUnits)) {
			return nil, fmt.Errorf("refetch: row-loc ordinal %d out of range (%d units)", ord, len(inner.rgUnits))
		}
		unit := inner.rgUnits[ord]
		fr, closeFn, err := openRefetchReader(ctx, inner.cat, unit.slot.entry)
		if err != nil {
			return nil, fmt.Errorf("refetch %s: %w", unit.slot.entry.Path, err)
		}
		wide, err := scan.ReadRowGroupNative(fr, unit.rgIndex, inner.schema, nil)
		closeFn()
		if err != nil {
			return nil, fmt.Errorf("refetch %s row group %d: %w", unit.slot.entry.Path, unit.rgIndex, err)
		}
		for _, pos := range positions {
			row := int(locs[pos] & 0xffffffff)
			if wide == nil || row >= wide.Len {
				return nil, fmt.Errorf("refetch %s row group %d: row %d out of range", unit.slot.entry.Path, unit.rgIndex, row)
			}
			for c := range inner.schema {
				copyVecRange(stage.Columns[c], si, wide.Columns[c], row, 1)
			}
			stageIdx[pos] = si
			si++
		}
	}
	out := batch.NewRecordBatch(inner.schema, len(locs))
	for pos := range locs {
		for c := range inner.schema {
			copyVecRange(out.Columns[c], pos, stage.Columns[c], stageIdx[pos], 1)
		}
	}
	return out, nil
}

// openRefetchReader opens a file reader for the refetch phase, preferring
// the staged-pread path (only the needed column chunks are read) and
// falling back to a whole-file read. Untracked: the refetch touches at
// most a handful of files and frees them immediately.
func openRefetchReader(ctx context.Context, cat *catalog.Catalog, entry catalog.FileEntry) (*parquet.FileReader, func(), error) {
	if ras, ok := cat.Store().(objstore.ReaderAtStore); ok {
		if ra, size, err := ras.GetReaderAt(ctx, cat.Bucket(), entry.Path); err == nil {
			if f, isFile := ra.(*os.File); isFile {
				if fr, ferr := parquet.OpenFileReaderAt(f, size); ferr == nil {
					return fr, func() { f.Close() }, nil
				}
				f.Close()
			} else {
				ra.Close()
			}
		}
	}
	rc, _, err := cat.Store().Get(ctx, cat.Bucket(), entry.Path)
	if err != nil {
		return nil, nil, err
	}
	data, err := readAllSized(rc, entry.SizeBytes, false)
	rc.Close()
	if err != nil {
		return nil, nil, err
	}
	reader, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		return nil, nil, err
	}
	return reader.FileReader(), func() {}, nil
}

// copyVecRange copies count values from src[srcOff..] to dst[dstOff..] using
// bulk typed copies. One type switch per column instead of per row.
func copyVecRange(dst *batch.Vector, dstOff int, src *batch.Vector, srcOff, count int) {
	if src.Nulls.HasNulls() {
		for i := 0; i < count; i++ {
			if src.Nulls.IsNullFast(srcOff + i) {
				dst.Nulls.SetNull(dstOff + i)
			}
		}
	}
	switch dst.Type {
	case batch.TypeBool:
		copy(dst.BoolData[dstOff:dstOff+count], src.BoolData[srcOff:srcOff+count])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		copy(dst.Int32Data[dstOff:dstOff+count], src.Int32Data[srcOff:srcOff+count])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		copy(dst.Int64Data[dstOff:dstOff+count], src.Int64Data[srcOff:srcOff+count])
	case batch.TypeFloat32:
		copy(dst.Float32Data[dstOff:dstOff+count], src.Float32Data[srcOff:srcOff+count])
	case batch.TypeFloat64:
		copy(dst.Float64Data[dstOff:dstOff+count], src.Float64Data[srcOff:srcOff+count])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.BulkCopy(dstOff, &src.BytesData, srcOff, count)
	case batch.TypeDecimal:
		copy(dst.DecimalData.Data[dstOff:dstOff+count], src.DecimalData.Data[srcOff:srcOff+count])
	}
}

// buildReadSchema applies column projection to the table schema.
func buildReadSchema(schema []parquet.Column, requiredCols []string) []parquet.Column {
	if len(requiredCols) == 0 {
		return schema
	}
	// Row-count-only scans (bare COUNT(*)): the sentinel means "any one
	// narrow column suffices". Drop it when real columns are present
	// (predicate columns already give a row count); when it stands alone,
	// project the narrowest column instead of the full schema.
	countOnly := false
	realCols := requiredCols[:0:0]
	for _, c := range requiredCols {
		if c == logical.RowCountOnlyColumn {
			countOnly = true
			continue
		}
		realCols = append(realCols, c)
	}
	if countOnly && len(realCols) == 0 {
		return []parquet.Column{narrowestColumn(schema)}
	}
	if countOnly {
		requiredCols = realCols
	}
	needed := make(map[string]bool, len(requiredCols))
	// Dotted references: "attrs.score" must pull in the parent column when
	// the qualifier names a ROW column ("attrs"), or the field access in a
	// downstream filter/projection silently evaluated NULL because the
	// parent was pruned from the scan (issue #147). Qualifiers that are
	// table aliases ("lineitem.l_quantity") never match a column name, so
	// this adds nothing for them.
	prefixNeeded := make(map[string]bool, 2)
	for _, c := range requiredCols {
		needed[c] = true
		if dot := strings.IndexByte(c, '.'); dot > 0 {
			prefixNeeded[c[:dot]] = true
		}
	}
	filtered := make([]parquet.Column, 0, len(requiredCols))
	for _, col := range schema {
		if needed[col.Name] || (prefixNeeded[col.Name] && col.Type == parquet.TypeRow) {
			filtered = append(filtered, col)
		}
	}
	if len(filtered) > 0 {
		return filtered
	}
	return schema
}

// narrowestColumn picks the cheapest-to-read column for row-count-only
// scans: fixed narrow types beat wide ones beat variable-length ones.
func narrowestColumn(schema []parquet.Column) parquet.Column {
	width := func(t parquet.TypeID) int {
		switch t {
		case parquet.TypeBool:
			return 1
		case parquet.TypeInt32, parquet.TypeDate, parquet.TypePort, parquet.TypeProtocol, parquet.TypeFloat32:
			return 4
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeFloat64, parquet.TypeIPv4,
			parquet.TypeMAC, parquet.TypeDuration, parquet.TypeDecimal:
			return 8
		case parquet.TypeUUID, parquet.TypeIPv6:
			return 16
		default:
			return 64 // strings/bytes/nested — avoid
		}
	}
	best := schema[0]
	for _, c := range schema[1:] {
		if width(c.Type) < width(best.Type) {
			best = c
		}
	}
	return best
}

// expandRowFieldRequirements rewrites required column names of the form
// "parent.field" to "parent" when parent names a ROW column in schema, so
// row-level readers (which match by exact name) pull the parent in.
// Non-matching entries pass through unchanged.
func expandRowFieldRequirements(schema []parquet.Column, requiredCols []string) []string {
	if len(requiredCols) == 0 {
		return requiredCols
	}
	rowCols := make(map[string]bool, 2)
	for _, col := range schema {
		if col.Type == parquet.TypeRow {
			rowCols[col.Name] = true
		}
	}
	out := make([]string, 0, len(requiredCols))
	seen := make(map[string]bool, len(requiredCols))
	for _, c := range requiredCols {
		name := c
		if dot := strings.IndexByte(c, '.'); dot > 0 && rowCols[c[:dot]] {
			name = c[:dot]
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}
