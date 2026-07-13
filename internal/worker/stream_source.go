package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/diskio"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// progressWriter wraps an io.Writer and reports each successful Write as
// bytes-of-progress on the active per-task ProgressReporter. Used while
// streaming a multi-GB build cache file to local NVMe so the per-task
// progress heartbeat keeps flowing during the otherwise-silent download +
// decompression window — without this, a long broadcast cache load shows
// up as "no per-task progress" to the coord and risks a stale-worker reap
// for an instance that's making real I/O progress.
type progressWriter struct {
	w   io.Writer
	rep exec.ProgressReporter
}

func (p *progressWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if n > 0 && p.rep != nil {
		p.rep.AddBytes(int64(n))
	}
	return n, err
}

// cachedFileStreamSource lazily reads pre-scanned build-cache files one at a
// time, yielding batches from each file before moving to the next.
//
// For shuffle-format (.wshf) files, the source streams the S3 object directly
// to a local spill file on the worker's NVMe and then mmaps the local file.
// The shuffleChunkReader walks the mmap'd region in place; the kernel pages
// in pieces lazily, so peak resident memory is bounded by the kernel's page
// cache for the active region rather than the full file size. Without this,
// the previous io.ReadAll path would allocate a single byte slice the size of
// the cache file (10+ GB at SF100) and OOM the worker before it could
// process a single batch.
//
// For Parquet files we use a streaming row-group iterator: only one
// decoded row group is held in memory at a time per source, instead of
// materializing every row group of the active file up front. At SF100 a
// lineitem file decodes to ~280 MB per row group × ~10 row groups, so
// the eager-decode that this replaced cost ~2.8 GB live per file —
// dominant contributor to per-task working set during scan-aggregate.
type cachedFileStreamSource struct {
	executor *Executor
	bucket   string
	queryID  string // used to look up same-worker LocalStageCache entries; "" disables
	files    []string

	// projectColumns optionally restricts parquet reads to these columns
	// by name. Applied only when EVERY requested column exists in the
	// file's schema — if any are missing (e.g. a derived column name that
	// the aggregate operator will compute via pre-project), the projection
	// is silently skipped and the full schema is read. This keeps the
	// optimization safe in the presence of expression-based aggregates
	// without needing to teach the source about derivation rules.
	projectColumns []string

	// Row-group sharding. When shardCount > 1, parquet reads only row groups
	// [idx*N/count, (idx+1)*N/count). Only applies to parquet inputs (.wshf
	// shuffle outputs are unaffected — those are already partitioned by the
	// upstream stage). shardCount = 0 or 1 means whole file.
	shardIdx   int
	shardCount int

	fileIdx int

	// Active WSHF chunk reader for the current file (nil if current file is
	// Parquet or no file is open). The reader walks an mmap'd byte slice
	// owned by mmapData below.
	chunkReader *shuffleChunkReader
	mmapData    []byte      // mmap'd view of the current local cache file (nil if Parquet)
	mmapRegion  *mmapRegion // Phase-5 relief handle for mmapData; nil when relief is disabled
	localPath   string      // path the source is responsible for unlinking; "" when the file is owned by LocalStageCache or in-memory

	// Streaming row-group iterator for the current Parquet file. Yields one
	// decoded RecordBatch per Next() call; the previous batch is GC-eligible
	// once the consumer Releases it. Bounds Parquet-side live memory to one
	// decoded row group at a time per source.
	parquetIter *scan.RowGroupIter
	// parquetReader is held alongside the iterator so its underlying buffer
	// (the whole-file byte slice) stays live for the iterator's row-group
	// reads. Released alongside parquetIter when the file is exhausted.
	parquetReader *parquet.Reader

	// Fallback slice for schemas with Array/Map types — RowGroupIter does not
	// support those yet, so we keep the existing eager-decode path for them.
	// Empty for all TPC-H scans.
	fallbackBatches  []*batch.RecordBatch
	fallbackBatchIdx int

	// Dynamic-filter pushdowns. Set by the fragment runner from
	// OpSpec.DynamicFilters; consumed by RowGroupIter to skip row groups
	// whose stats don't intersect the build-side bloom or range.
	dynamicRanges []exec.DynamicRange
	bloomFilters  []*exec.BloomScanFilter

	// Download-ahead for S3 parquet inputs (see filePrefetcher). Started
	// lazily on the first openNextFile so it inherits the task context;
	// nil when the input has no parquet files, there's no spill dir, or
	// prefetchDisabled is set (tests exercising the serial path).
	prefetch         *filePrefetcher
	prefetchStarted  bool
	prefetchDisabled bool
}

// SetDynamicFilters attaches dynamic-filter pushdowns to this source.
// Idempotent — safe to call before Init.
func (s *cachedFileStreamSource) SetDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	if s == nil {
		return
	}
	s.dynamicRanges = ranges
	s.bloomFilters = blooms
}

func newCachedFileStreamSource(executor *Executor, queryID, bucket string, files []string) *cachedFileStreamSource {
	return &cachedFileStreamSource{
		executor: executor,
		queryID:  queryID,
		bucket:   bucket,
		files:    files,
	}
}

// newCachedFileStreamSourceWithProjection is like newCachedFileStreamSource
// but will attempt to restrict parquet reads to the named columns. The
// projection is applied ONLY when every requested column is present in the
// parquet file's schema — a safety guard against derived/expression names
// that would otherwise over-prune the scan and break downstream operators.
func newCachedFileStreamSourceWithProjection(executor *Executor, queryID, bucket string, files []string, projectColumns []string) *cachedFileStreamSource {
	return &cachedFileStreamSource{
		executor:       executor,
		queryID:        queryID,
		bucket:         bucket,
		files:          files,
		projectColumns: projectColumns,
	}
}

// SetShard configures row-group sharding for parquet reads on this source.
// shardCount <= 1 disables sharding (whole file). Idempotent and safe to
// call before Init.
func (s *cachedFileStreamSource) SetShard(shardIdx, shardCount int) {
	s.shardIdx = shardIdx
	s.shardCount = shardCount
}

func (s *cachedFileStreamSource) Init(_ context.Context) error { return nil }

func (s *cachedFileStreamSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	// Phase-5 relief: mark the current mapping as recently used so it isn't
	// chosen as a cold MADV_DONTNEED victim while actively streamed. nil when
	// relief is disabled — one branch, no cost on the hot path.
	s.mmapRegion.touch()
	for {
		// Stream from active WSHF chunk reader.
		if s.chunkReader != nil {
			b, err := s.chunkReader.Next()
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			// Current file exhausted — release its mmap and remove the local
			// copy before moving on. The chunkReader's batches have already
			// copied their column data into RecordBatch arrays, so dropping
			// the mmap region here is safe.
			s.releaseCurrentFile()
		}

		// Yield next decoded row group from the active Parquet iterator.
		// The iterator decodes lazily — only one row group's worth of
		// memory is live at a time per source.
		if s.parquetIter != nil {
			b, err := s.parquetIter.Next()
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			// Iterator exhausted — release reader and the mmap/local file it
			// was reading from before opening the next file. Order matters:
			// releaseParquetIter drops s.parquetReader, which drops the last
			// reference to the byte slice that the streaming parquet-mmap
			// path (added 2026-05-22) handed in as FileReader.data. Once
			// nothing is holding the slice, releaseCurrentFile can safely
			// munmap. WSHF path is unaffected because chunkReader is nil
			// here (its release happens in its own branch above).
			s.releaseParquetIter()
			s.releaseCurrentFile()
		}

		// Yield from the fallback slice (Array/Map schemas only).
		if s.fallbackBatchIdx < len(s.fallbackBatches) {
			b := s.fallbackBatches[s.fallbackBatchIdx]
			s.fallbackBatches[s.fallbackBatchIdx] = nil
			s.fallbackBatchIdx++
			return b, nil
		}
		if s.fallbackBatches != nil {
			// Fallback exhausted — release the mmap/local temp backing it
			// before advancing, mirroring the parquetIter branch above.
			// Without this, openNextFile overwrote mmapData/localPath and a
			// multi-file Array/Map source leaked N-1 mmaps + temp files (in
			// the worker-lifetime spill dir, outside the per-task sweeps)
			// plus permanent mmapRegistry entries.
			s.fallbackBatches = nil
			s.fallbackBatchIdx = 0
			s.releaseParquetIter()
			s.releaseCurrentFile()
		}

		// All files exhausted.
		if s.fileIdx >= len(s.files) {
			return nil, nil
		}

		// Open next file and set up the appropriate reader.
		if err := s.openNextFile(ctx); err != nil {
			return nil, err
		}
	}
}

func (s *cachedFileStreamSource) Close() error {
	// Stop the download-ahead workers first and reap any temp files they
	// downloaded that were never consumed.
	if s.prefetch != nil {
		s.prefetch.Close()
		s.prefetch = nil
	}
	// Parquet refs must be cleared BEFORE munmap: parquet.NewReaderFromBytes
	// keeps the input slice live inside FileReader.data, and the streaming
	// parquet-mmap path (added 2026-05-22) hands the mmap'd region in as
	// that slice. Munmapping before releasing the reader would leave the
	// reader holding dangling mmap bytes if any caller still has a handle.
	s.releaseParquetIter()
	s.releaseCurrentFile()
	for i := s.fallbackBatchIdx; i < len(s.fallbackBatches); i++ {
		s.fallbackBatches[i] = nil
	}
	s.fallbackBatches = nil
	return nil
}

// releaseParquetIter closes the active row-group iterator and drops the
// reader reference so the underlying file buffer can be GC'd. Safe to
// call multiple times.
func (s *cachedFileStreamSource) releaseParquetIter() {
	if s.parquetIter != nil {
		_ = s.parquetIter.Close()
		s.parquetIter = nil
	}
	s.parquetReader = nil
}

// releaseCurrentFile munmaps and deletes the local cache file backing the
// current chunkReader. Safe to call multiple times.
func (s *cachedFileStreamSource) releaseCurrentFile() {
	s.chunkReader = nil
	if s.mmapData != nil {
		// Unregister from the relief registry BEFORE munmapping so relieveMmap
		// (which holds the registry lock during its MADV syscalls) can never
		// advise a region that is about to be unmapped.
		unregisterMmap(s.mmapRegion)
		s.mmapRegion = nil
		_ = syscall.Munmap(s.mmapData)
		s.mmapData = nil
	}
	if s.localPath != "" {
		if rmErr := os.Remove(s.localPath); rmErr != nil && !os.IsNotExist(rmErr) && s.executor.logger != nil {
			// Visibility only — clearing localPath below means nothing will
			// retry this delete, and a leaked multi-GB cache file eats spill
			// volume until the next process start sweeps it.
			s.executor.logger.Warn("local cache file cleanup failed",
				"path", s.localPath, "error", rmErr)
		}
		s.localPath = ""
	}
}

// openNextFile downloads and opens the next file, setting either chunkReader
// (for WSHF) or batches (for Parquet). Advances fileIdx.
func (s *cachedFileStreamSource) openNextFile(ctx context.Context) error {
	filePath := s.files[s.fileIdx]
	idx := s.fileIdx
	s.fileIdx++

	// Start the download-ahead pipeline on first open. Before this
	// existed, a scan task's files were downloaded strictly one at a time
	// with decode idle during each download — on a standalone box that
	// capped effective S3 read parallelism at MaxConcurrent streams
	// (2026-07-05 SF10 cold-S3: suite 20m15s vs DuckDB 2m51s, same box).
	if !s.prefetchStarted {
		s.prefetchStarted = true
		if !s.prefetchDisabled && s.executor.spillDir != "" &&
			len(s.files) > 1 && hasPrefetchableFiles(s.files) {
			s.prefetch = startFilePrefetcher(ctx, s)
		}
	}
	if s.prefetch != nil {
		if res := s.prefetch.take(ctx, idx); !res.skipped && res.err == nil {
			if err := s.openPrefetchedParquet(res.localPath, filePath); err == nil {
				return nil
			}
			// Open failed (truncated download, unexpected payload) — the
			// temp was removed; fall through to the tiered path below,
			// which re-resolves from the durable copy.
		}
		// skipped or err: strictly best-effort — the tiered path below is
		// authoritative for both resolution and error reporting.
	}

	// Base-table cache tier: the process-wide NVMe cache already holds
	// this parquet object — mmap its file in place, no GET and no disk
	// copy (docs/design/base-table-nvme-cache.md §7 S2). The cache owns
	// the file (LRU eviction unlinks it; POSIX keeps the inode alive
	// under a live mmap), so s.localPath stays empty. Any failure —
	// evicted between lookup and open, unreadable entry — falls through
	// to the tiered path, whose durable copy is authoritative.
	if lps, ok := s.executor.store.(objstore.LocalPathStore); ok {
		if cached, ok := lps.CachedLocalPath(s.bucket, filePath); ok {
			if err := s.openParquetFromBaseCache(cached, filePath); err == nil {
				return nil
			} else if s.executor.logger != nil {
				s.executor.logger.Debug("base-table cache open fell through",
					"key", filePath, "path", cached, "error", err)
			}
		}
	}

	// Tier-0 same-worker fast path: if a producer task on this worker
	// already wrote this stage output to local disk and registered it in
	// LocalStageCache, mmap it directly — no KV / S3 round-trip. The cache
	// owns the file (it'll unlink on query-complete cleanup); the consumer
	// must NOT unlink, so we leave s.localPath empty.
	if s.queryID != "" && s.executor.localCache != nil {
		if cached := s.executor.localCache.Get(s.queryID, filePath); cached != "" {
			if err := s.openShuffleFromLocalFile(cached); err == nil {
				return nil
			}
			// On any failure (file vanished, mmap error), fall through to
			// the existing KV/S3 path — the durable copy is still there.
		}
	}

	// KV fast-path: small stage outputs (Phase 3 native-DAG) are written
	// only to NATS KV and skip S3 entirely. Consult KV first; on miss,
	// fall through to S3 as before. The KV key is a dot-sanitized version
	// of the S3 key (matching writeUnpartitionedWSHF + natsKVKey).
	var kvErr error
	var kvDataLen int
	var kvMagic string
	if s.executor.resultKV != nil {
		kvKey := natsKVKey(filePath)
		var entry jetstream.KeyValueEntry
		entry, kvErr = s.executor.resultKV.Get(ctx, kvKey)
		if kvErr == nil {
			data := entry.Value()
			kvDataLen = len(data)
			if len(data) >= 4 {
				magic := [4]byte{data[0], data[1], data[2], data[3]}
				kvMagic = string(magic[:])
				wshf := magic == shuffleMagic
				wshc := magic == compressedMagic
				if wshf || wshc {
					return s.openShuffleInMemory(filePath, magic[:], bytes.NewReader(data[4:]), wshc)
				}
				// Non-shuffle payload (e.g., parquet) — fall through to S3.
			}
		}
	}

	// Tier-1.5 peer fetch (streaming exchange Phase A): when the coordinator
	// hinted which worker still holds this file on local disk, stream it from
	// that peer over gRPC instead of S3. Any failure — peer gone, token
	// stale, file evicted, mid-stream disconnect — falls through to the
	// durable S3 copy, which is guaranteed to exist because the producing
	// stage completed only after its synchronous upload.
	if peers := s.executor.peers; peers != nil && peers.client != nil {
		if addr, token := peers.hintFor(filePath); addr != "" {
			if perr := s.openShuffleFromPeer(ctx, filePath, addr, token); perr == nil {
				peers.fetchHits.Add(1)
				return nil
			} else {
				peers.fetchFallthrough.Add(1)
				if s.executor.logger != nil {
					s.executor.logger.Debug("peer fetch fell through to S3",
						"key", filePath, "peer", addr, "error", perr)
				}
			}
		}
	}

	// Try the streaming path first: open a streaming reader from S3, peek
	// at the magic bytes, and decide whether this is a WSHF (or compressed
	// WSHC) shuffle file or some legacy Parquet payload.
	rc, _, err := s.executor.store.Get(ctx, s.bucket, filePath)
	if err != nil {
		// Streaming-query key with no durable copy: the producer reported
		// this file, so its Phase-B background upload is either in flight
		// (bounded re-poll below catches it landing) or died with the
		// producer (missingInputError → coordinator classifies via liveness
		// + durability bits). Trigger on a hint OR on the query's fetch
		// token alone — a producer reaped before this task was dispatched
		// leaves no hint, but the key can still be upload-pending.
		if peers := s.executor.peers; peers != nil && errors.Is(err, objstore.ErrNotFound) {
			if addr, token := peers.hintFor(filePath); addr != "" || token != "" {
				waited, waitErr := s.awaitDurableObject(ctx, filePath)
				if waitErr != nil {
					return &missingInputError{key: filePath, cause: waitErr}
				}
				rc = waited
				err = nil
			}
		}
	}
	if err != nil {
		// Both KV and store missed. Annotate so the diagnostic gap from
		// 2026-04-25 (object not found cascade in pgwire→coord routing)
		// is visible the next time it surfaces.
		return fmt.Errorf("opening cached file %s (kvErr=%v, kvDataLen=%d, kvMagic=%q, bucket=%s): %w",
			filePath, kvErr, kvDataLen, kvMagic, s.bucket, err)
	}
	defer rc.Close()

	// Read enough header bytes to detect the format. Both WSHF and WSHC
	// have a 4-byte magic at offset 0.
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("reading magic from %s: %w", filePath, err)
	}

	wshf := magic == shuffleMagic
	wshc := magic == compressedMagic

	if wshf || wshc {
		return s.openShuffleFile(ctx, filePath, magic[:], rc, wshc)
	}

	// Parquet path: when a spill dir is available, stream the body to a
	// local NVMe temp file, mmap it PROT_READ, and hand the mmap'd byte
	// slice to parquet.NewReaderFromBytes (zero-copy). Heap is bounded
	// by the kernel's page-cache footprint for the active mmap region
	// instead of the full file size. Mirrors the WSHF path's streaming
	// pattern (openShuffleFile above).
	//
	// Pre-2026-05-22 this used io.ReadAll(rc) + parquet.NewReader
	// (bytes.NewReader(data), len), which kept TWO full-file buffers
	// alive per open file: the io.ReadAll slice AND OpenFileReader's
	// internal make([]byte, size). Q21 SF1 alloc-profile attributed
	// 1110 MB to io.ReadAll + 204 MB to OpenFileReader's make([]byte,
	// size) — the dominant heap source during join-6 (peak 3.9 GB).
	//
	// Fallback: when spillDir is empty (tests, MemStore-only setups)
	// keep the in-memory path but drop the double-buffer by using
	// NewReaderFromBytes (zero-copy) instead of NewReader+bytes.Reader.
	var data []byte
	var mmapData []byte
	var localPath string
	if s.executor.spillDir != "" {
		md, lp, err := s.streamParquetToLocalMmap(ctx, filePath, magic[:], rc)
		if err != nil {
			return fmt.Errorf("streaming parquet %s to local mmap: %w", filePath, err)
		}
		mmapData = md
		localPath = lp
		data = mmapData
	} else {
		rest, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("reading parquet body for %s: %w", filePath, err)
		}
		data = append(magic[:], rest...)
	}
	return s.finishOpenParquet(filePath, data, mmapData, localPath)
}

// openPrefetchedParquet opens a parquet file the prefetcher already
// downloaded to localPath: mmap it and hand the region to the shared
// open tail. On any failure the temp file is removed and the caller
// falls back to the normal tiered path.
func (s *cachedFileStreamSource) openPrefetchedParquet(localPath, filePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("opening prefetched file %s: %w", localPath, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		_ = os.Remove(localPath)
		return fmt.Errorf("prefetched file %s unusable (err=%v)", localPath, err)
	}
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("mmap prefetched file %s: %w", localPath, err)
	}
	// finishOpenParquet unwinds mmap+file itself if the payload isn't
	// valid parquet (e.g. a legacy shuffle object under a .parquet key).
	return s.finishOpenParquet(filePath, mmapData, mmapData, localPath)
}

// openParquetFromBaseCache opens a parquet file the process-wide
// base-table cache holds on local disk: mmap it in place and hand the
// region to the shared open tail. localPath stays empty throughout — the
// cache owns the file, so neither releaseCurrentFile nor the open-failure
// unwind may unlink it.
func (s *cachedFileStreamSource) openParquetFromBaseCache(cachePath, filePath string) error {
	f, err := os.Open(cachePath)
	if err != nil {
		return fmt.Errorf("opening base-cache file %s: %w", cachePath, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		return fmt.Errorf("base-cache file %s unusable (err=%v)", cachePath, err)
	}
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap base-cache file %s: %w", cachePath, err)
	}
	// finishOpenParquet unwinds the mmap itself on a bad payload; its
	// os.Remove of the empty localPath is a no-op.
	return s.finishOpenParquet(filePath, mmapData, mmapData, "")
}

// finishOpenParquet is the shared tail of the parquet open path: build the
// reader over data, transfer mmap/file ownership to the source, apply the
// column projection guard, and set up the row-group iterator (or the
// eager fallback for Array/Map schemas). mmapData/localPath may be
// nil/empty for the in-memory path.
func (s *cachedFileStreamSource) finishOpenParquet(filePath string, data, mmapData []byte, localPath string) error {
	reader, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		if mmapData != nil {
			_ = syscall.Munmap(mmapData)
			_ = os.Remove(localPath)
		}
		return fmt.Errorf("opening parquet file %s: %w", filePath, err)
	}
	// Transfer mmap/file ownership to the source so Next()/Close()
	// release them when the iterator exhausts. nil/empty are fine for
	// the in-memory fallback.
	s.mmapData = mmapData
	s.mmapRegion = registerMmap(mmapData) // nil when relief disabled
	s.localPath = localPath
	projCols := reader.Schema().Columns
	// Apply column projection only when EVERY requested column is present
	// in the file schema. If any requested name is missing (likely a
	// derived/expression column that will be computed by a pre-project
	// pass), skip projection entirely and read the full schema — the
	// downstream operator needs the raw columns to compute the derivation.
	if len(s.projectColumns) > 0 {
		schemaSet := make(map[string]bool, len(projCols))
		for _, c := range projCols {
			schemaSet[c.Name] = true
		}
		allPresent := true
		for _, name := range s.projectColumns {
			if !schemaSet[name] {
				allPresent = false
				break
			}
		}
		if allPresent {
			wanted := make(map[string]bool, len(s.projectColumns))
			for _, c := range s.projectColumns {
				wanted[c] = true
			}
			filtered := make([]parquet.Column, 0, len(s.projectColumns))
			for _, c := range projCols {
				if wanted[c.Name] {
					filtered = append(filtered, c)
				}
			}
			if len(filtered) > 0 {
				projCols = filtered
			}
		}
	}
	shardIdx, shardCount := s.shardIdx, s.shardCount
	if shardCount < 1 {
		shardCount = 1
	}
	// Schemas with Array/Map types (none in TPC-H) still go through the
	// eager-decode slice path because RowGroupIter doesn't support them.
	// Everything else streams one row group at a time.
	if scan.HasUnsupportedColumnarTypes(projCols) {
		batches, err := scan.ReadFileBatchesShard(reader, projCols, nil, shardIdx, shardCount)
		if err != nil {
			return fmt.Errorf("reading parquet file %s (fallback): %w", filePath, err)
		}
		s.fallbackBatches = batches
		s.fallbackBatchIdx = 0
		return nil
	}
	iter, err := scan.OpenRowGroupIter(reader, projCols, nil, shardIdx, shardCount)
	if err != nil {
		return fmt.Errorf("opening row-group iterator for %s: %w", filePath, err)
	}
	if len(s.dynamicRanges) > 0 || len(s.bloomFilters) > 0 {
		iter.SetDynamicFilters(s.dynamicRanges, s.bloomFilters)
	}
	s.parquetIter = iter
	s.parquetReader = reader
	return nil
}

// openShuffleFile streams the (possibly compressed) shuffle body from rc to a
// local spill file, decompressing on the fly if needed, then mmaps the result
// and hands it to a shuffleChunkReader. Memory is bounded by the kernel's
// page cache footprint over the active mmap region rather than the full file
// size.
//
// While the body streams to disk, each Write is reported to the per-task
// ProgressReporter (read from ctx). This keeps liveness signals flowing
// during the otherwise-silent download window of a large broadcast cache
// load — without it, a multi-GB load can run for minutes with no per-task
// progress, and the coord's stale-worker reap fires on an instance that's
// making real I/O progress.
func (s *cachedFileStreamSource) openShuffleFile(ctx context.Context, srcPath string, magic []byte, rc io.Reader, compressed bool) error {
	spillDir := s.executor.spillDir
	if spillDir == "" {
		// Fall back to the in-memory path if no NVMe spill is available.
		// This is primarily a test-environment safety net; production
		// workers always have a spill dir.
		return s.openShuffleInMemory(srcPath, magic, rc, compressed)
	}
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return fmt.Errorf("creating spill dir: %w", err)
	}
	tmp, err := os.CreateTemp(spillDir, "build-cache-load-*.wshf")
	if err != nil {
		return fmt.Errorf("creating local cache file: %w", err)
	}
	localPath := tmp.Name()
	// On any error before we successfully mmap, the deferred close+remove
	// here releases the resources. On success we transfer ownership to s
	// and clear localPath so this defer is a no-op.
	cleanupLocal := true
	defer func() {
		if cleanupLocal {
			tmp.Close()
			_ = os.Remove(localPath)
		}
	}()

	// The local file must hold a plain-WSHF stream because the chunk reader
	// only knows the WSHF magic. For a compressed (WSHC) input the s2 stream
	// already encodes the original WSHF body — including its own WSHF magic
	// — so we write nothing yet and let streamDecompressShuffle below produce
	// the entire WSHF stream. For a plain (WSHF) input we re-prepend the
	// magic that we already consumed from rc.
	rep := exec.ProgressReporterFromContext(ctx)
	// KeepResident: the file is mmap'd and walked immediately after the
	// download, so only the dirty bound applies — windowed writeback caps
	// the dirty page-cache footprint of a multi-GB download at ~2 windows
	// without dropping the pages the imminent walk needs.
	wf, _ := diskio.NewWriter(tmp, diskio.KeepResident)
	dst := io.Writer(wf)
	if rep != nil {
		dst = &progressWriter{w: wf, rep: rep}
	}

	if compressed {
		if err := streamDecompressShuffle(rc, dst); err != nil {
			return fmt.Errorf("decompressing %s into local cache: %w", srcPath, err)
		}
	} else {
		if _, err := wf.Write(magic); err != nil {
			return fmt.Errorf("writing magic to local cache: %w", err)
		}
		if rep != nil {
			rep.AddBytes(int64(len(magic)))
		}
		if _, err := io.Copy(dst, rc); err != nil {
			return fmt.Errorf("streaming %s to local cache: %w", srcPath, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("syncing local cache: %w", err)
	}

	fi, err := tmp.Stat()
	if err != nil {
		return fmt.Errorf("stat local cache: %w", err)
	}
	if fi.Size() == 0 {
		return fmt.Errorf("local cache for %s is empty after stream", srcPath)
	}

	mmapData, err := syscall.Mmap(int(tmp.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap local cache %s: %w", localPath, err)
	}
	// We can close the file descriptor immediately — the mmap keeps the
	// underlying inode alive until Munmap.
	if err := tmp.Close(); err != nil {
		_ = syscall.Munmap(mmapData)
		return fmt.Errorf("closing local cache fd: %w", err)
	}

	// Build a chunk reader over the mmap'd region. The reader walks the
	// slice in place; readColumnData copies bytes into the RecordBatch's
	// typed column arrays, so subsequent batches don't depend on the mmap
	// region after they're returned to the caller.
	r, err := newShuffleChunkReader(mmapData)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		return fmt.Errorf("opening shuffle file %s: %w", srcPath, err)
	}

	cleanupLocal = false
	s.chunkReader = r
	s.mmapData = mmapData
	s.mmapRegion = registerMmap(mmapData) // nil when relief disabled
	s.localPath = localPath
	return nil
}

// streamParquetToLocalMmap streams a parquet body (with `magic` prepended)
// from rc to a temp file on the worker's NVMe spill dir, then mmaps the
// file PROT_READ and returns the mmap'd slice. The caller takes ownership:
// on success it must munmap the slice and remove the local path. Used by
// the parquet branch of openNextFile to avoid the double-full-file-buffer
// allocation pattern of io.ReadAll + OpenFileReader.
func (s *cachedFileStreamSource) streamParquetToLocalMmap(ctx context.Context, srcPath string, magic []byte, rc io.Reader) ([]byte, string, error) {
	spillDir := s.executor.spillDir
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("creating spill dir: %w", err)
	}
	tmp, err := os.CreateTemp(spillDir, "parquet-stream-*.parquet")
	if err != nil {
		return nil, "", fmt.Errorf("creating local parquet file: %w", err)
	}
	localPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			tmp.Close()
			_ = os.Remove(localPath)
		}
	}()

	rep := exec.ProgressReporterFromContext(ctx)
	// KeepResident: mmap'd and read by the parquet row-group iterator right
	// after the download — bound dirty pages, keep them resident.
	wf, _ := diskio.NewWriter(tmp, diskio.KeepResident)
	dst := io.Writer(wf)
	if rep != nil {
		dst = &progressWriter{w: wf, rep: rep}
	}
	if _, err := dst.Write(magic); err != nil {
		return nil, "", fmt.Errorf("writing magic to local parquet: %w", err)
	}
	if _, err := io.Copy(dst, rc); err != nil {
		return nil, "", fmt.Errorf("streaming %s to local parquet: %w", srcPath, err)
	}
	if err := tmp.Sync(); err != nil {
		return nil, "", fmt.Errorf("syncing local parquet: %w", err)
	}

	fi, err := tmp.Stat()
	if err != nil {
		return nil, "", fmt.Errorf("stat local parquet: %w", err)
	}
	if fi.Size() == 0 {
		return nil, "", fmt.Errorf("local parquet for %s is empty after stream", srcPath)
	}

	mmapData, err := syscall.Mmap(int(tmp.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, "", fmt.Errorf("mmap local parquet %s: %w", localPath, err)
	}
	// Close the fd — the mmap keeps the inode alive until Munmap.
	if err := tmp.Close(); err != nil {
		_ = syscall.Munmap(mmapData)
		return nil, "", fmt.Errorf("closing local parquet fd: %w", err)
	}
	cleanup = false
	return mmapData, localPath, nil
}

// openShuffleFromLocalFile mmaps an existing local file (typically owned by
// LocalStageCache) directly, without staging a copy. The chunkReader walks
// the mmap'd region; the cache will unlink the file on CleanupQuery, so the
// consumer must NOT delete it — releaseCurrentFile only munmaps.
func (s *cachedFileStreamSource) openShuffleFromLocalFile(localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening cached local file %s: %w", localPath, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat cached local file %s: %w", localPath, err)
	}
	if fi.Size() == 0 {
		return fmt.Errorf("cached local file %s is empty", localPath)
	}
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap cached local file %s: %w", localPath, err)
	}
	r, err := newShuffleChunkReader(mmapData)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		return fmt.Errorf("opening cached shuffle file %s: %w", localPath, err)
	}
	s.chunkReader = r
	s.mmapData = mmapData
	s.mmapRegion = registerMmap(mmapData) // nil when relief disabled
	// LocalStageCache owns this file — clear localPath EXPLICITLY rather
	// than assuming it is empty: a stale path from a previous file would
	// make releaseCurrentFile delete the wrong file later.
	s.localPath = ""
	return nil
}

// openShuffleInMemory is the legacy fallback for environments without a
// spill directory (mainly tests). Downloads the full payload, decompresses
// in memory, and hands the resulting byte slice to a chunk reader. Bounded
// by the test data sizes.
func (s *cachedFileStreamSource) openShuffleInMemory(srcPath string, magic []byte, rc io.Reader, compressed bool) error {
	rest, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading shuffle body for %s: %w", srcPath, err)
	}
	data := append(magic[:], rest...)
	if compressed {
		decoded, err := DecompressShuffleData(data)
		if err != nil {
			return fmt.Errorf("decompressing %s: %w", srcPath, err)
		}
		data = decoded
	}
	r, err := newShuffleChunkReader(data)
	if err != nil {
		return fmt.Errorf("opening shuffle file %s: %w", srcPath, err)
	}
	s.chunkReader = r
	return nil
}

// localCachePath returns a deterministic-but-unique local spill path for a
// given remote object key. Used for debugging.
func localCachePath(spillDir, key string) string {
	return filepath.Join(spillDir, "build-cache-"+filepath.Base(key))
}

var _ = localCachePath // currently unused, kept for future cache reuse
