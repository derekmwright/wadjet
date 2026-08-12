package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

	// projectionSkipWarned dedupes the once-per-source WARN emitted when
	// projectColumns names a column absent from the file schema (the
	// all-or-nothing guard reverting to full width).
	projectionSkipWarned bool

	// Row-group sharding. When shardCount > 1, parquet reads only row groups
	// [idx*N/count, (idx+1)*N/count). Only applies to parquet inputs (.wshf
	// shuffle outputs are unaffected — those are already partitioned by the
	// upstream stage). shardCount = 0 or 1 means whole file.
	shardIdx   int
	shardCount int

	fileIdx int

	// Active WSHF chunk reader for the current file (nil if current file is
	// Parquet or no file is open). Either an mmap-backed shuffleChunkReader
	// walking mmapData below, or a streamingShuffleReader decoding the
	// transport body directly (--streaming-shuffle-read).
	chunkReader shuffleBatchIter
	mmapData    []byte        // mmap'd view of the current local cache file (nil if Parquet)
	mmapRegion  *mmapRegion   // Phase-5 relief handle for mmapData; nil when relief is disabled
	toucher     *rangeToucher // touch-ahead worker over the scan mmap; stopped before munmap
	file        *os.File      // fd backing a pread-mode parquet reader; closed after the iterator quiesces
	localPath   string        // path the source is responsible for unlinking; "" when the file is owned by LocalStageCache or in-memory

	// walkDropMark is the byte offset below which the current owned WSHF
	// mmap's pages have already been MADV_DONTNEED'd behind the walk
	// (single-pass drop-behind). Reset with each file.
	walkDropMark int

	// Streaming-decode state (docs/design/exchange-streaming-consumption.md
	// §3 D1). When the current file is being stream-decoded, streamReader
	// == chunkReader and streamKey names the object so a mid-stream
	// transport failure can fall back to the durable copy, skipping the
	// batches already delivered (no double delivery). The read-staged
	// local path (shuffle_pread.go) also parks its reader here for the
	// release obligation, but leaves streamKey "" — local read errors are
	// terminal, not transport failures.
	streamReader *streamingShuffleReader
	streamKey    string

	// Streaming row-group iterator for the current Parquet file. Yields one
	// decoded RecordBatch per Next() call; the previous batch is GC-eligible
	// once the consumer Releases it. Serial (RowGroupIter) bounds
	// Parquet-side live memory to one decoded row group at a time per
	// source; decode-ahead (DecodeAheadIter, --scan-decode-ahead) to its
	// byte window. Close MUST fully quiesce decode before returning —
	// releaseCurrentFile munmaps the bytes the iterator reads.
	parquetIter parquetGroupIter
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

	// Cross-file decode continuation (--scan-decode-ahead, memo S2): the
	// next parquet file, pre-opened once the current file's row groups
	// are all assigned, so its head decodes while the current tail
	// delivers. decodeWin is the one byte window every file of this
	// source draws from. preOpenSkip is (fileIdx+1) of an index whose
	// prefetch resolved unusable — stop polling it; the tiered
	// openNextFile path is authoritative there (0 = none).
	nextParquet *pendingParquet
	decodeWin   *scan.DecodeWindow
	preOpenSkip int
	preOpens    int // files pre-opened by the continuation (tests + DEBUG log)
}

// shuffleBatchIter is the common face of the mmap-backed and streaming
// WSHF chunk readers.
type shuffleBatchIter interface {
	Next() (*batch.RecordBatch, error)
}

// parquetGroupIter is the common face of the serial (scan.RowGroupIter)
// and decode-ahead (scan.DecodeAheadIter) parquet row-group iterators.
type parquetGroupIter interface {
	Next() (*batch.RecordBatch, error)
	SetDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter)
	Close() error
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

// AddDynamicFilters appends iterator-level dynamic filters mid-scan — the
// attach-on-arrival delivery for the row-group pruning layer. The union set
// is forwarded to the currently open parquet iterator (both iterator kinds
// consult filters per group, so every not-yet-read/assigned group prunes)
// and to a pre-opened next file; files opened later pick the set up from
// the source fields as usual. Must be called from the producer goroutine
// (the Next loop) — the same thread that opens files and iterators.
func (s *cachedFileStreamSource) AddDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	if s == nil || (len(ranges) == 0 && len(blooms) == 0) {
		return
	}
	s.dynamicRanges = append(s.dynamicRanges, ranges...)
	s.bloomFilters = append(s.bloomFilters, blooms...)
	if s.parquetIter != nil {
		s.parquetIter.SetDynamicFilters(s.dynamicRanges, s.bloomFilters)
	}
	if s.nextParquet != nil && s.nextParquet.iter != nil {
		s.nextParquet.iter.SetDynamicFilters(s.dynamicRanges, s.bloomFilters)
	}
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
			if err != nil && s.streamKey != "" {
				// Mid-stream transport failure on a streaming decode: the
				// durable copy is authoritative — reopen it staged and skip
				// the batches this file already delivered.
				if fbErr := s.fallbackFromStream(ctx, err); fbErr != nil {
					return nil, fbErr
				}
				continue
			}
			if err != nil {
				return nil, err
			}
			if b != nil {
				s.dropBehindWalk()
				return b, nil
			}
			// Current file exhausted — release its mmap and remove the local
			// copy before moving on. The chunkReader's batches have already
			// copied their column data into RecordBatch arrays, so dropping
			// the mmap region here is safe.
			s.releaseCurrentFile()
		}

		// Yield next decoded row group from the active Parquet iterator.
		// Serial: only one row group's worth of memory is live at a time
		// per source. Decode-ahead: bounded by the source's DecodeWindow.
		if s.parquetIter != nil {
			b, err := s.parquetIter.Next()
			if err != nil {
				return nil, err
			}
			if b != nil {
				s.tryPreOpenNextParquet()
				return b, nil
			}
			// Cross-file continuation: the next file was pre-opened and
			// its head is already decoding — swap it in without touching
			// the tiered open path. Release order matters exactly as in
			// the serial branch below: iterator (quiesces decode), then
			// reader ref, then mmap.
			if s.nextParquet != nil {
				s.releaseParquetIter()
				s.releaseCurrentFile()
				s.installParquet(s.nextParquet)
				s.nextParquet = nil
				continue
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
	// Pre-opened next file first: its iterator joins in-flight decodes
	// before the mmap drops (same invariant as the current file below).
	if s.nextParquet != nil {
		s.nextParquet.release(s.executor.logger)
		s.nextParquet = nil
	}
	// Stop the download-ahead workers and reap any temp files they
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
	// After iter.Close (the advising decode workers are joined), before
	// releaseCurrentFile's munmap — a live toucher must never outlast the
	// mapping it walks.
	s.toucher.stop()
	s.toucher = nil
	s.parquetReader = nil
}

// releaseCurrentFile munmaps and deletes the local cache file backing the
// current chunkReader. Safe to call multiple times.
func (s *cachedFileStreamSource) releaseCurrentFile() {
	s.chunkReader = nil
	s.walkDropMark = 0
	if s.streamReader != nil {
		_ = s.streamReader.Close()
		s.streamReader = nil
	}
	s.streamKey = ""
	if s.mmapData != nil {
		// Unregister from the relief registry BEFORE munmapping so relieveMmap
		// (which holds the registry lock during its MADV syscalls) can never
		// advise a region that is about to be unmapped.
		unregisterMmap(s.mmapRegion)
		s.mmapRegion = nil
		_ = syscall.Munmap(s.mmapData)
		s.mmapData = nil
	}
	// Pread-mode fd: callers release the iterator first (decode workers
	// joined), so no staging read can hit a closed fd.
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
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
			len(s.files) > 1 && hasPrefetchableFiles(s.files, s.executor.streamingShuffleRead) {
			s.prefetch = startFilePrefetcher(ctx, s)
		}
	}
	if s.prefetch != nil {
		if res := s.prefetch.take(ctx, idx); !res.skipped && res.err == nil {
			var err error
			if strings.HasSuffix(filePath, ".wshf") {
				err = s.openPrefetchedShuffle(res.localPath, filePath)
			} else {
				err = s.openPrefetchedParquet(res.localPath, filePath)
			}
			if err == nil {
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
			if n, err := s.openShuffleFromLocalFile(cached); err == nil {
				s.executor.shuffleIO.localFiles.Add(1)
				s.executor.shuffleIO.localBytes.Add(n)
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
					if err := s.openShuffleInMemory(filePath, magic[:], bytes.NewReader(data[4:]), wshc); err != nil {
						return err
					}
					s.executor.shuffleIO.kvFiles.Add(1)
					s.executor.shuffleIO.kvBytes.Add(int64(kvDataLen))
					return nil
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
				s.executor.shuffleIO.peerFiles.Add(1)
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
	rcOwnedByReader := false
	defer func() {
		if !rcOwnedByReader {
			rc.Close()
		}
	}()

	// Read enough header bytes to detect the format. Both WSHF and WSHC
	// have a 4-byte magic at offset 0.
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("reading magic from %s: %w", filePath, err)
	}

	wshf := magic == shuffleMagic
	wshc := magic == compressedMagic

	if wshf || wshc {
		// Per-tier ledger: whatever consumes rc past this point drains the
		// durable-S3 copy, so the counting wrapper records exactly the wire
		// bytes moved (WSHC stays compressed through it). The 4 magic bytes
		// were already consumed off the raw stream above — credit them here.
		crc := &countingReadCloser{rc: rc, n: &s.executor.shuffleIO.s3Bytes}
		crc.n.Add(int64(len(magic)))
		// Streaming decode (memo §3 D1): decode chunks off the S3 body as
		// they arrive instead of staging the file. A failed streaming open
		// has partially consumed rc — re-resolve the durable copy for the
		// staged path rather than reusing the stream.
		if s.executor.streamingShuffleRead {
			if err := s.openShuffleStreaming(ctx, filePath, crc, wshc); err == nil {
				s.executor.shuffleIO.s3Files.Add(1)
				rcOwnedByReader = true
				return nil
			} else if s.executor.logger != nil {
				s.executor.logger.Debug("streaming shuffle open failed; restaging from durable copy",
					"key", filePath, "error", err)
			}
			return s.openShuffleStagedFromStore(ctx, filePath)
		}
		if err := s.openShuffleFile(ctx, filePath, magic[:], crc, wshc); err != nil {
			return err
		}
		s.executor.shuffleIO.s3Files.Add(1)
		return nil
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
	// Just-written temp: always the zero-copy mmap (pages resident; the
	// SF100 pair priced pread staging of this hot path at +15.9% cold —
	// see the design doc's refinement section).
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
	// buildParquetFromLocal unwinds mmap+file itself if the payload isn't
	// valid parquet (e.g. a legacy shuffle object under a .parquet key).
	// justWritten: the prefetcher streamed this temp moments ago.
	p, err := s.buildParquetFromLocal(localPath, filePath, localPath, true)
	if err != nil {
		return err
	}
	s.installParquet(p)
	return nil
}

// progressReadCloser reports each successful Read to the per-task
// ProgressReporter — the streaming-decode analog of progressWriter, so
// the coordinator's stale-worker reap sees I/O progress during long
// streamed reads.
type progressReadCloser struct {
	rc  io.ReadCloser
	rep exec.ProgressReporter
}

func (p *progressReadCloser) Read(b []byte) (int, error) {
	n, err := p.rc.Read(b)
	if n > 0 {
		p.rep.AddBytes(int64(n))
	}
	return n, err
}

func (p *progressReadCloser) Close() error { return p.rc.Close() }

// countingReadCloser adds every byte read to a tier counter of the per-tier
// shuffle-read ledger (Executor.shuffleIO). Wire-level: wraps the transfer
// stream before any decompression, so WSHC counts compressed bytes.
type countingReadCloser struct {
	rc io.ReadCloser
	n  *atomic.Int64
}

func (c *countingReadCloser) Read(b []byte) (int, error) {
	n, err := c.rc.Read(b)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error { return c.rc.Close() }

// openShuffleStreaming decodes a WSHF/WSHC body directly from rc
// (--streaming-shuffle-read, memo §3 D1). rc must be positioned after the
// outer 4-byte magic. On success the reader owns rc; on error the caller
// still owns it — but note rc has been partially consumed either way, so
// a failed streaming open must re-resolve the object, not reuse rc.
func (s *cachedFileStreamSource) openShuffleStreaming(ctx context.Context, srcPath string, rc io.ReadCloser, compressed bool) error {
	if rep := exec.ProgressReporterFromContext(ctx); rep != nil {
		rc = &progressReadCloser{rc: rc, rep: rep}
	}
	r, err := newStreamingShuffleReader(rc, compressed)
	if err != nil {
		return err
	}
	s.chunkReader = r
	s.streamReader = r
	s.streamKey = srcPath
	s.executor.shuffleStreamReads.Add(1)
	return nil
}

// openShuffleStagedFromStore re-resolves srcPath from the durable store
// and opens it via the staged NVMe+mmap path — the authoritative recovery
// route when a streaming decode could not start or died mid-file. Waits
// out an in-flight async upload exactly like the primary S3 branch.
func (s *cachedFileStreamSource) openShuffleStagedFromStore(ctx context.Context, srcPath string) error {
	rc, _, err := s.executor.store.Get(ctx, s.bucket, srcPath)
	if err != nil {
		if peers := s.executor.peers; peers != nil && errors.Is(err, objstore.ErrNotFound) {
			if addr, token := peers.hintFor(srcPath); addr != "" || token != "" {
				waited, waitErr := s.awaitDurableObject(ctx, srcPath)
				if waitErr != nil {
					return &missingInputError{key: srcPath, cause: waitErr}
				}
				rc = waited
				err = nil
			}
		}
		if err != nil {
			return err
		}
	}
	defer rc.Close()
	crc := &countingReadCloser{rc: rc, n: &s.executor.shuffleIO.s3Bytes}
	var magic [4]byte
	if _, err := io.ReadFull(crc, magic[:]); err != nil {
		return fmt.Errorf("reading magic from durable copy of %s: %w", srcPath, err)
	}
	wshf := magic == shuffleMagic
	wshc := magic == compressedMagic
	if !wshf && !wshc {
		return fmt.Errorf("durable copy of %s is not a shuffle payload (magic %q)", srcPath, magic[:])
	}
	if err := s.openShuffleFile(ctx, srcPath, magic[:], crc, wshc); err != nil {
		return err
	}
	s.executor.shuffleIO.s3Files.Add(1)
	return nil
}

// fallbackFromStream recovers from a mid-stream failure of the current
// streaming decode: reopen the durable copy staged, then skip the batches
// the stream already delivered — the file is immutable and completed, so
// the batch sequence of a full re-read is identical (memo §3 D1's
// no-double-delivery constraint).
func (s *cachedFileStreamSource) fallbackFromStream(ctx context.Context, cause error) error {
	key := s.streamKey
	delivered := 0
	if s.streamReader != nil {
		delivered = s.streamReader.Delivered()
		_ = s.streamReader.Close()
		s.streamReader = nil
	}
	s.chunkReader = nil
	s.streamKey = ""
	s.executor.shuffleStreamFallbacks.Add(1)
	if s.executor.logger != nil {
		s.executor.logger.Debug("streaming shuffle decode fell back to staged read",
			"key", key, "delivered", delivered, "error", cause)
	}
	if err := s.openShuffleStagedFromStore(ctx, key); err != nil {
		return fmt.Errorf("streaming read of %s failed (%v); staged fallback failed: %w", key, cause, err)
	}
	for i := 0; i < delivered; i++ {
		b, err := s.chunkReader.Next()
		if err != nil {
			return fmt.Errorf("staged fallback for %s: re-reading batch %d/%d: %w", key, i, delivered, err)
		}
		if b == nil {
			return fmt.Errorf("staged fallback for %s: durable copy has %d batches but stream delivered %d", key, i, delivered)
		}
	}
	s.executor.shuffleStreamSkipResumes.Add(int64(delivered))
	return nil
}

// openParquetFromBaseCache opens a parquet file the process-wide
// base-table cache holds on local disk: mmap it in place and hand the
// region to the shared open tail. localPath stays empty throughout — the
// cache owns the file, so neither releaseCurrentFile nor the open-failure
// unwind may unlink it.
func (s *cachedFileStreamSource) openParquetFromBaseCache(cachePath, filePath string) error {
	// ownedPath "" — the cache owns the file; only the reader state
	// unwinds on a bad payload. Not just-written: cache entries may be
	// arbitrarily cold — this is the pread (drift-killing) tier.
	p, err := s.buildParquetFromLocal(cachePath, filePath, "", false)
	if err != nil {
		return err
	}
	s.installParquet(p)
	return nil
}

// openPrefetchedShuffle opens a shuffle file the prefetcher already landed
// on spill as plain WSHF: mmap it and build the standard chunk reader. On
// any failure the temp is removed and the caller falls back to the tiered
// path (memo §3 D2).
func (s *cachedFileStreamSource) openPrefetchedShuffle(localPath, filePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("opening prefetched shuffle file %s: %w", localPath, err)
	}
	if shufflePreadEnabled {
		fi, err := f.Stat()
		if err != nil || fi.Size() == 0 {
			f.Close()
			_ = os.Remove(localPath)
			return fmt.Errorf("prefetched shuffle file %s unusable (err=%v)", localPath, err)
		}
		if err := s.openShuffleFromFileStreaming(f, fi.Size(), localPath); err != nil {
			f.Close()
			_ = os.Remove(localPath)
			return fmt.Errorf("opening prefetched shuffle file %s: %w", filePath, err)
		}
		return nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		_ = os.Remove(localPath)
		return fmt.Errorf("prefetched shuffle file %s unusable (err=%v)", localPath, err)
	}
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		_ = os.Remove(localPath)
		return fmt.Errorf("mmap prefetched shuffle file %s: %w", localPath, err)
	}
	r, err := newShuffleChunkReader(mmapData)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		_ = os.Remove(localPath)
		return fmt.Errorf("opening prefetched shuffle file %s: %w", filePath, err)
	}
	adviseSequential(mmapData)
	s.chunkReader = r
	s.mmapData = mmapData
	s.mmapRegion = registerMmap(mmapData)
	s.localPath = localPath
	return nil
}

// finishOpenParquet is the shared tail of the parquet open path: build the
// reader over data, transfer mmap/file ownership to the source, apply the
// column projection guard, and set up the row-group iterator (or the
// eager fallback for Array/Map schemas). mmapData/localPath may be
// nil/empty for the in-memory path.
// pendingParquet is a fully-opened parquet file (reader + iterator +
// mmap/file ownership) not yet installed as the source's current file.
// The cross-file continuation holds at most one while the current file
// drains, so its head decodes into the shared window early.
type pendingParquet struct {
	iter            parquetGroupIter
	reader          *parquet.Reader
	mmapData        []byte
	mmapRegion      *mmapRegion
	file            *os.File // pread mode: fd the reader stages chunks from; nil on mmap/in-memory paths
	localPath       string
	toucher         *rangeToucher
	fallbackBatches []*batch.RecordBatch
}

// release drops a never-installed pending file: quiesce decode first
// (iter.Close joins in-flight workers), then the reader ref, then the
// mmap — same order Close documents for the current file.
func (p *pendingParquet) release(logger *slog.Logger) {
	if p == nil {
		return
	}
	if p.iter != nil {
		_ = p.iter.Close()
		p.iter = nil
	}
	p.toucher.stop() // after iter.Close (enqueuers joined), before munmap
	p.toucher = nil
	p.reader = nil
	if p.mmapData != nil {
		unregisterMmap(p.mmapRegion)
		p.mmapRegion = nil
		_ = syscall.Munmap(p.mmapData)
		p.mmapData = nil
	}
	if p.file != nil { // after iter.Close: no staging read may hit a closed fd
		_ = p.file.Close()
		p.file = nil
	}
	if p.localPath != "" {
		if rmErr := os.Remove(p.localPath); rmErr != nil && !os.IsNotExist(rmErr) && logger != nil {
			logger.Warn("pending parquet cleanup failed", "path", p.localPath, "error", rmErr)
		}
		p.localPath = ""
	}
}

// installParquet makes a pending file the source's current file. The
// caller must have released the previous current file already.
func (s *cachedFileStreamSource) installParquet(p *pendingParquet) {
	s.mmapData = p.mmapData
	s.mmapRegion = p.mmapRegion
	s.file = p.file
	s.localPath = p.localPath
	s.parquetIter = p.iter
	s.parquetReader = p.reader
	s.toucher = p.toucher
	s.fallbackBatches = p.fallbackBatches
	s.fallbackBatchIdx = 0
}

func (s *cachedFileStreamSource) finishOpenParquet(filePath string, data, mmapData []byte, localPath string) error {
	p, err := s.buildParquetState(filePath, data, mmapData, localPath)
	if err != nil {
		return err
	}
	s.installParquet(p)
	return nil
}

// buildParquetState opens a parquet payload into a pendingParquet without
// touching the source's current-file fields. On error the mmap/local file
// are unwound (empty localPath = caller-owned file, not unlinked).
func (s *cachedFileStreamSource) buildParquetState(filePath string, data, mmapData []byte, localPath string) (*pendingParquet, error) {
	reader, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		if mmapData != nil {
			_ = syscall.Munmap(mmapData)
			_ = os.Remove(localPath)
		}
		return nil, fmt.Errorf("opening parquet file %s: %w", filePath, err)
	}
	// nil/empty mmap/localPath are fine for the in-memory fallback.
	p := &pendingParquet{
		reader:     reader,
		mmapData:   mmapData,
		mmapRegion: registerMmap(mmapData), // nil when relief disabled
		localPath:  localPath,
	}
	return s.finishParquetState(p, filePath)
}

// buildParquetStateFromFile is buildParquetState for the pread path
// (docs/design/scan-pread-reads.md): the reader stages column chunks
// from f on demand — no mmap, no toucher, no whole-file buffer. f's
// ownership transfers to the returned pendingParquet (closed on
// release after the iterator quiesces); ownedPath is "" when the file
// belongs to a cache and must not be unlinked.
func (s *cachedFileStreamSource) buildParquetStateFromFile(filePath string, f *os.File, size int64, ownedPath string) (*pendingParquet, error) {
	reader, err := parquet.NewReaderAt(f, size)
	if err != nil {
		f.Close()
		if ownedPath != "" {
			_ = os.Remove(ownedPath)
		}
		return nil, fmt.Errorf("opening parquet file %s: %w", filePath, err)
	}
	p := &pendingParquet{
		reader:    reader,
		file:      f,
		localPath: ownedPath,
	}
	return s.finishParquetState(p, filePath)
}

// finishParquetState is the shared open tail over a constructed reader:
// column projection guard, iterator setup (decode-ahead or serial), and
// the I/O-ahead wiring appropriate to the backing (fadvise on a pread
// fd, madvise + touch-ahead on an mmap). On error p is released.
func (s *cachedFileStreamSource) finishParquetState(p *pendingParquet, filePath string) (*pendingParquet, error) {
	reader := p.reader
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
		var missing []string
		for _, name := range s.projectColumns {
			if !schemaSet[name] {
				allPresent = false
				missing = append(missing, name)
			}
		}
		if !allPresent && !s.projectionSkipWarned {
			// Once per source, not per file: a 600-file scan reverting to
			// full width is one event. Legitimate for derived columns
			// (pre-project computes them); polluted planner lists were the
			// silent 5-6x shuffle-byte amplifier of memo §2 A1 — this line
			// is how the next regression of that class gets seen.
			s.projectionSkipWarned = true
			s.executor.logger.Warn("parquet projection skipped: requested columns missing from schema (reading full width)",
				"query_id", s.queryID, "missing", strings.Join(missing, ","),
				"requested", len(s.projectColumns))
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
			p.release(s.executor.logger)
			return nil, fmt.Errorf("reading parquet file %s (fallback): %w", filePath, err)
		}
		p.fallbackBatches = batches
		return p, nil
	}
	var iter parquetGroupIter
	if s.executor.scanDecodeAhead {
		if s.decodeWin == nil {
			// One window per source, shared across every file it opens —
			// the cross-file continuation draws the next file's head from
			// the same budget as the current file's tail. Window bytes are
			// charged to the worker's shared memory pool (memo §9): the
			// configured window is only the ceiling, actual occupancy is
			// bounded by pool headroom, collapsing to the cursor-only
			// serial floor when concurrent operators hold the budget. The
			// held batches are real heap the Go-pressure hook cannot see
			// displacing page cache — they must compete on the same ledger
			// as everything else. NOTE: nil-check the tracker here — a nil
			// *memory.Tracker wrapped in the interface reads as non-nil.
			var ledger scan.WindowLedger
			if s.executor.sharedTracker != nil {
				ledger = s.executor.sharedTracker
			}
			s.decodeWin = scan.NewDecodeWindowWithLedger(s.executor.scanDecodeAheadBytes, ledger)
		}
		// CPU is budgeted per ROW GROUP inside the iterator (one token
		// held only for the duration of one decode, cursor group exempt)
		// — never per source lifetime: the 2026-07-16 SF100 pair showed
		// window-stalled decode workers hoarding source-held tokens and
		// starving concurrent join fragments' morsel width. A nil pool
		// (morsels disabled → serial consumers) leaves decode unbudgeted
		// at the default width. NOTE: the nil check must happen here — a
		// nil *cpuTokens wrapped in the interface would read as non-nil
		// and silently serialize decode.
		// Admission pressure = Go-heap tide gauge OR kernel page-cache
		// thrash (memo §9: refault-rate sensor — the displacement channel
		// the heap hook cannot see; the capped repro measured decode-ahead
		// width as worthless-to-harmful precisely when refaults run hot).
		opts := scan.DecodeAheadOpts{Window: s.decodeWin, Pressure: scanDecodeAheadPressure,
			PressureStrict: scanDecodeAheadStrictPressure()}
		if s.executor.cpuTokens != nil {
			opts.Tokens = s.executor.cpuTokens
		}
		// Row-group I/O-ahead: on the pread path, FADV_WILLNEED on the fd
		// starts kernel readahead so the staging pread copies from page
		// cache; no toucher — there are no faults to pre-take. On the mmap
		// path the WILLNEED madvise plus the touch-ahead worker guarantee
		// residency before a token-holding decode arrives
		// (rowgroup_touch.go); the toucher's stop happens after iter.Close
		// joins the advising decode workers and before munmap. The
		// in-memory fallback decodes from heap and has nothing to fault.
		if rowGroupReadaheadEnabled {
			if p.file != nil {
				if adv := fdWillNeedAdviser(p.file); adv != nil {
					opts.Advise = adv
				}
			} else if len(p.mmapData) > 0 {
				advise := willNeedAdviser(p.mmapData)
				if rowGroupTouchEnabled {
					p.toucher = newRangeToucher(p.mmapData)
					toucher := p.toucher
					opts.Advise = func(off, n int64) {
						advise(off, n)
						toucher.enqueue(off, n)
					}
				} else {
					opts.Advise = advise
				}
			}
		}
		da, err := scan.OpenDecodeAheadIter(reader, projCols, nil, shardIdx, shardCount, opts)
		if err != nil {
			p.release(s.executor.logger)
			return nil, fmt.Errorf("opening decode-ahead iterator for %s: %w", filePath, err)
		}
		iter = &decodeAheadStatsIter{DecodeAheadIter: da, executor: s.executor, queryID: s.queryID}
	} else {
		rgIter, err := scan.OpenRowGroupIter(reader, projCols, nil, shardIdx, shardCount)
		if err != nil {
			p.release(s.executor.logger)
			return nil, fmt.Errorf("opening row-group iterator for %s: %w", filePath, err)
		}
		iter = rgIter
	}
	if len(s.dynamicRanges) > 0 || len(s.bloomFilters) > 0 {
		iter.SetDynamicFilters(s.dynamicRanges, s.bloomFilters)
	}
	p.iter = iter
	return p, nil
}

// tryPreOpenNextParquet pre-opens the next file for the cross-file decode
// continuation. Called after every delivered parquet batch; all guards
// are cheap. Only the non-blocking local tiers pre-open (base-table cache
// hit, completed prefetch download) — anything else falls back to the
// tiered openNextFile at the boundary, keeping this strictly best-effort:
// it can never change results and never blocks the consumer.
func (s *cachedFileStreamSource) tryPreOpenNextParquet() {
	if !s.executor.scanDecodeAhead || s.nextParquet != nil || s.fileIdx >= len(s.files) {
		return
	}
	idx := s.fileIdx
	if s.preOpenSkip == idx+1 {
		return
	}
	filePath := s.files[idx]
	if !strings.HasSuffix(filePath, ".parquet") {
		return
	}
	da, ok := s.parquetIter.(*decodeAheadStatsIter)
	if !ok || !da.AssignmentDrained() {
		return
	}

	// Base-table cache tier: map lookup + local open, never blocks.
	if lps, ok := s.executor.store.(objstore.LocalPathStore); ok {
		if cached, ok := lps.CachedLocalPath(s.bucket, filePath); ok {
			if p, err := s.buildParquetFromLocal(cached, filePath, "", false); err == nil {
				s.adoptPending(p, idx)
				return
			} else if s.executor.logger != nil {
				s.executor.logger.Debug("decode-ahead pre-open fell through",
					"key", filePath, "path", cached, "error", err)
			}
		}
	}
	// Prefetch tier: consume the download only if it already finished.
	if s.prefetch != nil {
		res, resolved := s.prefetch.tryTake(idx)
		if !resolved {
			return // still downloading — poll again on the next batch
		}
		if res == nil {
			s.preOpenSkip = idx + 1 // skipped/err — openNextFile owns it
			return
		}
		if p, err := s.buildParquetFromLocal(res.localPath, filePath, res.localPath, true); err == nil {
			s.adoptPending(p, idx)
			return
		}
		// buildParquetFromLocal removed the temp; the tiered path
		// re-resolves this index from the durable copy.
		s.preOpenSkip = idx + 1
	}
}

// buildParquetFromLocal opens a local parquet file and builds its pending
// state. justWritten selects the backing (docs/design/scan-pread-reads.md
// §refinement): files this process just streamed to disk are page-cache-
// resident, so the mmap reads them zero-copy and its faults are minor —
// that path was never implicated in the drift, and the SF100 pair priced
// pread's alloc+memcpy on it at +15.9% cold. Cache-hit files are the
// opposite: potentially cold on NVMe, exactly the fault class whose STW
// interaction drives the R2 drift — those decode from pread-staged
// buffers (WADJET_SCAN_PREAD=0 forces mmap everywhere). ownedPath is ""
// when the file belongs to a cache (must not be unlinked) and the file's
// own path when ownership transfers to us.
func (s *cachedFileStreamSource) buildParquetFromLocal(localPath, filePath, ownedPath string, justWritten bool) (*pendingParquet, error) {
	f, err := os.Open(localPath)
	if err != nil {
		if ownedPath != "" {
			_ = os.Remove(ownedPath)
		}
		return nil, fmt.Errorf("opening local parquet %s: %w", localPath, err)
	}
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		f.Close()
		if ownedPath != "" {
			_ = os.Remove(ownedPath)
		}
		return nil, fmt.Errorf("local parquet %s unusable (err=%v)", localPath, err)
	}
	if scanPreadEnabled && !justWritten {
		fadviseSequential(f, fi.Size())
		// buildParquetStateFromFile owns f (and unlinks ownedPath when
		// set) on a bad payload.
		return s.buildParquetStateFromFile(filePath, f, fi.Size(), ownedPath)
	}
	defer f.Close()
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		if ownedPath != "" {
			_ = os.Remove(ownedPath)
		}
		return nil, fmt.Errorf("mmap local parquet %s: %w", localPath, err)
	}
	adviseSequential(mmapData)
	// buildParquetState unwinds mmap (and unlinks ownedPath when set) on a
	// bad payload.
	return s.buildParquetState(filePath, mmapData, mmapData, ownedPath)
}

// adoptPending records a pre-opened next file and starts its decode
// workers so the head decodes into the shared window immediately.
func (s *cachedFileStreamSource) adoptPending(p *pendingParquet, idx int) {
	s.nextParquet = p
	s.fileIdx = idx + 1
	s.preOpens++
	if da, ok := p.iter.(*decodeAheadStatsIter); ok {
		da.Start()
	}
}

// decodeAheadStatsIter folds a DecodeAheadIter's engagement counters into
// the executor-wide markers — and the per-query attribution map — when
// the iterator closes (a source opens one iterator per parquet file;
// per-file granularity is noise).
type decodeAheadStatsIter struct {
	*scan.DecodeAheadIter
	executor *Executor
	queryID  string
	folded   bool
}

func (d *decodeAheadStatsIter) Close() error {
	if !d.folded {
		d.folded = true
		groups, windowFulls, pressureStalls, tokenStalls, ledgerStalls := d.Stats()
		windowFullNs, pressureNs, tokenNs, ledgerNs := d.StallDurations()
		decodeNs, decodeBytes := d.DecodeSpans()
		prunedBloom, prunedRange, _ := d.PruneStats()
		pruned := int64(prunedBloom + prunedRange)
		d.executor.scanDecodeAheadPrunedGroups.Add(pruned)
		d.executor.scanDecodeAheadGroups.Add(groups)
		d.executor.scanDecodeAheadWindowFulls.Add(windowFulls)
		d.executor.scanDecodeAheadPressureStalls.Add(pressureStalls)
		d.executor.scanDecodeAheadTokenStalls.Add(tokenStalls)
		d.executor.scanDecodeAheadLedgerStalls.Add(ledgerStalls)
		d.executor.scanDecodeAheadWindowFullNs.Add(windowFullNs)
		d.executor.scanDecodeAheadPressureNs.Add(pressureNs)
		d.executor.scanDecodeAheadTokenNs.Add(tokenNs)
		d.executor.scanDecodeAheadLedgerNs.Add(ledgerNs)
		d.executor.scanDecodeAheadDecodeNs.Add(decodeNs)
		d.executor.scanDecodeAheadDecodeBytes.Add(decodeBytes)
		d.executor.foldScanDecodeAheadQueryStats(d.queryID,
			groups, windowFulls, pressureStalls, tokenStalls, ledgerStalls,
			windowFullNs, pressureNs, tokenNs, ledgerNs, decodeNs, decodeBytes, pruned)
	}
	return d.DecodeAheadIter.Close()
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

	// Read-staged consumption (shuffle_pread.go): rewind the staged temp
	// and decode it with sequential read() calls — no mmap, no fault class
	// in decode goroutines. On error the deferred cleanup still owns tmp.
	if shufflePreadEnabled {
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewinding local cache %s: %w", localPath, err)
		}
		if err := s.openShuffleFromFileStreaming(tmp, fi.Size(), localPath); err != nil {
			return fmt.Errorf("opening shuffle file %s: %w", srcPath, err)
		}
		cleanupLocal = false
		return nil
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
	adviseSequential(mmapData)

	cleanupLocal = false
	s.chunkReader = r
	s.mmapData = mmapData
	s.mmapRegion = registerMmap(mmapData) // nil when relief disabled
	s.localPath = localPath
	return nil
}

// streamParquetToLocalFile streams a parquet body (with `magic`
// prepended) from rc to a temp file on the worker's NVMe spill dir and
// returns the still-open file positioned for ReadAt. The caller takes
// ownership of both the fd and the path. Shared tail of the two
// openNextFile parquet stagings (pread reader / mmap).
func (s *cachedFileStreamSource) streamParquetToLocalFile(ctx context.Context, srcPath string, magic []byte, rc io.Reader) (*os.File, string, int64, error) {
	spillDir := s.executor.spillDir
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return nil, "", 0, fmt.Errorf("creating spill dir: %w", err)
	}
	tmp, err := os.CreateTemp(spillDir, "parquet-stream-*.parquet")
	if err != nil {
		return nil, "", 0, fmt.Errorf("creating local parquet file: %w", err)
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
	// KeepResident: read back by the parquet row-group iterator right
	// after the download — bound dirty pages, keep them resident.
	wf, _ := diskio.NewWriter(tmp, diskio.KeepResident)
	dst := io.Writer(wf)
	if rep != nil {
		dst = &progressWriter{w: wf, rep: rep}
	}
	if _, err := dst.Write(magic); err != nil {
		return nil, "", 0, fmt.Errorf("writing magic to local parquet: %w", err)
	}
	if _, err := io.Copy(dst, rc); err != nil {
		return nil, "", 0, fmt.Errorf("streaming %s to local parquet: %w", srcPath, err)
	}
	if err := tmp.Sync(); err != nil {
		return nil, "", 0, fmt.Errorf("syncing local parquet: %w", err)
	}

	fi, err := tmp.Stat()
	if err != nil {
		return nil, "", 0, fmt.Errorf("stat local parquet: %w", err)
	}
	if fi.Size() == 0 {
		return nil, "", 0, fmt.Errorf("local parquet for %s is empty after stream", srcPath)
	}
	cleanup = false
	return tmp, localPath, fi.Size(), nil
}

// streamParquetToLocalMmap is streamParquetToLocalFile plus an mmap of
// the staged file — the WADJET_SCAN_PREAD=0 kill-switch path. The caller
// takes ownership: on success it must munmap the slice and remove the
// local path.
func (s *cachedFileStreamSource) streamParquetToLocalMmap(ctx context.Context, srcPath string, magic []byte, rc io.Reader) ([]byte, string, error) {
	tmp, localPath, size, err := s.streamParquetToLocalFile(ctx, srcPath, magic, rc)
	if err != nil {
		return nil, "", err
	}
	mmapData, err := syscall.Mmap(int(tmp.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		tmp.Close()
		_ = os.Remove(localPath)
		return nil, "", fmt.Errorf("mmap local parquet %s: %w", localPath, err)
	}
	// Close the fd — the mmap keeps the inode alive until Munmap.
	if err := tmp.Close(); err != nil {
		_ = syscall.Munmap(mmapData)
		_ = os.Remove(localPath)
		return nil, "", fmt.Errorf("closing local parquet fd: %w", err)
	}
	adviseSequential(mmapData)
	return mmapData, localPath, nil
}

// openShuffleFromLocalFile opens an existing local file (typically owned
// by LocalStageCache) directly, without staging a copy — read-staged by
// default, mmap'd under the kill switch. Returns the file size for the
// tier ledger. The cache will unlink the file on CleanupQuery, so the
// consumer must NOT delete it — releaseCurrentFile only drops the reader.
func (s *cachedFileStreamSource) openShuffleFromLocalFile(localPath string) (int64, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return 0, fmt.Errorf("opening cached local file %s: %w", localPath, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return 0, fmt.Errorf("stat cached local file %s: %w", localPath, err)
	}
	if fi.Size() == 0 {
		f.Close()
		return 0, fmt.Errorf("cached local file %s is empty", localPath)
	}
	// ownedPath "" — the cache owns the file: never unlinked here, and no
	// drop-behind (other consumers or a peer fetch may re-read it).
	if shufflePreadEnabled {
		if err := s.openShuffleFromFileStreaming(f, fi.Size(), ""); err != nil {
			f.Close()
			return 0, fmt.Errorf("opening cached shuffle file %s: %w", localPath, err)
		}
		return fi.Size(), nil
	}
	defer f.Close()
	mmapData, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return 0, fmt.Errorf("mmap cached local file %s: %w", localPath, err)
	}
	r, err := newShuffleChunkReader(mmapData)
	if err != nil {
		_ = syscall.Munmap(mmapData)
		return 0, fmt.Errorf("opening cached shuffle file %s: %w", localPath, err)
	}
	adviseSequential(mmapData)
	s.chunkReader = r
	s.mmapData = mmapData
	s.mmapRegion = registerMmap(mmapData) // nil when relief disabled
	// LocalStageCache owns this file — clear localPath EXPLICITLY rather
	// than assuming it is empty: a stale path from a previous file would
	// make releaseCurrentFile delete the wrong file later.
	s.localPath = ""
	return int64(len(mmapData)), nil
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
