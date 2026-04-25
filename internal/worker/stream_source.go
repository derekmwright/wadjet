package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/scan"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

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
// For Parquet files (only used by older shuffle paths) we still buffer
// row-group batches eagerly via scan.ReadFileBatches because they're already
// lazy by row-group in the parquet reader and the file sizes are bounded.
type cachedFileStreamSource struct {
	executor *Executor
	bucket   string
	files    []string

	// projectColumns optionally restricts parquet reads to these columns
	// by name. Applied only when EVERY requested column exists in the
	// file's schema — if any are missing (e.g. a derived column name that
	// the aggregate operator will compute via pre-project), the projection
	// is silently skipped and the full schema is read. This keeps the
	// optimization safe in the presence of expression-based aggregates
	// without needing to teach the source about derivation rules.
	projectColumns []string

	fileIdx int

	// Active WSHF chunk reader for the current file (nil if current file is
	// Parquet or no file is open). The reader walks an mmap'd byte slice
	// owned by mmapData below.
	chunkReader *shuffleChunkReader
	mmapData    []byte // mmap'd view of the current local cache file (nil if Parquet)
	localPath   string // path of the current local cache file on the spill volume

	// Buffered batches for the current Parquet file.
	batches  []*batch.RecordBatch
	batchIdx int
}

func newCachedFileStreamSource(executor *Executor, bucket string, files []string) *cachedFileStreamSource {
	return &cachedFileStreamSource{
		executor: executor,
		bucket:   bucket,
		files:    files,
	}
}

// newCachedFileStreamSourceWithProjection is like newCachedFileStreamSource
// but will attempt to restrict parquet reads to the named columns. The
// projection is applied ONLY when every requested column is present in the
// parquet file's schema — a safety guard against derived/expression names
// that would otherwise over-prune the scan and break downstream operators.
func newCachedFileStreamSourceWithProjection(executor *Executor, bucket string, files []string, projectColumns []string) *cachedFileStreamSource {
	return &cachedFileStreamSource{
		executor:       executor,
		bucket:         bucket,
		files:          files,
		projectColumns: projectColumns,
	}
}

func (s *cachedFileStreamSource) Init(_ context.Context) error { return nil }

func (s *cachedFileStreamSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
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

		// Yield buffered Parquet batches.
		if s.batchIdx < len(s.batches) {
			b := s.batches[s.batchIdx]
			s.batches[s.batchIdx] = nil // release for GC
			s.batchIdx++
			return b, nil
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
	s.releaseCurrentFile()
	for i := s.batchIdx; i < len(s.batches); i++ {
		s.batches[i] = nil
	}
	s.batches = nil
	return nil
}

// releaseCurrentFile munmaps and deletes the local cache file backing the
// current chunkReader. Safe to call multiple times.
func (s *cachedFileStreamSource) releaseCurrentFile() {
	s.chunkReader = nil
	if s.mmapData != nil {
		_ = syscall.Munmap(s.mmapData)
		s.mmapData = nil
	}
	if s.localPath != "" {
		_ = os.Remove(s.localPath)
		s.localPath = ""
	}
}

// openNextFile downloads and opens the next file, setting either chunkReader
// (for WSHF) or batches (for Parquet). Advances fileIdx.
func (s *cachedFileStreamSource) openNextFile(ctx context.Context) error {
	filePath := s.files[s.fileIdx]
	s.fileIdx++

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

	// Try the streaming path first: open a streaming reader from S3, peek
	// at the magic bytes, and decide whether this is a WSHF (or compressed
	// WSHC) shuffle file or some legacy Parquet payload.
	rc, _, err := s.executor.store.Get(ctx, s.bucket, filePath)
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

	// Legacy Parquet path: download the rest of the body, hand to the
	// parquet reader. These payloads are bounded by the inline-result
	// threshold (KB to a few MB) so io.ReadAll is fine here.
	rest, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading parquet body for %s: %w", filePath, err)
	}
	data := append(magic[:], rest...)
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("opening parquet file %s: %w", filePath, err)
	}
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
	batches, err := scan.ReadFileBatches(reader, projCols, nil)
	if err != nil {
		return fmt.Errorf("reading parquet file %s: %w", filePath, err)
	}
	s.batches = batches
	s.batchIdx = 0
	return nil
}

// openShuffleFile streams the (possibly compressed) shuffle body from rc to a
// local spill file, decompressing on the fly if needed, then mmaps the result
// and hands it to a shuffleChunkReader. Memory is bounded by the kernel's
// page cache footprint over the active mmap region rather than the full file
// size.
func (s *cachedFileStreamSource) openShuffleFile(_ context.Context, srcPath string, magic []byte, rc io.Reader, compressed bool) error {
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

	// We already consumed the magic bytes from rc; write them back first.
	if _, err := tmp.Write(magic); err != nil {
		return fmt.Errorf("writing magic to local cache: %w", err)
	}

	if compressed {
		// Stream-decompress the body. After writing the WSHF magic to disk,
		// the body is the s2 stream that follows the WSHC magic. The shuffle
		// reader expects a plain WSHF file, so we transcode here.
		if err := streamDecompressShuffle(rc, tmp); err != nil {
			return fmt.Errorf("decompressing %s into local cache: %w", srcPath, err)
		}
	} else {
		// Plain WSHF: just stream the body to disk.
		if _, err := io.Copy(tmp, rc); err != nil {
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
	s.localPath = localPath
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
