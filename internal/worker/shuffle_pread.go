package worker

import (
	"fmt"
	"io"
	"os"

	"github.com/derekmwright/wadjet/internal/engine/diskio"
)

// Read-staged WSHF shuffle consumption (docs/design/shuffle-pread-reads.md):
// local .wshf opens (staged S3 downloads, tier-0 LocalStageCache hits,
// prefetched downloads) decode from sequential read() calls into the
// streaming reader's reused heap scratch instead of walking an mmap of the
// file. A goroutine blocked in a read syscall parks at a GC-safe point; one
// faulting on an mmap'd page inside a decode span stretches every
// stop-the-world pause in the process — the frozen-spin mechanism
// (2026-08-11/12: barrier-synchronized writeback storms × WSHF mmap faults
// → multi-second GC mark-termination STW → trap SIGABRT). The parquet scan
// side shipped the same lever as WADJET_SCAN_PREAD (ca856eb).
//
// Unlike the parquet lever, just-written temps convert too: the WSHF walk
// is strictly sequential and single-pass with one reused scratch buffer
// (none of the pool-overflow allocation class that priced parquet pread at
// +15.9% cold), and the freeze evidence sits exactly on just-written staged
// temps inside stage barriers.
//
// WADJET_SHUFFLE_PREAD=0 is the kill switch, restoring the mmap +
// wshf.ChunkReader path unchanged.
var shufflePreadEnabled = os.Getenv("WADJET_SHUFFLE_PREAD") != "0"

// fileBody pairs a (possibly drop-behind-wrapped) reader with the fd it
// reads, so the streaming reader's Close releases the file.
type fileBody struct {
	io.Reader
	io.Closer
}

// openShuffleFromFileStreaming is the shared open tail of the read-staged
// shuffle path: build a streamingShuffleReader over an already-open local
// WSHF file positioned at offset 0. On success the reader owns f (its
// Close closes the fd) and s.localPath is set to ownedPath — "" when a
// cache owns the file and it must not be unlinked. On error the caller
// still owns f and any temp it staged.
func (s *cachedFileStreamSource) openShuffleFromFileStreaming(f *os.File, size int64, ownedPath string) error {
	fadviseSequential(f, size)
	var rc io.ReadCloser = f
	// Drop-behind, owned single-pass temps only — exactly dropBehindWalk's
	// eligibility: cache-owned files may be walked again by other consumers
	// or peer-served, so their pages stay. NewDropBehindReader is a no-op
	// pass-through when the diskio kill switch is off.
	if ownedPath != "" {
		rc = fileBody{Reader: diskio.NewDropBehindReader(f), Closer: f}
	}
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("reading magic from local shuffle file %s: %w", f.Name(), err)
	}
	// Local shuffle files are always plain WSHF: WSHC bodies are
	// decompressed during staging (openShuffleFile, filePrefetcher), and
	// producer sinks write plain WSHF to local disk.
	if magic != shuffleMagic {
		return fmt.Errorf("local shuffle file %s is not WSHF (magic %q)", f.Name(), magic[:])
	}
	r, err := newStreamingShuffleReader(rc, codecNone)
	if err != nil {
		return err
	}
	// WIDX extent index: a validated footer lets decode-ahead skip the
	// stage walk — the scanner emits extents, workers pread their own
	// chunks (docs/design/shuffle-extent-index.md). Falls back to the walk
	// silently when absent/invalid.
	r.tryEnableExtentIndex(f, size, ownedPath != "")
	s.maybeStartShuffleDecodeAhead(r)
	s.chunkReader = r
	// streamReader carries the release obligation (Close → fd close);
	// streamKey stays "" — a read error on a local file is terminal, not a
	// transport failure to fall back from.
	s.streamReader = r
	s.localPath = ownedPath
	s.executor.shuffleFilePreadFiles.Add(1)
	s.executor.shuffleFilePreadBytes.Add(size)
	return nil
}
