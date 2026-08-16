package worker

import (
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/derekmwright/wadjet/internal/engine/diskio"
)

// Single-pass drop-behind for the WSHF mmap walk
// (docs/benchmarks/steady-slower-than-cold-2026-08-08.md): a downloaded
// shuffle temp is written once, walked once by the chunk reader, then
// unlinked. While the walk runs, its pages accumulate in the page cache
// and evict the reusable pages (base-table cache files) a steady-state
// suite re-reads. Dropping fully-decoded windows behind the cursor bounds
// the walk's cache footprint to one window. Gated on the same
// WADJET_DROP_BEHIND kill switch as the diskio mechanisms.

// walkDropWindowBytes matches the diskio 8 MiB window: large enough that
// the madvise amortizes to noise, and a multiple of every page size so
// sub-slice advises stay page-aligned (mmapData itself starts on a page).
const walkDropWindowBytes = 8 << 20

// dropBehindWalk discards fully-decoded pages behind the WSHF walk cursor.
// Only owned temps (localPath != "") are eligible: LocalStageCache-owned
// files may be walked again by other consumers or peer-served, and the
// streaming-decode path has no mmap at all. MADV_DONTNEED on a MAP_SHARED
// PROT_READ file mapping merely discards clean pages — a later touch
// re-faults from NVMe — so this is advisory, never a correctness risk.
func (s *cachedFileStreamSource) dropBehindWalk() {
	if s.localPath == "" || s.mmapData == nil {
		return
	}
	cr, ok := s.chunkReader.(*shuffleChunkReader)
	if !ok || !diskio.DropBehindEnabled() {
		return
	}
	cut := cr.Pos() &^ (walkDropWindowBytes - 1)
	if cut <= s.walkDropMark {
		return
	}
	if unix.Madvise(s.mmapData[s.walkDropMark:cut], unix.MADV_DONTNEED) == nil {
		diskio.AddReadDropBytes(int64(cut - s.walkDropMark))
		s.walkDropMark = cut
	}
}

// adviseSequential hints the kernel that data will be walked front-to-back:
// doubled readahead, and pages behind the read become preferred reclaim
// victims. Applied to every scan mmap (owned temps and cache-owned files
// alike) — in the saturated steady regime the behind-pages would be
// evicted anyway, and the stronger readahead helps the cold walk.
func adviseSequential(data []byte) {
	if len(data) == 0 {
		return
	}
	_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
}

// Row-group I/O-ahead (docs/design/rowgroup-readahead.md): the
// decode-ahead iterator announces the projected column-chunk ranges of
// row groups about to be decoded, and this adviser turns them into
// madvise(MADV_WILLNEED) on the scan mmap — asynchronous kernel
// readahead that lands the pages BEFORE a token-holding decode worker
// faults on them. On a page-cache-hot mmap (cold runs re-reading their
// own tee) the advise is a no-op; in the saturated steady regime it
// converts synchronous per-fault NVMe reads under a held CPU token into
// batched background I/O. WADJET_ROWGROUP_READAHEAD=0 is the kill
// switch.
var rowGroupReadaheadEnabled = os.Getenv("WADJET_ROWGROUP_READAHEAD") != "0"

// readaheadAdviseBytes counts bytes handed to MADV_WILLNEED, the
// engagement marker beside the drop-behind counters in wlogs.
var readaheadAdviseBytes atomic.Int64

// willNeedAdviser returns the scan.DecodeAheadOpts.Advise closure for one
// whole-file scan mmap. Offsets are file-relative, which is mmap-relative
// here (offset-0 whole-file map). The closure clamps to the mapping,
// page-aligns downward (madvise requires an aligned start; the base is
// page-aligned), and never errors out loud — WILLNEED is advisory.
func willNeedAdviser(data []byte) func(off, n int64) {
	pageMask := int64(os.Getpagesize()) - 1
	return func(off, n int64) {
		if off < 0 || n <= 0 || off >= int64(len(data)) {
			return
		}
		end := min(off+n, int64(len(data)))
		off &^= pageMask
		if unix.Madvise(data[off:end], unix.MADV_WILLNEED) == nil {
			readaheadAdviseBytes.Add(end - off)
		}
	}
}
