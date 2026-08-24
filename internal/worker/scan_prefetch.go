package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/diskio"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// prefetchAtInit moves the download-ahead start from the first file open
// to source Init, so the first file downloads WHILE the runner loads the
// join build side instead of after it.
//
// The straggler tier this closes (docs/benchmarks/
// straggler-tier-verdict-2026-08-16.md, re-confirmed in
// sf100-window2-analysis-2026-08-22.md): degraded scan tasks charge
// 5.8-7.2 s to acq_prefetch (Q09 join-6's slow mode: 8.9 s) while their
// clean-mode siblings charge 0-1 ms to acq_basecache — the whole first
// download is exposed because it starts only after the build load
// finished, with the connection idle for the entire build.
//
// Registered as a kill switch even though prefetch is best-effort and
// cannot change a row set (every failure falls through to the tiered open
// path): it is enumerated by the optimization-invariance oracle and
// bisectable on EC2 by env alone, the same rationale as decode-admission.
var prefetchAtInit = optswitch.Register("prefetch-at-init", "WADJET_PREFETCH_AT_INIT",
	"start the scan file prefetcher at source Init so it overlaps the build load; off = start at the first file open")

// prefetchCacheSkip gates the post-Get HasCachedPath re-probe in fetch:
// when the store's Get call itself populated the base-table cache (a
// peer-tier fetch, or a concurrent task's tee, spools the whole object
// onto the cache volume before Get returns), the prefetcher skips instead
// of copying the now-cache-resident file a second time into the spill dir
// (see the comment on that probe for the full rationale — 283 MB write +
// fsync per SF100 file, serialized behind the 256 MB window, for a copy
// the consumer's cache tier already serves via in-place mmap).
//
// DEFAULT ON. OFF restores the pre-2026-08-22 behavior: fetch always
// copies into spill regardless of what Get populated, so a peer-populated
// file pays the redundant copy and the consumer opens the prefetch
// spill-temp copy instead of the base-cache tier.
//
// Best-effort I/O overlap only — every skip here falls through to the
// authoritative tiered open path in openNextFile, so the row set cannot
// move. Registered anyway for EC2 bisectability against the same-commit
// scheduler reorder (affinityBeforeLocality) and invariance-oracle
// enumeration.
var prefetchCacheSkip = optswitch.Register("prefetch-cache-skip", "WADJET_PREFETCH_CACHE_SKIP",
	"skip the prefetcher's spill copy when Get already populated the base-table cache; off = always copy into spill")

// Prefetch tuning. Vars (not consts) so tests can shrink them to exercise
// the window/ordering machinery with small files.
var (
	// scanPrefetchConcurrency bounds concurrent S3 GETs per source. Each
	// worker task can hold one source per input alias, and the worker runs
	// up to MaxConcurrent tasks, so total GET fan-out on one box is roughly
	// MaxConcurrent × this. 4 matches the shape of the existing 8-way
	// download pools (readParquetFilesConcurrentBatches) halved to leave
	// headroom for the async-upload pool sharing the NIC.
	scanPrefetchConcurrency = 4

	// scanPrefetchWindowBytes bounds bytes downloaded-but-not-yet-consumed
	// on the spill volume per source. Disk-backed, so this is a disk/page-
	// cache bound, not heap — SF10 files (~6.5 MB) never hit it; SF100
	// lineitem files (~200 MB) get ~1 file of read-ahead plus the file
	// currently decoding.
	scanPrefetchWindowBytes = int64(256 << 20)
)

// prefetchResult is the outcome of one prefetch job. Exactly one of the
// three states holds: a usable localPath, skipped (file not eligible —
// the consumer must run the normal tiered open path), or err (download
// attempted and failed — the consumer falls back to the normal path,
// which re-resolves through every tier and produces the authoritative
// error if the object is truly gone).
type prefetchResult struct {
	localPath   string
	windowBytes int64 // amount acquired from the byte window; released on take
	skipped     bool
	err         error
}

// filePrefetcher downloads a source's upcoming S3 parquet files to the
// spill dir while the current file decodes. Before it existed, the scan
// path was strictly serial per task: one full-object GET blocked in
// io.Copy until the entire file landed on NVMe, then decode ran with the
// connection idle — on a standalone box that capped effective S3 read
// parallelism at MaxConcurrent streams and produced the 2026-07-05 SF10
// cold-S3 finding (suite 20m15s vs DuckDB httpfs 2m51s, same instance).
//
// Design constraints:
//   - Strictly best-effort: any failure (miss, transient error, open
//     failure on the temp) makes the consumer fall through to the
//     untouched tiered open path in openNextFile. Prefetch can therefore
//     never change results, only overlap I/O with decode.
//   - Parquet keys only. Shuffle inputs (.wshf / partition=) resolve via
//     the LocalStageCache / NATS-KV / peer tiers, which are either local
//     or explicitly preferred over the durable S3 copy; blind-GETting
//     them here would race the producer's async upload for no benefit.
//   - Delivery is by file index and the consumer takes indices in order.
//     The byte window admits the lowest not-yet-taken index regardless of
//     occupancy: workers can finish downloads out of order, so without
//     the bypass the window could fill with later files while the one the
//     consumer is blocked on cannot start — a deadlock, not just a stall.
//   - Whole-object GETs, no ranged reads: per-column ranged reads were
//     tried 2026-03-20 and reverted the same day for S3 throttling
//     (fe52a79); file-granularity requests at this fan-out are the shape
//     S3 likes.
type filePrefetcher struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	results []chan *prefetchResult

	// startedAt is when the download workers were spawned. The wall from
	// here to the first take() is the prefetch LEAD — the part of the
	// download that overlapped whatever the runner did between source
	// Init and the first Next (on a join fragment: the build load). Read
	// once by the producer goroutine; never written after construction.
	startedAt time.Time

	mu         sync.Mutex
	window     int64         // bytes admitted, not yet released by take/fetch-error
	nextNeeded int           // lowest index take() has not been called for
	windowFree chan struct{} // closed+replaced whenever window/nextNeeded changes
}

// hasPrefetchableFiles reports whether any key in the list is an object
// the prefetcher would act on: parquet always; shuffle (.wshf) keys when
// streaming shuffle read is on (memo §3 D2 — their download-ahead fetches
// ride the peer tier, so they never blind-GET a not-yet-durable key).
func hasPrefetchableFiles(files []string, includeShuffle bool) bool {
	for _, f := range files {
		if strings.HasSuffix(f, ".parquet") {
			return true
		}
		if includeShuffle && strings.HasSuffix(f, ".wshf") {
			return true
		}
	}
	return false
}

// startFilePrefetcher spawns the download workers for s.files. ctx MUST be
// the task context (source Init's, or the first Next's when the at-Init
// start is off) — cancelling it stops every worker, which is what bounds
// the goroutines to the task on cancel, retry and drain. The caller must
// Close() the returned prefetcher.
func startFilePrefetcher(ctx context.Context, s *cachedFileStreamSource) *filePrefetcher {
	pctx, cancel := context.WithCancel(ctx)
	p := &filePrefetcher{
		cancel:     cancel,
		startedAt:  time.Now(),
		results:    make([]chan *prefetchResult, len(s.files)),
		windowFree: make(chan struct{}),
	}
	jobs := make(chan int, len(s.files))
	for i := range p.results {
		p.results[i] = make(chan *prefetchResult, 1)
		jobs <- i
	}
	close(jobs)
	workers := scanPrefetchConcurrency
	if workers > len(s.files) {
		workers = len(s.files)
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.run(pctx, s, jobs)
	}
	return p
}

func (p *filePrefetcher) run(ctx context.Context, s *cachedFileStreamSource, jobs <-chan int) {
	defer p.wg.Done()
	// The index being fetched, so the boundary below can answer for it.
	inFlight := -1
	// fetch reaches decode and decompression paths on a goroutine nobody
	// joins for errors. Unrecovered, a panic there ends the worker process;
	// recovered but undelivered, the consumer waits forever on an empty
	// result slot. Deliver it as that file's error (#511). One defer per
	// prefetch goroutine, not per file.
	//
	// The in-flight index is not the only obligation this goroutine owes.
	// jobs is pre-filled with EVERY index and closed, and the pool is the
	// only thing draining it: a panicking worker that resolves just its own
	// index leaves the rest queued, and once all scanPrefetchConcurrency
	// workers have died that way nothing will ever fill those slots — take()
	// blocks forever on a result nobody owns. So the boundary takes
	// ownership of what is left and fails it with the same error.
	//
	// It deliberately does NOT cancel: p.cancel reaches only this
	// prefetcher's child context, while take() waits on the CALLER's, so
	// cancelling would make live siblings abandon indices they had already
	// received and reintroduce the hang from the other side. Draining is
	// race-free instead — every index is delivered by channel receive, so
	// exactly one goroutine owns it, whether that is a healthy sibling
	// mid-fetch or this drain loop.
	defer exec.CatchQueryPanic(ctx, "scan file prefetch", func(err error) {
		if inFlight >= 0 {
			p.results[inFlight] <- &prefetchResult{err: err}
			inFlight = -1
		}
		for idx := range jobs {
			p.results[idx] <- &prefetchResult{err: err}
		}
	})
	for idx := range jobs {
		if ctx.Err() != nil {
			return
		}
		// results[idx] is buffered (cap 1) and each index is fetched by
		// exactly one worker, so the send never blocks. inFlight is cleared
		// BEFORE the send, so a panic in the send itself cannot make the
		// boundary send a second time.
		inFlight = idx
		res := p.fetch(ctx, s, idx)
		inFlight = -1
		p.results[idx] <- res
	}
}

// fetch resolves one file index. It re-checks the cheap tiers (local
// stage cache, peer hint) so it only spends a GET on files that would
// have reached the S3 tier anyway.
func (p *filePrefetcher) fetch(ctx context.Context, s *cachedFileStreamSource, idx int) *prefetchResult {
	filePath := s.files[idx]
	if strings.HasSuffix(filePath, ".wshf") {
		if s.executor.streamingShuffleRead {
			return p.fetchShuffle(ctx, s, idx)
		}
		return &prefetchResult{skipped: true}
	}
	if !strings.HasSuffix(filePath, ".parquet") {
		return &prefetchResult{skipped: true}
	}
	if s.queryID != "" && s.executor.localCache != nil &&
		s.executor.localCache.Get(s.queryID, filePath) != "" {
		return &prefetchResult{skipped: true}
	}
	if peers := s.executor.peers; peers != nil && peers.client != nil {
		if addr, _ := peers.hintFor(filePath); addr != "" {
			return &prefetchResult{skipped: true}
		}
	}
	// Base-table cache resident: the consumer's cache tier mmaps the file
	// in place — downloading it again here would waste the GET the cache
	// exists to save. Counting-free probe: the serve (and the hit stat)
	// happens in openNextFile.
	if lps, ok := s.executor.store.(objstore.LocalPathStore); ok && lps.HasCachedPath(s.bucket, filePath) {
		return &prefetchResult{skipped: true}
	}

	rc, info, err := s.executor.store.Get(ctx, s.bucket, filePath)
	if err != nil {
		return &prefetchResult{err: err}
	}
	defer rc.Close()

	// The Get itself may have populated the base-table cache: a peer-tier
	// fetch (or a concurrent task's tee) spools the whole object onto the
	// cache volume before Get returns, and rc is then the admitted cache
	// file. Copying it a second time into the spill dir — 283 MB write +
	// fsync per SF100 file, behind a 256 MB window that serializes the
	// copies — bought nothing the consumer's cache tier does not already
	// give it with an in-place mmap. Skip, and let openNextFile take the
	// base-cache tier (a concurrent eviction just falls through to the
	// tiered path as every skip does). Gated by prefetchCacheSkip.
	if prefetchCacheSkip.On() {
		if lps, ok := s.executor.store.(objstore.LocalPathStore); ok && lps.HasCachedPath(s.bucket, filePath) {
			return &prefetchResult{skipped: true}
		}
	}

	// Admit against the byte window using the object's advertised size.
	// A size the store didn't report (0) gets a nominal charge so an
	// endless run of unknown-size objects can't bypass the bound.
	winBytes := info.Size
	if winBytes <= 0 {
		winBytes = 8 << 20
	}
	if err := p.acquireWindow(ctx, idx, winBytes); err != nil {
		return &prefetchResult{err: err}
	}

	localPath, err := p.streamToSpill(ctx, s, filePath, rc)
	if err != nil {
		p.releaseWindow(winBytes)
		return &prefetchResult{err: err}
	}
	return &prefetchResult{localPath: localPath, windowBytes: winBytes}
}

// streamToSpill copies the object body to a temp file in the spill dir,
// reporting bytes to the per-task ProgressReporter so the coordinator's
// stale-worker reap sees I/O progress during long download windows (same
// rationale as streamParquetToLocalMmap).
func (p *filePrefetcher) streamToSpill(ctx context.Context, s *cachedFileStreamSource, srcPath string, rc io.Reader) (string, error) {
	spillDir := s.executor.spillDir
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return "", fmt.Errorf("creating spill dir: %w", err)
	}
	tmp, err := os.CreateTemp(spillDir, "scan-prefetch-*.parquet")
	if err != nil {
		return "", fmt.Errorf("creating prefetch file: %w", err)
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
	// KeepResident: the file is mmap'd and decoded shortly after landing —
	// bound dirty pages during the write, keep them resident for the read.
	wf, _ := diskio.NewWriter(tmp, diskio.KeepResident)
	dst := io.Writer(wf)
	if rep != nil {
		dst = &progressWriter{w: wf, rep: rep}
	}
	if _, err := io.Copy(dst, rc); err != nil {
		return "", fmt.Errorf("prefetching %s: %w", srcPath, err)
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("syncing prefetch file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("closing prefetch file: %w", err)
	}
	cleanup = false
	return localPath, nil
}

// fetchShuffle downloads one hinted shuffle file ahead of consumption,
// landing it DECOMPRESSED (plain WSHF) on the spill volume so the
// consumer mmaps it directly with the standard chunk reader. Only
// peer-hinted keys are fetched — a key without a hint resolves through
// the normal tiers (never a blind S3 GET racing an async upload, the
// scan_prefetch original exclusion rationale). Every failure is
// best-effort: the consumer's tiered path is authoritative.
func (p *filePrefetcher) fetchShuffle(ctx context.Context, s *cachedFileStreamSource, idx int) *prefetchResult {
	filePath := s.files[idx]
	if s.queryID != "" && s.executor.localCache != nil &&
		s.executor.localCache.Get(s.queryID, filePath) != "" {
		return &prefetchResult{skipped: true}
	}
	peers := s.executor.peers
	if peers == nil || peers.client == nil {
		return &prefetchResult{skipped: true}
	}
	addr, token := peers.hintFor(filePath)
	if addr == "" {
		return &prefetchResult{skipped: true}
	}
	prc, err := peers.client.FetchShuffle(ctx, addr, s.queryID, filePath, token)
	if err != nil {
		return &prefetchResult{err: err}
	}
	// Per-tier ledger: prefetched peer downloads are peer-tier transfers,
	// counted at fetch time (the transfer happens now, not at open).
	rc := io.ReadCloser(&countingReadCloser{rc: prc, n: &s.executor.shuffleIO.peerBytes})
	defer rc.Close()
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return &prefetchResult{err: fmt.Errorf("reading magic from peer %s: %w", addr, err)}
	}
	codec, isShuffle := wshf.CodecForMagic(magic)
	if !isShuffle {
		return &prefetchResult{err: fmt.Errorf("peer %s returned non-shuffle payload for %s (magic %q)", addr, filePath, magic[:])}
	}

	// Peer fetch advertises no size; nominal window charge, matching the
	// unknown-size convention on the parquet path.
	winBytes := int64(8 << 20)
	if err := p.acquireWindow(ctx, idx, winBytes); err != nil {
		return &prefetchResult{err: err}
	}
	localPath, err := p.streamShuffleToSpill(ctx, s, filePath, magic[:], rc, codec)
	if err != nil {
		p.releaseWindow(winBytes)
		return &prefetchResult{err: err}
	}
	s.executor.shuffleIO.peerFiles.Add(1)
	return &prefetchResult{localPath: localPath, windowBytes: winBytes}
}

// streamShuffleToSpill copies a shuffle body to a spill temp as plain
// WSHF (transcoding WSHC/WSHZ via the streaming codec decoder),
// reporting progress like streamToSpill.
func (p *filePrefetcher) streamShuffleToSpill(ctx context.Context, s *cachedFileStreamSource, srcPath string, magic []byte, rc io.Reader, codec shuffleCodec) (string, error) {
	spillDir := s.executor.spillDir
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		return "", fmt.Errorf("creating spill dir: %w", err)
	}
	tmp, err := os.CreateTemp(spillDir, "shuffle-prefetch-*.wshf")
	if err != nil {
		return "", fmt.Errorf("creating prefetch file: %w", err)
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
	wf, _ := diskio.NewWriter(tmp, diskio.KeepResident)
	dst := io.Writer(wf)
	if rep != nil {
		dst = &progressWriter{w: wf, rep: rep}
	}
	if codec != codecNone {
		// The compressed body re-carries the inner WSHF magic; write nothing first.
		if err := streamDecompressShuffle(rc, dst, codec); err != nil {
			return "", fmt.Errorf("prefetch-decompressing %s: %w", srcPath, err)
		}
	} else {
		if _, err := dst.Write(magic); err != nil {
			return "", fmt.Errorf("writing magic to prefetch file: %w", err)
		}
		if _, err := io.Copy(dst, rc); err != nil {
			return "", fmt.Errorf("prefetching %s: %w", srcPath, err)
		}
	}
	if err := tmp.Sync(); err != nil {
		return "", fmt.Errorf("syncing prefetch file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("closing prefetch file: %w", err)
	}
	cleanup = false
	return localPath, nil
}

// acquireWindow blocks until winBytes fits in the byte window OR idx is
// the lowest index the consumer has not yet taken. The bypass is what
// makes the window deadlock-free: the consumer takes indices strictly in
// order, so the file it is (or will next be) blocked on must always be
// admitted even when later, out-of-order completions filled the window.
func (p *filePrefetcher) acquireWindow(ctx context.Context, idx int, winBytes int64) error {
	for {
		p.mu.Lock()
		if idx <= p.nextNeeded || p.window+winBytes <= scanPrefetchWindowBytes {
			p.window += winBytes
			p.mu.Unlock()
			return nil
		}
		wait := p.windowFree
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
		}
	}
}

func (p *filePrefetcher) releaseWindow(winBytes int64) {
	p.mu.Lock()
	p.window -= winBytes
	close(p.windowFree)
	p.windowFree = make(chan struct{})
	p.mu.Unlock()
}

// take returns the prefetch outcome for idx, blocking until the download
// resolves or ctx is cancelled. On a successful result the caller owns
// localPath (must remove it when done). Must be called for indices in
// strictly increasing order — the window's deadlock-freedom depends on it.
func (p *filePrefetcher) take(ctx context.Context, idx int) *prefetchResult {
	p.mu.Lock()
	if idx+1 > p.nextNeeded {
		p.nextNeeded = idx + 1
	}
	// Wake any worker parked in acquireWindow: the bypass condition just
	// changed even though window occupancy didn't.
	close(p.windowFree)
	p.windowFree = make(chan struct{})
	p.mu.Unlock()

	select {
	case res := <-p.results[idx]:
		if res.windowBytes > 0 {
			p.releaseWindow(res.windowBytes)
		}
		return res
	case <-ctx.Done():
		return &prefetchResult{err: ctx.Err()}
	}
}

// tryTake is take's non-blocking sibling for the decode-ahead pre-open
// path (docs/design/scan-decode-pipelining.md S2): it never waits on a
// download. Returns (res, true) when idx resolved to a usable local file
// (ownership transfers to the caller), (nil, true) when idx resolved to
// skipped/err — the result is PUT BACK for the authoritative take() in
// openNextFile, which must still observe it — and (nil, false) when the
// download is simply not done yet (poll again later).
func (p *filePrefetcher) tryTake(idx int) (*prefetchResult, bool) {
	p.mu.Lock()
	if idx+1 > p.nextNeeded {
		p.nextNeeded = idx + 1
	}
	close(p.windowFree)
	p.windowFree = make(chan struct{})
	p.mu.Unlock()

	select {
	case res := <-p.results[idx]:
		if res.skipped || res.err != nil {
			// Put back (cap-1 channel, single producer per idx — the
			// send cannot block) so take() reaps it and releases any
			// window bytes exactly once, there.
			p.results[idx] <- res
			return nil, true
		}
		if res.windowBytes > 0 {
			p.releaseWindow(res.windowBytes)
		}
		return res, true
	default:
		return nil, false
	}
}

// Close cancels outstanding downloads, waits for workers to exit, and
// removes any downloaded-but-never-taken temp files. Idempotent.
func (p *filePrefetcher) Close() {
	if p == nil {
		return
	}
	p.cancel()
	p.wg.Wait()
	for _, ch := range p.results {
		select {
		case res := <-ch:
			if res != nil && res.localPath != "" {
				_ = os.Remove(res.localPath)
			}
		default:
		}
	}
}
