package scan

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DecodeAheadIter is the parallel sibling of RowGroupIter
// (docs/design/scan-decode-pipelining.md): k decode workers pull
// row-group indices in file order, each runs ReadRowGroupNative for its
// group, and results deliver to the consumer strictly in source order.
// The consumer-facing contract is identical to RowGroupIter — same
// batches in the same order, same error surfaced at the same position,
// same prune behavior — only the decode of group N+1..N+w overlaps the
// consumption of group N instead of waiting for it.
//
// Memory is bounded by WindowBytes of decoded-but-undelivered batches,
// estimated per group from the projected columns' TotalUncompressedSize
// metadata (estimation error is bounded by one group per worker). The
// group at the delivery cursor is always admitted regardless of the
// window so a single oversized row group cannot deadlock the pipeline.
//
// Concurrency safety rests on ReadRowGroupNative's documented contract:
// FileReader is read-only after construction and every ColumnPages call
// allocates a fresh ColumnPageReader, so concurrent decodes of distinct
// row groups never share mutable state (columnar_native.go:124-126).
//
// Lifecycle: workers start on the FIRST Next() call, not at Open —
// filters attach after Open on the worker scan path (and may keep
// arriving mid-scan; assignments re-read them per group). Close()
// stops assignment and JOINS in-flight decodes before returning: the
// caller munmaps the file bytes right after Close, so no decode may
// touch the underlying slice once Close returns.
type DecodeAheadIter struct {
	fr         *pqt.FileReader
	readSchema []pqt.Column
	start, end int
	// estLeaves[i] is the leaf column index for readSchema[i], -1 when
	// unresolved (ROW children, aliased or missing columns) — used only
	// for window estimation, resolved once at Open.
	estLeaves []int

	workers        int
	pressure       func() bool
	pressureStrict bool
	tokens         TokenPool
	advise         func(off, n int64)
	advisedIdx     int // next row-group index to I/O-advise (win.mu)
	cache          *DecodedChunkCache

	dynamicRanges []exec.DynamicRange
	bloomFilters  []*exec.BloomScanFilter

	// win serializes ALL iterator state below and carries the byte
	// window. Chained iterators (cross-file continuation) share one
	// DecodeWindow, so one file's deliveries wake the next file's
	// admission waiters — and share one mutex, which keeps the two
	// iterators' state transitions trivially race-free.
	win     *DecodeWindow
	started bool
	closed  bool
	nextIdx int                 // next row-group index to assign to a worker
	deliver int                 // next row-group index to hand to the consumer
	slots   map[int]*decodeSlot // decoded (or pruned/failed), awaiting delivery
	wg      sync.WaitGroup

	// Empty-shard sentinel (mirrors RowGroupIter).
	empty bool

	// Counters. Prune counters mirror RowGroupIter's for diagnostic
	// parity; the rest are the memo §5 markers, read via Stats.
	rgPrunedBloom    atomic.Int64
	rgPrunedRange    atomic.Int64
	rgRead           atomic.Int64
	windowFullStalls atomic.Int64
	pressureStalls   atomic.Int64
	tokenStalls      atomic.Int64
	ledgerStalls     atomic.Int64

	// Stall DURATIONS (ns), one per counter above. The 2026-07-20 Q08
	// diagnosis showed counts alone cannot rank stall classes: slow and
	// fast tasks logged near-identical counts while their wall time
	// differed 3× — the time was in how long each wait lasted.
	windowFullStallNs atomic.Int64
	pressureStallNs   atomic.Int64
	tokenStallNs      atomic.Int64
	ledgerStallNs     atomic.Int64

	// Decode-span accounting: wall time inside ReadRowGroupNative and the
	// projected column chunks' compressed (on-disk/mmap) bytes for groups
	// actually decoded. The stall counters above only see PARKED waiters —
	// a worker that holds its token and faults inline on a non-resident
	// mmap page is invisible to them, its cost surfacing only as a longer
	// decode span. ns/byte across identically-shaped runs is therefore the
	// direct fault-stretch discriminator: page-cache-hot runs measure pure
	// decode CPU per compressed byte, and any steady-regime excess over
	// that baseline is time spent faulting under a held token
	// (docs/design/rowgroup-readahead.md, residual diagnosis).
	decodeSpanNs    atomic.Int64
	decodeSpanBytes atomic.Int64
}

// timedWait waits on the window condvar and adds the blocked span to ns.
// Callers hold win.mu (cond.Wait's contract).
func (it *DecodeAheadIter) timedWait(ns *atomic.Int64) {
	t0 := time.Now()
	it.win.cond.Wait()
	ns.Add(time.Since(t0).Nanoseconds())
}

// decodeSlot is one row group's outcome, parked until the delivery
// cursor reaches its index.
type decodeSlot struct {
	batch *batch.RecordBatch // nil when pruned or empty
	err   error
	est   int64 // window bytes reserved for this group
	ahead bool  // admitted as non-cursor: delivery/drop decrements win.ahead
}

// DecodeAheadOpts sizes a DecodeAheadIter.
type DecodeAheadOpts struct {
	// Workers is the decode worker count. <= 0 selects
	// DefaultDecodeAheadWorkers capped at GOMAXPROCS.
	Workers int
	// WindowBytes bounds decoded-but-undelivered batch bytes. <= 0
	// selects DefaultDecodeAheadWindowBytes. Ignored when Window is set.
	WindowBytes int64
	// Window shares an existing byte window (and its lock) with another
	// iterator — the cross-file continuation shape: the tail of file F
	// and the head of file F+1 draw from one budget.
	Window *DecodeWindow
	// Pressure, when set, is consulted before admitting any group beyond
	// the delivery cursor — the memory-pressure collapse hook. Must be
	// safe to call from multiple goroutines.
	//
	// While it reports true, admission is OCCUPANCY-FLOORED by default:
	// one non-cursor group may be in flight or parked (a 2-deep
	// pipeline), further admission waits. The 2026-07-18 SF100 sensor
	// A/B (memo §9.5) split the pressure regime in two: when decode
	// outruns the consumer the window fills and its held bytes displace
	// page cache (collapse is right — Q06 +46 % without it), but on
	// producer-bound repartition stages the window sits EMPTY and
	// cursor-only collapse is pure serialization with nothing to shed
	// (Q05 −12.7 % when spared). One group ahead holds ~one group-est of
	// bytes — nothing to displace with — while keeping the producer
	// pipelined.
	Pressure func() bool
	// PressureStrict restores cursor-only collapse under Pressure (no
	// group ahead at all). Set on edge-class envelopes (< 2 GiB
	// GOMEMLIMIT), where the capped repro measured even one extra
	// in-flight group as harmful (32 MiB window arm +37 % vs cursor-only
	// winning outright).
	PressureStrict bool
	// Tokens, when set, budgets decode CPU per ROW GROUP: a worker
	// acquires one token at admission and releases it when the decoded
	// group parks, so a worker stalled on the window (or between groups)
	// holds nothing. The delivery-cursor group is token-exempt — serial
	// progress is always allowed, mirroring the morsel "first consumer is
	// free" rule. The 2026-07-16 SF100 pair convicted the previous
	// source-lifetime acquisition: decode workers sat window-stalled
	// holding tokens (~35k stalls/40k groups), starving concurrent join
	// fragments' morsel width (Q20 +54%, Q05 +17%) while utilization
	// stayed flat.
	Tokens TokenPool
	// Advise, when set, receives the file-relative byte range of each
	// projected column chunk shortly BEFORE its row group is decoded —
	// the I/O-ahead seam (docs/design/rowgroup-readahead.md). The worker
	// wires an madvise(MADV_WILLNEED) closure over the scan mmap so a
	// steady-state re-read faults asynchronously via kernel readahead
	// instead of synchronously under a held CPU token. Ranges for group
	// N+workers are issued as group N is assigned, so the advice leads
	// decode by roughly one full assignment wave. Must be safe for
	// concurrent use; calls stop before Close returns (decode workers
	// are joined), so an mmap-backed closure never outlives its munmap.
	Advise func(off, n int64)
	// Cache, when set, consults/feeds the worker's decoded-chunk cache
	// inside ReadRowGroupNativeCached (docs/design/decoded-rowgroup-cache.md).
	// Inert unless the reader carries a CacheIdentity. nil = uncached.
	Cache *DecodedChunkCache
}

// TokenPool is the compute-budget seam shared with the caller's pool
// (the worker's cpuTokens). Both methods must be safe for concurrent
// use; TryAcquire must not block.
type TokenPool interface {
	TryAcquire(n int) int
	Release(n int)
}

// WindowLedger is the memory-budget seam shared with the caller's task
// ledger (the worker's shared pool tracker — *memory.Tracker satisfies
// it directly). Decoded-but-undelivered window bytes are charged here so
// they are visible to spill decisions and so admission collapses toward
// the cursor-only serial floor as budget headroom vanishes — the memo §9
// fix: under memory pressure the window's held batches displace page
// cache and GC headroom that the Go-heap pressure hook cannot see, so
// the byte cost must ride the same ledger as every other operator.
//
// Reserve returns a non-nil error to deny (nothing retained on denial);
// ForceReserve charges unconditionally — used for the delivery-cursor
// group, which is always admitted but whose bytes are still real.
// All methods must be safe for concurrent use and must not block.
type WindowLedger interface {
	Reserve(n int64) error
	ForceReserve(n int64)
	Release(n int64)
}

// DecodeWindow is a byte budget shared by one or more DecodeAheadIters.
// Its mutex doubles as the owning iterators' state lock.
type DecodeWindow struct {
	mu       sync.Mutex
	cond     *sync.Cond
	inflight int64
	limit    int64
	// ledger, when non-nil, additionally charges every inflight byte to
	// the task memory ledger: limit is the ceiling, ledger headroom is
	// the floating bound below it. Reserved and released in lockstep
	// with inflight, so the ledger balance returns to zero when every
	// owning iterator has closed.
	ledger WindowLedger
	// ahead counts non-cursor groups admitted but not yet delivered (or
	// dropped), across every iterator sharing this window — the
	// occupancy floor the pressure hook gates on. Guarded by mu.
	ahead int
}

// NewDecodeWindow returns a window bounding decoded-but-undelivered
// bytes across every iterator opened with it. bytes <= 0 selects
// DefaultDecodeAheadWindowBytes.
func NewDecodeWindow(bytes int64) *DecodeWindow {
	return NewDecodeWindowWithLedger(bytes, nil)
}

// NewDecodeWindowWithLedger is NewDecodeWindow with a memory ledger
// attached: beyond the fixed byte ceiling, non-cursor admission must
// also clear ledger.Reserve, and every inflight byte is charged to the
// ledger for its parked lifetime. A nil ledger yields the fixed-window
// behavior. Callers passing a concrete pointer type must nil-check it
// themselves — a nil *T wrapped in the interface reads as non-nil here.
func NewDecodeWindowWithLedger(bytes int64, ledger WindowLedger) *DecodeWindow {
	if bytes <= 0 {
		bytes = DefaultDecodeAheadWindowBytes
	}
	w := &DecodeWindow{limit: bytes, ledger: ledger}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// credit returns a slot's reservations — window bytes, ledger bytes,
// and its occupancy-floor count — when it is delivered or dropped.
// Caller holds w.mu.
func (w *DecodeWindow) credit(slot *decodeSlot) {
	w.inflight -= slot.est
	if slot.ahead {
		slot.ahead = false
		w.ahead--
	}
	if w.ledger != nil {
		w.ledger.Release(slot.est)
	}
}

const (
	// DefaultDecodeAheadWorkers is deliberately modest: decode workers
	// multiply with the per-column errgroup inside ReadRowGroupNative
	// (min(#cols, GOMAXPROCS) each), and with concurrent fragments per
	// worker process. cpuToken integration (memo S3) replaces this with
	// budget-aware width.
	DefaultDecodeAheadWorkers = 4

	// DefaultDecodeAheadWindowBytes matches the scanPrefetch byte-window
	// order of magnitude; the morsel dispenser's budget bounds the next
	// stage downstream.
	DefaultDecodeAheadWindowBytes int64 = 256 << 20

	// minGroupEstimateBytes floors the per-group window reservation so
	// files with absent/zero size metadata cannot admit unboundedly.
	minGroupEstimateBytes int64 = 1 << 20
)

// OpenDecodeAheadIter constructs a decode-ahead iterator over the
// row-group range assigned to (shardIdx, shardCount), mirroring
// OpenRowGroupIter's contract (empty-shard sentinel, Array/Map
// rejection, projection via selectedCols).
func OpenDecodeAheadIter(reader *pqt.Reader, schema []pqt.Column, selectedCols []string, shardIdx, shardCount int, opts DecodeAheadOpts) (*DecodeAheadIter, error) {
	if reader == nil {
		return nil, fmt.Errorf("OpenDecodeAheadIter: nil reader")
	}
	if shardCount < 1 {
		shardCount = 1
	}

	readSchema := schema
	if len(selectedCols) > 0 {
		readSchema = projectSchema(schema, selectedCols)
	}
	if HasUnsupportedColumnarTypes(readSchema) {
		return nil, fmt.Errorf("DecodeAheadIter does not support Array/Map types; caller must fall back to ReadFileBatchesShard")
	}

	it := &DecodeAheadIter{
		workers:        opts.Workers,
		win:            opts.Window,
		pressure:       opts.Pressure,
		pressureStrict: opts.PressureStrict,
		tokens:         opts.Tokens,
		advise:         opts.Advise,
		cache:          opts.Cache,
	}
	if it.win == nil {
		it.win = NewDecodeWindow(opts.WindowBytes)
	}
	if it.workers <= 0 {
		it.workers = DefaultDecodeAheadWorkers
	}
	if gp := runtime.GOMAXPROCS(0); it.workers > gp {
		it.workers = gp
	}

	if shardIdx < 0 || shardIdx >= shardCount {
		it.empty = true
		return it, nil
	}
	fr := reader.FileReader()
	if fr == nil {
		return nil, fmt.Errorf("OpenDecodeAheadIter: reader has no FileReader")
	}
	it.fr = fr
	it.readSchema = readSchema
	it.start, it.end = rowGroupRangeForShard(fr.NumRowGroups(), shardIdx, shardCount)
	it.nextIdx, it.deliver = it.start, it.start
	it.advisedIdx = it.start
	it.slots = make(map[int]*decodeSlot, it.workers)

	leaves := fr.Leaves()
	leafByName := make(map[string]int, len(leaves))
	for i, leaf := range leaves {
		leafByName[leaf.Name] = i
	}
	it.estLeaves = make([]int, len(readSchema))
	for i, col := range readSchema {
		if li, ok := leafByName[col.Name]; ok {
			it.estLeaves[i] = li
		} else {
			it.estLeaves[i] = -1
		}
	}
	return it, nil
}

// SetDynamicFilters attaches dynamic-filter pushdowns. Safe mid-scan:
// decode workers re-read the filter set under the window lock at every
// group assignment, so a set that lands while the scan is running prunes
// every group not yet assigned (attach-on-arrival delivery). Groups
// already assigned or advised before the call decode/advise unfiltered —
// drop-only semantics, results identical. Multiple calls overwrite;
// callers pass the accumulated union. Mirrors RowGroupIter.SetDynamicFilters.
func (it *DecodeAheadIter) SetDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	if it == nil {
		return
	}
	it.win.mu.Lock()
	defer it.win.mu.Unlock()
	it.dynamicRanges = ranges
	it.bloomFilters = blooms
}

// Start spawns the decode workers immediately instead of on the first
// Next — the cross-file continuation pre-opens the next file's iterator
// and wants its head decoding while the current file's tail delivers.
// Dispatch-time filters should already be attached; late (attach-on-
// arrival) filters may still land afterward via SetDynamicFilters.
// Idempotent.
func (it *DecodeAheadIter) Start() {
	if it == nil || it.empty {
		return
	}
	it.win.mu.Lock()
	it.startLocked()
	it.win.mu.Unlock()
}

func (it *DecodeAheadIter) startLocked() {
	if it.started || it.closed {
		return
	}
	it.started = true
	for w := 0; w < it.workers; w++ {
		it.wg.Add(1)
		go it.decodeLoop()
	}
}

// AssignmentDrained reports whether every row group has been assigned to
// a decode worker (not necessarily delivered) — the cross-file
// continuation's pre-open trigger: once true, idle workers exist or soon
// will, and the next file's head is the only work left to feed them.
func (it *DecodeAheadIter) AssignmentDrained() bool {
	if it == nil || it.empty {
		return true
	}
	it.win.mu.Lock()
	defer it.win.mu.Unlock()
	return !it.closed && it.started && it.nextIdx >= it.end
}

// PruneStats mirrors RowGroupIter.PruneStats.
func (it *DecodeAheadIter) PruneStats() (bloom, rangeP, read int) {
	if it == nil {
		return 0, 0, 0
	}
	return int(it.rgPrunedBloom.Load()), int(it.rgPrunedRange.Load()), int(it.rgRead.Load())
}

// Stats returns the decode-ahead engagement counters (memo §5/§9
// markers): row groups decoded, worker stalls on a full window,
// admissions refused under memory pressure, admissions deferred for
// lack of a cpu token, and admissions denied by the memory ledger (the
// group still decodes later — serially at worst, in every case).
func (it *DecodeAheadIter) Stats() (groupsRead, windowFullStalls, pressureStalls, tokenStalls, ledgerStalls int64) {
	if it == nil {
		return 0, 0, 0, 0, 0
	}
	return it.rgRead.Load(), it.windowFullStalls.Load(), it.pressureStalls.Load(), it.tokenStalls.Load(), it.ledgerStalls.Load()
}

// StallDurations returns the total blocked time (ns) behind each Stats
// counter, in the same order (window-full, pressure, token, ledger).
// Counts say how often a gate closed; these say how much wall it cost.
func (it *DecodeAheadIter) StallDurations() (windowFullNs, pressureNs, tokenNs, ledgerNs int64) {
	if it == nil {
		return 0, 0, 0, 0
	}
	return it.windowFullStallNs.Load(), it.pressureStallNs.Load(), it.tokenStallNs.Load(), it.ledgerStallNs.Load()
}

// DecodeSpans returns the total wall time (ns) spent inside
// ReadRowGroupNative and the projected compressed bytes those decodes
// covered. Unlike StallDurations — which only measures parked waiters —
// this captures inline mmap fault time hidden inside token-holding
// decode spans: ns/byte materially above a page-cache-hot run's ratio
// means decode workers are faulting synchronously despite I/O-ahead.
func (it *DecodeAheadIter) DecodeSpans() (ns, bytes int64) {
	if it == nil {
		return 0, 0
	}
	return it.decodeSpanNs.Load(), it.decodeSpanBytes.Load()
}


// Next returns the next decoded row group in source order, or (nil, nil)
// when exhausted. Contract identical to RowGroupIter.Next.
func (it *DecodeAheadIter) Next() (*batch.RecordBatch, error) {
	if it == nil || it.empty {
		return nil, nil
	}
	it.win.mu.Lock()
	defer it.win.mu.Unlock()
	it.startLocked()
	for {
		if it.closed {
			return nil, nil
		}
		if slot, ok := it.slots[it.deliver]; ok {
			delete(it.slots, it.deliver)
			it.deliver++
			it.win.credit(slot)
			it.win.cond.Broadcast()
			if slot.err != nil {
				// Mirror serial semantics: the error surfaces exactly at
				// this group's position and the iterator is dead after.
				it.closed = true
				it.win.cond.Broadcast()
				return nil, slot.err
			}
			if slot.batch == nil {
				continue // pruned or empty group — same silent skip as serial
			}
			return slot.batch, nil
		}
		if it.deliver >= it.end {
			return nil, nil // all groups delivered
		}
		it.win.cond.Wait()
	}
}

// decodeLoop is one worker: admit the next index against the byte
// window, decode it outside the lock, park the result in its slot.
func (it *DecodeAheadIter) decodeLoop() {
	defer it.wg.Done()
	it.win.mu.Lock()
	for {
		if it.closed || it.nextIdx >= it.end {
			it.win.mu.Unlock()
			return
		}
		idx := it.nextIdx
		est := it.estimateGroupBytes(idx)
		// The delivery-cursor group is always admitted — token-free and
		// past any window/pressure/ledger gate: the consumer is (or will
		// be) waiting on precisely this index, so reserving past the
		// window here cannot grow it further, and refusing would deadlock
		// on any single group larger than the window. Its bytes are still
		// force-charged to the ledger — real bytes stay visible to spill
		// decisions; overshoot is bounded to one group per chained
		// iterator. Every non-cursor group must clear the window, the
		// pressure hook, the memory ledger, AND acquire a cpu token held
		// only for the decode itself. Wakeups: every delivery and every
		// park broadcasts, and the cursor group is always decodable, so a
		// stalled worker is re-checked at least once per delivered group —
		// which is also why a ledger denial needs no external wakeup:
		// deliveries keep crediting the ledger and re-running this check.
		var holdsToken bool
		isAhead := idx != it.deliver
		if isAhead {
			if it.win.inflight+est > it.win.limit {
				it.windowFullStalls.Add(1)
				it.timedWait(&it.windowFullStallNs)
				continue
			}
			// Occupancy-floored pressure collapse (memo §9.5): under
			// pressure, one non-cursor group may be in flight or parked
			// across the shared window; strict mode (edge envelopes)
			// admits none.
			if it.pressure != nil && (it.pressureStrict || it.win.ahead > 0) && it.pressure() {
				it.pressureStalls.Add(1)
				it.timedWait(&it.pressureStallNs)
				continue
			}
			if it.win.ledger != nil {
				if err := it.win.ledger.Reserve(est); err != nil {
					it.ledgerStalls.Add(1)
					it.timedWait(&it.ledgerStallNs)
					continue
				}
			}
			if it.tokens != nil {
				if it.tokens.TryAcquire(1) == 0 {
					if it.win.ledger != nil {
						it.win.ledger.Release(est)
					}
					it.tokenStalls.Add(1)
					it.timedWait(&it.tokenStallNs)
					continue
				}
				holdsToken = true
			}
		} else if it.win.ledger != nil {
			it.win.ledger.ForceReserve(est)
		}
		it.nextIdx++
		it.win.inflight += est
		if isAhead {
			it.win.ahead++
		}
		// I/O-ahead: claim the advise range for groups up to one worker
		// wave past this assignment while the lock is held, issue the
		// (syscall-bearing) advises after release.
		adviseLo, adviseHi := it.claimAdviseLocked(idx)
		ranges, blooms := it.dynamicRanges, it.bloomFilters
		it.win.mu.Unlock()

		for g := adviseLo; g < adviseHi; g++ {
			it.adviseGroup(g, ranges, blooms)
		}
		slot := &decodeSlot{est: est, ahead: isAhead}
		if pruned := it.pruneGroup(idx, ranges, blooms); !pruned {
			t0 := time.Now()
			b, err := ReadRowGroupNativeCached(it.fr, idx, it.readSchema, nil, it.cache)
			it.decodeSpanNs.Add(time.Since(t0).Nanoseconds())
			it.decodeSpanBytes.Add(it.projectedCompressedBytes(idx))
			if err != nil {
				slot.err = fmt.Errorf("reading row group %d: %w", idx, err)
			} else {
				slot.batch = b // nil for empty (zero-row) groups
				if b != nil {
					it.rgRead.Add(1)
				}
			}
		}
		if holdsToken {
			it.tokens.Release(1)
		}

		it.win.mu.Lock()
		it.slots[idx] = slot
		it.win.cond.Broadcast()
	}
}

// claimAdviseLocked advances the advise cursor to one worker wave past
// the group just assigned, returning the half-open range of groups this
// worker should advise. Covering assignedIdx+workers (rather than
// assignedIdx itself, which is about to be decoded and would gain
// nothing) makes the advice lead decode by roughly one group-decode
// duration — long enough for kernel readahead to land the pages, short
// enough that a saturated LRU hasn't evicted them again. Caller holds
// win.mu.
func (it *DecodeAheadIter) claimAdviseLocked(assignedIdx int) (lo, hi int) {
	if it.advise == nil {
		return 0, 0
	}
	target := assignedIdx + it.workers + 1
	if target > it.end {
		target = it.end
	}
	if target <= it.advisedIdx {
		return 0, 0
	}
	lo, hi = it.advisedIdx, target
	it.advisedIdx = target
	return lo, hi
}

// adviseGroup issues the projected column chunks of one row group to the
// Advise hook. Offsets follow the page-reader convention: a chunk starts
// at its dictionary page when one precedes the data pages, else at the
// first data page, and spans TotalCompressedSize.
//
// Groups the attached dynamic filters would prune are SKIPPED — the same
// metadata-only checks pruneGroup runs at decode time. Advising them was
// merely useless under WILLNEED alone (the kernel is free to ignore it),
// but the touch-ahead worker behind the hook faults ranges in
// unconditionally, and on prune-heavy scans that forced I/O for groups
// no decode will ever read: the 2026-08-09 SF100 touch-ahead pair
// measured Q17 (the heaviest dyn-filter pruner) +96% steady from exactly
// this while every low-prune query improved. The filter snapshot is the
// assigning worker's — a filter arriving between this wave and the
// group's own decode still prunes authoritatively there; skipping here
// is only ever an I/O hint withheld.
func (it *DecodeAheadIter) adviseGroup(rgIdx int, ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	rg := it.fr.RowGroupMeta(rgIdx)
	if rg == nil {
		return
	}
	if len(ranges) > 0 || len(blooms) > 0 {
		stats := it.fr.RowGroupStats(rgIdx)
		if len(ranges) > 0 && CanRangePruneRowGroup(ranges, stats) {
			return
		}
		for _, bf := range blooms {
			if CanBloomPruneRowGroup(bf, stats) {
				return
			}
		}
	}
	for _, li := range it.estLeaves {
		if li < 0 || li >= len(rg.Columns) {
			continue
		}
		md := rg.Columns[li].MetaData
		if md == nil || md.TotalCompressedSize <= 0 {
			continue
		}
		off := md.DataPageOffset
		if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < md.DataPageOffset {
			off = md.DictionaryPageOffset
		}
		it.advise(off, md.TotalCompressedSize)
	}
}

// projectedCompressedBytes sums the projected leaf columns' on-disk
// compressed sizes for one row group — the mmap bytes a decode of that
// group touches, and the denominator of the DecodeSpans ns/byte
// discriminator. Same metadata walk as adviseGroup.
func (it *DecodeAheadIter) projectedCompressedBytes(rgIdx int) int64 {
	rg := it.fr.RowGroupMeta(rgIdx)
	if rg == nil {
		return 0
	}
	var n int64
	for _, li := range it.estLeaves {
		if li < 0 || li >= len(rg.Columns) {
			continue
		}
		if md := rg.Columns[li].MetaData; md != nil && md.TotalCompressedSize > 0 {
			n += md.TotalCompressedSize
		}
	}
	return n
}

// pruneGroup applies the dynamic-filter row-group pruning, mirroring the
// serial path in RowGroupIter.Next.
func (it *DecodeAheadIter) pruneGroup(rgIdx int, ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) bool {
	if len(ranges) == 0 && len(blooms) == 0 {
		return false
	}
	stats := it.fr.RowGroupStats(rgIdx)
	if len(ranges) > 0 && CanRangePruneRowGroup(ranges, stats) {
		it.rgPrunedRange.Add(1)
		return true
	}
	for _, bf := range blooms {
		if CanBloomPruneRowGroup(bf, stats) {
			it.rgPrunedBloom.Add(1)
			return true
		}
	}
	return false
}

// estimateGroupBytes estimates the decoded size of a row group as the
// sum of the projected leaf columns' TotalUncompressedSize — under a
// narrow projection the whole-group TotalByteSize would over-reserve by
// the inverse of the projected fraction and starve the window. Callers
// hold it.mu (reads immutable metadata only; the lock is incidental).
func (it *DecodeAheadIter) estimateGroupBytes(rgIdx int) int64 {
	rg := it.fr.RowGroupMeta(rgIdx)
	if rg == nil {
		return minGroupEstimateBytes
	}
	var est int64
	for _, li := range it.estLeaves {
		if li < 0 || li >= len(rg.Columns) {
			continue
		}
		if md := rg.Columns[li].MetaData; md != nil {
			est += md.TotalUncompressedSize
		}
	}
	if est <= 0 {
		est = rg.TotalByteSize
	}
	if est < minGroupEstimateBytes {
		est = minGroupEstimateBytes
	}
	return est
}

// Close stops assignment, wakes everything, and joins in-flight decodes.
// The caller may munmap the file bytes as soon as Close returns —
// workers never touch the FileReader after the join. Undelivered decoded
// batches are dropped (GC-eligible). Idempotent.
func (it *DecodeAheadIter) Close() error {
	if it == nil || it.empty {
		return nil
	}
	it.win.mu.Lock()
	it.closed = true
	// Release this iterator's undelivered reservations so a chained
	// iterator sharing the window doesn't inherit dead inflight bytes.
	for idx, slot := range it.slots {
		it.win.credit(slot)
		delete(it.slots, idx)
	}
	it.win.cond.Broadcast()
	started := it.started
	it.win.mu.Unlock()
	if started {
		it.wg.Wait()
	}
	// Workers parked slots between the drain above and their exit; drop
	// those reservations too. No new slots can appear once closed.
	it.win.mu.Lock()
	for idx, slot := range it.slots {
		it.win.credit(slot)
		delete(it.slots, idx)
	}
	it.win.cond.Broadcast()
	it.win.mu.Unlock()
	return nil
}
