package physical

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// One object GET, many buffers.
//
// The whole-file read charges a scan's entire parquet file to the query's
// memory tracker and releases it only when the file's LAST row group has been
// decoded (ADR-0006 producer 1). While that charge is resident every other
// operator's admission is measured against a floor that has nothing to do with
// what the query is holding, and how far the scan ran ahead of its consumer
// decides whether the query answers or refuses (#789).
//
// The bytes are read the same way — ONE Get per file, so the request count the
// object store sees is unchanged, which is the recorded decision
// (docs/design/scan-pread-reads.md: "one object GET beats per-chunk ranged
// GETs") — but the body is landed into one buffer per ROW GROUP instead of one
// per file, using the byte ranges the footer already carries. Each buffer is
// charged when it lands and released when its row group has been decoded, so
// the scan's resident charge is the row groups actually in flight rather than
// the file.
//
// The read is DEMAND-DRIVEN and ADMITTED: the stream advances only when a
// decode worker asks for a row group the loader has not reached, and each row
// group's bytes are reserved before they are read — memory.ReserveOrForce,
// the same bounded-wait-then-force every other non-discretionary charge uses,
// so admission can neither fail a load nor deadlock. With room (or no budget)
// every reservation is clean and the read runs at full speed; without room the
// loader waits for the row group ahead of it to decode instead of piling the
// whole file onto the ledger. One row group is always admitted without waiting
// — the floor that stops a scan holding nothing from waiting on itself.
//
// It requires the file's footer to be decoded ALREADY (the process footer
// cache, populated by buildRGUnits' pruning pass): the row-group byte ranges
// live in it, and reading it from the object separately would be the second
// request this design exists to avoid. A file whose row-group metadata came
// from the catalog's persisted blob has no footer in that cache and keeps the
// whole-file path.
//
// KNOWN BOUNDARY — the object body is held open across the file's decode.
// The whole-file read did Get, read, Close in one span; this one opens the
// body in `advance` and closes it in `close`, which runs when the file's last
// row group has decoded. Between them the read is paced by the decode and, at
// a tight budget, by admission — `fileLoadReserveWait` is 2 s per row group —
// so a many-row-group file can hold one HTTP body open across a mostly idle
// socket, times up to the load gate's lanes. MemStore and FileStore cannot
// observe this and no S3 or MinIO run was made for it, so the cost is stated
// rather than measured: a server-side idle reap becomes a mid-file read error
// where it was previously impossible, and that error fails the query loudly
// (`read <path> row group N: ...`) rather than answering short. Re-issuing the
// Get from `s.pos` on a mid-stream read error is the fix if it is ever seen;
// it is not written on speculation.

// ScanRowGroupBuffers is the kill switch for row-group-at-a-time file loads.
// Off, every scan takes the whole-file read this replaced.
var ScanRowGroupBuffers = optswitch.Register("scan-rg-buffers", "WADJET_SCAN_RG_BUFFERS",
	"land a scan's parquet file into one buffer per row group, charged and released per row group, instead of one whole-file buffer")

// Engagement counters. A row set cannot tell "read row group at a time" from
// "read whole file" — both answer identically, which is the point — so the
// gates assert the counter beside the rows.
var (
	rgLoadsRowGroup  atomic.Int64
	rgLoadsWholeFile atomic.Int64

	// rgBuffersResident is the process-wide count of row-group buffers held
	// right now, across every scan. It is what a leak looks like from
	// outside: after the last query of a process has finished, it is zero.
	rgBuffersResident atomic.Int64

	// rgSlabReuses counts buffers handed back out of a slab pool. The
	// aliasing gate asserts it moved during its scan: a scan that allocated
	// every buffer fresh never recycled one under a live batch, so it would
	// have proved nothing.
	rgSlabReuses atomic.Int64

	// rgSlabAllocs counts row-group buffers allocated rather than reused.
	rgSlabAllocs atomic.Int64

	// rgSlabReleases counts row-group buffers returned to the pool — the
	// moment a batch decoded from one becomes able to outlive it. The
	// aliasing gate asserts this moved during its scan, which is a property
	// of the SCAN and not of sync.Pool's willingness to hand a buffer back.
	rgSlabReleases atomic.Int64
)

// RowGroupSlabReleases is how many row-group buffers this process has returned
// to the pool. See rgSlabReleases.
func RowGroupSlabReleases() int64 { return rgSlabReleases.Load() }

// RowGroupSlabAllocs is how many row-group buffers this process has allocated
// rather than taken from a pool. See rgSlabAllocs.
func RowGroupSlabAllocs() int64 { return rgSlabAllocs.Load() }

// ResetSlabPoolsForTest empties every size-class bucket. TEST-ONLY: the pool is
// process-wide, so a gate that asserts an exact reuse or allocation count has
// to start from a state it owns — otherwise it reads whatever the test before
// it left in the bucket, which made the reuse gate fail two runs in three
// under -race.
func ResetSlabPoolsForTest() {
	slabPoolMu.Lock()
	defer slabPoolMu.Unlock()
	slabPools = map[int]*sync.Pool{}
}

// RowGroupSlabReuses is how many row-group buffers this process has taken back
// out of a pool. See rgSlabReuses.
func RowGroupSlabReuses() int64 { return rgSlabReuses.Load() }

// RowGroupLoadStats returns how many parquet file loads this process has done
// row group at a time and how many took the whole-file read, since start.
func RowGroupLoadStats() (rowGroup, wholeFile int64) {
	return rgLoadsRowGroup.Load(), rgLoadsWholeFile.Load()
}

// RowGroupBuffersResident is how many row-group buffers this process is
// holding. Zero between queries; anything else is a buffer, and the tracker
// charge that goes with it, that no scan gave back.
func RowGroupBuffersResident() int64 { return rgBuffersResident.Load() }

// rgSlabs holds one parquet file's bytes one row group at a time and feeds
// them to a parquet.FileReader in row-group mode.
//
// Locking: loadMu serializes advancing the stream and is held across the
// admission wait; mu guards the residency maps, and is the ONLY lock a
// decoding worker's release takes — so a release can always proceed while
// another worker waits for the room that release is about to create.
type rgSlabs struct {
	inner  *scanSourceInner
	slot   *fileSlot
	ctx    context.Context
	want   []int   // row groups that survived pruning, ascending
	starts []int64 // byte range of want[i]
	ends   []int64

	loadMu  sync.Mutex
	body    io.ReadCloser
	pos     int64 // file offset the body is positioned at
	next    int   // index into want of the next row group to load
	loadErr error

	mu      sync.Mutex
	bufs    map[int][]byte
	bases   map[int]int64
	charges map[int]int64
	// forcedCharge[rg] records that this row group's bytes were taken PAST
	// the budget, so its release comes off the forced census rather than off
	// the plain counter (memory/forced.go).
	forcedCharge map[int]bool
	released     map[int]bool
	closed       bool
}

// rowGroupRanges is the ascending byte range of every row group in want, or
// ok=false when this file cannot be read row group at a time: a row group with
// no bytes, ranges not in ascending file order (validateChunkLayoutSorted's
// case — no writer in the corpus produces one), or a footer whose numbers the
// reader refuses. The caller then takes the whole-file read, which is always
// correct.
func rowGroupRanges(rdr *parquet.FileReader, want []int) (starts, ends []int64, ok bool) {
	starts = make([]int64, len(want))
	ends = make([]int64, len(want))
	var prevEnd int64
	for i, rg := range want {
		start, end, has, err := rdr.RowGroupByteRange(rg)
		if err != nil || !has || start < prevEnd {
			return nil, nil, false
		}
		starts[i], ends[i] = start, end
		prevEnd = end
	}
	return starts, ends, true
}

// RowGroupBytes implements parquet.RowGroupBytes: the buffer holding rgIdx's
// bytes and the file offset it starts at, advancing the stream when the loader
// has not reached rgIdx yet. Called once per projected column of the row
// group; only the first call does any work.
func (s *rgSlabs) RowGroupBytes(rgIdx int) ([]byte, int64, error) {
	if buf, base, ok, err := s.resident(rgIdx); ok || err != nil {
		return buf, base, err
	}
	// Advance the stream. One worker reads at a time; the others wait here
	// and re-check, because the row group they want may be the one that
	// worker just landed.
	s.loadMu.Lock()
	defer s.loadMu.Unlock()
	for {
		if buf, base, ok, err := s.resident(rgIdx); ok || err != nil {
			return buf, base, err
		}
		if s.loadErr != nil {
			return nil, 0, s.loadErr
		}
		if err := s.advance(); err != nil {
			s.loadErr = err
			return nil, 0, err
		}
	}
}

// resident reports rgIdx's buffer when it is held, or the reason it will never
// arrive. ok=false with a nil error means "not yet".
func (s *rgSlabs) resident(rgIdx int) (buf []byte, base int64, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, held := s.bufs[rgIdx]; held {
		return b, s.bases[rgIdx], true, nil
	}
	if s.closed {
		return nil, 0, false, fmt.Errorf("scan of %s was torn down before row group %d was read",
			s.slot.entry.Path, rgIdx)
	}
	if s.released[rgIdx] {
		// A released row group's buffer went back to the pool and may
		// already hold another file's bytes. Never hand it out again.
		return nil, 0, false, fmt.Errorf("row group %d of %s was already released",
			rgIdx, s.slot.entry.Path)
	}
	return nil, 0, false, nil
}

// advance loads exactly one more row group from the body. Caller holds loadMu
// and no other lock.
func (s *rgSlabs) advance() error {
	s.mu.Lock()
	if s.next >= len(s.want) {
		s.mu.Unlock()
		return fmt.Errorf("scan of %s asked for a row group the stream has already passed",
			s.slot.entry.Path)
	}
	i := s.next
	rg := s.want[i]
	skip := s.released[rg]
	s.mu.Unlock()

	start, end := s.starts[i], s.ends[i]
	if s.body == nil {
		rc, _, err := s.inner.cat.Store().Get(s.ctx, s.inner.cat.Bucket(), s.slot.entry.Path)
		if err != nil {
			return fmt.Errorf("get %s: %w", s.slot.entry.Path, err)
		}
		s.body = rc
		s.pos = 0
	}
	if start > s.pos {
		if _, err := io.CopyN(io.Discard, s.body, start-s.pos); err != nil {
			return fmt.Errorf("read %s: skipping to row group %d: %w", s.slot.entry.Path, rg, err)
		}
		s.pos = start
	}
	n := end - start
	if skip {
		// The decode path finished with this row group before its bytes were
		// ever demanded (a dictionary or filter prune). Step over it rather
		// than charging a buffer nobody will read.
		if _, err := io.CopyN(io.Discard, s.body, n); err != nil {
			return fmt.Errorf("read %s: skipping row group %d: %w", s.slot.entry.Path, rg, err)
		}
		s.pos = end
		s.mu.Lock()
		s.next++
		s.mu.Unlock()
		return nil
	}

	// Reserve BEFORE the allocation, so admission and spill pressure see the
	// load coming instead of discovering it as drift — the same order and the
	// same call the whole-file load uses, at row-group granularity, and
	// released at row-group granularity. A scan holding nothing never waits:
	// the floor that makes this deadlock-free.
	wait := fileLoadReserveWait
	if s.inner.residentSlabs.Load() == 0 {
		wait = 0
	}
	forced := memory.ReserveOrForce(s.ctx, s.inner.memTracker, s.inner.spillMgr, n, wait,
		memory.ForceScanFileLoad)

	// The charge stands at the row group's own byte range. The buffer it is
	// held in is a power-of-two class of that range, so it is at most twice as
	// big, and that slack is pool capacity — bounded by the row group itself
	// and handed to the next row group — rather than this query's memory.
	// ADR-0006's producer row 2 records the decision and its bound.
	buf := s.inner.getSlab(int(n))
	charge := n

	if _, err := io.ReadFull(s.body, buf[:n]); err != nil {
		s.inner.putSlab(buf)
		s.releaseCharge(charge, forced)
		return fmt.Errorf("read %s row group %d: %w", s.slot.entry.Path, rg, err)
	}
	s.pos = end
	s.mu.Lock()
	s.bufs[rg] = buf[:n]
	s.bases[rg] = start
	s.charges[rg] = charge
	s.forcedCharge[rg] = forced
	s.next++
	s.mu.Unlock()
	s.inner.residentSlabs.Add(1)
	rgBuffersResident.Add(1)
	return nil
}

// releaseCharge gives a row group's bytes back. forced says whether they were
// taken PAST the budget: the forced census counts OUTSTANDING bytes, so the two
// halves come off different counters (memory/forced.go).
func (s *rgSlabs) releaseCharge(n int64, forced bool) {
	if n <= 0 || s.inner.memTracker == nil {
		return
	}
	if forced {
		s.inner.memTracker.ReleaseForced(n, memory.ForceScanFileLoad)
		return
	}
	s.inner.memTracker.Release(n)
}

// release frees row group rgIdx's buffer and its tracker charge. Idempotent,
// and safe to call for a row group whose bytes were never demanded.
func (s *rgSlabs) release(rgIdx int) {
	s.mu.Lock()
	if s.released[rgIdx] {
		s.mu.Unlock()
		return
	}
	s.released[rgIdx] = true
	buf, had := s.bufs[rgIdx]
	delete(s.bufs, rgIdx)
	delete(s.bases, rgIdx)
	n := s.charges[rgIdx]
	delete(s.charges, rgIdx)
	forced := s.forcedCharge[rgIdx]
	delete(s.forcedCharge, rgIdx)
	s.mu.Unlock()

	if had {
		s.inner.putSlab(buf)
		s.inner.residentSlabs.Add(-1)
		rgBuffersResident.Add(-1)
	}
	s.releaseCharge(n, forced)
}

// close releases every buffer and charge still held and closes the body. Safe
// to call more than once. Callers must ensure no decode is still READING a
// buffer (the rg workers have exited).
//
// It takes loadMu first and holds it throughout, which is the same order
// RowGroupBytes takes (loadMu, then mu) and is what makes it safe against a
// loader mid-advance: a slab published into the maps after they were emptied
// would be a buffer and a charge nobody ever releases.
func (s *rgSlabs) close() {
	s.loadMu.Lock()
	defer s.loadMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	bufs, charges, forced := s.bufs, s.charges, s.forcedCharge
	s.bufs, s.bases, s.charges = map[int][]byte{}, map[int]int64{}, map[int]int64{}
	s.forcedCharge = map[int]bool{}
	s.mu.Unlock()

	var clean, forcedTotal int64
	for rg, buf := range bufs {
		s.inner.putSlab(buf)
		s.inner.residentSlabs.Add(-1)
		rgBuffersResident.Add(-1)
		if forced[rg] {
			forcedTotal += charges[rg]
		} else {
			clean += charges[rg]
		}
	}
	s.releaseCharge(forcedTotal, true)
	s.releaseCharge(clean, false)

	if s.body != nil {
		s.body.Close()
		s.body = nil
	}
}

// poisonReleasedSlabs makes putSlab overwrite every buffer as it is RELEASED.
// TEST-ONLY: it is how a gate observes the hazard this design introduces — a
// row group's buffer goes back to the pool the moment THAT row group has
// decoded, while batches decoded from it are still travelling downstream. If
// any decoded value aliased the buffer instead of being copied out, the poison
// lands in a live batch.
//
// On RELEASE rather than on reuse, deliberately: reuse depends on sync.Pool
// choosing to hand the buffer back, which it may decline at any GC, so a gate
// waiting for a reuse is a gate that sometimes tests nothing. A release
// happens every time, so the hazard is observed every time.
var poisonReleasedSlabs atomic.Bool

// PoisonReleasedSlabs turns that on and returns the previous setting.
// Test-only in spirit; production never calls it.
func PoisonReleasedSlabs(on bool) (prev bool) { return poisonReleasedSlabs.Swap(on) }

// getSlab and putSlab reuse row-group buffers within one scan source, in
// buckets that are power-of-two size classes OF THE ROW GROUP'S OWN byte
// range.
//
// What a row group's buffer may be is not a matter of taste: it is charged to
// the query's memory budget, so a buffer bigger than the row group is a charge
// for memory the row group does not need. Three shapes were tried and each
// failed at one end or the other, which is why the rule that ships states an
// invariant instead of a preference:
//
//   - The process-wide readBufPool, whose only rule is "big enough". It also
//     holds whole-FILE buffers, so a row-group request draws one and the
//     charge becomes the largest file the process ever read.
//   - The parquet chunk pool's size classes. They have a 64 KiB FLOOR, so a
//     5 KiB row group is held in 64 KiB — a fixed floor is a tuning constant
//     with a pool's manners, and the gates caught it.
//   - One "big enough" pool per scan SOURCE. A source reads one TABLE, and a
//     table's files do not share a row-group size: a compacted file beside a
//     freshly ingested one is ordinary. Measured, a 332-byte row group of a
//     1,988-byte file drew a 105,900-byte buffer another file left behind and
//     was charged for it — 319x the row group, 53x the file.
//   - Bucketing by the FILE's exact largest row group fixed that but keyed on
//     a byte count that compression makes different for every file, so no two
//     files shared a bucket and every row group allocated: +29.2% heap over
//     the TPC-H SF1 suite, separated across five pairs.
//
// THE INVARIANT: a row group is CHARGED its own byte range, and HELD in a
// buffer of at most twice that. The class is derived from the row group, so
// there is no floor and no chosen number, and it decides only WHICH BUCKET a
// buffer is reused from — a buffer is always allocated at exactly the row
// group's size. Both halves are load-bearing: without the bucket, another
// file's shape serves this row group (319x); allocating AT the class instead
// of at the row group rounds every fresh buffer up to a power of two, which
// measured +6.6% suite heap (see getSlab). Two row groups in one class differ by less than
// 2x, which is where the bound comes from. The slack between the charge and
// the buffer is pool capacity, bounded by the row group itself and reused
// across the scan; ADR-0006's producer row 2 records that the row-group path
// does not reconcile the charge up to it, and why.
//
// Gated by TestARowGroupIsHeldInABufferAtMostTwiceItsSize (the invariant, over
// sizes from one byte to a megabyte) and
// TestOneSourceWithTwoRowGroupSizesChargesEachRowGroupItsOwnBytes (the charge,
// end to end over a table whose files have different row-group sizes).

// slabClass is the smallest power of two at least n — the row group's own size
// class. No minimum: a 332-byte row group's class is 512, not a floor.
func slabClass(n int) int {
	if n <= 1 {
		return 1
	}
	c := 1
	for c < n {
		c <<= 1
	}
	return c
}

func (inner *scanSourceInner) getSlab(n int) []byte {
	// This class, then the one above it. A scan's row groups straddle a class
	// boundary as soon as compression moves them a few percent across one, and
	// looking only in this class keeps two working sets alive where one would
	// do. The upper bucket is admitted only when the buffer still satisfies
	// the invariant, so probing it cannot hold a row group in more than twice
	// its bytes.
	//
	// A BUCKET'S MEMBERS CAN DIFFER IN CAPACITY, because a buffer is allocated
	// at the ROW GROUP's size and not at the class's, so one Get can draw a
	// buffer too small for this request and miss. That is deliberate and it is
	// measured: allocating every buffer at the class makes any member serve
	// any request of the class — one Get, no miss — and costs **+6.6% suite
	// heap over TPC-H SF1, separated across five base/tip pairs**, because
	// sync.Pool sheds at every GC, so the rounding is paid again and again
	// rather than once per class. (Its wall reading in that run, +10.9%, is
	// not a second cost: the arms were measured in one order, and alternating
	// them gave −1.1%. Heap is the number that separated; wall did not.) The
	// miss costs one allocation of exactly what the row group needs; the
	// uniformity costs a fraction of every buffer forever. The cheaper of the
	// two is the one that ships.
	class := slabClass(n)
	for _, c := range [2]int{class, 2 * class} {
		p := slabBucket(c)
		if v := p.Get(); v != nil {
			buf := v.([]byte)
			if cap(buf) >= n && cap(buf) <= 2*n {
				rgSlabReuses.Add(1)
				return buf[:n]
			}
			p.Put(buf) // not ours: too small, or bigger than the bound allows
		}
	}
	rgSlabAllocs.Add(1)
	return make([]byte, n)
}

func (inner *scanSourceInner) putSlab(buf []byte) {
	if cap(buf) == 0 {
		return
	}
	b := buf[:cap(buf)]
	if poisonReleasedSlabs.Load() {
		for i := range b {
			b[i] = 0xEE
		}
	}
	rgSlabReleases.Add(1)
	slabBucket(slabClass(cap(b))).Put(b)
}

// slabBucket returns the pool for a size class, creating it on first use.
//
// PROCESS-wide, like the whole-file read's readBufPool and for the same
// reason: a scan source lives for one query, so a pool that lives with it is
// cold at every query and the scan allocates its whole working set again —
// measured at +8.4% suite heap over TPC-H SF1 against the whole-file read,
// whose pool is warm from the query before. What makes a shared pool safe here
// is the CLASS: readBufPool mixes whole-file buffers with row-group ones and
// hands out whatever is big enough, which is how a 332-byte row group came to
// be held in 105,900 bytes; a class bucket can only ever hand back a buffer
// within a factor of two of what was asked for.
//
// A handful of classes exist in any process, so the map stays tiny, and
// sync.Pool sheds idle buffers at GC exactly as readBufPool's do.
var (
	slabPoolMu sync.Mutex
	slabPools  = map[int]*sync.Pool{}
)

func slabBucket(class int) *sync.Pool {
	slabPoolMu.Lock()
	defer slabPoolMu.Unlock()
	p := slabPools[class]
	if p == nil {
		p = &sync.Pool{}
		slabPools[class] = p
	}
	return p
}

// tryRowGroupLoad builds the row-group-at-a-time backing for this slot when
// everything it needs is already in hand: a decoded footer for this exact
// object and row groups whose byte ranges are usable. Returns nil when the
// caller should take the whole-file path instead. It installs nothing on the
// slot — the caller does that once it has the load gate's admission.
func (s *fileSlot) tryRowGroupLoad(inner *scanSourceInner, ctx context.Context) (*parquet.FileReader, *rgSlabs) {
	if !ScanRowGroupBuffers.On() || len(s.wantRG) == 0 || s.entry.SizeBytes <= 0 || inner.cat == nil {
		return nil, nil
	}
	// The footer must already be decoded: reading it from the object here
	// would be a second request per file, which is the decision this design
	// keeps.
	//
	// The identity carries the SIZE, and every site that populates the cache
	// keys it by the authoritative size (the one GetReaderAt or the body
	// itself reports), never by the manifest's hint. So a hit here means the
	// manifest's SizeBytes and the object agree, which is what the byte ranges
	// below are measured against; a stale manifest entry misses and takes the
	// whole-file read, which handles a wrong size explicitly.
	rdr := parquet.LookupFooter(footerCacheIdentity(inner.cat, s.entry, s.entry.SizeBytes))
	if rdr == nil {
		return nil, nil
	}
	rdr.SetRowGroupBytes(nil, s.entry.SizeBytes) // size first: the ranges are measured against it
	starts, ends, ok := rowGroupRanges(rdr, s.wantRG)
	if !ok {
		return nil, nil
	}
	slabs := &rgSlabs{
		inner: inner, slot: s, ctx: ctx,
		want: s.wantRG, starts: starts, ends: ends,
		bufs:     make(map[int][]byte, len(s.wantRG)),
		bases:    make(map[int]int64, len(s.wantRG)),
		charges:  make(map[int]int64, len(s.wantRG)),
		released: make(map[int]bool, len(s.wantRG)),

		forcedCharge: make(map[int]bool, len(s.wantRG)),
	}
	rdr.SetRowGroupBytes(slabs, s.entry.SizeBytes)
	return rdr, slabs
}

// peakRowGroupBytes is the largest row group this slot can hold at once — what
// the byte-budgeted load gate is admitting when the slot reads a row group at
// a time instead of the whole file.
func peakRowGroupBytes(starts, ends []int64) int64 {
	var peak int64
	for i := range starts {
		if n := ends[i] - starts[i]; n > peak {
			peak = n
		}
	}
	return peak
}

var _ parquet.RowGroupBytes = (*rgSlabs)(nil)
