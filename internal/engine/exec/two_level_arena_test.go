package exec

import (
	"testing"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// Shared-arena accounting for the two-level group index. The 256 buckets are
// carved out of ONE off-heap reservation; a bucket that grows out of it hands
// its pages back and stops being charged, and the mapping goes when the last
// one leaves. Before that, MemoryUsage charged the whole reservation ON TOP
// OF every departed bucket's new array.

// TestTwoLevelArenaChargesLiveBucketsOnly covers the accounting half: the 256
// buckets share ONE reservation, and a bucket that grows out of it returns
// its pages (releaseArenaSlot → memory.DiscardSlice). MemoryUsage must then
// charge the arena by the buckets that still slice it, not by the whole
// mapping plus every departed bucket's new array — which double-charged the
// vacated fraction for as long as 0 < arenaLive < 256, i.e. across every
// conversion that grows buckets inside its own loop.
func TestTwoLevelArenaChargesLiveBucketsOnly(t *testing.T) {
	reg := memory.NewOffheapRegistry()
	defer reg.Close()
	// 256 × 512 × 16 B = 2 MiB — exactly offheapSubMinBytes, so the shared
	// arena engages.
	const capPerSub = 512
	tbl := newIntTwoLevelTableSub(capPerSub, reg)
	if tbl.arena == nil {
		t.Skip("off-heap unavailable on this platform/build; the arena path is linux-only")
	}
	if tbl.arenaLive != twoLevelBuckets || tbl.arenaFreed != 0 {
		t.Fatalf("arena bookkeeping: live=%d freed=%d, want %d/0",
			tbl.arenaLive, tbl.arenaFreed, twoLevelBuckets)
	}
	entryBytes := int64(unsafe.Sizeof(intHashEntry{}))
	headerBytes := int64(unsafe.Sizeof(tbl.subs))

	want := int64(twoLevelBuckets*capPerSub)*entryBytes + headerBytes
	if got := tbl.MemoryUsage(); got != want {
		t.Fatalf("fresh table MemoryUsage = %d, want %d", got, want)
	}

	// Grow two buckets out of the arena.
	tbl.growSub(0)
	tbl.growSub(7)
	if tbl.arenaLive != twoLevelBuckets-2 {
		t.Fatalf("arenaLive = %d after 2 growSub, want %d", tbl.arenaLive, twoLevelBuckets-2)
	}
	// Each bucket is 512 slots x 16 B = 8 KiB = two whole pages starting on
	// a page boundary, so both vacated buckets went back to the kernel in
	// full: the arena is charged for the 254 that remain.
	if want := int64(2*capPerSub) * entryBytes; tbl.arenaFreed != want {
		t.Fatalf("arenaFreed = %d, want %d (two whole 8 KiB buckets)", tbl.arenaFreed, want)
	}
	want = int64((twoLevelBuckets-2)*capPerSub+2*2*capPerSub)*entryBytes + headerBytes
	if got := tbl.MemoryUsage(); got != want {
		t.Fatalf("partially-vacated arena MemoryUsage = %d, want %d "+
			"(live arena buckets + the two grown arrays, each counted once)", got, want)
	}
	// The pre-fix charge — whole arena + both new arrays — is what this
	// replaces; make the difference explicit so a regression is legible.
	stale := int64(twoLevelBuckets*capPerSub+2*2*capPerSub)*entryBytes + headerBytes
	if want >= stale {
		t.Fatalf("expected the fix to charge LESS than the whole-arena form (%d vs %d)", want, stale)
	}
}

// TestTwoLevelArenaReleasedWhenLastBucketLeaves keeps the lifecycle honest:
// once every bucket has grown out, the reservation itself is unmapped and the
// table charges only the buckets' own arrays.
func TestTwoLevelArenaReleasedWhenLastBucketLeaves(t *testing.T) {
	reg := memory.NewOffheapRegistry()
	defer reg.Close()
	const capPerSub = 512
	tbl := newIntTwoLevelTableSub(capPerSub, reg)
	if tbl.arena == nil {
		t.Skip("off-heap unavailable on this platform/build")
	}
	for b := 0; b < twoLevelBuckets; b++ {
		tbl.growSub(uint64(b))
	}
	if tbl.arena != nil || tbl.arenaLive != 0 {
		t.Fatalf("arena still held after every bucket left: arena=%v live=%d",
			tbl.arena != nil, tbl.arenaLive)
	}
	entryBytes := int64(unsafe.Sizeof(intHashEntry{}))
	want := int64(twoLevelBuckets*2*capPerSub)*entryBytes + int64(unsafe.Sizeof(tbl.subs))
	if got := tbl.MemoryUsage(); got != want {
		t.Fatalf("MemoryUsage = %d, want %d", got, want)
	}
}
