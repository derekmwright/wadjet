package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// withTwoLevel runs fn with the kill switch and the conversion threshold
// forced, restoring both. A low threshold is what lets a unit-scale test
// exercise the bucketed path at all.
// withTwoLevel runs fn with the bucketed index forced on/off at a given
// size threshold. It stands in for the WADJET_TWO_LEVEL_AT override and
// carries the same eager-conversion semantics: an explicit threshold means
// "convert on size alone", because a few-thousand-group test table never
// reaches the doubling the shipped gate waits for. The shipped gate itself
// is pinned by TestTwoLevelConvertsAtTheDoubling.
func withTwoLevel(tb testing.TB, on bool, convertAt int, fn func()) {
	tb.Helper()
	prevOn := twoLevelToggle.Set(on)
	prevAt, prevEager := twoLevelConvertAt, twoLevelConvertEager
	twoLevelConvertAt, twoLevelConvertEager = convertAt, true
	defer func() {
		twoLevelToggle.Set(prevOn)
		twoLevelConvertAt, twoLevelConvertEager = prevAt, prevEager
	}()
	fn()
}

// --- bit budget ----------------------------------------------------------

// TestTwoLevelBitBudgetWindowsAreDisjoint pins the arithmetic claim in
// two_level_hash.go's header: the partition window (top bits), the slot
// window (bits 8..8+log2(subcap)) and the bucket window (low 8) cannot
// overlap for any table this code can build.
func TestTwoLevelBitBudgetWindowsAreDisjoint(t *testing.T) {
	// Slot window top edge, worst case allowed by growSub's cap.
	slotTop := twoLevelSlotShift + twoLevelMaxSubBits - 1
	// Partition window bottom edge for the largest plausible worker count.
	const maxParts = 4096
	partBits := 0
	for 1<<partBits < maxParts {
		partBits++
	}
	partBottom := 64 - partBits
	if slotTop >= partBottom {
		t.Fatalf("slot window reaches bit %d, partition window starts at bit %d — windows overlap",
			slotTop, partBottom)
	}
	if twoLevelSlotShift != 8 || twoLevelBucketMask != 255 {
		t.Fatalf("bucket window changed (shift=%d mask=%d) — update the bit budget doc",
			twoLevelSlotShift, twoLevelBucketMask)
	}
	// The bucket window must sit strictly below the slot window.
	for h := uint64(0); h < 256; h++ {
		if bucketOf(h) != h {
			t.Fatalf("bucketOf(%d) = %d — bucket is not the low 8 bits", h, bucketOf(h))
		}
	}
}

// TestTwoLevelMatchesFlatCollisions is the property that makes the bucket =
// low-8-bits choice safe: (bucket, slot) together are exactly the low
// 8+log2(subcap) bits, so two keys collide in the bucketed table iff they
// would have collided in a FLAT table of 256*subcap slots. Everything the
// flat tables' spread analysis rests on therefore carries over unchanged.
func TestTwoLevelMatchesFlatCollisions(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	for _, subBits := range []uint{4, 8, 12} {
		subCap := uint64(1) << subBits
		flatCap := subCap * twoLevelBuckets
		for i := 0; i < 20000; i++ {
			a, b := rng.Uint64(), rng.Uint64()
			if i%3 == 0 {
				// Force frequent agreement on the low bits.
				b = (a & (flatCap - 1)) | (rng.Uint64() &^ (flatCap - 1))
			}
			twoLevelSame := bucketOf(a) == bucketOf(b) &&
				(a>>twoLevelSlotShift)&(subCap-1) == (b>>twoLevelSlotShift)&(subCap-1)
			flatSame := a&(flatCap-1) == b&(flatCap-1)
			if twoLevelSame != flatSame {
				t.Fatalf("subBits=%d a=%#x b=%#x: two-level collide=%v, flat collide=%v",
					subBits, a, b, twoLevelSame, flatSame)
			}
		}
	}
}

// TestTwoLevelThreeWindowSpread extends the G5 spread sweep
// (TestUnifiedHashSpread) to the three-window claim: for each key family the
// partition bits must divide the keys evenly across owners, the bucket bits
// must divide them evenly across sub-tables — including INSIDE a fixed
// partition, which is the disjointness claim — and the slot bits must spread
// inside a bucket, measured as the average linear-probe count of filling a
// sub-table sized for its own share.
//
// Every family clears every bound now. The strided int family used to be
// EXCLUDED from the bucket bound because it landed entirely in one bucket:
// fibHash was multiply-only, so keys that are all multiples of 2^s share
// their low bits, and both the bucket and the slot window read low bits.
// #306's mixing step folds the high bits down, and the only concession left
// is ownerBucketSkew below.
func TestTwoLevelThreeWindowSpread(t *testing.T) {
	const n = 1 << 17
	const parts = 12 // NumCPU-shaped, not a power of two

	families := []struct {
		name string
		// lowBitSpread: whether this family's LOW hash bits spread at all.
		// Every family does now. The stride-2^k integers did not until #306
		// gave fibHash a mixing step: their low bits were identically zero,
		// and since both the bucket window and the slot window read low
		// bits, the family collapsed onto one bucket AND one probe chain,
		// exactly as it collapsed in the flat table. The field stays so a
		// future unspreadable family can declare itself rather than silently
		// fail the bucket bound.
		lowBitSpread bool
		// ownerBucketSkew is the numerator of the per-owner bucket bound
		// (bound = mean * skew/2). fibHash's mixing step is a LINEAR fold,
		// not an avalanche, so for a STRIDED family the top window (the
		// owner) and the low window (the bucket) stay correlated, and
		// conditioning on one skews the other — measured at 1.8x the mean
		// where an avalanching hash gives 1.0x. The guard is against a family
		// COLLAPSING onto one bucket, which is exactly what the bare multiply
		// did to this family (every key in one bucket); 1.8x is not that.
		// Avalanching instead was measured and rejected — see fibHash.
		ownerBucketSkew int
		hashes          []uint64
	}{
		{"int-dense", true, 3, genHashes(n, func(i int) uint64 { return fibHash(int64(i)) })},
		{"int-negative", true, 3, genHashes(n, func(i int) uint64 { return fibHash(int64(-i)) })},
		{"int-scaled", true, 3, genHashes(n, func(i int) uint64 { return fibHash(int64(i) * 2654435761) })},
		{"int-strided-64k", true, 5, genHashes(n, func(i int) uint64 { return fibHash(int64(i) << 16) })},
		{"packed-two-dense", true, 3, genHashes(n, func(i int) uint64 { return packedHash(uint64(i)*2654435761, uint64(i)) })},
		{"packed-hi-word", true, 3, genHashes(n, func(i int) uint64 { return packedHash(0, uint64(i)) })},
	}

	// Bounds are several times the mean: they catch a family collapsing onto
	// one bucket/owner, not ordinary hashing variance.
	partBound := n / parts * 3 / 2
	bucketBound := n / twoLevelBuckets * 3 / 2
	const probeBound = 4.0 // measured worst across these families is ~3.3

	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			if w := worstBucket(f.hashes, parts, func(h uint64) int { return int(partitionFor(h, parts)) }); w > partBound {
				t.Errorf("partition bits: worst of %d owners holds %d of %d keys", parts, w, n)
			}
			if !f.lowBitSpread {
				return
			}
			{
				if w := worstBucket(f.hashes, twoLevelBuckets, func(h uint64) int { return int(bucketOf(h)) }); w > bucketBound {
					t.Errorf("bucket bits: worst of %d buckets holds %d of %d keys", twoLevelBuckets, w, n)
				}
				// Disjointness: fixing the OWNER must not skew the bucket bits.
				for p := 0; p < parts; p++ {
					var inPart []uint64
					for _, h := range f.hashes {
						if int(partitionFor(h, parts)) == p {
							inPart = append(inPart, h)
						}
					}
					if len(inPart) == 0 {
						t.Fatalf("owner %d received no keys", p)
					}
					bound := len(inPart)/twoLevelBuckets*f.ownerBucketSkew/2 + 8
					if w := worstBucket(inPart, twoLevelBuckets, func(h uint64) int { return int(bucketOf(h)) }); w > bound {
						t.Errorf("owner %d bucket bits: worst of %d buckets holds %d of %d keys",
							p, twoLevelBuckets, w, len(inPart))
					}
				}
			}
			// Slot bits INSIDE each bucket: fill a sub-table sized for the
			// bucket's own share and count linear probes. This is the window
			// pair that the naive middle-bits bucket choice breaks.
			if avg := avgProbesPerBucket(f.hashes); avg > probeBound {
				t.Errorf("slot bits: %.2f average probes per insert inside a bucket (bound %.1f)", avg, probeBound)
			}
		})
	}
}

// avgProbesPerBucket simulates the bucketed table: split by bucketOf, size
// each sub-table at the 70% load factor, and fill it by linear probing on
// the slot window. Returns the mean probes per insert.
func avgProbesPerBucket(hashes []uint64) float64 {
	buckets := make([][]uint64, twoLevelBuckets)
	for _, h := range hashes {
		b := bucketOf(h)
		buckets[b] = append(buckets[b], h)
	}
	totalProbes, totalKeys := 0, 0
	for _, bk := range buckets {
		if len(bk) == 0 {
			continue
		}
		c := uint64(subCapFor(len(bk) * twoLevelBuckets))
		occupied := make([]bool, c)
		for _, h := range bk {
			idx := (h >> twoLevelSlotShift) & (c - 1)
			totalProbes++
			for occupied[idx] {
				idx = (idx + 1) & (c - 1)
				totalProbes++
			}
			occupied[idx] = true
		}
		totalKeys += len(bk)
	}
	if totalKeys == 0 {
		return 0
	}
	return float64(totalProbes) / float64(totalKeys)
}

// worstBucket is shared with the G5 spread sweep; redeclared here would
// collide, so this file uses the one in agg_hash_once_test.go.

// --- table-level parity --------------------------------------------------

func TestIntTwoLevelTableMapParity(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	ref := map[int64]int32{}
	tl := newIntTwoLevelTable(64, nil)
	for i := 0; i < 60000; i++ {
		k := int64(rng.Intn(9000) - 4000)
		want, exists := ref[k]
		next := int32(len(ref))
		got, found := tl.GetOrInsert(k, next)
		if exists != found {
			t.Fatalf("key %d: found=%v want %v", k, found, exists)
		}
		if !exists {
			ref[k] = next
			want = next
		}
		if got != want {
			t.Fatalf("key %d: got %d want %d", k, got, want)
		}
	}
	if tl.Len() != len(ref) {
		t.Fatalf("Len = %d, want %d", tl.Len(), len(ref))
	}
	// Every key readable, and ForEach sees exactly the live set.
	seen := map[int64]int32{}
	tl.ForEach(func(k int64, v int32) {
		if _, dup := seen[k]; dup {
			t.Fatalf("key %d emitted twice by ForEach", k)
		}
		seen[k] = v
	})
	if len(seen) != len(ref) {
		t.Fatalf("ForEach saw %d entries, want %d", len(seen), len(ref))
	}
	for k, v := range ref {
		if got, ok := tl.Get(k); !ok || got != v {
			t.Fatalf("Get(%d) = (%d,%v), want (%d,true)", k, got, ok, v)
		}
		if seen[k] != v {
			t.Fatalf("ForEach value for %d = %d, want %d", k, seen[k], v)
		}
	}
	// Delete half the keys (back-shift), then re-verify the survivors.
	i := 0
	for k := range ref {
		if i%2 == 0 {
			if _, ok := tl.Delete(k); !ok {
				t.Fatalf("Delete(%d) reported missing", k)
			}
			delete(ref, k)
		}
		i++
	}
	if tl.Len() != len(ref) {
		t.Fatalf("after deletes Len = %d, want %d", tl.Len(), len(ref))
	}
	for k, v := range ref {
		if got, ok := tl.Get(k); !ok || got != v {
			t.Fatalf("after deletes Get(%d) = (%d,%v), want (%d,true)", k, got, ok, v)
		}
	}
}

func TestPackedTwoLevelTableMapParity(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	type key struct{ lo, hi uint64 }
	ref := map[key]int32{}
	tl := newPackedTwoLevelTable(64, nil)
	for i := 0; i < 60000; i++ {
		k := key{lo: uint64(rng.Intn(500)), hi: uint64(rng.Intn(120))}
		want, exists := ref[k]
		next := int32(len(ref))
		got, found := tl.GetOrInsert(k.lo, k.hi, next)
		if exists != found {
			t.Fatalf("key %v: found=%v want %v", k, found, exists)
		}
		if !exists {
			ref[k] = next
			want = next
		}
		if got != want {
			t.Fatalf("key %v: got %d want %d", k, got, want)
		}
	}
	if tl.Len() != len(ref) {
		t.Fatalf("Len = %d, want %d", tl.Len(), len(ref))
	}
	for k, v := range ref {
		if got, ok := tl.Get(k.lo, k.hi); !ok || got != v {
			t.Fatalf("Get(%v) = (%d,%v), want (%d,true)", k, got, ok, v)
		}
	}
	// The all-zero key is legal and must be a real group, not a free slot.
	if _, ok := tl.Get(0, 0); !ok {
		if _, found := tl.GetOrInsert(0, 0, int32(len(ref))); found {
			t.Fatal("zero key reported as pre-existing")
		}
		if _, ok := tl.Get(0, 0); !ok {
			t.Fatal("zero key not retrievable after insert")
		}
	}
}

// TestTwoLevelConversionPreservesEntries pins the conversion itself: every
// live entry of the flat table survives with its group id, the count
// matches, and the flat table's storage is released.
func TestTwoLevelConversionPreservesEntries(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		flat := newIntHashTable(64)
		want := map[int64]int32{}
		for i := 0; i < 30000; i++ {
			k := int64(i*7 - 5000)
			flat.Put(k, int32(i))
			want[k] = int32(i)
		}
		tl := convertIntHashTableToTwoLevel(flat, nil)
		if flat.entries != nil {
			t.Fatal("flat entry array not released by conversion")
		}
		if tl.Len() != len(want) {
			t.Fatalf("converted Len = %d, want %d", tl.Len(), len(want))
		}
		for k, v := range want {
			if got, ok := tl.Get(k); !ok || got != v {
				t.Fatalf("Get(%d) = (%d,%v), want (%d,true)", k, got, ok, v)
			}
		}
	})
	t.Run("packed", func(t *testing.T) {
		flat := newPackedHashTable(64)
		type key struct{ lo, hi uint64 }
		want := map[key]int32{}
		for i := 0; i < 30000; i++ {
			k := key{lo: uint64(i) * 2654435761, hi: uint64(i % 977)}
			flat.GetOrInsert(k.lo, k.hi, int32(i))
			want[k] = int32(i)
		}
		tl := convertPackedHashTableToTwoLevel(flat, nil)
		if flat.entries != nil {
			t.Fatal("flat entry array not released by conversion")
		}
		if tl.Len() != len(want) {
			t.Fatalf("converted Len = %d, want %d", tl.Len(), len(want))
		}
		for k, v := range want {
			if got, ok := tl.Get(k.lo, k.hi); !ok || got != v {
				t.Fatalf("Get(%v) = (%d,%v), want (%d,true)", k, got, ok, v)
			}
		}
	})
}

// --- off-heap accounting --------------------------------------------------

// TestTwoLevelOffheapReservationDiscipline asserts how the bucketed index
// holds off-heap memory: ONE shared reservation carved into 256 bucket
// slices at construction (the unit that wants a huge page is the table, not
// the bucket — see offheapSubMinBytes), a bucket that outgrows its slice
// taking a reservation of its own, the shared arena released the moment the
// last bucket has left it, and every per-bucket grow releasing the old
// reservation instead of stacking a new one on top (the ADR-0006 amendment
// property).
func TestTwoLevelOffheapReservationDiscipline(t *testing.T) {
	if !memory.OffheapAvailable() {
		t.Skip("off-heap unavailable on this platform/toggle")
	}
	prev := offheapSubMinBytes
	offheapSubMinBytes = 1 // force off-heap at every size
	defer func() { offheapSubMinBytes = prev }()

	reg := memory.NewOffheapRegistry()
	defer reg.Close()

	tl := newIntTwoLevelTable(twoLevelBuckets*8, reg)
	if got := reg.Mappings(); got != 1 {
		t.Fatalf("after construction: %d mappings, want 1 (all 256 buckets carved from one arena)", got)
	}
	for i := range tl.subs {
		if !tl.subs[i].arena || !tl.subs[i].offheap {
			t.Fatalf("bucket %d not arena-backed at construction", i)
		}
	}
	if tl.arenaLive != twoLevelBuckets {
		t.Fatalf("arenaLive = %d, want %d", tl.arenaLive, twoLevelBuckets)
	}
	// Fill enough to grow every bucket several times over.
	for i := 0; i < 200000; i++ {
		k := int64(i)
		tl.GetOrInsertAt(k, fibHash(k), int32(i))
	}
	// Every bucket has left the arena, so the arena is gone and each bucket
	// holds exactly one reservation of its own.
	if tl.arena != nil || tl.arenaLive != 0 {
		t.Fatalf("arena still held after every bucket grew (live=%d)", tl.arenaLive)
	}
	if got := reg.Mappings(); got != twoLevelBuckets {
		t.Fatalf("after growth: %d mappings, want %d — grow must release the old sub-table "+
			"and the arena must be released once vacated", got, twoLevelBuckets)
	}
	if tl.Len() != 200000 {
		t.Fatalf("Len = %d, want 200000", tl.Len())
	}
	for i := 0; i < 200000; i++ {
		if got, ok := tl.Get(int64(i)); !ok || got != int32(i) {
			t.Fatalf("Get(%d) = (%d,%v) after off-heap growth", i, got, ok)
		}
	}
	for i := range tl.subs {
		if !tl.subs[i].offheap || tl.subs[i].arena {
			t.Fatalf("bucket %d: offheap=%v arena=%v, want own off-heap reservation",
				i, tl.subs[i].offheap, tl.subs[i].arena)
		}
	}
}

// TestTwoLevelArenaSurvivesPartialGrowth pins the half-vacated state: while
// SOME buckets still slice the arena it must stay mapped, and MemoryUsage
// must still charge it in full — under-reporting it would let an aggregate
// hold bytes the spill accounting cannot see.
func TestTwoLevelArenaSurvivesPartialGrowth(t *testing.T) {
	if !memory.OffheapAvailable() {
		t.Skip("off-heap unavailable on this platform/toggle")
	}
	prev := offheapSubMinBytes
	offheapSubMinBytes = 1
	defer func() { offheapSubMinBytes = prev }()

	reg := memory.NewOffheapRegistry()
	defer reg.Close()

	const capPerSub = 16
	tl := newIntTwoLevelTableSub(capPerSub, reg)
	arenaBytes := int64(twoLevelBuckets*capPerSub) * int64(unsafe.Sizeof(intHashEntry{}))
	headers := int64(unsafe.Sizeof(tl.subs))
	if got, want := tl.MemoryUsage(), arenaBytes+headers; got != want {
		t.Fatalf("MemoryUsage = %d, want %d at construction", got, want)
	}
	// Grow exactly one bucket by hand.
	tl.growSub(0)
	if tl.arena == nil {
		t.Fatal("arena released while 255 buckets still slice it")
	}
	if tl.arenaLive != twoLevelBuckets-1 {
		t.Fatalf("arenaLive = %d, want %d", tl.arenaLive, twoLevelBuckets-1)
	}
	if got := reg.Mappings(); got != 2 {
		t.Fatalf("mappings = %d, want 2 (arena + the one grown bucket)", got)
	}
	grown := int64(cap(tl.subs[0].entries)) * int64(unsafe.Sizeof(intHashEntry{}))
	if got, want := tl.MemoryUsage(), arenaBytes+grown+headers; got != want {
		t.Fatalf("MemoryUsage = %d, want %d with one bucket grown out", got, want)
	}
}

// --- aggregate-level parity ----------------------------------------------

// twoLevelParityShapes drives the int and packed key modes at cardinalities
// that straddle the conversion threshold, comparing every emitted group
// against the flat (switch-off) oracle.
func TestTwoLevelAggregateParity(t *testing.T) {
	const convertAt = 4096
	shapes := []struct {
		name string
		cols []string
	}{
		{"single-int64", []string{"k"}},
		{"single-int32", []string{"k32"}},
		{"packed-two-int64", []string{"k", "k2"}},
		{"packed-int64-int32", []string{"k", "k32"}},
	}
	// Below, at, and far above the threshold.
	cards := []int{convertAt / 2, convertAt, convertAt + 1, convertAt * 8}

	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeInt64},
		{Name: "k32", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeInt64},
	}
	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
			{Func: AggMin, InputCol: "v", OutputCol: "mn", OutputType: parquet.TypeInt64},
			{Func: AggMax, InputCol: "v", OutputCol: "mx", OutputType: parquet.TypeInt64},
			{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		}
	}

	for _, sh := range shapes {
		for _, card := range cards {
			t.Run(fmt.Sprintf("%s/groups=%d", sh.name, card), func(t *testing.T) {
				// 1.5 rows per group so some probes are lookups.
				nRows := card * 3 / 2
				batches := make([]*batch.RecordBatch, 0, nRows/1024+1)
				rows := make([]map[string]any, 0, 1024)
				for i := 0; i < nRows; i++ {
					g := i % card
					rows = append(rows, map[string]any{
						"k":   int64(g) * 2654435761,
						"k2":  int64(g % 7919),
						"k32": int32(g),
						"v":   int64(i%1000) - 500,
					})
					if len(rows) == 1024 {
						batches = append(batches, batch.FromRows(schema, rows))
						rows = rows[:0]
					}
				}
				if len(rows) > 0 {
					batches = append(batches, batch.FromRows(schema, rows))
				}

				var flat, bucketed []map[string]any
				var conversions int64
				withTwoLevel(t, false, convertAt, func() {
					flat = runHashAggToMap(t, NewHashAggregate(sh.cols, aggs()), batches)
				})
				withTwoLevel(t, true, convertAt, func() {
					before := TwoLevelConversions.Load()
					bucketed = runHashAggToMap(t, NewHashAggregate(sh.cols, aggs()), batches)
					conversions = TwoLevelConversions.Load() - before
				})
				if card > convertAt && conversions == 0 {
					t.Fatal("no conversion at a cardinality above the threshold — parity would be vacuous")
				}
				if card < convertAt && conversions != 0 {
					t.Fatalf("converted %d times below the threshold", conversions)
				}
				assertSameRowSets(t, sh.cols, flat, bucketed)
			})
		}
	}
}

// assertSameRowSets compares two aggregate outputs group for group.
func assertSameRowSets(t *testing.T, keyCols []string, want, got []map[string]any) {
	t.Helper()
	index := func(rows []map[string]any) map[string]map[string]any {
		out := make(map[string]map[string]any, len(rows))
		for _, r := range rows {
			k := ""
			for _, c := range keyCols {
				k += fmt.Sprintf("%v|", r[c])
			}
			if _, dup := out[k]; dup {
				t.Fatalf("duplicate group %q in output", k)
			}
			out[k] = r
		}
		return out
	}
	w, g := index(want), index(got)
	if len(w) != len(g) {
		t.Fatalf("group count: flat %d, two-level %d", len(w), len(g))
	}
	keys := make([]string, 0, len(w))
	for k := range w {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		gr, ok := g[k]
		if !ok {
			t.Fatalf("group %q missing from the two-level result", k)
		}
		for col, wv := range w[k] {
			if fmt.Sprintf("%v", gr[col]) != fmt.Sprintf("%v", wv) {
				t.Fatalf("group %q col %s: two-level %v vs flat %v", k, col, gr[col], wv)
			}
		}
	}
}

// TestTwoLevelConversionMidStream drives the conversion at every possible
// point relative to the batch boundaries: a run of batches whose cumulative
// group count crosses the threshold mid-stream must produce exactly the flat
// result, and the groups minted BEFORE the conversion must still resolve to
// their original ids afterwards (which is what keeps the SoA accumulators
// aligned).
func TestTwoLevelConversionMidStream(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	// Each batch introduces 500 new groups and re-touches every group seen
	// so far, so post-conversion batches probe pre-conversion groups.
	const perBatch = 500
	const nBatches = 12
	batches := make([]*batch.RecordBatch, 0, nBatches)
	for bi := 0; bi < nBatches; bi++ {
		rows := make([]map[string]any, 0, perBatch*2)
		for i := 0; i < perBatch; i++ {
			g := bi*perBatch + i
			rows = append(rows, map[string]any{"k": int64(g), "v": int64(g)})
		}
		for i := 0; i <= bi*perBatch; i += 37 { // revisit older groups
			rows = append(rows, map[string]any{"k": int64(i), "v": int64(1)})
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		}
	}
	var flat []map[string]any
	withTwoLevel(t, false, 1<<30, func() {
		flat = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), batches)
	})
	// Sweep the threshold across the whole stream so the conversion lands
	// after each batch in turn. The check runs at the END of a batch, so a
	// threshold above the stream's total group count never fires — asserted
	// too, since "results are still right" is the point either way.
	for _, at := range []int{1, perBatch - 1, perBatch, perBatch + 1, 2 * perBatch, 3*perBatch + 7,
		nBatches * perBatch, nBatches*perBatch + 1} {
		t.Run(fmt.Sprintf("convertAt=%d", at), func(t *testing.T) {
			var got []map[string]any
			var conversions int64
			withTwoLevel(t, true, at, func() {
				before := TwoLevelConversions.Load()
				got = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), batches)
				conversions = TwoLevelConversions.Load() - before
			})
			want := int64(0)
			if at <= perBatch*nBatches {
				want = 1
			}
			if conversions != want {
				t.Fatalf("conversions = %d, want %d", conversions, want)
			}
			assertSameRowSets(t, []string{"k"}, flat, got)
		})
	}
}

// withTwoLevelStrict is withTwoLevel under the SHIPPED conversion policy:
// the size threshold moves but the load-factor lookahead stays armed, so the
// index converts only where a flat doubling was already due. Everything that
// tests WHEN the conversion fires has to use this; withTwoLevel's eager mode
// exists to give the corpus oracles coverage, not to define the rule.
func withTwoLevelStrict(tb testing.TB, on bool, convertAt int, fn func()) {
	tb.Helper()
	prevOn := twoLevelToggle.Set(on)
	prevAt, prevEager := twoLevelConvertAt, twoLevelConvertEager
	twoLevelConvertAt, twoLevelConvertEager = convertAt, false
	defer func() {
		twoLevelToggle.Set(prevOn)
		twoLevelConvertAt, twoLevelConvertEager = prevAt, prevEager
	}()
	fn()
}

// TestTwoLevelConvertsAtTheDoubling pins the shipped conversion gate.
//
// The rule under test: past the size threshold, a flat index converts on the
// batch that brings it within one batch of its 70% load factor — the moment
// its own grow() would have rehashed everything anyway — and at no other
// point. That is what makes the conversion free rather than merely cheap; a
// conversion anywhere else in the fill replaces nothing and the table still
// owes its doubling (the SF100 regression recorded in two_level_hash.go).
func TestTwoLevelConvertsAtTheDoubling(t *testing.T) {
	t.Run("gate", func(t *testing.T) {
		prevOn := twoLevelToggle.Set(true)
		prevAt, prevEager := twoLevelConvertAt, twoLevelConvertEager
		twoLevelConvertAt, twoLevelConvertEager = 1000, false
		defer func() {
			twoLevelToggle.Set(prevOn)
			twoLevelConvertAt, twoLevelConvertEager = prevAt, prevEager
		}()
		cases := []struct {
			name                  string
			live, slots, incoming int
			want                  bool
		}{
			{"below the size threshold, doubling due", 999, 1024, 2048, false},
			{"past the threshold, table at 17%", 1400, 8192, 2048, false},
			{"past the threshold, one batch from 70%", 6000, 8192, 2048, true},
			{"saturated: no new groups, room left", 5000, 8192, 0, false},
			{"crawling: a trickle, far from the load factor", 4000, 16384, 10, false},
			{"crawling: a trickle, but AT the load factor", 11460, 16384, 10, true},
		}
		for _, c := range cases {
			if got := convertsToTwoLevel(c.live, c.slots, c.incoming); got != c.want {
				t.Errorf("%s: convertsToTwoLevel(%d, %d, %d) = %v, want %v",
					c.name, c.live, c.slots, c.incoming, got, c.want)
			}
		}
		// The kill switch outranks everything.
		twoLevelToggle.Set(false)
		if convertsToTwoLevel(6000, 8192, 2048) {
			t.Error("converted with the kill switch off")
		}
	})

	// Aggregate level: feed a near-unique key one batch at a time and watch
	// the index flip. This is the shape the regression lived in — every row
	// mints a group, so the retired growth-rate test passed on every batch.
	t.Run("fill", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "k", Type: parquet.TypeInt64},
			{Name: "v", Type: parquet.TypeInt64},
		}
		const perBatch = 2048
		const nBatches = 24
		batches := make([]*batch.RecordBatch, 0, nBatches)
		for bi := 0; bi < nBatches; bi++ {
			rows := make([]map[string]any, 0, perBatch)
			for i := 0; i < perBatch; i++ {
				g := int64(bi*perBatch + i)
				rows = append(rows, map[string]any{"k": g * 2654435761, "v": g})
			}
			batches = append(batches, batch.FromRows(schema, rows))
		}
		aggs := []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		}
		ctx := context.Background()

		var conversions int64
		var liveAt, slotsAt, flatSlots int
		withTwoLevelStrict(t, true, 4096, func() {
			h := NewHashAggregate([]string{"k"}, aggs)
			if err := h.Init(ctx); err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			before := TwoLevelConversions.Load()
			for _, b := range batches {
				if err := h.Consume(ctx, b); err != nil {
					t.Fatal(err)
				}
				if idx := h.intGroupIndex; idx != nil {
					// Still flat: remember the capacity the conversion will
					// be judged against. A batch that grows the flat table
					// leaves it well under its load factor, so it cannot
					// also convert -- the slot count seen here is therefore
					// the slot count at the conversion.
					flatSlots = idx.Slots()
					continue
				}
				if liveAt == 0 {
					// State AT the conversion: nothing has been inserted
					// into the bucketed table yet (it lands at the batch's
					// end), so Len is the flat table's live count.
					liveAt = h.intTwoLevel.Len()
					for i := range h.intTwoLevel.subs {
						slotsAt += len(h.intTwoLevel.subs[i].entries)
					}
				}
			}
			conversions = TwoLevelConversions.Load() - before
		})
		if conversions != 1 {
			t.Fatalf("conversions = %d, want exactly 1", conversions)
		}
		// It converted where a doubling was due, and not before.
		if liveAt*10 > flatSlots*7 {
			t.Fatalf("flat table was already over its load factor at conversion "+
				"(live=%d slots=%d) -- CheckGrow should have grown it first",
				liveAt, flatSlots)
		}
		if (liveAt+perBatch)*10 <= flatSlots*7 {
			t.Fatalf("converted with a doubling still %d entries away "+
				"(live=%d slots=%d): the conversion replaced nothing",
				flatSlots*7/10-liveAt, liveAt, flatSlots)
		}
		// And it was born at the flat table's own slot count, so the doubling
		// it displaced happens per bucket. (Buckets that cross their load
		// factor during the conversion itself grow immediately, which is why
		// this is a range and not an equality.)
		if slotsAt < flatSlots || slotsAt > 2*flatSlots {
			t.Fatalf("bucketed slots = %d, want within [%d, %d] -- the destination "+
				"is the flat table's slot count split 256 ways", slotsAt, flatSlots, 2*flatSlots)
		}
	})

	// A table that crosses the size threshold while CRAWLING — mostly
	// repeats with a trickle of new keys — stays flat for as long as its
	// capacity holds, and answers identically either way.
	t.Run("crawl", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "k", Type: parquet.TypeInt64},
			{Name: "v", Type: parquet.TypeInt64},
		}
		const perBatch = 4000
		const newPerBatch = 100
		crawl := make([]*batch.RecordBatch, 0, 20)
		for bi := 0; bi < 20; bi++ {
			rows := make([]map[string]any, 0, perBatch)
			for i := 0; i < newPerBatch; i++ {
				g := bi*newPerBatch + i
				rows = append(rows, map[string]any{"k": int64(g), "v": int64(g)})
			}
			for i := len(rows); i < perBatch; i++ {
				rows = append(rows, map[string]any{"k": int64(i % (bi*newPerBatch + newPerBatch)), "v": int64(1)})
			}
			crawl = append(crawl, batch.FromRows(schema, rows))
		}
		aggs := func() []AggColumn {
			return []AggColumn{
				{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
				{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
			}
		}
		var crawled, flatCrawl []map[string]any
		withTwoLevelStrict(t, true, 1000, func() {
			before := TwoLevelConversions.Load()
			crawled = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), crawl)
			if n := TwoLevelConversions.Load() - before; n != 0 {
				t.Fatalf("conversions = %d, want 0 — 2000 groups in a 16K-slot "+
					"table is not about to rehash", n)
			}
		})
		withTwoLevelStrict(t, false, 1<<30, func() {
			flatCrawl = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), crawl)
		})
		assertSameRowSets(t, []string{"k"}, flatCrawl, crawled)
	})
}

// TestTwoLevelDirectBuildFromNDVHint covers the second construction path:
// an NDV hint already past the threshold builds bucketed with no conversion.
func TestTwoLevelDirectBuildFromNDVHint(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := make([]map[string]any, 0, 8000)
	for i := 0; i < 8000; i++ {
		rows = append(rows, map[string]any{"k": int64(i % 3000), "v": int64(i)})
	}
	batches := []*batch.RecordBatch{batch.FromRows(schema, rows)}
	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		}
	}
	var flat, direct []map[string]any
	withTwoLevel(t, false, 4096, func() {
		flat = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), batches)
	})
	withTwoLevel(t, true, 4096, func() {
		beforeDirect := TwoLevelDirectBuilds.Load()
		beforeConv := TwoLevelConversions.Load()
		h := NewHashAggregate([]string{"k"}, aggs())
		h.GroupNDVHint = 100000 // past the threshold before a row arrives
		direct = runHashAggToMap(t, h, batches)
		if TwoLevelDirectBuilds.Load() == beforeDirect {
			t.Fatal("NDV hint past the threshold did not build a bucketed index")
		}
		if TwoLevelConversions.Load() != beforeConv {
			t.Fatal("a directly-built bucketed index should never convert")
		}
	})
	assertSameRowSets(t, []string{"k"}, flat, direct)
}

// --- integration with the partitioned / emit / spill machinery -----------

// TestTwoLevelPartitionedParity re-runs the G5 partitioned-vs-serial parity
// with the bucketed index engaged, so the routed hash (which now also picks
// the bucket) is exercised end to end.
func TestTwoLevelPartitionedParity(t *testing.T) {
	schema := []parquet.Column{
		{Name: "i64", Type: parquet.TypeInt64},
		{Name: "j64", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rng := rand.New(rand.NewSource(23))
	const n = 40000
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"i64": int64(rng.Intn(6000)),
			"j64": int64(rng.Intn(37)),
			"v":   int64(rng.Intn(1000)) - 500,
		}
	}
	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
			{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		}
	}
	run := func(t *testing.T, cols []string, workers int) []map[string]any {
		t.Helper()
		agg := NewHashAggregate(cols, aggs())
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: workers}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		for {
			b, err := agg.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			out = append(out, b.ToRows()...)
		}
		return out
	}
	for _, cols := range [][]string{{"i64"}, {"i64", "j64"}} {
		t.Run(fmt.Sprintf("%v", cols), func(t *testing.T) {
			var serial, par []map[string]any
			withTwoLevel(t, false, 1<<30, func() {
				serial = run(t, cols, 1)
			})
			// Threshold low enough that even a single worker's share of the
			// key space converts.
			withTwoLevel(t, true, 200, func() {
				before := TwoLevelConversions.Load()
				par = run(t, cols, 8)
				if TwoLevelConversions.Load() == before {
					t.Fatal("no bucketed conversion under partitioned aggregation")
				}
			})
			assertSameRowSets(t, cols, serial, par)
		})
	}
}

// TestTwoLevelParallelEmitParity drives the parallel emit drain over adopted
// partitions whose indexes are bucketed. Emission reads group state by dense
// group id from the SoA arrays and never touches the index, so this asserts
// the property rather than a behavior change.
func TestTwoLevelParallelEmitParity(t *testing.T) {
	var serialRows, parRows map[string]map[string]any
	withTwoLevel(t, false, 1<<30, func() {
		withParallelEmit(t, false, func() {
			serialRows, _ = drainToRows(t, buildAdoptedAggregate(t, 8, 20000, 2))
		})
	})
	withTwoLevel(t, true, 500, func() {
		before := TwoLevelConversions.Load()
		withParallelEmit(t, true, func() {
			parRows, _ = drainToRows(t, buildAdoptedAggregate(t, 8, 20000, 2))
		})
		if TwoLevelConversions.Load() == before {
			t.Fatal("no bucketed conversion in the adopted units")
		}
	})
	assertSameGroups(t, serialRows, parRows)
}

// TestTwoLevelPartialDrainParity exercises the partial-drain spill path
// against a bucketed index: SpillSome walks the index (ForEach) to pick the
// drained partition and Deletes each drained key (back-shift inside its
// bucket), while the drain cursor itself reads keys from the intKeys SoA and
// never touches the index at all.
func TestTwoLevelPartialDrainParity(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const numGroups = 4000
	const rowsPerBatch = 500
	const numBatches = 24
	expected := make(map[int64]int64)
	batches := make([]*batch.RecordBatch, 0, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		rows := make([]map[string]any, 0, rowsPerBatch)
		for ri := 0; ri < rowsPerBatch; ri++ {
			k := int64((bi*rowsPerBatch + ri) % numGroups)
			v := int64(bi*1000 + ri + 1)
			rows = append(rows, map[string]any{"k": k, "v": v})
			expected[k] += v
		}
		batches = append(batches, batch.FromRows(schema, rows))
	}

	withTwoLevel(t, true, 1000, func() {
		tracker := memory.NewTracker("two-level-drain", 1<<30)
		sm, err := memory.NewSpillManager(t.TempDir(), tracker)
		if err != nil {
			t.Fatal(err)
		}
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "total", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		ctx := context.Background()
		if err := h.Init(ctx); err != nil {
			t.Fatal(err)
		}
		drains := 0
		for i, b := range batches {
			if err := h.Consume(ctx, b); err != nil {
				t.Fatalf("Consume #%d: %v", i, err)
			}
			if i%2 == 1 {
				footprint := h.Inspect().OwnedBytes
				if footprint <= 0 {
					continue
				}
				freed, err := h.SpillSome(footprint / 8)
				if err != nil {
					t.Fatalf("SpillSome #%d: %v", i, err)
				}
				if freed > 0 && freed < footprint {
					drains++
				}
			}
		}
		if h.intTwoLevel == nil {
			t.Fatal("index never converted — the drain path was not exercised bucketed")
		}
		if drains < 3 {
			t.Fatalf("partial drains observed = %d, want >= 3", drains)
		}
		if len(h.freeGroupIDs) == 0 {
			t.Fatal("freeGroupIDs never populated — slot reclaim did not run")
		}
		if err := h.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		got := map[int64]int64{}
		for {
			out, err := h.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if out == nil {
				break
			}
			for _, r := range out.ToRows() {
				k := r["k"].(int64)
				if _, dup := got[k]; dup {
					t.Fatalf("group %d emitted twice", k)
				}
				got[k] = r["total"].(int64)
			}
		}
		h.Close()
		if len(got) != numGroups {
			t.Fatalf("group count %d, want %d", len(got), numGroups)
		}
		for k, want := range expected {
			if got[k] != want {
				t.Errorf("k=%d: got %d want %d", k, got[k], want)
			}
		}
	})
}

// TestTwoLevelMergeParity covers the clone-merge paths (mergeIntGroupSoA /
// mergePackedGroupSoA), where the destination converts before absorbing the
// source's groups.
func TestTwoLevelMergeParity(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "k2", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		}
	}
	mkBatches := func(lo, hi int) []*batch.RecordBatch {
		rows := make([]map[string]any, 0, hi-lo)
		for i := lo; i < hi; i++ {
			rows = append(rows, map[string]any{
				"k": int64(i % 5000), "k2": int64(i % 91), "v": int64(i),
			})
		}
		return []*batch.RecordBatch{batch.FromRows(schema, rows)}
	}

	for _, cols := range [][]string{{"k"}, {"k", "k2"}} {
		t.Run(fmt.Sprintf("%v", cols), func(t *testing.T) {
			merged := func(t *testing.T) []map[string]any {
				t.Helper()
				ctx := context.Background()
				prim := NewHashAggregate(cols, aggs())
				if err := prim.Init(ctx); err != nil {
					t.Fatal(err)
				}
				clone := prim.CloneSink().(*HashAggregate)
				if err := clone.Init(ctx); err != nil {
					t.Fatal(err)
				}
				for _, b := range mkBatches(0, 9000) {
					if err := prim.Consume(ctx, b); err != nil {
						t.Fatal(err)
					}
				}
				for _, b := range mkBatches(4000, 13000) {
					if err := clone.Consume(ctx, b); err != nil {
						t.Fatal(err)
					}
				}
				prim.MergeSink(clone)
				if err := prim.Finalize(ctx); err != nil {
					t.Fatal(err)
				}
				var out []map[string]any
				for {
					b, err := prim.Next(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if b == nil {
						break
					}
					out = append(out, b.ToRows()...)
				}
				prim.Close()
				return out
			}
			var flat, bucketed []map[string]any
			withTwoLevel(t, false, 1<<30, func() { flat = merged(t) })
			withTwoLevel(t, true, 2000, func() {
				before := TwoLevelConversions.Load()
				bucketed = merged(t)
				if TwoLevelConversions.Load() == before {
					t.Fatal("no conversion during the merge run")
				}
			})
			assertSameRowSets(t, cols, flat, bucketed)
		})
	}
}
