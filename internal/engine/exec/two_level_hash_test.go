package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// withTwoLevel runs fn with the kill switch and the conversion threshold
// forced, restoring both. A low threshold is what lets a unit-scale test
// exercise the bucketed path at all.
func withTwoLevel(tb testing.TB, on bool, convertAt int, fn func()) {
	tb.Helper()
	prevOn := twoLevelToggle.Set(on)
	prevAt := twoLevelConvertAt
	twoLevelConvertAt = convertAt
	defer func() {
		twoLevelToggle.Set(prevOn)
		twoLevelConvertAt = prevAt
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
// The strided int family is excluded from the BUCKET bound for the reason
// TestFibHashStrideCollapse records: fibHash is multiply-only, so keys that
// are all multiples of 2^s have identical low bits and land in one bucket.
// Its slot spread is still asserted — the collapse costs bucket balance, not
// probe locality, which is strictly better than what the flat table does with
// the same family.
func TestTwoLevelThreeWindowSpread(t *testing.T) {
	const n = 1 << 17
	const parts = 12 // NumCPU-shaped, not a power of two

	families := []struct {
		name string
		// lowBitSpread: whether this family's LOW hash bits spread at all.
		// False only for the stride-2^k int family, whose low bits are
		// identically zero — the pre-existing fibHash property recorded in
		// TestFibHashStrideCollapse. Both the bucket window and the slot
		// window read low bits, so that family collapses in both, exactly as
		// it already collapses in the flat table. Its partition spread (top
		// bits) is still asserted.
		lowBitSpread bool
		hashes       []uint64
	}{
		{"int-dense", true, genHashes(n, func(i int) uint64 { return fibHash(int64(i)) })},
		{"int-negative", true, genHashes(n, func(i int) uint64 { return fibHash(int64(-i)) })},
		{"int-scaled", true, genHashes(n, func(i int) uint64 { return fibHash(int64(i) * 2654435761) })},
		{"int-strided-64k", false, genHashes(n, func(i int) uint64 { return fibHash(int64(i) << 16) })},
		{"packed-two-dense", true, genHashes(n, func(i int) uint64 { return packedHash(uint64(i)*2654435761, uint64(i)) })},
		{"packed-hi-word", true, genHashes(n, func(i int) uint64 { return packedHash(0, uint64(i)) })},
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
				// Documented collapse: assert it is EXACTLY the flat table's
				// behavior (every key in one bucket, on one probe chain) so a
				// silent change in fibHash surfaces here too.
				if w := worstBucket(f.hashes, twoLevelBuckets, func(h uint64) int { return int(bucketOf(h)) }); w != n {
					t.Fatalf("stride family no longer collapses onto one bucket (worst %d of %d) — "+
						"re-check the bit budget in two_level_hash.go", w, n)
				}
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
					bound := len(inPart)/twoLevelBuckets*3/2 + 8
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

// TestTwoLevelOffheapMappingsPerSub asserts the per-sub-table reservation
// discipline: one mapping per bucket once a bucket is big enough to earn
// one, and growth RELEASES the old reservation instead of stacking a new one
// on top (the ADR-0006 amendment property, now per bucket).
func TestTwoLevelOffheapMappingsPerSub(t *testing.T) {
	if !memory.OffheapAvailable() {
		t.Skip("off-heap unavailable on this platform/toggle")
	}
	prev := offheapSubMinBytes
	offheapSubMinBytes = 1 // force every sub-table off-heap
	defer func() { offheapSubMinBytes = prev }()

	reg := memory.NewOffheapRegistry()
	defer reg.Close()

	tl := newIntTwoLevelTable(twoLevelBuckets*8, reg)
	if got := reg.Mappings(); got != twoLevelBuckets {
		t.Fatalf("after construction: %d mappings, want %d (one per bucket)", got, twoLevelBuckets)
	}
	// Fill enough to grow many buckets several times over.
	for i := 0; i < 200000; i++ {
		k := int64(i)
		tl.GetOrInsertAt(k, fibHash(k), int32(i))
	}
	if got := reg.Mappings(); got != twoLevelBuckets {
		t.Fatalf("after growth: %d mappings, want %d — grow must release the old sub-table", got, twoLevelBuckets)
	}
	if tl.Len() != 200000 {
		t.Fatalf("Len = %d, want 200000", tl.Len())
	}
	for i := 0; i < 200000; i++ {
		if got, ok := tl.Get(int64(i)); !ok || got != int32(i) {
			t.Fatalf("Get(%d) = (%d,%v) after off-heap growth", i, got, ok)
		}
	}
	// Every bucket must actually be off-heap under the forced threshold, so
	// the mapping count above is really per sub-table.
	for i := range tl.subs {
		if !tl.subs[i].offheap {
			t.Fatalf("bucket %d not off-heap under a 1-byte threshold", i)
		}
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

// TestTwoLevelSaturatedTableDoesNotConvert pins the growth half of the
// conversion decision: a table that has crossed the size threshold but whose
// batches now mint almost no new groups must NOT pay a conversion it can
// never amortize. It must still produce the same rows either way.
func TestTwoLevelSaturatedTableDoesNotConvert(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const groups = 4000
	// Phase 1 fills the group space; phase 2 replays it, so those batches
	// create no new groups at all.
	fill := make([]map[string]any, 0, groups)
	for i := 0; i < groups; i++ {
		fill = append(fill, map[string]any{"k": int64(i), "v": int64(i)})
	}
	batches := []*batch.RecordBatch{batch.FromRows(schema, fill)}
	for r := 0; r < 6; r++ {
		batches = append(batches, batch.FromRows(schema, fill))
	}
	aggs := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
		}
	}
	var flat, saturated []map[string]any
	withTwoLevel(t, false, 1<<30, func() {
		flat = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), batches)
	})
	// Threshold well below the group count: only the growth gate can hold
	// the conversion back.
	withTwoLevel(t, true, 1000, func() {
		before := TwoLevelConversions.Load()
		h := NewHashAggregate([]string{"k"}, aggs())
		saturated = runHashAggToMap(t, h, batches)
		if n := TwoLevelConversions.Load() - before; n != 1 {
			t.Fatalf("conversions = %d, want 1 (the first, still-filling batch)", n)
		}
	})
	assertSameRowSets(t, []string{"k"}, flat, saturated)

	// Now the shape the gate exists for: a table that crosses the threshold
	// while CRAWLING — each batch is mostly repeats with a trickle of new
	// keys. It reaches the same size, and must never convert.
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
	var crawled, flatCrawl []map[string]any
	withTwoLevel(t, true, 1000, func() {
		before := TwoLevelConversions.Load()
		crawled = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), crawl)
		if n := TwoLevelConversions.Load() - before; n != 0 {
			t.Fatalf("conversions = %d, want 0 — a crawling table must not pay for a conversion", n)
		}
	})
	withTwoLevel(t, false, 1<<30, func() {
		flatCrawl = runHashAggToMap(t, NewHashAggregate([]string{"k"}, aggs()), crawl)
	})
	assertSameRowSets(t, []string{"k"}, flatCrawl, crawled)
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
