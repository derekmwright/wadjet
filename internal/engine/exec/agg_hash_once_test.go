package exec

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// --- parity: partitioned parallel == serial, per key shape ----------------

// TestSerialVsParallelKeyShapes drives every key shape whose sink table now
// consumes the ROUTER's hash instead of computing its own — single int
// (intHashTable/fibHash), two-int composite (packedHashTable/packedHash),
// single string (strHashTable/strHash) — plus a mixed shape that threads
// nothing, and asserts the parallel result equals the serial oracle. Each
// shape runs with the hash-once switch on and off, so the kill switch is
// exercised as an equivalence, not just as a code path.
func TestSerialVsParallelKeyShapes(t *testing.T) {
	shapes := []struct {
		name string
		cols []string
		kind keyHashKind
		// threads: whether the SINK ends up consuming the routed hash. The
		// router's plan kind is not sufficient on its own — a NULL in an
		// int-class key column migrates the whole aggregate to the generic
		// path before the first batch is consumed, so nullable-int64 routes by
		// fibHash but never probes with it. The string path keeps its fast
		// path (NULLs get a dedicated group slot), so it still threads.
		threads bool
	}{
		{"single-int64", []string{"i64"}, hashKindInt, true},
		{"single-int32", []string{"i32"}, hashKindInt, true},
		{"packed-two-int64", []string{"i64", "j64"}, hashKindPacked, true},
		{"packed-int64-int32", []string{"i64", "i32"}, hashKindPacked, true},
		{"single-string", []string{"s"}, hashKindStr, true},
		{"mixed-int-string", []string{"i64", "s"}, hashKindNone, false},
		{"nullable-int64", []string{"n64"}, hashKindInt, false},
		{"nullable-string", []string{"ns"}, hashKindStr, true},
	}

	schema := []parquet.Column{
		{Name: "i64", Type: parquet.TypeInt64},
		{Name: "j64", Type: parquet.TypeInt64},
		{Name: "i32", Type: parquet.TypeInt32},
		{Name: "s", Type: parquet.TypeString},
		{Name: "n64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "ns", Type: parquet.TypeString, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	rng := rand.New(rand.NewSource(7))
	const n = 30000
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{
			"i64": int64(rng.Intn(700)),
			"j64": int64(rng.Intn(11)),
			"i32": int32(rng.Intn(400) - 200),
			"s":   fmt.Sprintf("key-%d", rng.Intn(500)),
		}
		if rng.Intn(15) != 0 {
			r["n64"] = int64(rng.Intn(300))
		}
		if rng.Intn(15) != 0 {
			r["ns"] = fmt.Sprintf("nk-%d", rng.Intn(200))
		}
		if rng.Intn(8) != 0 {
			r["v"] = int64(rng.Intn(1000)) - 500
		}
		rows[i] = r
	}

	aggsOf := func() []AggColumn {
		return []AggColumn{
			{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeFloat64},
			{Func: AggMin, InputCol: "v", OutputCol: "mn", OutputType: parquet.TypeFloat64},
			{Func: AggAvg, InputCol: "v", OutputCol: "av", OutputType: parquet.TypeFloat64},
		}
	}

	run := func(t *testing.T, cols []string, workers int) map[string]map[string]any {
		t.Helper()
		agg := NewHashAggregate(cols, aggsOf())
		pipe := &Pipeline{Source: NewSliceSource(schema, rows), Sink: agg, Workers: workers}
		if err := pipe.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		out := map[string]map[string]any{}
		for {
			b, err := agg.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				key := ""
				for _, c := range cols {
					key += fmt.Sprintf("%v|", r[c])
				}
				if _, dup := out[key]; dup {
					t.Fatalf("duplicate group %q in output — a key reached two owners", key)
				}
				out[key] = r
			}
		}
		return out
	}

	for _, sh := range shapes {
		for _, on := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/hash_once=%v", sh.name, on), func(t *testing.T) {
				prev := hashOnceToggle.Set(on)
				defer hashOnceToggle.Set(prev)

				// The plan the router freezes must be the one the shape claims
				// (only meaningful with the switch on — off pins hashKindNone).
				if on {
					probe := NewHashAggregate(sh.cols, aggsOf())
					bb := sliceToBatch(t, schema, rows[:8])
					if got := probe.routePlanFor(bb).kind; got != sh.kind {
						t.Fatalf("route plan kind = %d, want %d", got, sh.kind)
					}
				}

				serial := run(t, sh.cols, 1)
				beforeParts := PartitionedAggRuns.Load()
				beforeRouted := HashOnceRoutedRows.Load()
				par := run(t, sh.cols, 8)
				if PartitionedAggRuns.Load() == beforeParts && partitionedAggToggle.On() {
					t.Fatal("partitioned mode did not engage — parity would be vacuous")
				}
				// The threading must actually happen for the shapes that
				// claim it, and must NOT happen with the switch off.
				routed := HashOnceRoutedRows.Load() - beforeRouted
				wantRouted := on && sh.threads
				if (routed > 0) != wantRouted {
					t.Fatalf("rows consumed with a routed hash = %d, want>0 = %v", routed, wantRouted)
				}
				if len(par) != len(serial) {
					t.Fatalf("group counts differ: parallel %d vs serial %d", len(par), len(serial))
				}
				keys := make([]string, 0, len(serial))
				for k := range serial {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					pr, ok := par[k]
					if !ok {
						t.Fatalf("group %q missing from parallel result", k)
					}
					for col, sv := range serial[k] {
						if fmt.Sprintf("%v", pr[col]) != fmt.Sprintf("%v", sv) {
							t.Fatalf("group %q col %s: parallel %v vs serial %v", k, col, pr[col], sv)
						}
					}
				}
			})
		}
	}
}

// sliceToBatch materializes rows into one batch for schema-level assertions.
func sliceToBatch(t *testing.T, schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	t.Helper()
	src := NewSliceSource(schema, rows)
	if err := src.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := src.Next(context.Background())
	if err != nil || b == nil {
		t.Fatalf("slice source produced no batch (err=%v)", err)
	}
	return b
}

// --- router determinism ---------------------------------------------------

// TestRouterDeterminism pins the property partitioned aggregation's
// correctness rests on: a given key always routes to the same owner. Not just
// across repeated calls on one batch — across BATCHES, across scratch reuse,
// and (the hash-once hazard) across the point where the router aggregate has
// resolved its own indices as worker 0's sink.
func TestRouterDeterminism(t *testing.T) {
	const parts = 7 // deliberately not a power of two
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rng := rand.New(rand.NewSource(11))
	mkRows := func(n int) []map[string]any {
		rows := make([]map[string]any, n)
		for i := range rows {
			id := rng.Intn(300)
			rows[i] = map[string]any{
				"k": int64(id),
				"s": fmt.Sprintf("s-%d", id),
				"v": int64(i),
			}
		}
		return rows
	}

	for _, cols := range [][]string{{"k"}, {"s"}, {"k", "s"}} {
		t.Run(fmt.Sprint(cols), func(t *testing.T) {
			agg := NewHashAggregate(cols, []AggColumn{
				{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
			})
			owner := map[string]int{}
			var sc partitionScratch
			ctx := context.Background()
			src := NewSliceSource(schema, mkRows(20000))
			if err := src.Init(ctx); err != nil {
				t.Fatal(err)
			}
			batches := 0
			for {
				b, err := src.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					break
				}
				batches++
				rt := agg.PartitionSelectors(b, parts, &sc)
				if rt == nil {
					t.Fatal("router refused to partition a supported shape")
				}
				// Re-running with a fresh scratch must reproduce the split
				// exactly (the scratch must not carry state that steers it).
				again := agg.PartitionSelectors(b, parts, &partitionScratch{})
				for p := range rt.sels {
					if len(rt.sels[p]) != len(again.sels[p]) {
						t.Fatalf("part %d: %d rows vs %d on re-run", p, len(rt.sels[p]), len(again.sels[p]))
					}
					for i, row := range rt.sels[p] {
						if again.sels[p][i] != row {
							t.Fatalf("part %d row %d: %d vs %d on re-run", p, i, row, again.sels[p][i])
						}
						key := ""
						for _, c := range cols {
							ci := columnIndexFallback(b, c)
							key += fmt.Sprintf("%v|", b.Columns[ci].GetValue(int(row)))
						}
						if prev, seen := owner[key]; seen && prev != p {
							t.Fatalf("key %q routed to owner %d and %d", key, prev, p)
						}
						owner[key] = p
					}
					if rt.hashes[p] != nil && len(rt.hashes[p]) != len(rt.sels[p]) {
						t.Fatalf("part %d: %d hashes for %d rows", p, len(rt.hashes[p]), len(rt.sels[p]))
					}
				}
				routedBufPool.Put(rt.buf)
				b.Release()
			}
			if batches < 2 {
				t.Fatalf("need multiple batches to test cross-batch stability, got %d", batches)
			}
			if len(owner) == 0 {
				t.Fatal("no keys routed")
			}
		})
	}
}

// TestRoutedHashMatchesTableHash asserts the contract the threading rests on:
// the hash the router hands the sink is bit-for-bit the one the sink's table
// would have computed. If these ever diverge, groups land in wrong slots.
func TestRoutedHashMatchesTableHash(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "j", Type: parquet.TypeInt32},
		{Name: "s", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := make([]map[string]any, 512)
	for i := range rows {
		rows[i] = map[string]any{
			"k": int64(i*7919 - 1000),
			"j": int32(i - 200),
			"s": fmt.Sprintf("https://example.com/path/%d", i),
			"v": int64(i),
		}
	}
	b := sliceToBatch(t, schema, rows)

	check := func(t *testing.T, cols []string, want func(row int) uint64) {
		t.Helper()
		agg := NewHashAggregate(cols, []AggColumn{
			{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
		})
		var sc partitionScratch
		rt := agg.PartitionSelectors(b, 4, &sc)
		if rt == nil {
			t.Fatal("router refused to partition")
		}
		seen := 0
		for p := range rt.sels {
			if rt.hashes[p] == nil {
				t.Fatal("no hashes threaded for a supported shape")
			}
			for i, row := range rt.sels[p] {
				if got := rt.hashes[p][i]; got != want(int(row)) {
					t.Fatalf("row %d: routed hash %#x, table hash %#x", row, got, want(int(row)))
				}
				seen++
			}
		}
		if seen != b.ActiveLen() {
			t.Fatalf("checked %d rows, batch has %d", seen, b.ActiveLen())
		}
	}

	t.Run("int64", func(t *testing.T) {
		v := b.Columns[columnIndexFallback(b, "k")]
		check(t, []string{"k"}, func(row int) uint64 { return fibHash(v.Int64Data[row]) })
	})
	t.Run("int32", func(t *testing.T) {
		v := b.Columns[columnIndexFallback(b, "j")]
		check(t, []string{"j"}, func(row int) uint64 { return fibHash(int64(v.Int32Data[row])) })
	})
	t.Run("string", func(t *testing.T) {
		v := b.Columns[columnIndexFallback(b, "s")]
		check(t, []string{"s"}, func(row int) uint64 { return strHash(v.BytesData.Value(row)) })
	})
	t.Run("packed", func(t *testing.T) {
		layout := buildPackedLayout([]batch.TypeID{batch.TypeInt64, batch.TypeInt32})
		if layout == nil {
			t.Fatal("expected a packable layout")
		}
		k := b.Columns[columnIndexFallback(b, "k")]
		j := b.Columns[columnIndexFallback(b, "j")]
		cols := []packedKeyCol{
			{i64: k.Int64Data, nulls: &k.Nulls, word: layout[0].word, shift: layout[0].shift},
			{i32: j.Int32Data, nulls: &j.Nulls, word: layout[1].word, shift: layout[1].shift},
		}
		check(t, []string{"k", "j"}, func(row int) uint64 {
			lo, hi := packedKeyAt(cols, row)
			return packedHash(lo, hi)
		})
	})
}

// --- spread of the unified hash ------------------------------------------

// spreadStats returns the largest bucket count for hashes distributed over
// nBuckets by the given extractor.
func worstBucket(hashes []uint64, nBuckets int, pick func(uint64) int) int {
	buckets := make([]int, nBuckets)
	for _, h := range hashes {
		buckets[pick(h)]++
	}
	worst := 0
	for _, c := range buckets {
		if c > worst {
			worst = c
		}
	}
	return worst
}

// TestUnifiedHashSpread is the spread argument for hash-once, run over every
// key family the threading covers. One function now feeds BOTH consumers, so
// both bit windows are checked:
//
//   - partition = partitionFor(h, parts), the TOP bits. Skew here means an
//     overloaded worker.
//   - slot = h & (slots-1), the LOW bits. Skew here means probe-chain
//     pile-ups — the failure TestPackedHashSpreadsHighHalfKeys guards for
//     packed keys, extended to the families fibHash and strHash now serve.
func TestUnifiedHashSpread(t *testing.T) {
	const n = 1 << 14
	const slots = 1 << 15 // what a table holding n keys at 70% load would use
	const parts = 12      // NumCPU-shaped: not a power of two

	families := []struct {
		name   string
		hashes []uint64
		// slotSpread: whether the table's own hash claims a spread over the
		// LOW bits for this key family. Every family claims it now; the
		// stride-2^k integers did not until #306 gave fibHash a mixing step
		// (TestFibHashStrideBreak).
		slotSpread bool
	}{
		{"int-dense", genHashes(n, func(i int) uint64 { return fibHash(int64(i)) }), true},
		{"int-negative", genHashes(n, func(i int) uint64 { return fibHash(int64(-i)) }), true},
		// Was `false` — this family collapsed onto one probe chain until
		// #306 gave fibHash a mixing step. TestFibHashStrideBreak checks
		// every stride width; this entry keeps it in the general sweep.
		{"int-strided-64k", genHashes(n, func(i int) uint64 { return fibHash(int64(i) << 16) }), true},
		{"packed-low-half", genHashes(n, func(i int) uint64 { return packedHash(uint64(i), 0) }), true},
		{"packed-high-half", genHashes(n, func(i int) uint64 { return packedHash(uint64(i)<<32, 0) }), true},
		{"packed-hi-word", genHashes(n, func(i int) uint64 { return packedHash(0, uint64(i)) }), true},
		{"packed-two-dense", genHashes(n, func(i int) uint64 { return packedHash(uint64(i), uint64(i)) }), true},
		{"str-url", genHashes(n, func(i int) uint64 {
			return strHash([]byte(fmt.Sprintf("https://example.com/watch/section/id=%012d", i)))
		}), true},
		{"str-suffix", genHashes(n, func(i int) uint64 {
			return strHash([]byte(fmt.Sprintf("%06d-https://example.com/watch", i)))
		}), true},
	}

	// Uniform hashing puts the worst bucket a few standard deviations above
	// the mean; these bounds are several times the mean and still catch the
	// real failure (a family collapsing onto one bucket, worst == n).
	const slotBound = 12                // mean 0.5 per slot
	const partBound = n / parts * 3 / 2 // mean n/parts
	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			// Partition bits are what hash-once newly asks of these functions:
			// every family must divide evenly across owners, or one worker
			// takes the whole aggregate.
			if w := worstBucket(f.hashes, parts, func(h uint64) int { return int(partitionFor(h, parts)) }); w > partBound {
				t.Errorf("partition bits: worst of %d owners holds %d of %d keys", parts, w, n)
			}
			if !f.slotSpread {
				return
			}
			if w := worstBucket(f.hashes, slots, func(h uint64) int { return int(h & (slots - 1)) }); w > slotBound {
				t.Errorf("slot bits: worst of %d slots holds %d of %d keys", slots, w, n)
			}
			// Disjointness: fixing the owner must not skew the slot bits. This
			// is the bit-budget claim — the top log2(parts) bits and the low
			// log2(slots) bits are independent windows of the same value.
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
				sub := 1 << 12 // a per-owner table sized for n/parts keys
				if w := worstBucket(inPart, sub, func(h uint64) int { return int(h & uint64(sub-1)) }); w > slotBound {
					t.Errorf("owner %d slot bits: worst of %d slots holds %d of %d keys", p, sub, w, len(inPart))
				}
			}
		})
	}
}

// TestFibHashStrideBreak is #306's regression, and the inverse of what this
// test asserted when it was written.
//
// fibHash was multiply-only, so the low k bits of key*phi depended ONLY on
// the low k bits of the key: a single-int GROUP BY whose keys are all
// multiples of 2^s put every key on ONE probe chain in any table with at most
// 2^s slots. The test recorded that collapse (worst bucket == n) rather than
// fixing it, because the fix costs the collision-free bijection dense ids
// enjoyed. fibHash now runs murmur3's fmix64 finalizer, so every output bit
// depends on every input bit and no stride survives.
//
// Every stride is checked, not just the 2^16 the original recorded: a
// finalizer that happened to break one shift width and not another would pass
// a single-stride test and leave the defect in place for the rest.
func TestFibHashStrideBreak(t *testing.T) {
	const n = 4096
	const slots = 1 << 15
	// Uniform hashing puts the worst of 32768 slots a few above the 0.125
	// mean; 12 is several times that and still catches the real failure
	// (a family collapsing onto one chain, worst == n).
	const slotBound = 12
	for _, s := range []uint{1, 2, 4, 8, 12, 16, 20, 24, 32, 40} {
		strided := genHashes(n, func(i int) uint64 { return fibHash(int64(i) << s) })
		if w := worstBucket(strided, slots, func(h uint64) int { return int(h & (slots - 1)) }); w > slotBound {
			t.Errorf("stride 2^%d: worst of %d slots holds %d of %d keys — the mixing step "+
				"does not break this stride", s, slots, w, n)
		}
		// The owner split stays even too: the partition bits come off the
		// TOP of the same word, and hash-once reuses one value for both.
		const parts = 12
		if w := worstBucket(strided, parts, func(h uint64) int { return int(partitionFor(h, parts)) }); w > n/parts*3/2 {
			t.Errorf("stride 2^%d partition bits: worst of %d owners holds %d of %d keys", s, parts, w, n)
		}
	}
}

// TestFibHashIsInjective: mixing must not merge two keys into one hash, or a
// GROUP BY would still be correct (the tables compare keys, not hashes) but
// would pay a probe chain for a collision that never had to happen. fmix64 is
// a bijection on 64 bits — each of its five steps is invertible — so the
// worst case is the birthday rate on the TRUNCATION to slots, never on the
// full word.
func TestFibHashIsInjective(t *testing.T) {
	const n = 1 << 16
	keys := make(map[int64]bool, n+192)
	for i := int64(0); i < n; i++ {
		keys[i] = true
	}
	for s := uint(0); s < 63; s++ {
		keys[1<<s] = true
		keys[-(1 << s)] = true
		keys[(1<<s)-1] = true
	}
	seen := make(map[uint64]int64, len(keys))
	for key := range keys {
		h := fibHash(key)
		if prev, dup := seen[h]; dup {
			t.Fatalf("fibHash(%d) == fibHash(%d) == %#x", key, prev, h)
		}
		seen[h] = key
	}
}

func genHashes(n int, f func(int) uint64) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = f(i)
	}
	return out
}

// TestPartitionForIsShiftOnPowerOfTwo pins the DuckDB equivalence claimed in
// partitioned_agg.go: for a power-of-two owner count, the multiply-shift is
// exactly the radix shift `hash >> (64 - radixBits)`.
func TestPartitionForIsShiftOnPowerOfTwo(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for radix := 1; radix <= 12; radix++ {
		parts := uint64(1) << radix
		for i := 0; i < 2000; i++ {
			h := rng.Uint64()
			if got, want := partitionFor(h, parts), h>>(64-radix); got != want {
				t.Fatalf("radix %d hash %#x: partitionFor=%d, shift=%d", radix, h, got, want)
			}
		}
	}
}

// TestPartitionForInRange covers the non-power-of-two owner counts the real
// planner produces (runtime.NumCPU()).
func TestPartitionForInRange(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for _, parts := range []uint64{1, 3, 6, 7, 12, 20, 24, 96} {
		for i := 0; i < 5000; i++ {
			if p := partitionFor(rng.Uint64(), parts); p >= parts {
				t.Fatalf("parts=%d: partitionFor returned %d", parts, p)
			}
		}
		if p := partitionFor(^uint64(0), parts); p >= parts {
			t.Fatalf("parts=%d: max hash returned %d", parts, p)
		}
		if p := partitionFor(0, parts); p != 0 {
			t.Fatalf("parts=%d: zero hash returned %d", parts, p)
		}
	}
}

// TestProvidedHashTablesMatchRecompute asserts each table's caller-hash entry
// point behaves identically to its self-hashing twin.
func TestProvidedHashTablesMatchRecompute(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		a, b := newIntHashTable(64), newIntHashTable(64)
		for i := 0; i < 5000; i++ {
			k := int64(i*i - 700)
			va, oka := a.GetOrInsertNoGrow(k, int32(i))
			vb, okb := b.GetOrInsertNoGrowAt(k, fibHash(k), int32(i))
			if va != vb || oka != okb {
				t.Fatalf("key %d: (%d,%v) vs (%d,%v)", k, va, oka, vb, okb)
			}
			a.CheckGrow()
			b.CheckGrow()
		}
		if a.Len() != b.Len() {
			t.Fatalf("len %d vs %d", a.Len(), b.Len())
		}
	})
	t.Run("packed", func(t *testing.T) {
		a, b := newPackedHashTable(64), newPackedHashTable(64)
		for i := 0; i < 5000; i++ {
			lo, hi := uint64(i%977), uint64(i%31)<<32
			va := a.GetOrInsertNoGrow(lo, hi, int32(i))
			vb := b.GetOrInsertNoGrowAt(lo, hi, packedHash(lo, hi), int32(i))
			if va != vb {
				t.Fatalf("key (%d,%d): %d vs %d", lo, hi, va, vb)
			}
			a.CheckGrow()
			b.CheckGrow()
		}
		if a.Len() != b.Len() {
			t.Fatalf("len %d vs %d", a.Len(), b.Len())
		}
	})
	t.Run("str", func(t *testing.T) {
		a, b := newStrHashTable(64), newStrHashTable(64)
		for i := 0; i < 5000; i++ {
			k := []byte(fmt.Sprintf("key-%d-%d", i%733, i%17))
			va, oka, sa := a.GetOrInsertRef(k, int32(i))
			vb, okb, sb := b.GetOrInsertRefAt(k, strHash(k), int32(i))
			if va != vb || oka != okb || string(sa) != string(sb) {
				t.Fatalf("key %q: (%d,%v,%q) vs (%d,%v,%q)", k, va, oka, sa, vb, okb, sb)
			}
		}
		if a.Len() != b.Len() {
			t.Fatalf("len %d vs %d", a.Len(), b.Len())
		}
	})
}
