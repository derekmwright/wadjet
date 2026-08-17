package exec

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// packedHashTable parity against a Go map reference across many grows. The
// reference is the definition of correctness for an open-addressing table:
// every key that was inserted resolves to the id it was inserted with, and
// no key resolves to another key's id.
func TestPackedHashTableMapParity(t *testing.T) {
	ht := newPackedHashTable(16)
	ref := map[packedKey]int32{}
	rng := rand.New(rand.NewSource(7))
	const n = 120000
	for i := 0; i < n; i++ {
		// Mix fresh keys with repeats so both the insert and the hit path
		// are exercised, and include high-half-only variation (the layout
		// puts whole columns in a word's high half).
		var k packedKey
		switch i % 4 {
		case 0:
			k = packedKey{lo: uint64(i), hi: uint64(i) << 32}
		case 1:
			k = packedKey{lo: rng.Uint64(), hi: rng.Uint64()}
		case 2:
			k = packedKey{lo: uint64(i) << 32, hi: 0}
		default:
			k = packedKey{lo: uint64(i / 3), hi: uint64(i / 5)}
		}
		want, seen := ref[k]
		if !seen {
			want = int32(len(ref))
			ref[k] = want
		}
		got := ht.GetOrInsertNoGrow(k.lo, k.hi, int32(len(ref)-1))
		ht.CheckGrow()
		if got != want {
			t.Fatalf("insert %d key %+v: got id %d, want %d", i, k, got, want)
		}
	}
	if ht.Len() != len(ref) {
		t.Fatalf("Len %d, want %d", ht.Len(), len(ref))
	}
	for k, want := range ref {
		got, ok := ht.Get(k.lo, k.hi)
		if !ok || got != want {
			t.Fatalf("lookup %+v: got (%d,%v), want (%d,true)", k, got, ok, want)
		}
	}
	// ForEach must visit exactly the live set.
	seen := map[packedKey]int32{}
	ht.ForEach(func(lo, hi uint64, val int32) { seen[packedKey{lo, hi}] = val })
	if len(seen) != len(ref) {
		t.Fatalf("ForEach visited %d entries, want %d", len(seen), len(ref))
	}
	for k, want := range ref {
		if seen[k] != want {
			t.Fatalf("ForEach %+v: %d, want %d", k, seen[k], want)
		}
	}
}

// The empty marker lives in the VALUE field precisely because no key bit
// pattern is reserved. The all-zero key — a real group whenever two group
// columns are both 0 — must be storable and distinguishable from a free
// slot, and so must the all-ones key.
func TestPackedHashExtremeKeysAreRealGroups(t *testing.T) {
	ht := newPackedHashTable(16)
	max := uint64(math.MaxUint64)
	cases := []packedKey{{0, 0}, {max, max}, {0, max}, {max, 0}, {1, 0}, {0, 1}}
	for i, k := range cases {
		if _, ok := ht.Get(k.lo, k.hi); ok {
			t.Fatalf("key %+v found in an empty table", k)
		}
		if got := ht.GetOrInsertNoGrow(k.lo, k.hi, int32(i)); got != int32(i) {
			t.Fatalf("insert %+v: got %d, want %d", k, got, int32(i))
		}
	}
	for i, k := range cases {
		got, ok := ht.Get(k.lo, k.hi)
		if !ok || got != int32(i) {
			t.Fatalf("key %+v: got (%d,%v), want (%d,true)", k, got, ok, int32(i))
		}
	}
	if ht.Len() != len(cases) {
		t.Fatalf("Len %d, want %d", ht.Len(), len(cases))
	}
	// The zero key must survive a grow (which re-stamps every slot with the
	// empty marker and rehashes) rather than being swallowed as "empty".
	for i := 0; i < 5000; i++ {
		ht.GetOrInsertNoGrow(uint64(i)+2, uint64(i)+2, int32(100+i))
		ht.CheckGrow()
	}
	if got, ok := ht.Get(0, 0); !ok || got != 0 {
		t.Fatalf("zero key after grows: got (%d,%v), want (0,true)", got, ok)
	}
}

// Off-heap entries must behave exactly like heap entries, and each grow must
// release the previous reservation (one live mapping, not one per doubling).
// Mirrors TestIntHashTableOffheapParity.
func TestPackedHashTableOffheapParity(t *testing.T) {
	reg := memory.NewOffheapRegistry()
	defer reg.Close()
	ht := newPackedHashTableReg(16, reg)
	heap := newPackedHashTable(16)
	const n = 200000
	for i := 0; i < n; i++ {
		lo, hi := uint64(i*7919+3), uint64(i)<<32
		a := ht.GetOrInsertNoGrow(lo, hi, int32(i))
		b := heap.GetOrInsertNoGrow(lo, hi, int32(i))
		ht.CheckGrow()
		heap.CheckGrow()
		if a != b {
			t.Fatalf("insert %d diverged: offheap %d heap %d", i, a, b)
		}
	}
	for i := 0; i < n; i++ {
		lo, hi := uint64(i*7919+3), uint64(i)<<32
		a, aok := ht.Get(lo, hi)
		b, bok := heap.Get(lo, hi)
		if a != b || aok != bok || !aok {
			t.Fatalf("lookup %d diverged: offheap (%d,%v) heap (%d,%v)", i, a, aok, b, bok)
		}
	}
	if ht.entriesOffheap {
		if m := reg.Mappings(); m != 1 {
			t.Fatalf("registry holds %d mappings after grows, want 1 (old tables must release)", m)
		}
		if ht.MemoryUsage() >= int64(4)<<30 {
			t.Fatalf("MemoryUsage %d reflects the reservation, not the table", ht.MemoryUsage())
		}
	}
}

// EnsureCapacity must leave room for the promised inserts without a
// mid-loop grow.
func TestPackedHashEnsureCapacity(t *testing.T) {
	ht := newPackedHashTable(16)
	ht.EnsureCapacity(10000)
	slots := len(ht.entries)
	for i := 0; i < 10000; i++ {
		ht.GetOrInsertNoGrow(uint64(i), uint64(i)*3, int32(i))
	}
	if len(ht.entries) != slots {
		t.Fatalf("table grew from %d to %d despite EnsureCapacity", slots, len(ht.entries))
	}
	if ht.Len() != 10000 {
		t.Fatalf("Len %d, want 10000", ht.Len())
	}
}

// packedKeyWidth's accepted set must stay exactly isAggIntType's: the
// routing decision reads one and the packing reads the other, so a drift
// would either decline eligible shapes or pack a type with no storage rule.
func TestPackedKeyWidthMatchesAggIntTypes(t *testing.T) {
	for tid := batch.TypeID(0); tid < 64; tid++ {
		w := packedKeyWidth(tid)
		if (w > 0) != isAggIntType(tid) {
			t.Errorf("type %d: packedKeyWidth=%d but isAggIntType=%v", tid, w, isAggIntType(tid))
		}
		if w != 0 && w != 4 && w != 8 {
			t.Errorf("type %d: width %d is neither 4 nor 8", tid, w)
		}
	}
}

func TestBuildPackedLayoutEligibility(t *testing.T) {
	i64 := batch.TypeInt64
	i32 := batch.TypeInt32
	cases := []struct {
		name  string
		types []batch.TypeID
		want  bool
	}{
		{"single column declines", []batch.TypeID{i64}, false},
		{"two int64 = 16B exactly", []batch.TypeID{i64, i64}, true},
		{"int64+int32 = 12B", []batch.TypeID{i64, i32}, true},
		{"three int32 = 12B", []batch.TypeID{i32, i32, i32}, true},
		{"four int32 = 16B exactly", []batch.TypeID{i32, i32, i32, i32}, true},
		{"int32,int64,int32 = 16B needs widest-first", []batch.TypeID{i32, i64, i32}, true},
		{"two int64 + int32 = 20B declines", []batch.TypeID{i64, i64, i32}, false},
		{"five int32 = 20B declines", []batch.TypeID{i32, i32, i32, i32, i32}, false},
		{"string column declines", []batch.TypeID{i64, batch.TypeString}, false},
		{"float column declines", []batch.TypeID{i64, batch.TypeFloat64}, false},
		{"bool column declines", []batch.TypeID{i32, batch.TypeBool}, false},
		{"network types pack", []batch.TypeID{batch.TypeIPv4, batch.TypeMAC}, true},
		{"port+protocol+date+int32", []batch.TypeID{batch.TypePort, batch.TypeProtocol, batch.TypeDate, i32}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildPackedLayout(tc.types)
			if (got != nil) != tc.want {
				t.Fatalf("buildPackedLayout(%v) = %v, want eligible=%v", tc.types, got, tc.want)
			}
			if got == nil {
				return
			}
			if len(got) != len(tc.types) {
				t.Fatalf("layout has %d fields for %d columns", len(got), len(tc.types))
			}
			// No two columns may share bits.
			used := map[[2]uint8]int{}
			for i, f := range got {
				width := packedKeyWidth(tc.types[i])
				if f.i32 != (width == 4) {
					t.Fatalf("field %d: i32=%v for a %d-byte type", i, f.i32, width)
				}
				spans := [][2]uint8{{f.word, f.shift}}
				if width == 8 {
					spans = [][2]uint8{{f.word, 0}, {f.word, 32}}
				}
				for _, s := range spans {
					if prev, dup := used[s]; dup {
						t.Fatalf("column %d overlaps column %d at word %d shift %d", i, prev, s[0], s[1])
					}
					used[s] = i
				}
			}
		})
	}
}

// Every packable type must round-trip through the layout bit-exactly,
// including negative values, the int32 sign boundary, MAC's 48 bits and
// IPv4's unsigned high bit (which is why 4-byte-wide packing is reserved
// for Int32Data-backed columns).
func TestPackedKeyRoundTrip(t *testing.T) {
	shapes := [][]batch.TypeID{
		{batch.TypeInt64, batch.TypeInt64},
		{batch.TypeInt64, batch.TypeInt32},
		{batch.TypeInt32, batch.TypeInt64},
		{batch.TypeInt32, batch.TypeInt32, batch.TypeInt32},
		{batch.TypeInt32, batch.TypeInt64, batch.TypeInt32},
		{batch.TypeDate, batch.TypePort, batch.TypeProtocol, batch.TypeInt32},
		{batch.TypeIPv4, batch.TypeMAC},
		{batch.TypeTimestamp, batch.TypeDuration},
	}
	i64vals := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1 << 40, -(1 << 40),
		0xFFFFFFFF /* IPv4 255.255.255.255 */, 0xFFFFFFFFFFFF /* MAC broadcast */}
	i32vals := []int64{0, 1, -1, math.MaxInt32, math.MinInt32, 1 << 30, -(1 << 30)}

	for _, types := range shapes {
		layout := buildPackedLayout(types)
		if layout == nil {
			t.Fatalf("shape %v declined", types)
		}
		rng := rand.New(rand.NewSource(11))
		for iter := 0; iter < 300; iter++ {
			want := make([]int64, len(types))
			var k packedKey
			for i, tp := range types {
				var v int64
				if packedKeyWidth(tp) == 4 {
					if iter < len(i32vals) {
						v = i32vals[iter]
					} else {
						v = int64(int32(rng.Uint32()))
					}
					k.set(layout[i], uint64(uint32(int32(v))))
				} else {
					if iter < len(i64vals) {
						v = i64vals[iter]
					} else {
						v = int64(rng.Uint64())
					}
					k.set(layout[i], uint64(v))
				}
				want[i] = v
			}
			for i := range types {
				if got := layout[i].get(k); got != want[i] {
					t.Fatalf("shape %v col %d: packed %d, unpacked %d", types, i, want[i], got)
				}
			}
		}
	}
}

// set writes one field into the key exactly as the consume loop does
// (packedKeyAt's OR of the shifted value). Test-only helper: production
// packs straight from the typed column slices.
func (k *packedKey) set(f packedField, v uint64) {
	v <<= f.shift
	if f.word == 0 {
		k.lo |= v
	} else {
		k.hi |= v
	}
}

// Distinct keys must not collide in the low bits the table masks. The
// layout puts whole columns in a word's HIGH half, which a multiply-only
// Fibonacci hash (whose low bits depend only on the input's low bits) maps
// to a single slot — the failure this guards.
func TestPackedHashSpreadsHighHalfKeys(t *testing.T) {
	const n = 4096
	const slots = 8192 // what a table holding n keys at 70% load would use
	buckets := make([]int, slots)
	for i := 0; i < n; i++ {
		// Vary only the high half of lo, i.e. a second narrow column.
		h := packedHash(uint64(i)<<32, 0) & (slots - 1)
		buckets[h]++
	}
	worst := 0
	for _, c := range buckets {
		if c > worst {
			worst = c
		}
	}
	// Uniform hashing over 8192 buckets with 4096 keys puts the max bucket
	// at ~5 with overwhelming probability; 20 is a wide margin that still
	// catches "everything landed in one slot" (worst == n).
	if worst > 20 {
		t.Fatalf("worst bucket holds %d of %d high-half-only keys (hash ignores high bits)", worst, n)
	}
}

// --- entry layout measurement -------------------------------------------

// packedHashEntry32 is the 32-byte alternative to the shipped 24-byte entry:
// exactly two entries per cache line, so no entry ever straddles a line and
// the slot index becomes a shift. It costs 33% more memory. Kept in the test
// file as the control for BenchmarkPackedHashLayout — the measurement behind
// packedHashEntry's size comment.
type packedHashEntry32 struct {
	lo, hi uint64
	val    int32
	_      int32
	_      [8]byte
}

type packedHashTable32 struct {
	entries []packedHashEntry32
	mask    uint64
	size    int
}

func newPackedHashTable32(n int) *packedHashTable32 {
	capacity := 16
	target := n + n/3
	for capacity < target {
		capacity <<= 1
	}
	h := &packedHashTable32{entries: make([]packedHashEntry32, capacity), mask: uint64(capacity - 1)}
	for i := range h.entries {
		h.entries[i].val = packedHashEmpty
	}
	return h
}

func (h *packedHashTable32) get(lo, hi uint64) (int32, bool) {
	idx := packedHash(lo, hi) & h.mask
	for {
		e := &h.entries[idx]
		if e.val == packedHashEmpty {
			return 0, false
		}
		if e.lo == lo && e.hi == hi {
			return e.val, true
		}
		idx = (idx + 1) & h.mask
	}
}

func (h *packedHashTable32) getOrInsert(lo, hi uint64, val int32) int32 {
	idx := packedHash(lo, hi) & h.mask
	for {
		e := &h.entries[idx]
		if e.val == packedHashEmpty {
			e.lo, e.hi, e.val = lo, hi, val
			h.size++
			return val
		}
		if e.lo == lo && e.hi == hi {
			return e.val
		}
		idx = (idx + 1) & h.mask
	}
}

// BenchmarkPackedHashLayout compares the 24-byte entry against a 32-byte
// (cache-line-aligned) one on insert-heavy and lookup-heavy loads at
// cardinalities that span L2 (100K) and main memory (8M).
func BenchmarkPackedHashLayout(b *testing.B) {
	for _, n := range []int{100_000, 8_000_000} {
		b.Run(fmt.Sprintf("insert-24B/n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ht := newPackedHashTable(16)
				for j := 0; j < n; j++ {
					ht.GetOrInsertNoGrow(uint64(j)*2654435761, uint64(j), int32(j))
					ht.CheckGrow()
				}
			}
		})
		b.Run(fmt.Sprintf("insert-32B/n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ht := newPackedHashTable32(16)
				for j := 0; j < n; j++ {
					ht.getOrInsert(uint64(j)*2654435761, uint64(j), int32(j))
					if ht.size*10 > len(ht.entries)*7 {
						old := ht.entries
						grown := newPackedHashTable32(len(old) * 2)
						for k := range old {
							if old[k].val != packedHashEmpty {
								grown.getOrInsert(old[k].lo, old[k].hi, old[k].val)
							}
						}
						ht.entries, ht.mask = grown.entries, grown.mask
					}
				}
			}
		})
		b.Run(fmt.Sprintf("lookup-24B/n=%d", n), func(b *testing.B) {
			ht := newPackedHashTable(n)
			for j := 0; j < n; j++ {
				ht.GetOrInsertNoGrow(uint64(j)*2654435761, uint64(j), int32(j))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					if _, ok := ht.Get(uint64(j)*2654435761, uint64(j)); !ok {
						b.Fatal("missing key")
					}
				}
			}
		})
		b.Run(fmt.Sprintf("lookup-32B/n=%d", n), func(b *testing.B) {
			ht := newPackedHashTable32(n)
			for j := 0; j < n; j++ {
				ht.getOrInsert(uint64(j)*2654435761, uint64(j), int32(j))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					if _, ok := ht.get(uint64(j)*2654435761, uint64(j)); !ok {
						b.Fatal("missing key")
					}
				}
			}
		})
	}
}
