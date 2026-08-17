package scan

import (
	"math/rand"
	"testing"
)

// refMask is the byte-per-row mask the bitmap replaced; every bitmap
// operation is checked against the obvious implementation over it.
type refMask []bool

func newRef(n int) refMask {
	m := make(refMask, n)
	for i := range m {
		m[i] = true
	}
	return m
}

func (m refMask) count() int {
	c := 0
	for _, b := range m {
		if b {
			c++
		}
	}
	return c
}

func (m refMask) sel() []uint32 {
	out := make([]uint32, 0, len(m))
	for i, b := range m {
		if b {
			out = append(out, uint32(i))
		}
	}
	return out
}

func assertSameMask(t *testing.T, bm *rowBitmap, ref refMask) {
	t.Helper()
	for i := range ref {
		if bm.get(i) != ref[i] {
			t.Fatalf("bit %d = %v, want %v", i, bm.get(i), ref[i])
		}
	}
	if got, want := bm.count(), ref.count(); got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
	got := bm.appendSel(nil)
	want := ref.sel()
	if len(got) != len(want) {
		t.Fatalf("sel length %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sel[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestRowBitmapResetSetsEveryRow(t *testing.T) {
	// Sizes on and around word boundaries: the tail mask is where an
	// off-by-one turns into phantom matching rows.
	for _, n := range []int{0, 1, 63, 64, 65, 127, 128, 129, 1000, 2048} {
		var bm rowBitmap
		bm.reset(n)
		if bm.count() != n {
			t.Fatalf("n=%d: count after reset = %d", n, bm.count())
		}
		assertSameMask(t, &bm, newRef(n))
		// Tail bits past n must be zero, or count/sel would invent rows.
		bm.allSet = false
		if bm.count() != n {
			t.Fatalf("n=%d: count with tail bits = %d, want %d", n, bm.count(), n)
		}
	}
}

func TestRowBitmapReuseClearsPreviousState(t *testing.T) {
	var bm rowBitmap
	bm.reset(200)
	bm.clearRange(0, 200)
	bm.reset(100) // shrink: the pooled words must come back all-set
	if bm.count() != 100 {
		t.Fatalf("count after reuse = %d, want 100", bm.count())
	}
	assertSameMask(t, &bm, newRef(100))
}

func TestRowBitmapClearRange(t *testing.T) {
	const n = 300
	cases := [][2]int{
		{0, 0}, {0, 1}, {0, 64}, {0, 300},
		{5, 6}, {5, 63}, {5, 64}, {5, 65},
		{63, 64}, {63, 65}, {64, 128}, {64, 129},
		{100, 200}, {299, 300}, {200, 300},
		{10, 5},    // empty (hi <= lo)
		{-5, 10},   // clamped low
		{290, 400}, // clamped high
	}
	for _, c := range cases {
		var bm rowBitmap
		bm.reset(n)
		ref := newRef(n)
		bm.clearRange(c[0], c[1])
		for i := c[0]; i < c[1]; i++ {
			if i >= 0 && i < n {
				ref[i] = false
			}
		}
		assertSameMask(t, &bm, ref)
	}
}

func TestRowBitmapRandomOps(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 1 + rng.Intn(500)
		var bm rowBitmap
		bm.reset(n)
		ref := newRef(n)
		for op := 0; op < 40; op++ {
			if rng.Intn(2) == 0 {
				i := rng.Intn(n)
				bm.clear(i)
				ref[i] = false
			} else {
				lo := rng.Intn(n)
				hi := lo + rng.Intn(n-lo+1)
				bm.clearRange(lo, hi)
				for i := lo; i < hi; i++ {
					ref[i] = false
				}
			}
		}
		assertSameMask(t, &bm, ref)
	}
}

// TestRowBitmapAllSetHint: allSet is a shortcut for count and for the
// per-row loops. It must be false the moment anything is cleared.
func TestRowBitmapAllSetHint(t *testing.T) {
	var bm rowBitmap
	bm.reset(100)
	if !bm.allSet {
		t.Fatal("reset must leave allSet true")
	}
	bm.clearRange(10, 10) // empty range clears nothing
	if bm.count() != 100 {
		t.Fatalf("count = %d after an empty clearRange", bm.count())
	}
	bm.clear(3)
	if bm.allSet {
		t.Fatal("clear must drop allSet")
	}
	if bm.count() != 99 {
		t.Fatalf("count = %d, want 99", bm.count())
	}
	bm.reset(100)
	bm.clearRange(4, 9)
	if bm.allSet {
		t.Fatal("clearRange must drop allSet")
	}
	if bm.count() != 95 {
		t.Fatalf("count = %d, want 95", bm.count())
	}
}
