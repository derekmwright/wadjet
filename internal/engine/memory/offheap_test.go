//go:build linux

package memory

import "testing"

func TestOffheapGrowsInPlaceAndCloses(t *testing.T) {
	if !OffheapAvailable() {
		t.Skip("offheap-agg disabled")
	}
	r := NewOffheapRegistry()
	s := Offheap[int64](r, 16)
	if r.Mappings() != 1 {
		t.Fatalf("mappings = %d, want 1", r.Mappings())
	}
	base := &s[:1][0]
	for i := range 1 << 20 {
		s = append(s, int64(i))
	}
	if &s[0] != base {
		t.Fatal("append relocated the off-heap slice (must grow in place)")
	}
	for _, i := range []int{0, 1, 12345, 1<<20 - 1} {
		if s[i] != int64(i) {
			t.Fatalf("s[%d] = %d", i, s[i])
		}
	}

	// Adoption moves ownership wholesale.
	r2 := NewOffheapRegistry()
	_ = Offheap[float64](r2, 4)
	r.AdoptFrom(r2)
	if r.Mappings() != 2 || r2.Mappings() != 0 {
		t.Fatalf("adopt: r=%d r2=%d, want 2/0", r.Mappings(), r2.Mappings())
	}

	r.Close()
	if r.Mappings() != 0 {
		t.Fatal("close left mappings")
	}
	r.Close() // idempotent
}

func TestOffheapKillSwitchFallsBackToHeap(t *testing.T) {
	prev := offheapAggToggle.Set(false)
	defer offheapAggToggle.Set(prev)
	r := NewOffheapRegistry()
	s := Offheap[int64](r, 8)
	if cap(s) != 8 || r.Mappings() != 0 {
		t.Fatalf("kill switch off: cap=%d mappings=%d, want heap slice cap 8 / 0 mappings", cap(s), r.Mappings())
	}
}

func TestOffheapNilRegistryHeapFallback(t *testing.T) {
	s := Offheap[int32](nil, 8)
	if cap(s) != 8 {
		t.Fatalf("nil registry: cap=%d, want 8", cap(s))
	}
}
