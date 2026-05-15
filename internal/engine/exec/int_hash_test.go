package exec

import (
	"math/rand"
	"testing"
)

func TestIntHashTable_DeleteBackShift(t *testing.T) {
	h := newIntHashTable(64)
	for i := int64(0); i < 32; i++ {
		h.Put(i, int32(i))
	}
	for i := int64(0); i < 32; i++ {
		if v, ok := h.Get(i); !ok || v != int32(i) {
			t.Fatalf("pre-delete: Get(%d)=%d,%v want %d,true", i, v, ok, i)
		}
	}
	// Delete a key whose hash chain straddles many slots; confirm the
	// survivors remain reachable. Repeat across enough deletions to exercise
	// the back-shift loop multiple times.
	deleted := []int64{0, 5, 10, 17, 23, 30}
	for _, k := range deleted {
		v, ok := h.Delete(k)
		if !ok || v != int32(k) {
			t.Fatalf("Delete(%d)=%d,%v want %d,true", k, v, ok, k)
		}
		if _, ok := h.Get(k); ok {
			t.Fatalf("Get(%d) after Delete: still present", k)
		}
	}
	delSet := make(map[int64]bool)
	for _, k := range deleted {
		delSet[k] = true
	}
	for i := int64(0); i < 32; i++ {
		if delSet[i] {
			continue
		}
		if v, ok := h.Get(i); !ok || v != int32(i) {
			t.Fatalf("post-delete: Get(%d)=%d,%v want %d,true", i, v, ok, i)
		}
	}
	if h.Len() != 32-len(deleted) {
		t.Fatalf("Len after deletes: got %d want %d", h.Len(), 32-len(deleted))
	}
}

func TestIntHashTable_DeleteMissing(t *testing.T) {
	h := newIntHashTable(16)
	h.Put(1, 11)
	h.Put(2, 22)
	if v, ok := h.Delete(99); ok || v != 0 {
		t.Fatalf("Delete(missing)=%d,%v want 0,false", v, ok)
	}
	if h.Len() != 2 {
		t.Fatalf("Len unchanged on missing delete: got %d want 2", h.Len())
	}
}

func TestIntHashTable_DeleteThenInsert(t *testing.T) {
	h := newIntHashTable(16)
	for i := int64(0); i < 8; i++ {
		h.Put(i, int32(i))
	}
	h.Delete(3)
	h.Delete(5)
	h.Put(100, 200)
	h.Put(101, 201)
	for _, k := range []int64{0, 1, 2, 4, 6, 7, 100, 101} {
		want := int32(k)
		if k >= 100 {
			want = int32(k + 100)
		}
		if v, ok := h.Get(k); !ok || v != want {
			t.Fatalf("Get(%d)=%d,%v want %d,true", k, v, ok, want)
		}
	}
	for _, k := range []int64{3, 5} {
		if _, ok := h.Get(k); ok {
			t.Fatalf("Get(%d) after delete: still present", k)
		}
	}
}

// Randomized stress test: insert+delete+lookup against a reference map, with
// many collisions to exercise back-shift. Verifies the table stays consistent
// across thousands of operations.
func TestIntHashTable_DeleteStress(t *testing.T) {
	h := newIntHashTable(16)
	ref := make(map[int64]int32)
	r := rand.New(rand.NewSource(42))
	for op := 0; op < 5000; op++ {
		k := int64(r.Intn(200))
		switch r.Intn(3) {
		case 0:
			v := int32(r.Intn(1 << 20))
			h.Put(k, v)
			ref[k] = v
		case 1:
			if _, ok := ref[k]; ok {
				h.Delete(k)
				delete(ref, k)
			} else {
				if _, ok := h.Delete(k); ok {
					t.Fatalf("Delete(%d) returned ok for missing key", k)
				}
			}
		case 2:
			wantV, wantOk := ref[k]
			gotV, gotOk := h.Get(k)
			if wantOk != gotOk || (wantOk && gotV != wantV) {
				t.Fatalf("Get(%d)=%d,%v want %d,%v", k, gotV, gotOk, wantV, wantOk)
			}
		}
	}
	if h.Len() != len(ref) {
		t.Fatalf("final Len: got %d want %d", h.Len(), len(ref))
	}
	for k, want := range ref {
		if got, ok := h.Get(k); !ok || got != want {
			t.Fatalf("final Get(%d)=%d,%v want %d,true", k, got, ok, want)
		}
	}
}
