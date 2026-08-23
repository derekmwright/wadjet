package exec

import (
	"math/rand"
	"testing"
)

// #431: back-shift deletion must never orphan a key. Random insert/delete/get
// sequences against a Go map as the oracle, across key families that
// actually produce probe chains (strided keys are the ones fibHash's fold
// spreads — see d177651, "break fibHash's stride collapse, and fix the
// back-shift it exposed").
//
// checkIntHashTableSequence drives ops operations against a fresh
// intHashTable and a reference map built from the same seed, requiring exact
// agreement on every Put/Delete/Get and a full reachability sweep every 97
// ops (every surviving key must still be Get-able, table size must match the
// reference's). The stride/span choice cycles through six key families that
// exercise different probe-chain shapes: 1 and 3 pack keys densely (long
// chains), the power-of-two strides (16, 256, 65536, 1<<24) land on
// different fibHash fold boundaries.
func checkIntHashTableSequence(t *testing.T, seed int64, ops int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	h := newIntHashTable(8)
	ref := map[int64]int32{}
	stride := []int64{1, 3, 1 << 4, 1 << 8, 1 << 16, 1 << 24}[int(seed%6+6)%6]
	span := int64(1 + rng.Intn(400))
	for op := 0; op < ops; op++ {
		k := (rng.Int63n(span) + 1) * stride
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5:
			v := int32(rng.Intn(1 << 20))
			h.Put(k, v)
			ref[k] = v
		case 6, 7, 8:
			gotV, gotOK := h.Delete(k)
			wantV, wantOK := ref[k]
			if gotOK != wantOK || (wantOK && gotV != wantV) {
				t.Fatalf("seed %d op %d: Delete(%d) = (%d,%v), want (%d,%v)",
					seed, op, k, gotV, gotOK, wantV, wantOK)
			}
			delete(ref, k)
		default:
			gotV, gotOK := h.Get(k)
			wantV, wantOK := ref[k]
			if gotOK != wantOK || (wantOK && gotV != wantV) {
				t.Fatalf("seed %d op %d: Get(%d) = (%d,%v), want (%d,%v)",
					seed, op, k, gotV, gotOK, wantV, wantOK)
			}
		}
		if op%97 == 0 {
			for rk, rv := range ref {
				gv, ok := h.Get(rk)
				if !ok || gv != rv {
					t.Fatalf("seed %d op %d: ORPHANED key %d (want %d, got %d/%v), live=%d size=%d",
						seed, op, rk, rv, gv, ok, len(ref), h.Len())
				}
			}
			if h.Len() != len(ref) {
				t.Fatalf("seed %d op %d: size %d, want %d", seed, op, h.Len(), len(ref))
			}
		}
	}
}

// checkIntTwoLevelTableSequence is checkIntHashTableSequence's counterpart
// for intTwoLevelTable — a separate implementation with its own probing and
// its own back-shift deletion, so it needs the same differential coverage
// rather than inheriting the other structure's.
func checkIntTwoLevelTableSequence(t *testing.T, seed int64, ops int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	h := newIntTwoLevelTable(8, nil)
	ref := map[int64]int32{}
	stride := []int64{1, 3, 1 << 4, 1 << 8, 1 << 16, 1 << 24}[int(seed%6+6)%6]
	span := int64(1 + rng.Intn(400))
	for op := 0; op < ops; op++ {
		k := (rng.Int63n(span) + 1) * stride
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5:
			v := int32(rng.Intn(1 << 20))
			got, existed := h.GetOrInsert(k, v)
			want, wantExisted := ref[k]
			if existed != wantExisted || (wantExisted && got != want) {
				t.Fatalf("seed %d op %d: GetOrInsert(%d) = (%d,%v), want (%d,%v)",
					seed, op, k, got, existed, want, wantExisted)
			}
			if !existed {
				ref[k] = v
			}
		case 6, 7, 8:
			gotV, gotOK := h.Delete(k)
			wantV, wantOK := ref[k]
			if gotOK != wantOK || (wantOK && gotV != wantV) {
				t.Fatalf("seed %d op %d: Delete(%d) = (%d,%v), want (%d,%v)",
					seed, op, k, gotV, gotOK, wantV, wantOK)
			}
			delete(ref, k)
		default:
			gotV, gotOK := h.Get(k)
			wantV, wantOK := ref[k]
			if gotOK != wantOK || (wantOK && gotV != wantV) {
				t.Fatalf("seed %d op %d: Get(%d) = (%d,%v), want (%d,%v)",
					seed, op, k, gotV, gotOK, wantV, wantOK)
			}
		}
		if op%97 == 0 {
			for rk, rv := range ref {
				gv, ok := h.Get(rk)
				if !ok || gv != rv {
					t.Fatalf("seed %d op %d: ORPHANED key %d (want %d, got %d/%v), live=%d size=%d",
						seed, op, rk, rv, gv, ok, len(ref), h.Len())
				}
			}
			if h.Len() != len(ref) {
				t.Fatalf("seed %d op %d: size %d, want %d", seed, op, h.Len(), len(ref))
			}
		}
	}
}

// TestIntHashTableDeleteRegression is the deterministic, always-run form:
// one seed per key family (six total), a fixed op count small enough to run
// in milliseconds while still covering several grow/rehash cycles (capacity
// starts at 8; 400 ops well past that).
func TestIntHashTableDeleteRegression(t *testing.T) {
	for seed := int64(0); seed < 6; seed++ {
		checkIntHashTableSequence(t, seed, 400)
	}
}

// TestIntTwoLevelTableDeleteRegression is TestIntHashTableDeleteRegression's
// counterpart for intTwoLevelTable.
func TestIntTwoLevelTableDeleteRegression(t *testing.T) {
	for seed := int64(0); seed < 6; seed++ {
		checkIntTwoLevelTableSequence(t, seed, 400)
	}
}

// FuzzIntHashTableDelete lets `go test -fuzz` search for a seed/op-count
// combination the deterministic test's six fixed seeds miss. opCount is
// clamped so one fuzz iteration stays cheap regardless of what the corpus
// mutates it to.
func FuzzIntHashTableDelete(f *testing.F) {
	for seed := int64(0); seed < 6; seed++ {
		f.Add(seed, 400)
	}
	f.Fuzz(func(t *testing.T, seed int64, opCount int) {
		ops := opCount % 800
		if ops < 0 {
			ops = -ops
		}
		checkIntHashTableSequence(t, seed, ops)
	})
}

// FuzzIntTwoLevelTableDelete is FuzzIntHashTableDelete's counterpart for
// intTwoLevelTable.
func FuzzIntTwoLevelTableDelete(f *testing.F) {
	for seed := int64(0); seed < 6; seed++ {
		f.Add(seed, 400)
	}
	f.Fuzz(func(t *testing.T, seed int64, opCount int) {
		ops := opCount % 800
		if ops < 0 {
			ops = -ops
		}
		checkIntTwoLevelTableSequence(t, seed, ops)
	})
}
