package catalog

import (
	"math"
	"testing"
)

func TestHLLAccuracy(t *testing.T) {
	cases := []int64{100, 1_000, 10_000, 100_000, 1_000_000}
	for _, n := range cases {
		var h HLL
		for i := int64(0); i < n; i++ {
			h.AddInt64(i)
		}
		est := h.Estimate()
		errPct := math.Abs(float64(est-n)) / float64(n) * 100
		t.Logf("n=%d est=%d err=%.2f%%", n, est, errPct)
		if errPct > 5 {
			t.Errorf("n=%d: err %.2f%% exceeds 5%% tolerance (est=%d)", n, errPct, est)
		}
	}
}

func TestHLLMerge(t *testing.T) {
	var a, b HLL
	for i := int64(0); i < 50_000; i++ {
		a.AddInt64(i)
	}
	for i := int64(25_000); i < 75_000; i++ {
		b.AddInt64(i)
	}
	a.Merge(&b)
	est := a.Estimate()
	want := int64(75_000)
	errPct := math.Abs(float64(est-want)) / float64(want) * 100
	if errPct > 5 {
		t.Errorf("merged estimate %d off by %.2f%% from %d", est, errPct, want)
	}
}

func TestHLLRoundTrip(t *testing.T) {
	var h HLL
	for i := int64(0); i < 10_000; i++ {
		h.AddInt64(i * 7)
	}
	orig := h.Estimate()
	b := h.Bytes()
	got := HLLFromBytes(b)
	if got == nil {
		t.Fatal("HLLFromBytes returned nil")
	}
	if got.Estimate() != orig {
		t.Errorf("roundtrip estimate diverged: orig=%d got=%d", orig, got.Estimate())
	}
}

func TestHLLBadBytes(t *testing.T) {
	if HLLFromBytes(nil) != nil {
		t.Error("nil bytes should return nil")
	}
	if HLLFromBytes([]byte{99, 0, 0}) != nil {
		t.Error("wrong version should return nil")
	}
	if HLLFromBytes(make([]byte, 100)) != nil {
		t.Error("wrong length should return nil")
	}
}
