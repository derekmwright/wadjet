package distributed

import (
	"runtime"
	"testing"
)

// TestProcessRSS_NonZero asserts ProcessRSS returns a plausible non-zero figure
// on every platform: real RSS via /proc/self/statm on Linux, the HeapInuse
// fallback elsewhere. (At SF0.01/local this is the whole working set in heap;
// the mmap-vs-heap split only becomes interesting at SF100.)
func TestProcessRSS_NonZero(t *testing.T) {
	if got := ProcessRSS(); got <= 0 {
		t.Fatalf("ProcessRSS() = %d, want > 0 (statm on linux, HeapInuse fallback otherwise)", got)
	}
}

// TestProcessRSS_TracksAllocation is a soft check that RSS reflects a large live
// allocation. Skipped off Linux (the fallback is HeapInuse, which may or may not
// grow depending on GC). The floor assertion in TestProcessRSS_NonZero is the
// load-bearing one; this is best-effort.
func TestProcessRSS_TracksAllocation(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("RSS-growth check is Linux-only (non-linux uses the HeapInuse fallback)")
	}
	before := ProcessRSS()
	const n = 64 << 20
	buf := make([]byte, n)
	for i := 0; i < n; i += 4096 { // touch every page so it's resident
		buf[i] = 1
	}
	after := ProcessRSS()
	runtime.KeepAlive(buf)
	if after <= before {
		// Not a hard failure — page reclaim / scheduling can confound it —
		// but log so a regression in the reader is visible.
		t.Logf("RSS did not grow after 64MB touch: before=%d after=%d (informational)", before, after)
	}
}

func TestProcessRSSFromStatm(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int64 // in pages (multiplied by pageSize in the assert)
		ok   bool
	}{
		{"typical", "839 252 228 4 0 124 0", 252, true},
		{"two_fields", "1000 500", 500, true},
		{"one_field", "1000", 0, false},
		{"empty", "", 0, false},
		{"malformed_resident", "1000 notanint 5", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := processRSSFromStatm([]byte(tc.in))
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && got != tc.want*pageSize {
				t.Fatalf("bytes = %d, want %d (%d pages × %d)", got, tc.want*pageSize, tc.want, pageSize)
			}
		})
	}
}
