package worker

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// TestBytePacerRate: draining N bytes through the pacer takes at least
// (N-burst)/rate — the token bucket actually bounds throughput.
func TestBytePacerRate(t *testing.T) {
	const rate = 10e6 // 10 MB/s
	const burst = 1e6 // 1 MB free headroom
	const total = 4e6 // 4 MB payload
	p := newBytePacer(rate, burst)
	ctx := context.Background()
	start := time.Now()
	for sent := 0; sent < total; sent += 256 << 10 {
		if !p.waitAfter(ctx, 256<<10, nil) {
			t.Fatal("pacer aborted without ctx cancellation")
		}
	}
	elapsed := time.Since(start)
	// (4MB - 1MB burst) / 10MB/s = 300ms minimum.
	if min := 250 * time.Millisecond; elapsed < min {
		t.Fatalf("4MB at 10MB/s with 1MB burst took %v; want >= %v (pacer not pacing)", elapsed, min)
	}
	if max := 2 * time.Second; elapsed > max {
		t.Fatalf("4MB at 10MB/s took %v; want <= %v (pacer over-throttling)", elapsed, max)
	}
}

// TestBytePacerCtxCancel: a cancelled ctx aborts the debt sleep promptly.
func TestBytePacerCtxCancel(t *testing.T) {
	p := newBytePacer(1, 1) // 1 byte/sec: any charge = long debt
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p.waitAfter(ctx, 1<<20, nil) {
		t.Fatal("waitAfter returned true on cancelled ctx")
	}
}

// TestBytePacerOffByDefault: rate<=0 yields a nil pacer (pacing off), and
// the manager built without WADJET_UPLOAD_PACE_MBPS has no pacer.
func TestBytePacerOffByDefault(t *testing.T) {
	if newBytePacer(0, 0) != nil {
		t.Fatal("rate=0 must return nil pacer")
	}
	if uploadPaceRate != 0 {
		t.Skip("WADJET_UPLOAD_PACE_MBPS set in test environment")
	}
	m := newUploadManager(nil, nil, nil)
	if m.pacer != nil {
		t.Fatal("manager has a pacer with pacing unset")
	}
}

// TestGovernedReaderPacedAndUrgentBypass: a paced PUT-body reader is rate-
// bounded; flipping the root urgent removes the pacing entirely (demand
// release must never be stretched by smoothing).
func TestGovernedReaderPacedAndUrgentBypass(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.pacer = newBytePacer(10e6, 256<<10) // 10 MB/s, 256KB burst
	payload := bytes.Repeat([]byte{0xAB}, 2<<20)

	read := func(qs *queryUploadState) time.Duration {
		g := &governedReader{
			ctx: context.Background(), m: m, qs: qs,
			r: bytes.NewReader(payload), paced: true,
		}
		start := time.Now()
		if _, err := io.Copy(io.Discard, g); err != nil {
			t.Fatalf("paced copy: %v", err)
		}
		return time.Since(start)
	}

	slow := read(&queryUploadState{root: "paced-root"})
	// (2MB - 256KB) / 10MB/s ≈ 175ms minimum.
	if min := 120 * time.Millisecond; slow < min {
		t.Fatalf("paced 2MB read took %v; want >= %v", slow, min)
	}
	if m.UploadPaceWaitNs() == 0 {
		t.Fatal("paced read recorded no pace-wait time")
	}

	urgent := &queryUploadState{root: "urgent-root"}
	urgent.urgent.Store(true)
	m.pacer = newBytePacer(10e6, 256<<10) // fresh bucket, same debt potential
	if fast := read(urgent); fast > 50*time.Millisecond {
		t.Fatalf("urgent read paced anyway: took %v", fast)
	}
}
